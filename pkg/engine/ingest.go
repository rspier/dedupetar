package engine

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"tarchive/pkg/cas"
	"tarchive/pkg/metadata"
	"tarchive/pkg/models"
	"tarchive/pkg/tarparser"

	"github.com/zeebo/blake3"
)

const (
	DefaultChunkEntryLimit = 1000
)

// IngestOptions configures archive ingestion.
type IngestOptions struct {
	Name            string
	ChunkEntryLimit int
	SourcePath      string
	SourceSize      int64
	SourceMtime     int64
	Force           bool
}

// IngestResult reports metrics from an ingestion operation.
type IngestResult struct {
	ArchiveID              string        `json:"archive_id"`
	Name                   string        `json:"name"`
	TotalEntries           uint32        `json:"total_entries"`
	TotalUncompressedBytes int64         `json:"total_uncompressed_bytes"`
	UncompressedSHA256     string        `json:"uncompressed_sha256"`
	NewBytesStored         int64         `json:"new_bytes_stored"`
	UncompressedNewBytes   int64         `json:"uncompressed_new_bytes"`
	DeduplicatedBytes      int64         `json:"deduplicated_bytes"`
	DeduplicationRatio     float64       `json:"deduplication_ratio"`
	CompressionRatio       float64       `json:"compression_ratio"`
	AlreadyExists          bool          `json:"already_exists"`
	Duration               time.Duration `json:"duration"`
}

var copyBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 128*1024)
		return &b
	},
}

func getCopyBuffer() []byte {
	return *copyBufferPool.Get().(*[]byte)
}

func putCopyBuffer(b []byte) {
	copyBufferPool.Put(&b)
}

// Engine coordinates ingestion, reassembly, and storage operations.
type Engine struct {
	db               *metadata.DB
	blobStore        cas.BlobStore
	packfileManager  *cas.PackfileManager
	blobThreshold    int64
	casCache         sync.Map // in-memory cache: hash -> *models.PayloadLocation
	fingerprintCache sync.Map // in-memory cache: fingerprint -> string
}

// New creates an Engine instance.
func New(db *metadata.DB, blobStore cas.BlobStore, pm *cas.PackfileManager, blobThreshold int64) *Engine {
	if blobThreshold <= 0 {
		blobThreshold = cas.DefaultBlobThreshold
	}
	return &Engine{
		db:              db,
		blobStore:       blobStore,
		packfileManager: pm,
		blobThreshold:   blobThreshold,
	}
}

func (e *Engine) lookupCAS(ctx context.Context, hash string) (*models.PayloadLocation, bool, error) {
	if val, ok := e.casCache.Load(hash); ok {
		return val.(*models.PayloadLocation), true, nil
	}
	loc, found, err := e.db.FindCASEntry(ctx, hash)
	if err != nil {
		return nil, false, err
	}
	if found && loc != nil {
		e.casCache.Store(hash, loc)
		return loc, true, nil
	}
	return nil, false, nil
}

func (e *Engine) recordCAS(ctx context.Context, hash string, loc models.PayloadLocation) error {
	e.casCache.Store(hash, &loc)
	return e.db.RecordCASEntry(ctx, hash, loc)
}

func (e *Engine) lookupFingerprint(ctx context.Context, fp string) (string, bool) {
	if val, ok := e.fingerprintCache.Load(fp); ok {
		return val.(string), true
	}
	hash, found, err := e.db.FindFingerprint(ctx, fp)
	if err == nil && found && hash != "" {
		e.fingerprintCache.Store(fp, hash)
		return hash, true
	}
	return "", false
}

func (e *Engine) recordFingerprint(ctx context.Context, fp, hash string) {
	if fp == "" || hash == "" {
		return
	}
	e.fingerprintCache.Store(fp, hash)
	_ = e.db.RecordFingerprint(ctx, fp, hash)
}

// Ingest streams a tar or tar.gz archive into DedupeTar with streaming multi-core compression and in-memory dedup caching.
func (e *Engine) Ingest(ctx context.Context, r io.Reader, opts IngestOptions) (*IngestResult, error) {
	startTime := time.Now()

	// Tier 1 check: if SourcePath is provided and not Force, check if the source file was already ingested unmodified
	if opts.SourcePath != "" && opts.SourceSize > 0 && !opts.Force {
		existing, err := e.db.FindArchiveBySourceFile(ctx, opts.SourcePath, opts.SourceSize, opts.SourceMtime)
		if err == nil && existing != nil && existing.Status == "active" {
			return &IngestResult{
				ArchiveID:              existing.ArchiveID,
				Name:                   existing.Name,
				TotalEntries:           existing.TotalEntries,
				TotalUncompressedBytes: existing.UncompressedSize,
				UncompressedSHA256:     existing.UncompressedSHA256,
				NewBytesStored:         0,
				UncompressedNewBytes:   0,
				DeduplicatedBytes:      existing.UncompressedSize,
				DeduplicationRatio:     1.0,
				CompressionRatio:       1.0,
				AlreadyExists:          true,
				Duration:               time.Since(startTime),
			}, nil
		}
	}

	// Sniff first two bytes to check for gzip magic header (0x1f, 0x8b)
	headerSniff := make([]byte, 2)
	nSniff, err := io.ReadFull(r, headerSniff)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("reading header bytes: %w", err)
	}

	var combinedReader io.Reader = bytes.NewReader(headerSniff[:nSniff])
	combinedReader = io.MultiReader(combinedReader, r)

	var uncompressedStream io.Reader = combinedReader
	if nSniff >= 2 && headerSniff[0] == 0x1f && headerSniff[1] == 0x8b {
		// Decompress gzip on the fly
		gzReader, err := gzip.NewReader(combinedReader)
		if err != nil {
			return nil, fmt.Errorf("initializing gzip reader: %w", err)
		}
		defer gzReader.Close()
		uncompressedStream = gzReader
	}

	// Large 1MB buffer to prevent 512-byte unbuffered read syscalls in tarparser
	bufferedStream := bufio.NewReaderSize(uncompressedStream, 1024*1024)

	// Compute overall uncompressed SHA256 while streaming
	shaHasher := sha256.New()
	teeStream := io.TeeReader(bufferedStream, shaHasher)

	parser := tarparser.NewParser(teeStream)

	archiveID := generateID("arc")
	entryLimit := opts.ChunkEntryLimit
	if entryLimit <= 0 {
		entryLimit = DefaultChunkEntryLimit
	}

	var chunks []models.RecipeChunk
	var currentChunkEntries []models.TarEntry
	var chunkRefs []models.RecipeChunkRef
	var referencedBlobIDs []string

	var totalUncompressedBytes int64
	var totalEntries uint32
	var newBytesStored int64
	var uncompressedNewBytes int64
	var deduplicatedBytes int64

	// Sync active packfile from database if available
	if activeID, activeSize, err := e.db.GetActivePackfile(ctx); err == nil && activeID != "" {
		e.packfileManager.SetActivePackfile(activeID, activeSize)
	}

	// Ingest batch metadata: batches all CAS entries, fingerprints, and storage objects
	// to eliminate per-file SQLite transactions and fsync stalls.
	batchMeta := metadata.IngestBatchMetadata{
		CASEntries:     make(map[string]models.PayloadLocation),
		Fingerprints:   make(map[string]string),
		StorageObjects: make(map[string]models.StorageObjectRecord),
	}

	type payloadJob struct {
		seq            uint64
		hdr            *tarparser.HeaderInfo
		entryOffset    int64
		rawData        []byte
		headerFP       string
		hash           string
		compressedData []byte
		codec          models.CompressionCodec
		inCAS          bool
		existingLoc    models.PayloadLocation
		err            error
	}

	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers < 1 {
		numWorkers = 1
	}

	jobsChan := make(chan *payloadJob, numWorkers*16)
	resultsChan := make(chan *payloadJob, numWorkers*16)

	var workerWg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for job := range jobsChan {
				if len(job.rawData) > 0 {
					job.hash = hashBLAKE3(job.rawData)
					if loc, found, err := e.lookupCAS(ctx, job.hash); err == nil && found && loc != nil {
						job.inCAS = true
						job.existingLoc = *loc
					} else {
						job.compressedData, job.codec, job.err = cas.CompressAdaptive(job.rawData)
					}
				}
				resultsChan <- job
			}
		}()
	}

	pendingResults := make(map[uint64]*payloadJob)
	var nextSeq uint64 = 0
	var curSeq uint64 = 0

	processJob := func(job *payloadJob) error {
		if job.err != nil {
			return job.err
		}

		var loc models.PayloadLocation
		payloadSize := job.hdr.PayloadSize

		if payloadSize > 0 {
			batchMeta.Fingerprints[job.headerFP] = job.hash

			// If worker didn't find in CAS, check in-memory cache again (in case an earlier duplicate in this batch added it)
			if !job.inCAS {
				if existingLoc, found, err := e.lookupCAS(ctx, job.hash); err == nil && found && existingLoc != nil {
					job.inCAS = true
					job.existingLoc = *existingLoc
				}
			}

			if job.inCAS {
				loc = job.existingLoc
				deduplicatedBytes += payloadSize
				if loc.PackfileSlice != nil {
					referencedBlobIDs = append(referencedBlobIDs, loc.PackfileSlice.PackfileID)
				}
			} else {
				// Write pre-compressed payload to active packfile
				newLoc, packID, storedBytes, err := e.packfileManager.WritePrecompressedPayload(
					ctx, job.hash, payloadSize, job.compressedData, job.codec,
				)
				if err != nil {
					return fmt.Errorf("writing packfile payload: %w", err)
				}
				loc = newLoc
				newBytesStored += storedBytes
				uncompressedNewBytes += payloadSize
				referencedBlobIDs = append(referencedBlobIDs, packID)

				// Update in-memory cache and batch metadata
				e.casCache.Store(job.hash, &loc)
				batchMeta.CASEntries[job.hash] = loc

				if loc.PackfileSlice != nil {
					curTotalSize := int64(loc.PackfileSlice.Offset) + int64(loc.PackfileSlice.Length)
					batchMeta.StorageObjects[packID] = models.StorageObjectRecord{
						ObjectID:   packID,
						ObjectType: "packfile",
						ByteSize:   curTotalSize,
					}
					batchMeta.ActivePackfileID = packID
					batchMeta.ActivePackfileSz = curTotalSize
				}
			}
		}

		entry := models.TarEntry{
			RawHeaderBlocks: job.hdr.RawHeaderBlocks,
			PayloadSize:     job.hdr.PayloadSize,
			PayloadPadding:  job.hdr.PayloadPadding,
			TarByteOffset:   job.entryOffset,
			Location:        loc,
		}

		currentChunkEntries = append(currentChunkEntries, entry)
		totalEntries++

		if len(currentChunkEntries) >= entryLimit {
			chunkIndex := uint32(len(chunks))
			chunks = append(chunks, models.RecipeChunk{
				ChunkIndex: chunkIndex,
				Entries:    currentChunkEntries,
			})
			chunkRefs = append(chunkRefs, models.RecipeChunkRef{
				ChunkIndex: chunkIndex,
				EntryCount: uint32(len(currentChunkEntries)),
				StorageKey: fmt.Sprintf("recipe_chunks/%s_%d.json", archiveID, chunkIndex),
			})
			currentChunkEntries = nil
		}

		if len(batchMeta.CASEntries) >= 5000 {
			if err := e.db.FlushBatch(ctx, batchMeta); err != nil {
				return fmt.Errorf("flushing metadata batch: %w", err)
			}
			batchMeta.CASEntries = make(map[string]models.PayloadLocation)
			batchMeta.Fingerprints = make(map[string]string)
			batchMeta.StorageObjects = make(map[string]models.StorageObjectRecord)
		}

		return nil
	}

	drainReady := func(block bool) error {
		for {
			if job, ok := pendingResults[curSeq]; ok {
				delete(pendingResults, curSeq)
				curSeq++
				if err := processJob(job); err != nil {
					return err
				}
				continue
			}
			if !block {
				select {
				case res := <-resultsChan:
					pendingResults[res.seq] = res
					continue
				default:
					return nil
				}
			} else {
				if curSeq == nextSeq {
					return nil
				}
				res := <-resultsChan
				pendingResults[res.seq] = res
			}
		}
	}

	for {
		hdr, payloadReader, err := parser.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("tar parser error: %w", err)
		}

		entryStartOffset := totalUncompressedBytes
		entryHeaderBytes := int64(len(hdr.RawHeaderBlocks))
		totalUncompressedBytes += entryHeaderBytes

		payloadSize := hdr.PayloadSize

		if payloadSize <= 0 {
			// Zero-size entry (dir, symlink, hardlink, etc.): dispatch as job with nil rawData
			if nextSeq-curSeq >= uint64(numWorkers*16) {
				if err := drainReady(true); err != nil {
					return nil, err
				}
			}
			job := &payloadJob{
				seq:         nextSeq,
				hdr:         hdr,
				entryOffset: entryStartOffset,
			}
			jobsChan <- job
			nextSeq++
			_ = drainReady(false)
			continue
		}

		totalUncompressedBytes += payloadSize + int64(hdr.PayloadPadding)
		headerFP := hashBLAKE3(hdr.RawHeaderBlocks)

		if payloadSize <= e.blobThreshold {
			// Small payload: read payload and dispatch to parallel worker pool
			buf := make([]byte, payloadSize)
			if _, err := io.ReadFull(payloadReader, buf); err != nil {
				return nil, fmt.Errorf("reading small payload: %w", err)
			}
			if nextSeq-curSeq >= uint64(numWorkers*16) {
				if err := drainReady(true); err != nil {
					return nil, err
				}
			}
			job := &payloadJob{
				seq:         nextSeq,
				hdr:         hdr,
				entryOffset: entryStartOffset,
				rawData:     buf,
				headerFP:    headerFP,
			}
			jobsChan <- job
			nextSeq++
			_ = drainReady(false)
		} else {
			// Large payload: first drain all pending small jobs to preserve exact sequence order
			if err := drainReady(true); err != nil {
				return nil, err
			}

			// Tier 2: check if duplicate file payload
			var loc models.PayloadLocation
			var shortCircuited bool
			if predictedHash, hasFP := e.lookupFingerprint(ctx, headerFP); hasFP {
				if existingLoc, inCAS, _ := e.lookupCAS(ctx, predictedHash); inCAS && existingLoc != nil {
					hasher := blake3.New()
					buf := getCopyBuffer()
					written, err := io.CopyBuffer(hasher, payloadReader, buf)
					putCopyBuffer(buf)
					if err != nil {
						return nil, fmt.Errorf("reading duplicate payload stream: %w", err)
					}
					if written != payloadSize {
						return nil, fmt.Errorf("duplicate payload size mismatch: read %d, want %d", written, payloadSize)
					}
					actualHash := hex.EncodeToString(hasher.Sum(nil))
					if actualHash == predictedHash {
						loc = *existingLoc
						deduplicatedBytes += payloadSize
						referencedBlobIDs = append(referencedBlobIDs, loc.StandaloneBlobHash)
						shortCircuited = true
					}
				}
			}

			if !shortCircuited {
				tmpWriter, tmpPath, err := e.blobStore.CreateTempBlob(ctx)
				if err != nil {
					return nil, fmt.Errorf("creating streaming temp blob: %w", err)
				}

				bufWriter := bufio.NewWriterSize(tmpWriter, 512*1024)
				hasher := blake3.New()
				zstdWriter, err := cas.NewStreamingCompressor(bufWriter)
				if err != nil {
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("creating streaming compressor: %w", err)
				}

				multiWriter := io.MultiWriter(zstdWriter, hasher)
				copyBuf := getCopyBuffer()
				written, err := io.CopyBuffer(multiWriter, payloadReader, copyBuf)
				putCopyBuffer(copyBuf)
				if err != nil {
					zstdWriter.Close()
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("streaming large payload: %w", err)
				}
				if written != payloadSize {
					zstdWriter.Close()
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("large payload size mismatch: wrote %d, want %d", written, payloadSize)
				}

				if err := zstdWriter.Close(); err != nil {
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("closing streaming compressor: %w", err)
				}
				if err := bufWriter.Flush(); err != nil {
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("flushing temp blob writer: %w", err)
				}
				if err := tmpWriter.Close(); err != nil {
					os.Remove(tmpPath)
					return nil, fmt.Errorf("closing temp blob: %w", err)
				}

				hash := hex.EncodeToString(hasher.Sum(nil))
				e.fingerprintCache.Store(headerFP, hash)
				batchMeta.Fingerprints[headerFP] = hash

				existingLoc, found, err := e.lookupCAS(ctx, hash)
				if err != nil {
					os.Remove(tmpPath)
					return nil, fmt.Errorf("checking CAS entry for large payload: %w", err)
				}

				if found && existingLoc != nil {
					os.Remove(tmpPath)
					loc = *existingLoc
					deduplicatedBytes += payloadSize
					referencedBlobIDs = append(referencedBlobIDs, loc.StandaloneBlobHash)
				} else {
					fi, err := os.Stat(tmpPath)
					if err != nil {
						os.Remove(tmpPath)
						return nil, err
					}
					storedSize := fi.Size()

					if err := e.blobStore.CommitBlob(ctx, tmpPath, hash); err != nil {
						os.Remove(tmpPath)
						return nil, fmt.Errorf("committing standalone blob: %w", err)
					}

					loc = models.PayloadLocation{
						Type:               models.StorageTypeBlob,
						StandaloneBlobHash: hash,
						CompressedSize:     storedSize,
						Codec:              models.CodecZSTD,
					}
					newBytesStored += storedSize
					uncompressedNewBytes += payloadSize
					referencedBlobIDs = append(referencedBlobIDs, hash)

					e.casCache.Store(hash, &loc)
					batchMeta.CASEntries[hash] = loc
					batchMeta.StorageObjects[hash] = models.StorageObjectRecord{
						ObjectID:   hash,
						ObjectType: "blob",
						ByteSize:   storedSize,
					}
				}
			}

			entry := models.TarEntry{
				RawHeaderBlocks: hdr.RawHeaderBlocks,
				PayloadSize:     hdr.PayloadSize,
				PayloadPadding:  hdr.PayloadPadding,
				TarByteOffset:   entryStartOffset,
				Location:        loc,
			}

			currentChunkEntries = append(currentChunkEntries, entry)
			totalEntries++

			if len(currentChunkEntries) >= entryLimit {
				chunkIndex := uint32(len(chunks))
				chunks = append(chunks, models.RecipeChunk{
					ChunkIndex: chunkIndex,
					Entries:    currentChunkEntries,
				})
				chunkRefs = append(chunkRefs, models.RecipeChunkRef{
					ChunkIndex: chunkIndex,
					EntryCount: uint32(len(currentChunkEntries)),
					StorageKey: fmt.Sprintf("recipe_chunks/%s_%d.json", archiveID, chunkIndex),
				})
				currentChunkEntries = nil
			}
		}
	}

	// Drain all remaining jobs from worker pool
	close(jobsChan)
	workerWg.Wait()
	if err := drainReady(true); err != nil {
		return nil, err
	}

	// Flush remaining entries into final chunk
	if len(currentChunkEntries) > 0 || len(chunks) == 0 {
		chunkIndex := uint32(len(chunks))
		chunks = append(chunks, models.RecipeChunk{
			ChunkIndex: chunkIndex,
			Entries:    currentChunkEntries,
		})
		chunkRefs = append(chunkRefs, models.RecipeChunkRef{
			ChunkIndex: chunkIndex,
			EntryCount: uint32(len(currentChunkEntries)),
			StorageKey: fmt.Sprintf("recipe_chunks/%s_%d.json", archiveID, chunkIndex),
		})
	}

	trailingBytes := parser.TrailingPaddingData()
	totalUncompressedBytes += int64(len(trailingBytes))

	uncompressedSHA256 := hex.EncodeToString(shaHasher.Sum(nil))

	// Tier 3 check: if an identical uncompressed archive was already ingested and is active, short-circuit
	if !opts.Force {
		existingArc, err := e.db.FindArchiveBySHA256(ctx, uncompressedSHA256)
		if err == nil && existingArc != nil && existingArc.Status == "active" {
			return &IngestResult{
				ArchiveID:              existingArc.ArchiveID,
				Name:                   existingArc.Name,
				TotalEntries:           existingArc.TotalEntries,
				TotalUncompressedBytes: existingArc.UncompressedSize,
				UncompressedSHA256:     existingArc.UncompressedSHA256,
				NewBytesStored:         0,
				UncompressedNewBytes:   0,
				DeduplicatedBytes:      existingArc.UncompressedSize,
				DeduplicationRatio:     1.0,
				CompressionRatio:       1.0,
				AlreadyExists:          true,
				Duration:               time.Since(startTime),
			}, nil
		}
	}

	name := opts.Name
	if name == "" {
		name = fmt.Sprintf("archive_%s.tar", archiveID)
	}

	manifest := &models.ArchiveManifest{
		ArchiveID:              archiveID,
		OriginalName:           name,
		TotalUncompressedBytes: totalUncompressedBytes,
		UncompressedSHA256:     uncompressedSHA256,
		TotalEntries:           totalEntries,
		TrailingPaddingData:    trailingBytes,
		RecipeChunks:           chunkRefs,
		SourcePath:             opts.SourcePath,
		SourceSize:             opts.SourceSize,
		SourceMtime:            opts.SourceMtime,
		CreatedAt:              time.Now(),
	}

	// Commit manifest, chunks, and blob references to database along with batched metadata
	if err := e.db.SaveArchiveTransaction(ctx, manifest, referencedBlobIDs, chunks, batchMeta); err != nil {
		return nil, fmt.Errorf("committing archive transaction: %w", err)
	}

	dedupRatio := 1.0
	if uncompressedNewBytes+deduplicatedBytes > 0 && uncompressedNewBytes > 0 {
		dedupRatio = float64(uncompressedNewBytes+deduplicatedBytes) / float64(uncompressedNewBytes)
	}

	compRatio := 1.0
	if newBytesStored > 0 && uncompressedNewBytes > 0 {
		compRatio = float64(uncompressedNewBytes) / float64(newBytesStored)
	}

	return &IngestResult{
		ArchiveID:              archiveID,
		Name:                   name,
		TotalEntries:           totalEntries,
		TotalUncompressedBytes: totalUncompressedBytes,
		UncompressedSHA256:     uncompressedSHA256,
		NewBytesStored:         newBytesStored,
		UncompressedNewBytes:   uncompressedNewBytes,
		DeduplicatedBytes:      deduplicatedBytes,
		DeduplicationRatio:     dedupRatio,
		CompressionRatio:       compRatio,
		Duration:               time.Since(startTime),
	}, nil
}

func hashBLAKE3(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func generateID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b))
}
