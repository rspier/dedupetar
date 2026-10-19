package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
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
	Duration               time.Duration `json:"duration"`
}

// Engine coordinates ingestion, reassembly, and storage operations.
type Engine struct {
	db              *metadata.DB
	blobStore       cas.BlobStore
	packfileManager *cas.PackfileManager
	blobThreshold   int64
	casCache        sync.Map // in-memory cache: hash -> *models.PayloadLocation
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

// Ingest streams a tar or tar.gz archive into DedupeTar with streaming multi-core compression and in-memory dedup caching.
func (e *Engine) Ingest(ctx context.Context, r io.Reader, opts IngestOptions) (*IngestResult, error) {
	startTime := time.Now()

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

	// Compute overall uncompressed SHA256 while streaming
	shaHasher := sha256.New()
	teeStream := io.TeeReader(uncompressedStream, shaHasher)

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

		var loc models.PayloadLocation
		payloadSize := hdr.PayloadSize

		if payloadSize > 0 {
			totalUncompressedBytes += payloadSize + int64(hdr.PayloadPadding)

			if payloadSize <= e.blobThreshold {
				// Small payload: read into buffer and compute BLAKE3
				buf := make([]byte, payloadSize)
				if _, err := io.ReadFull(payloadReader, buf); err != nil {
					return nil, fmt.Errorf("reading small payload: %w", err)
				}
				hash := hashBLAKE3(buf)

				// Check deduplication (fast in-memory cache)
				existingLoc, found, err := e.lookupCAS(ctx, hash)
				if err != nil {
					return nil, fmt.Errorf("checking CAS entry: %w", err)
				}

				if found && existingLoc != nil {
					loc = *existingLoc
					deduplicatedBytes += payloadSize
					if loc.PackfileSlice != nil {
						referencedBlobIDs = append(referencedBlobIDs, loc.PackfileSlice.PackfileID)
					}
				} else {
					// Write to active packfile with adaptive Zstandard compression
					newLoc, packID, storedBytes, err := e.packfileManager.WritePayload(ctx, hash, buf)
					if err != nil {
						return nil, fmt.Errorf("writing packfile payload: %w", err)
					}
					loc = newLoc
					newBytesStored += storedBytes
					uncompressedNewBytes += payloadSize
					referencedBlobIDs = append(referencedBlobIDs, packID)

					// Update DB and in-memory cache
					if err := e.recordCAS(ctx, hash, loc); err != nil {
						return nil, fmt.Errorf("recording cas entry: %w", err)
					}
					if loc.PackfileSlice != nil {
						curTotalSize := int64(loc.PackfileSlice.Offset) + int64(loc.PackfileSlice.Length)
						_ = e.db.RecordStorageObject(ctx, packID, "packfile", curTotalSize)
						_ = e.db.UpdateActivePackfile(ctx, packID, curTotalSize, false)
					}
				}
			} else {
				// Large payload (> threshold):
				// Single-pass streaming directly to disk while computing BLAKE3 and multi-core Zstandard!
				// Eliminates all double-reading, RAM bloat, and redundant disk copies.
				tmpWriter, tmpPath, err := e.blobStore.CreateTempBlob(ctx)
				if err != nil {
					return nil, fmt.Errorf("creating streaming temp blob: %w", err)
				}

				hasher := blake3.New()
				zstdWriter, err := cas.NewStreamingCompressor(tmpWriter)
				if err != nil {
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("creating streaming compressor: %w", err)
				}

				multiWriter := io.MultiWriter(zstdWriter, hasher)
				written, err := io.Copy(multiWriter, payloadReader)
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

				// Flush multi-core zstd encoder
				if err := zstdWriter.Close(); err != nil {
					tmpWriter.Close()
					os.Remove(tmpPath)
					return nil, fmt.Errorf("closing streaming compressor: %w", err)
				}
				if err := tmpWriter.Close(); err != nil {
					os.Remove(tmpPath)
					return nil, fmt.Errorf("closing temp blob: %w", err)
				}

				hash := hex.EncodeToString(hasher.Sum(nil))

				// Check deduplication (fast in-memory cache first)
				existingLoc, found, err := e.lookupCAS(ctx, hash)
				if err != nil {
					os.Remove(tmpPath)
					return nil, fmt.Errorf("checking CAS entry for large payload: %w", err)
				}

				if found && existingLoc != nil {
					os.Remove(tmpPath) // Deduplicated, discard temp file immediately
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

					// Atomically move temp blob directly to blobs/<hash>
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

					// Update DB records and cache
					if err := e.recordCAS(ctx, hash, loc); err != nil {
						return nil, fmt.Errorf("recording cas entry: %w", err)
					}
					if err := e.db.RecordStorageObject(ctx, hash, "blob", storedSize); err != nil {
						return nil, fmt.Errorf("recording storage object: %w", err)
					}
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
		CreatedAt:              time.Now(),
	}

	// Commit manifest, chunks, and blob references to database
	if err := e.db.SaveArchiveTransaction(ctx, manifest, referencedBlobIDs, chunks); err != nil {
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
