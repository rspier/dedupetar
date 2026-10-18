package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"tarchive/pkg/models"
)

// TarReadSeeker implements io.ReadSeeker and io.ReaderAt over an archive's virtual uncompressed tar stream.
type TarReadSeeker struct {
	ctx           context.Context
	engine        *Engine
	manifest      *models.ArchiveManifest
	currentOffset int64
	allEntries    []models.TarEntry
}

// NewTarReadSeeker constructs a seekable, random-access reader for an archive.
func (e *Engine) NewTarReadSeeker(ctx context.Context, archiveID string) (*TarReadSeeker, error) {
	manifest, err := e.db.GetManifest(ctx, archiveID)
	if err != nil {
		return nil, fmt.Errorf("getting manifest: %w", err)
	}

	var allEntries []models.TarEntry
	for _, chunkRef := range manifest.RecipeChunks {
		chunk, err := e.db.GetRecipeChunk(ctx, archiveID, chunkRef.ChunkIndex)
		if err != nil {
			return nil, fmt.Errorf("getting chunk %d: %w", chunkRef.ChunkIndex, err)
		}
		allEntries = append(allEntries, chunk.Entries...)
	}

	return &TarReadSeeker{
		ctx:        ctx,
		engine:     e,
		manifest:   manifest,
		allEntries: allEntries,
	}, nil
}

// Size returns the total uncompressed size of the archive.
func (r *TarReadSeeker) Size() int64 {
	return r.manifest.TotalUncompressedBytes
}

// Seek sets the offset for the next Read.
func (r *TarReadSeeker) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.currentOffset + offset
	case io.SeekEnd:
		target = r.manifest.TotalUncompressedBytes + offset
	default:
		return 0, errors.New("invalid seek whence")
	}

	if target < 0 {
		return 0, errors.New("negative seek position")
	}
	r.currentOffset = target
	return target, nil
}

// Read reads up to len(p) bytes from the uncompressed archive stream.
func (r *TarReadSeeker) Read(p []byte) (int, error) {
	if r.currentOffset >= r.manifest.TotalUncompressedBytes {
		return 0, io.EOF
	}

	n, err := r.ReadAt(p, r.currentOffset)
	r.currentOffset += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes into p starting at offset off in the archive.
func (r *TarReadSeeker) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset in ReadAt")
	}
	if off >= r.manifest.TotalUncompressedBytes {
		return 0, io.EOF
	}

	totalRead := 0
	toReadTotal := len(p)
	if off+int64(toReadTotal) > r.manifest.TotalUncompressedBytes {
		toReadTotal = int(r.manifest.TotalUncompressedBytes - off)
	}

	curOff := off
	for totalRead < toReadTotal {
		curNeed := toReadTotal - totalRead

		// Check if curOff is in the trailing padding data
		trailerStart := r.manifest.TotalUncompressedBytes - int64(len(r.manifest.TrailingPaddingData))
		if curOff >= trailerStart {
			trailerInternalOff := curOff - trailerStart
			availableInTrailer := int(int64(len(r.manifest.TrailingPaddingData)) - trailerInternalOff)
			n := curNeed
			if n > availableInTrailer {
				n = availableInTrailer
			}
			copy(p[totalRead:totalRead+n], r.manifest.TrailingPaddingData[trailerInternalOff:trailerInternalOff+int64(n)])
			totalRead += n
			curOff += int64(n)
			continue
		}

		// Binary search to find entry covering curOff:
		// Entry i covers [entry.TarByteOffset, entry.TarByteOffset + entry.TotalEntryBytes())
		entryIdx := sort.Search(len(r.allEntries), func(i int) bool {
			return r.allEntries[i].TarByteOffset+r.allEntries[i].TotalEntryBytes() > curOff
		})

		if entryIdx >= len(r.allEntries) {
			// No entry found (falls in trailer)
			break
		}

		entry := r.allEntries[entryIdx]
		entryRelOff := curOff - entry.TarByteOffset
		headerLen := int64(len(entry.RawHeaderBlocks))
		payloadLen := entry.PayloadSize
		paddingLen := int64(entry.PayloadPadding)

		if entryRelOff < headerLen {
			// Inside header blocks
			availHeader := int(headerLen - entryRelOff)
			n := curNeed
			if n > availHeader {
				n = availHeader
			}
			copy(p[totalRead:totalRead+n], entry.RawHeaderBlocks[entryRelOff:entryRelOff+int64(n)])
			totalRead += n
			curOff += int64(n)
		} else if entryRelOff < headerLen+payloadLen {
			// Inside payload
			payloadInternalOff := entryRelOff - headerLen
			availPayload := int(payloadLen - payloadInternalOff)
			n := curNeed
			if n > availPayload {
				n = availPayload
			}

			rc, err := r.engine.packfileManager.OpenSliceRange(r.ctx, entry.Location, payloadInternalOff, int64(n))
			if err != nil {
				return totalRead, fmt.Errorf("reading payload range at %d (len %d): %w", payloadInternalOff, n, err)
			}
			readBytes, err := io.ReadFull(rc, p[totalRead:totalRead+n])
			rc.Close()
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return totalRead, err
			}
			totalRead += readBytes
			curOff += int64(readBytes)
		} else if entryRelOff < headerLen+payloadLen+paddingLen {
			// Inside payload padding (zeroes)
			padInternalOff := entryRelOff - (headerLen + payloadLen)
			availPad := int(paddingLen - padInternalOff)
			n := curNeed
			if n > availPad {
				n = availPad
			}
			for i := 0; i < n; i++ {
				p[totalRead+i] = 0
			}
			totalRead += n
			curOff += int64(n)
		}
	}

	var err error
	if totalRead < len(p) {
		err = io.EOF
	}
	return totalRead, err
}

// Close closes any resources associated with the reader.
func (r *TarReadSeeker) Close() error {
	return nil
}
