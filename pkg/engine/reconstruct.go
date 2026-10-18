package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"tarchive/pkg/metadata"
	"tarchive/pkg/models"
)

// ZeroBlock512 provides a reusable 512-byte block of zeroes for padding.
var ZeroBlock512 = make([]byte, 512)

// Reconstruct writes the byte-identical original tarball to the provided io.Writer.
// It returns (bytesWritten, calculatedSHA256, error).
func (e *Engine) Reconstruct(ctx context.Context, archiveID string, dst io.Writer) (int64, string, error) {
	manifest, err := e.db.GetManifest(ctx, archiveID)
	if err != nil {
		return 0, "", fmt.Errorf("getting manifest for %s: %w", archiveID, err)
	}

	shaHasher := sha256.New()
	mw := io.MultiWriter(dst, shaHasher)

	var totalBytesWritten int64

	for _, chunkRef := range manifest.RecipeChunks {
		chunk, err := e.db.GetRecipeChunk(ctx, archiveID, chunkRef.ChunkIndex)
		if err != nil {
			return totalBytesWritten, "", fmt.Errorf("reading recipe chunk %d: %w", chunkRef.ChunkIndex, err)
		}

		for _, entry := range chunk.Entries {
			// 1. Write verbatim raw header blocks (including PAX / GNU aux blocks)
			if len(entry.RawHeaderBlocks) > 0 {
				n, err := mw.Write(entry.RawHeaderBlocks)
				totalBytesWritten += int64(n)
				if err != nil {
					return totalBytesWritten, "", fmt.Errorf("writing raw header blocks: %w", err)
				}
			}

			// 2. Stream payload from CAS / Packfile
			if entry.PayloadSize > 0 {
				rc, err := e.packfileManager.OpenSlice(ctx, entry.Location)
				if err != nil {
					return totalBytesWritten, "", fmt.Errorf("opening payload slice: %w", err)
				}

				written, err := io.Copy(mw, rc)
				rc.Close()
				totalBytesWritten += written
				if err != nil {
					return totalBytesWritten, "", fmt.Errorf("streaming payload: %w", err)
				}
				if written != entry.PayloadSize {
					return totalBytesWritten, "", fmt.Errorf("payload size mismatch: wrote %d, want %d", written, entry.PayloadSize)
				}
			}

			// 3. Write padding zero bytes
			if entry.PayloadPadding > 0 {
				pad := int(entry.PayloadPadding)
				for pad > 0 {
					toWrite := pad
					if toWrite > len(ZeroBlock512) {
						toWrite = len(ZeroBlock512)
					}
					n, err := mw.Write(ZeroBlock512[:toWrite])
					totalBytesWritten += int64(n)
					if err != nil {
						return totalBytesWritten, "", fmt.Errorf("writing payload padding: %w", err)
					}
					pad -= n
				}
			}
		}
	}

	// 4. Write verbatim trailing padding data (EOA trailer blocks)
	if len(manifest.TrailingPaddingData) > 0 {
		n, err := mw.Write(manifest.TrailingPaddingData)
		totalBytesWritten += int64(n)
		if err != nil {
			return totalBytesWritten, "", fmt.Errorf("writing trailing padding data: %w", err)
		}
	}

	calculatedSHA256 := hex.EncodeToString(shaHasher.Sum(nil))
	if calculatedSHA256 != manifest.UncompressedSHA256 {
		return totalBytesWritten, calculatedSHA256, fmt.Errorf("checksum mismatch: got %s, want %s", calculatedSHA256, manifest.UncompressedSHA256)
	}

	return totalBytesWritten, calculatedSHA256, nil
}

// GetManifest returns an archive manifest.
func (e *Engine) GetManifest(ctx context.Context, archiveID string) (*models.ArchiveManifest, error) {
	return e.db.GetManifest(ctx, archiveID)
}

// GetArchive returns the catalog record for an archive.
func (e *Engine) GetArchive(ctx context.Context, archiveID string) (*models.ArchiveRecord, error) {
	return e.db.GetArchive(ctx, archiveID)
}

// ListArchives lists archives.
func (e *Engine) ListArchives(ctx context.Context, activeOnly bool) ([]models.ArchiveRecord, error) {
	return e.db.ListArchives(ctx, activeOnly)
}

// DeleteArchive soft-deletes an archive.
func (e *Engine) DeleteArchive(ctx context.Context, archiveID string) error {
	return e.db.SoftDeleteArchive(ctx, archiveID)
}

// GetStatus retrieves repository-wide storage and deduplication statistics.
func (e *Engine) GetStatus(ctx context.Context) (*metadata.SystemStatus, error) {
	return e.db.GetSystemStatus(ctx)
}
