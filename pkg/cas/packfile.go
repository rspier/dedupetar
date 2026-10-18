package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"tarchive/pkg/models"
)

const (
	DefaultBlobThreshold   = 1024 * 1024       // 1 MB
	DefaultMaxPackfileSize = 32 * 1024 * 1024  // 32 MB
)

// PackfileManager manages appending small payloads to active packfiles.
type PackfileManager struct {
	store           BlobStore
	blobThreshold   int64
	maxPackfileSize int64

	mu             sync.Mutex
	activePackID   string
	activePackSize int64
}

// Config defines options for PackfileManager.
type Config struct {
	BlobThreshold   int64
	MaxPackfileSize int64
}

// NewPackfileManager creates a new packfile coordinator.
func NewPackfileManager(store BlobStore, cfg Config) *PackfileManager {
	threshold := cfg.BlobThreshold
	if threshold <= 0 {
		threshold = DefaultBlobThreshold
	}
	maxPackSize := cfg.MaxPackfileSize
	if maxPackSize <= 0 {
		maxPackSize = DefaultMaxPackfileSize
	}

	return &PackfileManager{
		store:           store,
		blobThreshold:   threshold,
		maxPackfileSize: maxPackSize,
	}
}

// SetActivePackfile sets the current active packfile and size (e.g., loaded from database).
func (pm *PackfileManager) SetActivePackfile(packID string, size int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.activePackID = packID
	pm.activePackSize = size
}

// WritePayload allocates and writes a new payload to either a standalone CAS blob or an active packfile.
// It applies adaptive Zstandard compression: if compression saves >= 5%, the compressed bytes are stored.
func (pm *PackfileManager) WritePayload(ctx context.Context, hash string, rawData []byte) (models.PayloadLocation, string, int64, error) {
	rawSize := int64(len(rawData))

	// Attempt adaptive Zstandard compression
	storedData, codec, err := CompressAdaptive(rawData)
	if err != nil {
		return models.PayloadLocation{}, "", 0, fmt.Errorf("compressing payload: %w", err)
	}
	storedSize := int64(len(storedData))

	if rawSize > pm.blobThreshold {
		// Large payload: store as standalone CAS blob
		if err := pm.store.PutBlob(ctx, hash, bytes.NewReader(storedData), storedSize); err != nil {
			return models.PayloadLocation{}, "", 0, fmt.Errorf("putting standalone blob: %w", err)
		}
		loc := models.PayloadLocation{
			Type:               models.StorageTypeBlob,
			StandaloneBlobHash: hash,
			CompressedSize:     storedSize,
			Codec:              codec,
		}
		return loc, hash, storedSize, nil
	}

	// Small payload (<= threshold): store into active packfile
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// If no active packfile or if adding this payload would exceed maxPackfileSize, roll to a new packfile
	if pm.activePackID == "" || (pm.activePackSize > 0 && pm.activePackSize+storedSize > pm.maxPackfileSize) {
		pm.activePackID = fmt.Sprintf("pack_%s_%d", time.Now().Format("20060102_150405"), time.Now().UnixNano()%1000000)
		pm.activePackSize = 0
	}

	packID := pm.activePackID
	offset, err := pm.store.AppendPackfileSlice(ctx, packID, bytes.NewReader(storedData), storedSize)
	if err != nil {
		return models.PayloadLocation{}, "", 0, fmt.Errorf("appending packfile slice: %w", err)
	}
	pm.activePackSize += storedSize

	loc := models.PayloadLocation{
		Type:           models.StorageTypePackfile,
		CompressedSize: storedSize,
		Codec:          codec,
		PackfileSlice: &models.PackfileSlice{
			PackfileID:         packID,
			Offset:             offset,
			Length:             uint32(storedSize),
			UncompressedLength: uint32(rawSize),
			Codec:              codec,
		},
	}
	return loc, packID, storedSize, nil
}

// OpenSlice opens a streaming reader for a payload given its location, decompressing transparently.
func (pm *PackfileManager) OpenSlice(ctx context.Context, loc models.PayloadLocation) (io.ReadCloser, error) {
	switch loc.Type {
	case models.StorageTypeBlob:
		rc, err := pm.store.GetBlob(ctx, loc.StandaloneBlobHash)
		if err != nil {
			return nil, err
		}
		return DecompressReader(rc, loc.Codec)

	case models.StorageTypePackfile:
		if loc.PackfileSlice == nil {
			return nil, fmt.Errorf("packfile slice metadata is nil")
		}
		rc, err := pm.store.GetPackfileSlice(ctx, loc.PackfileSlice.PackfileID, loc.PackfileSlice.Offset, loc.PackfileSlice.Length)
		if err != nil {
			return nil, err
		}
		return DecompressReader(rc, loc.PackfileSlice.Codec)

	default:
		return nil, fmt.Errorf("unknown storage type: %s", loc.Type)
	}
}

// OpenSliceRange opens a reader for a specific sub-range [internalOffset, internalOffset+length) within a payload.
func (pm *PackfileManager) OpenSliceRange(ctx context.Context, loc models.PayloadLocation, internalOffset int64, length int64) (io.ReadCloser, error) {
	codec := loc.Codec
	if loc.Type == models.StorageTypePackfile && loc.PackfileSlice != nil {
		codec = loc.PackfileSlice.Codec
	}

	if codec == models.CodecNone || codec == "" {
		// Uncompressed: direct zero-copy byte range from disk
		switch loc.Type {
		case models.StorageTypeBlob:
			return pm.store.GetBlobRange(ctx, loc.StandaloneBlobHash, internalOffset, length)
		case models.StorageTypePackfile:
			actualOffset := loc.PackfileSlice.Offset + uint64(internalOffset)
			return pm.store.GetPackfileSlice(ctx, loc.PackfileSlice.PackfileID, actualOffset, uint32(length))
		default:
			return nil, fmt.Errorf("unknown storage type: %s", loc.Type)
		}
	}

	// Compressed (e.g. Zstandard): read stored slice and decompress into memory for seeking
	var compressedData []byte
	switch loc.Type {
	case models.StorageTypeBlob:
		rc, err := pm.store.GetBlob(ctx, loc.StandaloneBlobHash)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("reading compressed blob: %w", err)
		}
		compressedData = data

	case models.StorageTypePackfile:
		rc, err := pm.store.GetPackfileSlice(ctx, loc.PackfileSlice.PackfileID, loc.PackfileSlice.Offset, loc.PackfileSlice.Length)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("reading compressed packfile slice: %w", err)
		}
		compressedData = data
	}

	decompressed, err := Decompress(compressedData, codec)
	if err != nil {
		return nil, fmt.Errorf("decompressing slice for range seek: %w", err)
	}

	return NewBufferSectionReader(decompressed, internalOffset, length), nil
}
