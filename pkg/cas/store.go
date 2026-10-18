package cas

import (
	"context"
	"io"
	"time"
)

// StorageObject represents metadata about a stored blob or packfile.
type StorageObject struct {
	ObjectID   string
	ObjectType string // "blob" or "packfile"
	ByteSize   int64
	CreatedAt  time.Time
}

// BlobStore defines the content-addressed storage engine interface.
type BlobStore interface {
	// PutBlob writes a standalone CAS blob. If an object with this hash exists, it is idempotent.
	PutBlob(ctx context.Context, hash string, r io.Reader, size int64) error

	// GetBlob returns a ReadCloser for the entire standalone blob.
	GetBlob(ctx context.Context, hash string) (io.ReadCloser, error)

	// GetBlobRange returns a ReadCloser for a byte range [offset, offset+length) of a standalone blob.
	GetBlobRange(ctx context.Context, hash string, offset int64, length int64) (io.ReadCloser, error)

	// AppendPackfileSlice appends payload bytes to a packfile and returns the byte offset where it was written.
	AppendPackfileSlice(ctx context.Context, packfileID string, r io.Reader, size int64) (uint64, error)

	// GetPackfileSlice returns a ReadCloser for a byte range [offset, offset+length) within a packfile.
	GetPackfileSlice(ctx context.Context, packfileID string, offset uint64, length uint32) (io.ReadCloser, error)

	// DeleteObject deletes a blob or packfile by its ID.
	DeleteObject(ctx context.Context, objectID string, objectType string) error

	// ListObjects returns all objects currently stored.
	ListObjects(ctx context.Context) ([]StorageObject, error)

	// Close cleans up any open file handles or resources.
	Close() error
}
