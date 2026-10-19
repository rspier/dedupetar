package cas

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// LocalBlobStore implements BlobStore using the local filesystem.
type LocalBlobStore struct {
	baseDir      string
	blobsDir     string
	packfilesDir string
	mu           sync.Mutex
	packLocks    sync.Map // packfileID -> *sync.Mutex
}

// NewLocalBlobStore initializes local directory structures.
func NewLocalBlobStore(baseDir string) (*LocalBlobStore, error) {
	blobsDir := filepath.Join(baseDir, "blobs")
	packfilesDir := filepath.Join(baseDir, "packfiles")

	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		return nil, fmt.Errorf("creating blobs dir: %w", err)
	}
	if err := os.MkdirAll(packfilesDir, 0755); err != nil {
		return nil, fmt.Errorf("creating packfiles dir: %w", err)
	}

	return &LocalBlobStore{
		baseDir:      baseDir,
		blobsDir:     blobsDir,
		packfilesDir: packfilesDir,
	}, nil
}

func (s *LocalBlobStore) blobPath(hash string) string {
	return filepath.Join(s.blobsDir, hash)
}

func (s *LocalBlobStore) packfilePath(packfileID string) string {
	return filepath.Join(s.packfilesDir, packfileID+".pack")
}

func (s *LocalBlobStore) getPackMutex(packfileID string) *sync.Mutex {
	v, _ := s.packLocks.LoadOrStore(packfileID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// CreateTempBlob creates a temporary writer directly in the blobs directory for streaming writes without buffering in RAM.
func (s *LocalBlobStore) CreateTempBlob(ctx context.Context) (io.WriteCloser, string, error) {
	f, err := os.CreateTemp(s.blobsDir, "tmp_blob_*")
	if err != nil {
		return nil, "", fmt.Errorf("creating temp blob: %w", err)
	}
	return f, f.Name(), nil
}

// CommitBlob renames a completed temporary blob to its final CAS hash destination.
func (s *LocalBlobStore) CommitBlob(ctx context.Context, tmpPath string, hash string) error {
	dst := s.blobPath(hash)
	if _, err := os.Stat(dst); err == nil {
		os.Remove(tmpPath)
		return nil
	}
	return os.Rename(tmpPath, dst)
}

// PutBlob atomically writes a standalone CAS blob.
func (s *LocalBlobStore) PutBlob(ctx context.Context, hash string, r io.Reader, size int64) error {
	dst := s.blobPath(hash)
	if fi, err := os.Stat(dst); err == nil && fi.Size() == size {
		// Already exists with expected size, skip writing
		return nil
	}

	// Write to temporary file first
	tmpFile, err := os.CreateTemp(s.blobsDir, "tmp_blob_*")
	if err != nil {
		return fmt.Errorf("creating temp blob file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpName)
	}()

	written, err := io.Copy(tmpFile, r)
	if err != nil {
		return fmt.Errorf("writing blob data: %w", err)
	}
	if size > 0 && written != size {
		return fmt.Errorf("blob size mismatch: wrote %d, want %d", written, size)
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("syncing blob: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp blob: %w", err)
	}

	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("renaming blob to destination: %w", err)
	}

	return nil
}

// GetBlob returns a ReadCloser for the entire standalone blob.
func (s *LocalBlobStore) GetBlob(ctx context.Context, hash string) (io.ReadCloser, error) {
	dst := s.blobPath(hash)
	f, err := os.Open(dst)
	if err != nil {
		return nil, fmt.Errorf("opening blob %s: %w", hash, err)
	}
	return f, nil
}

// GetBlobRange returns a ReadCloser for a byte slice [offset, offset+length) of a blob.
func (s *LocalBlobStore) GetBlobRange(ctx context.Context, hash string, offset int64, length int64) (io.ReadCloser, error) {
	dst := s.blobPath(hash)
	f, err := os.Open(dst)
	if err != nil {
		return nil, fmt.Errorf("opening blob %s: %w", hash, err)
	}

	sr := io.NewSectionReader(f, offset, length)
	return &sectionReadCloser{
		SectionReader: sr,
		closer:        f,
	}, nil
}

// AppendPackfileSlice appends payload bytes to a packfile and returns the byte offset where it was written.
func (s *LocalBlobStore) AppendPackfileSlice(ctx context.Context, packfileID string, r io.Reader, size int64) (uint64, error) {
	mu := s.getPackMutex(packfileID)
	mu.Lock()
	defer mu.Unlock()

	dst := s.packfilePath(packfileID)
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("opening packfile %s: %w", packfileID, err)
	}
	defer f.Close()

	curOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seeking to end of packfile: %w", err)
	}

	written, err := io.Copy(f, r)
	if err != nil {
		return 0, fmt.Errorf("appending to packfile: %w", err)
	}
	if size > 0 && written != size {
		return 0, fmt.Errorf("packfile append size mismatch: wrote %d, want %d", written, size)
	}

	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("syncing packfile: %w", err)
	}

	return uint64(curOffset), nil
}

// GetPackfileSlice returns a ReadCloser for a byte range [offset, offset+length) within a packfile.
func (s *LocalBlobStore) GetPackfileSlice(ctx context.Context, packfileID string, offset uint64, length uint32) (io.ReadCloser, error) {
	dst := s.packfilePath(packfileID)
	f, err := os.Open(dst)
	if err != nil {
		return nil, fmt.Errorf("opening packfile %s: %w", packfileID, err)
	}

	sr := io.NewSectionReader(f, int64(offset), int64(length))
	return &sectionReadCloser{
		SectionReader: sr,
		closer:        f,
	}, nil
}

// DeleteObject deletes a blob or packfile by its ID.
func (s *LocalBlobStore) DeleteObject(ctx context.Context, objectID string, objectType string) error {
	var target string
	if objectType == "packfile" {
		target = s.packfilePath(objectID)
	} else {
		target = s.blobPath(objectID)
	}

	err := os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting %s (%s): %w", objectID, objectType, err)
	}
	return nil
}

// ListObjects returns all objects currently stored in the local store.
func (s *LocalBlobStore) ListObjects(ctx context.Context) ([]StorageObject, error) {
	var results []StorageObject

	blobEntries, err := os.ReadDir(s.blobsDir)
	if err == nil {
		for _, e := range blobEntries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			results = append(results, StorageObject{
				ObjectID:   e.Name(),
				ObjectType: "blob",
				ByteSize:   info.Size(),
				CreatedAt:  info.ModTime(),
			})
		}
	}

	packEntries, err := os.ReadDir(s.packfilesDir)
	if err == nil {
		for _, e := range packEntries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".pack" {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			id := e.Name()[:len(e.Name())-5] // strip .pack
			results = append(results, StorageObject{
				ObjectID:   id,
				ObjectType: "packfile",
				ByteSize:   info.Size(),
				CreatedAt:  info.ModTime(),
			})
		}
	}

	return results, nil
}

func (s *LocalBlobStore) Close() error {
	return nil
}

type sectionReadCloser struct {
	*io.SectionReader
	closer io.Closer
}

func (s *sectionReadCloser) Close() error {
	return s.closer.Close()
}
