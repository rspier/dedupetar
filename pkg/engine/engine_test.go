package engine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tarchive/pkg/cas"
	"tarchive/pkg/metadata"
)

func createTestTarball(t *testing.T, files []struct{ Name, Body string }) []byte {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	for _, f := range files {
		hdr := &tar.Header{
			Name:     f.Name,
			Mode:     0644,
			Size:     int64(len(f.Body)),
			Typeflag: tar.TypeReg,
		}
		if len(f.Name) > 100 {
			hdr.Format = tar.FormatPAX
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if _, err := tw.Write([]byte(f.Body)); err != nil {
			t.Fatalf("Write body failed: %v", err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	return buf.Bytes()
}

func setupTestEngine(t *testing.T, blobThreshold int64) (*Engine, *metadata.DB, cas.BlobStore, string) {
	tmpDir, err := os.MkdirTemp("", "engine_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "meta.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("Open DB failed: %v", err)
	}

	blobStore, err := cas.NewLocalBlobStore(filepath.Join(tmpDir, "cas"))
	if err != nil {
		t.Fatalf("NewLocalBlobStore failed: %v", err)
	}

	pm := cas.NewPackfileManager(blobStore, cas.Config{
		BlobThreshold:   blobThreshold,
		MaxPackfileSize: 4096, // 4KB packfiles for testing packfile rollover
	})

	eng := New(db, blobStore, pm, blobThreshold)
	return eng, db, blobStore, tmpDir
}

func TestRoundTripFidelity(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 128) // 128B threshold
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	files := []struct{ Name, Body string }{
		{"small_1.txt", "Small file 1 content"},
		{"small_2.txt", "Small file 2 with slightly longer content that stays in packfile"},
		{"large_blob.bin", strings.Repeat("Large Blob Payload 12345! ", 20)}, // > 128B, goes to standalone blob
		{"deep/" + strings.Repeat("pax_long_path_name/", 5) + "test.txt", "PAX extended file body"},
	}

	originalTar := createTestTarball(t, files)
	origSHA := sha256.Sum256(originalTar)
	origSHAStr := hex.EncodeToString(origSHA[:])

	ctx := context.Background()

	// 1. Ingest tar
	res, err := eng.Ingest(ctx, bytes.NewReader(originalTar), IngestOptions{Name: "test.tar"})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	if res.UncompressedSHA256 != origSHAStr {
		t.Fatalf("Ingest SHA256 mismatch: got %s, want %s", res.UncompressedSHA256, origSHAStr)
	}
	if res.TotalUncompressedBytes != int64(len(originalTar)) {
		t.Fatalf("Ingest uncompressed bytes mismatch: got %d, want %d", res.TotalUncompressedBytes, len(originalTar))
	}

	// 2. Reconstruct tar
	reconstructedBuf := new(bytes.Buffer)
	bytesWritten, reconSHA, err := eng.Reconstruct(ctx, res.ArchiveID, reconstructedBuf)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if bytesWritten != int64(len(originalTar)) {
		t.Fatalf("Reconstructed bytes written = %d, want %d", bytesWritten, len(originalTar))
	}
	if reconSHA != origSHAStr {
		t.Fatalf("Reconstructed SHA256 = %s, want %s", reconSHA, origSHAStr)
	}

	// Verify byte-for-byte exact equality
	if !bytes.Equal(reconstructedBuf.Bytes(), originalTar) {
		t.Fatalf("BYTE-FOR-BYTE FIDELITY FAILED! Reconstructed bytes differ from original tar!")
	}
}

func TestGzipIngestion(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 256)
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	files := []struct{ Name, Body string }{
		{"readme.txt", "This is inside a gzipped tarball."},
	}
	originalTar := createTestTarball(t, files)
	origSHA := sha256.Sum256(originalTar)
	origSHAStr := hex.EncodeToString(origSHA[:])

	// Compress with gzip
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(originalTar)
	gw.Close()

	ctx := context.Background()
	res, err := eng.Ingest(ctx, &gzBuf, IngestOptions{Name: "archive.tar.gz"})
	if err != nil {
		t.Fatalf("Ingest gzipped failed: %v", err)
	}
	if res.UncompressedSHA256 != origSHAStr {
		t.Fatalf("Ingest SHA256 mismatch for gzipped tar: got %s, want %s", res.UncompressedSHA256, origSHAStr)
	}

	// Reconstruct and verify
	reconstructedBuf := new(bytes.Buffer)
	_, reconSHA, err := eng.Reconstruct(ctx, res.ArchiveID, reconstructedBuf)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if reconSHA != origSHAStr {
		t.Fatalf("Reconstruct SHA mismatch: got %s, want %s", reconSHA, origSHAStr)
	}
	if !bytes.Equal(reconstructedBuf.Bytes(), originalTar) {
		t.Fatalf("Byte comparison failed for gzipped input!")
	}
}

func TestCrossArchiveDeduplication(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 64)
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	sharedBody := "Identical payload shared across both archive A and archive B!"
	filesA := []struct{ Name, Body string }{
		{"shared.txt", sharedBody},
		{"file_a_only.txt", "Unique to A"},
	}
	filesB := []struct{ Name, Body string }{
		{"copy_of_shared.txt", sharedBody}, // Exact duplicate payload
		{"file_b_only.txt", "Unique to B"},
	}

	tarA := createTestTarball(t, filesA)
	tarB := createTestTarball(t, filesB)

	ctx := context.Background()
	resA, err := eng.Ingest(ctx, bytes.NewReader(tarA), IngestOptions{Name: "a.tar"})
	if err != nil {
		t.Fatalf("Ingest A failed: %v", err)
	}
	if resA.DeduplicatedBytes != 0 {
		t.Errorf("First ingest should have 0 deduplicated bytes, got %d", resA.DeduplicatedBytes)
	}

	resB, err := eng.Ingest(ctx, bytes.NewReader(tarB), IngestOptions{Name: "b.tar"})
	if err != nil {
		t.Fatalf("Ingest B failed: %v", err)
	}
	if resB.DeduplicatedBytes != int64(len(sharedBody)) {
		t.Errorf("Expected deduplicated bytes %d, got %d", len(sharedBody), resB.DeduplicatedBytes)
	}
	if resB.DeduplicationRatio <= 1.0 {
		t.Errorf("Expected deduplication ratio > 1.0, got %f", resB.DeduplicationRatio)
	}

	// Reconstruct both and verify fidelity
	bufA := new(bytes.Buffer)
	if _, _, err := eng.Reconstruct(ctx, resA.ArchiveID, bufA); err != nil {
		t.Fatalf("Reconstruct A failed: %v", err)
	}
	if !bytes.Equal(bufA.Bytes(), tarA) {
		t.Fatalf("Fidelity failure for A")
	}

	bufB := new(bytes.Buffer)
	if _, _, err := eng.Reconstruct(ctx, resB.ArchiveID, bufB); err != nil {
		t.Fatalf("Reconstruct B failed: %v", err)
	}
	if !bytes.Equal(bufB.Bytes(), tarB) {
		t.Fatalf("Fidelity failure for B")
	}
}

func TestRangeSeekingReadSeeker(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 64)
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	files := []struct{ Name, Body string }{
		{"f1.txt", strings.Repeat("One", 50)},
		{"f2.txt", strings.Repeat("Two", 80)},
		{"f3.txt", strings.Repeat("Three", 120)},
	}
	originalTar := createTestTarball(t, files)
	totalLen := int64(len(originalTar))

	ctx := context.Background()
	res, err := eng.Ingest(ctx, bytes.NewReader(originalTar), IngestOptions{Name: "seek_test.tar"})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	seeker, err := eng.NewTarReadSeeker(ctx, res.ArchiveID)
	if err != nil {
		t.Fatalf("NewTarReadSeeker failed: %v", err)
	}
	defer seeker.Close()

	if seeker.Size() != totalLen {
		t.Fatalf("Seeker size %d != original size %d", seeker.Size(), totalLen)
	}

	// Test seeking to multiple offsets and reading chunks
	testRanges := []struct {
		Offset int64
		Length int
	}{
		{0, 100},              // Header of f1
		{500, 25},             // Header boundary
		{512, 50},             // Payload of f1
		{1024, 80},            // Header of f2
		{1200, 150},           // Inside payload of f2
		{totalLen - 512, 200}, // Inside trailer padding
		{totalLen - 50, 50},   // End of trailer
	}

	for _, tr := range testRanges {
		t.Run(fmt.Sprintf("Range_%d_%d", tr.Offset, tr.Length), func(t *testing.T) {
			_, err := seeker.Seek(tr.Offset, io.SeekStart)
			if err != nil {
				t.Fatalf("Seek to %d failed: %v", tr.Offset, err)
			}

			buf := make([]byte, tr.Length)
			n, err := io.ReadFull(seeker, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("ReadFull failed: %v", err)
			}
			if n != tr.Length {
				t.Fatalf("Read %d bytes, want %d", n, tr.Length)
			}

			expectedSlice := originalTar[tr.Offset : tr.Offset+int64(tr.Length)]
			if !bytes.Equal(buf, expectedSlice) {
				t.Fatalf("Range read at %d (len %d) does not match original tar slice!", tr.Offset, tr.Length)
			}
		})
	}
}

func TestCompressionRatio(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 1024)
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	// Highly compressible file (repeated lines)
	highlyCompressible := strings.Repeat("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.\n", 30)
	files := []struct{ Name, Body string }{
		{"compressible.txt", highlyCompressible},
	}
	originalTar := createTestTarball(t, files)

	ctx := context.Background()
	res, err := eng.Ingest(ctx, bytes.NewReader(originalTar), IngestOptions{Name: "compressed.tar"})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify compression ratio > 2.0x
	if res.CompressionRatio < 2.0 {
		t.Errorf("Expected compression ratio > 2.0x, got %.2fx (stored %d bytes for %d uncompressed bytes)",
			res.CompressionRatio, res.NewBytesStored, res.UncompressedNewBytes)
	}

	// Verify byte-for-byte reconstruction
	buf := new(bytes.Buffer)
	if _, _, err := eng.Reconstruct(ctx, res.ArchiveID, buf); err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), originalTar) {
		t.Fatalf("Reconstructed tar does not match original!")
	}
}

func TestDuplicateArchiveShortCircuit(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 256)
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	files := []struct{ Name, Body string }{
		{"file1.txt", "Hello world"},
		{"file2.txt", strings.Repeat("Content payload 123 ", 30)},
	}
	originalTar := createTestTarball(t, files)

	// Save tarball to disk to test Tier 1 file-level short-circuiting
	tarPath := filepath.Join(tmpDir, "sample.tar")
	if err := os.WriteFile(tarPath, originalTar, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	fi, err := os.Stat(tarPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	ctx := context.Background()

	// 1. Initial Ingestion
	f1, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f1.Close()

	res1, err := eng.Ingest(ctx, f1, IngestOptions{
		Name:        "sample.tar",
		SourcePath:  tarPath,
		SourceSize:  fi.Size(),
		SourceMtime: fi.ModTime().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Initial Ingest failed: %v", err)
	}
	if res1.AlreadyExists {
		t.Fatalf("Initial ingest should not report AlreadyExists")
	}

	// 2. Second Ingestion with same source file (Tier 1 short-circuit)
	f2, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f2.Close()

	res2, err := eng.Ingest(ctx, f2, IngestOptions{
		Name:        "sample.tar",
		SourcePath:  tarPath,
		SourceSize:  fi.Size(),
		SourceMtime: fi.ModTime().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Second Ingest failed: %v", err)
	}
	if !res2.AlreadyExists {
		t.Fatalf("Expected second ingest to short-circuit with AlreadyExists=true")
	}
	if res2.ArchiveID != res1.ArchiveID {
		t.Fatalf("Expected ArchiveID %s, got %s", res1.ArchiveID, res2.ArchiveID)
	}

	// 3. Streaming Ingestion without source path (Tier 3 stream SHA256 short-circuit)
	res3, err := eng.Ingest(ctx, bytes.NewReader(originalTar), IngestOptions{
		Name: "sample_stream.tar",
	})
	if err != nil {
		t.Fatalf("Streaming Ingest failed: %v", err)
	}
	if !res3.AlreadyExists {
		t.Fatalf("Expected streaming duplicate ingest to short-circuit with AlreadyExists=true")
	}
	if res3.ArchiveID != res1.ArchiveID {
		t.Fatalf("Expected ArchiveID %s, got %s", res1.ArchiveID, res3.ArchiveID)
	}

	// 4. Force Ingestion (bypasses short-circuiting)
	res4, err := eng.Ingest(ctx, bytes.NewReader(originalTar), IngestOptions{
		Name:  "sample_forced.tar",
		Force: true,
	})
	if err != nil {
		t.Fatalf("Force Ingest failed: %v", err)
	}
	if res4.AlreadyExists {
		t.Fatalf("Forced ingest should not report AlreadyExists")
	}
	if res4.ArchiveID == res1.ArchiveID {
		t.Fatalf("Forced ingest should generate a new ArchiveID")
	}
}

func TestDuplicateFilePayloadShortCircuit(t *testing.T) {
	eng, db, _, tmpDir := setupTestEngine(t, 128) // 128B threshold -> files > 128B are blobs
	defer os.RemoveAll(tmpDir)
	defer db.Close()

	largeContent := strings.Repeat("Repeated large file content block for deduplication test. ", 50)

	// Archive 1 contains large file
	files1 := []struct{ Name, Body string }{
		{"large_common.bin", largeContent},
		{"unique_1.txt", "Unique content in archive 1"},
	}
	tar1 := createTestTarball(t, files1)

	ctx := context.Background()
	res1, err := eng.Ingest(ctx, bytes.NewReader(tar1), IngestOptions{Name: "archive1.tar"})
	if err != nil {
		t.Fatalf("Ingest 1 failed: %v", err)
	}
	if res1.AlreadyExists {
		t.Fatalf("Ingest 1 should not report AlreadyExists")
	}

	// Archive 2 contains the same large file + different unique file
	files2 := []struct{ Name, Body string }{
		{"large_common.bin", largeContent},
		{"unique_2.txt", "Unique content in archive 2"},
	}
	tar2 := createTestTarball(t, files2)

	res2, err := eng.Ingest(ctx, bytes.NewReader(tar2), IngestOptions{Name: "archive2.tar"})
	if err != nil {
		t.Fatalf("Ingest 2 failed: %v", err)
	}

	// Archive 2 should NOT report AlreadyExists (it's a distinct archive)
	if res2.AlreadyExists {
		t.Fatalf("Archive 2 should not be reported as whole archive duplicate")
	}

	// Large file must be reported as deduplicated
	if res2.DeduplicatedBytes < int64(len(largeContent)) {
		t.Errorf("Expected deduplicated bytes >= %d, got %d", len(largeContent), res2.DeduplicatedBytes)
	}

	// Reconstruct archive 2 and verify byte-for-byte fidelity
	buf := new(bytes.Buffer)
	if _, _, err := eng.Reconstruct(ctx, res2.ArchiveID, buf); err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), tar2) {
		t.Fatalf("Reconstructed archive2 does not match original tar2 byte-for-byte!")
	}
}

