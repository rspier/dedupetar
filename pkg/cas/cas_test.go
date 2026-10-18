package cas

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"tarchive/pkg/models"
)

func TestLocalBlobStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cas_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalBlobStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalBlobStore failed: %v", err)
	}

	ctx := context.Background()

	// 1. Put and Get standalone blob
	blobData := []byte("Standalone CAS payload data 1234567890")
	blobHash := "hash12345"
	if err := store.PutBlob(ctx, blobHash, bytes.NewReader(blobData), int64(len(blobData))); err != nil {
		t.Fatalf("PutBlob failed: %v", err)
	}

	rc, err := store.GetBlob(ctx, blobHash)
	if err != nil {
		t.Fatalf("GetBlob failed: %v", err)
	}
	readData, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(readData, blobData) {
		t.Errorf("GetBlob data mismatch: got %q, want %q", string(readData), string(blobData))
	}

	// 2. Blob Range
	rangeRC, err := store.GetBlobRange(ctx, blobHash, 11, 3) // "CAS"
	if err != nil {
		t.Fatalf("GetBlobRange failed: %v", err)
	}
	rangeData, err := io.ReadAll(rangeRC)
	rangeRC.Close()
	if err != nil {
		t.Fatalf("ReadAll range failed: %v", err)
	}
	if string(rangeData) != "CAS" {
		t.Errorf("GetBlobRange expected 'CAS', got %q", string(rangeData))
	}

	// 3. Append and Get Packfile Slice
	packID := "pack_test_01"
	slice1 := []byte("slice-one-content")
	slice2 := []byte("slice-two-content-larger")

	off1, err := store.AppendPackfileSlice(ctx, packID, bytes.NewReader(slice1), int64(len(slice1)))
	if err != nil {
		t.Fatalf("AppendPackfileSlice 1 failed: %v", err)
	}
	if off1 != 0 {
		t.Errorf("Expected off1 == 0, got %d", off1)
	}

	off2, err := store.AppendPackfileSlice(ctx, packID, bytes.NewReader(slice2), int64(len(slice2)))
	if err != nil {
		t.Fatalf("AppendPackfileSlice 2 failed: %v", err)
	}
	if off2 != uint64(len(slice1)) {
		t.Errorf("Expected off2 == %d, got %d", len(slice1), off2)
	}

	// Read slice 2
	s2RC, err := store.GetPackfileSlice(ctx, packID, off2, uint32(len(slice2)))
	if err != nil {
		t.Fatalf("GetPackfileSlice failed: %v", err)
	}
	s2Data, err := io.ReadAll(s2RC)
	s2RC.Close()
	if err != nil {
		t.Fatalf("ReadAll slice 2 failed: %v", err)
	}
	if !bytes.Equal(s2Data, slice2) {
		t.Errorf("Slice 2 mismatch: got %q, want %q", string(s2Data), string(slice2))
	}

	// 4. List Objects
	objs, err := store.ListObjects(ctx)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(objs) != 2 {
		t.Errorf("Expected 2 objects (1 blob, 1 packfile), got %d", len(objs))
	}

	// 5. Delete Object
	if err := store.DeleteObject(ctx, blobHash, "blob"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	objsAfter, err := store.ListObjects(ctx)
	if err != nil {
		t.Fatalf("ListObjects after delete failed: %v", err)
	}
	if len(objsAfter) != 1 {
		t.Errorf("Expected 1 object after delete, got %d", len(objsAfter))
	}
}

func TestCompression(t *testing.T) {
	// Highly compressible data
	compressible := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. 1234567890\n"), 50)
	compressed, codec, err := CompressAdaptive(compressible)
	if err != nil {
		t.Fatalf("CompressAdaptive failed: %v", err)
	}
	if codec != models.CodecZSTD {
		t.Errorf("Expected CodecZSTD for repetitive data, got %s", codec)
	}
	if len(compressed) >= len(compressible)/2 {
		t.Errorf("Expected >50%% compression, got %d -> %d", len(compressible), len(compressed))
	}

	decompressed, err := Decompress(compressed, codec)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	if !bytes.Equal(decompressed, compressible) {
		t.Errorf("Decompressed content mismatch")
	}

	// Tiny incompressible data should bypass compression
	tiny := []byte("short")
	cTiny, codecTiny, err := CompressAdaptive(tiny)
	if err != nil {
		t.Fatalf("CompressAdaptive tiny failed: %v", err)
	}
	if codecTiny != models.CodecNone {
		t.Errorf("Expected CodecNone for tiny payload, got %s", codecTiny)
	}
	if !bytes.Equal(cTiny, tiny) {
		t.Errorf("Tiny data altered")
	}
}

func TestPackfileManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cas_pm_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalBlobStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalBlobStore failed: %v", err)
	}

	pm := NewPackfileManager(store, Config{
		BlobThreshold:   100, // 100 bytes threshold
		MaxPackfileSize: 200, // 200 bytes max packfile size
	})

	ctx := context.Background()

	// 1. Small compressible payload -> packfile with Zstandard
	smallData := bytes.Repeat([]byte("small repeated data "), 4) // 80 bytes
	loc1, objID1, storedSize1, err := pm.WritePayload(ctx, "hash_s1", smallData)
	if err != nil {
		t.Fatalf("WritePayload small 1 failed: %v", err)
	}
	if loc1.Type != models.StorageTypePackfile {
		t.Errorf("Expected StorageTypePackfile, got %v", loc1.Type)
	}
	if loc1.Codec != models.CodecZSTD {
		t.Errorf("Expected CodecZSTD, got %v", loc1.Codec)
	}
	if storedSize1 >= int64(len(smallData)) {
		t.Errorf("Expected storedSize < uncompressed for repetitive data: %d vs %d", storedSize1, len(smallData))
	}
	if objID1 == "" {
		t.Errorf("Expected non-empty objID")
	}

	// Read small payload via OpenSlice (transparent decompression)
	r1, err := pm.OpenSlice(ctx, loc1)
	if err != nil {
		t.Fatalf("OpenSlice failed: %v", err)
	}
	d1, err := io.ReadAll(r1)
	r1.Close()
	if err != nil {
		t.Fatalf("ReadAll slice 1 failed: %v", err)
	}
	if !bytes.Equal(d1, smallData) {
		t.Errorf("OpenSlice mismatch: got %q, want %q", string(d1), string(smallData))
	}

	// Range seek inside compressed small payload
	rRange, err := pm.OpenSliceRange(ctx, loc1, 6, 8)
	if err != nil {
		t.Fatalf("OpenSliceRange failed: %v", err)
	}
	rangeBytes, err := io.ReadAll(rRange)
	rRange.Close()
	if err != nil {
		t.Fatalf("ReadAll range failed: %v", err)
	}
	if !bytes.Equal(rangeBytes, smallData[6:14]) {
		t.Errorf("OpenSliceRange mismatch: got %q, want %q", string(rangeBytes), string(smallData[6:14]))
	}

	// 2. Large payload (> 100 bytes) -> standalone CAS blob
	largeData := bytes.Repeat([]byte("A"), 150)
	loc2, objID2, _, err := pm.WritePayload(ctx, "hash_large", largeData)
	if err != nil {
		t.Fatalf("WritePayload large failed: %v", err)
	}
	if loc2.Type != models.StorageTypeBlob {
		t.Errorf("Expected StorageTypeBlob, got %v", loc2.Type)
	}
	if objID2 != "hash_large" {
		t.Errorf("Expected objID == hash_large, got %s", objID2)
	}

	r2, err := pm.OpenSlice(ctx, loc2)
	if err != nil {
		t.Fatalf("OpenSlice large failed: %v", err)
	}
	d2, _ := io.ReadAll(r2)
	r2.Close()
	if !bytes.Equal(d2, largeData) {
		t.Errorf("OpenSlice large mismatch")
	}
}

func TestS3Coalesce(t *testing.T) {
	requests := []S3SliceRequest{
		{PackfileID: "p1", Offset: 0, Length: 100},
		{PackfileID: "p1", Offset: 120, Length: 50}, // gap of 20
		{PackfileID: "p1", Offset: 500, Length: 200}, // gap of 330 (> 50)
		{PackfileID: "p2", Offset: 0, Length: 100},   // different packfile
	}

	batches := CoalesceRequests(requests, 50)
	if len(batches) != 3 {
		t.Fatalf("Expected 3 batches, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("Expected batch 0 to have 2 merged requests, got %d", len(batches[0]))
	}
}
