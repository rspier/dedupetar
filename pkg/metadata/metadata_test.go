package metadata

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"tarchive/pkg/cas"
	"tarchive/pkg/models"
)

func TestMetadataAndGC(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "meta_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open DB failed: %v", err)
	}
	defer db.Close()

	blobStore, err := cas.NewLocalBlobStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalBlobStore failed: %v", err)
	}

	ctx := context.Background()

	// 1. Create two blobs on disk and record in storage_objects
	blob1Data := []byte("active-payload")
	blob1Hash := "hash_active"
	if err := blobStore.PutBlob(ctx, blob1Hash, bytes.NewReader(blob1Data), int64(len(blob1Data))); err != nil {
		t.Fatalf("PutBlob 1 failed: %v", err)
	}
	if err := db.RecordStorageObject(ctx, blob1Hash, "blob", int64(len(blob1Data))); err != nil {
		t.Fatalf("RecordStorageObject 1 failed: %v", err)
	}

	blob2Data := []byte("orphan-payload")
	blob2Hash := "hash_orphan"
	if err := blobStore.PutBlob(ctx, blob2Hash, bytes.NewReader(blob2Data), int64(len(blob2Data))); err != nil {
		t.Fatalf("PutBlob 2 failed: %v", err)
	}
	if err := db.RecordStorageObject(ctx, blob2Hash, "blob", int64(len(blob2Data))); err != nil {
		t.Fatalf("RecordStorageObject 2 failed: %v", err)
	}

	// 2. Save active archive referencing blob1Hash
	manifest := &models.ArchiveManifest{
		ArchiveID:              "arc_001",
		OriginalName:           "test.tar",
		TotalUncompressedBytes: 100,
		UncompressedSHA256:     "sha256_dummy",
		TotalEntries:           1,
		TrailingPaddingData:    []byte{0, 0, 0, 0},
		CreatedAt:              time.Now(),
	}
	chunks := []models.RecipeChunk{
		{
			ChunkIndex: 0,
			Entries: []models.TarEntry{
				{
					RawHeaderBlocks: make([]byte, 512),
					PayloadSize:     int64(len(blob1Data)),
					Location: models.PayloadLocation{
						Type:               models.StorageTypeBlob,
						StandaloneBlobHash: blob1Hash,
					},
				},
			},
		},
	}

	if err := db.SaveArchiveTransaction(ctx, manifest, []string{blob1Hash}, chunks); err != nil {
		t.Fatalf("SaveArchiveTransaction failed: %v", err)
	}

	// 3. Verify archive can be queried
	rec, err := db.GetArchive(ctx, "arc_001")
	if err != nil {
		t.Fatalf("GetArchive failed: %v", err)
	}
	if rec.Name != "test.tar" || rec.Status != "active" {
		t.Errorf("Unexpected record: %+v", rec)
	}

	m, err := db.GetManifest(ctx, "arc_001")
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if m.ArchiveID != "arc_001" {
		t.Errorf("Manifest ID mismatch: %s", m.ArchiveID)
	}

	ch, err := db.GetRecipeChunk(ctx, "arc_001", 0)
	if err != nil {
		t.Fatalf("GetRecipeChunk failed: %v", err)
	}
	if len(ch.Entries) != 1 {
		t.Fatalf("Expected 1 entry in chunk, got %d", len(ch.Entries))
	}

	// 4. Test GC with 6 hour grace period:
	// Both objects are young (<6 hours old), so orphan should be quarantined (skipped).
	stats, err := db.RunTwoPhaseGC(ctx, blobStore, 6*time.Hour)
	if err != nil {
		t.Fatalf("RunTwoPhaseGC with grace period failed: %v", err)
	}
	if stats.ActiveObjectsMarked != 1 {
		t.Errorf("Expected 1 active object marked, got %d", stats.ActiveObjectsMarked)
	}
	if stats.QuarantinedObjectsSkipped != 1 {
		t.Errorf("Expected 1 quarantined object skipped, got %d", stats.QuarantinedObjectsSkipped)
	}
	if stats.ObjectsDeleted != 0 {
		t.Errorf("Expected 0 objects deleted due to quarantine, got %d", stats.ObjectsDeleted)
	}

	// 5. Test GC with gracePeriod = 0 (simulate aged quarantine)
	statsZeroGrace, err := db.RunTwoPhaseGC(ctx, blobStore, 0)
	if err != nil {
		t.Fatalf("RunTwoPhaseGC with 0 grace failed: %v", err)
	}
	if statsZeroGrace.ObjectsDeleted != 1 {
		t.Errorf("Expected 1 orphan object deleted, got %d", statsZeroGrace.ObjectsDeleted)
	}
	if statsZeroGrace.BytesReclaimed != int64(len(blob2Data)) {
		t.Errorf("Expected %d bytes reclaimed, got %d", len(blob2Data), statsZeroGrace.BytesReclaimed)
	}

	// Verify orphan blob2 is removed from physical storage
	_, err = blobStore.GetBlob(ctx, blob2Hash)
	if err == nil {
		t.Errorf("Expected error getting deleted blob2, but succeeded")
	}

	// Verify active blob1 is still intact in physical storage
	rc, err := blobStore.GetBlob(ctx, blob1Hash)
	if err != nil {
		t.Errorf("Active blob1 was mistakenly deleted: %v", err)
	} else {
		rc.Close()
	}

	// 6. Test Soft Delete and subsequent GC
	if err := db.SoftDeleteArchive(ctx, "arc_001"); err != nil {
		t.Fatalf("SoftDeleteArchive failed: %v", err)
	}
	arcAfter, err := db.GetArchive(ctx, "arc_001")
	if err != nil || arcAfter.Status != "deleted" {
		t.Errorf("Expected status 'deleted', got: %v, err: %v", arcAfter, err)
	}

	// Now blob1 has no active references, so GC with 0 grace should sweep it
	statsAfterDelete, err := db.RunTwoPhaseGC(ctx, blobStore, 0)
	if err != nil {
		t.Fatalf("RunTwoPhaseGC after archive delete failed: %v", err)
	}
	if statsAfterDelete.ObjectsDeleted != 1 {
		t.Errorf("Expected blob1 deleted after archive soft-delete, got %d", statsAfterDelete.ObjectsDeleted)
	}
}
