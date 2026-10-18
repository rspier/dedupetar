package metadata

import (
	"context"
	"fmt"
	"time"

	"tarchive/pkg/cas"
)

// GCStats reports metrics from a garbage collection run.
type GCStats struct {
	TotalObjectsScanned      int   `json:"total_objects_scanned"`
	ActiveObjectsMarked      int   `json:"active_objects_marked"`
	QuarantinedObjectsSkipped int   `json:"quarantined_objects_skipped"`
	ObjectsDeleted           int   `json:"objects_deleted"`
	BytesReclaimed           int64 `json:"bytes_reclaimed"`
	Duration                 time.Duration `json:"duration"`
}

// RunTwoPhaseGC executes the Two-Phase Generational Mark-and-Sweep algorithm.
func (d *DB) RunTwoPhaseGC(ctx context.Context, blobStore cas.BlobStore, gracePeriod time.Duration) (*GCStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	startTime := time.Now()
	stats := &GCStats{}

	// Phase 1: Mark Phase
	// Query all blob/packfile IDs referenced by currently ACTIVE archives.
	activeQuery := `
		SELECT DISTINCT blob_id 
		FROM archive_blob_refs
		INNER JOIN archives ON archive_blob_refs.archive_id = archives.archive_id
		WHERE archives.status = 'active'
	`
	rows, err := d.db.QueryContext(ctx, activeQuery)
	if err != nil {
		return nil, fmt.Errorf("GC Mark phase query failed: %w", err)
	}
	defer rows.Close()

	activeRefs := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning active ref: %w", err)
		}
		activeRefs[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	stats.ActiveObjectsMarked = len(activeRefs)

	// Phase 2: Sweep Phase
	// Find all storage objects
	objQuery := `SELECT object_id, object_type, byte_size, created_at FROM storage_objects`
	objRows, err := d.db.QueryContext(ctx, objQuery)
	if err != nil {
		return nil, fmt.Errorf("GC Sweep phase query failed: %w", err)
	}
	defer objRows.Close()

	type sweepCandidate struct {
		id        string
		objType   string
		byteSize  int64
		createdAt time.Time
	}

	var candidates []sweepCandidate
	cutoffTime := time.Now().Add(-gracePeriod)

	for objRows.Next() {
		var c sweepCandidate
		var createdAtStr string
		if err := objRows.Scan(&c.id, &c.objType, &c.byteSize, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scanning storage object: %w", err)
		}
		c.createdAt, _ = time.Parse(time.RFC3339, createdAtStr)
		if c.createdAt.IsZero() {
			// Fallback parse SQLite default datetime "2006-01-02 15:04:05"
			c.createdAt, _ = time.Parse("2006-01-02 15:04:05", createdAtStr)
		}
		stats.TotalObjectsScanned++

		if activeRefs[c.id] {
			continue // Object is actively referenced, keep it
		}

		// Phase 3: Quarantine Filter
		// Any object created within the grace period is protected from deletion (TOCTOU protection)
		if c.createdAt.After(cutoffTime) {
			stats.QuarantinedObjectsSkipped++
			continue
		}

		candidates = append(candidates, c)
	}

	// Phase 4: Reclaim unreferenced, unquarantined objects
	for _, c := range candidates {
		// 1. Delete physical object from blobStore
		if err := blobStore.DeleteObject(ctx, c.id, c.objType); err != nil {
			// Log and continue to delete as many as possible
			fmt.Printf("warning: error deleting object %s (%s) from storage: %v\n", c.id, c.objType, err)
		}

		// 2. Remove from DB storage_objects
		if _, err := d.db.ExecContext(ctx, "DELETE FROM storage_objects WHERE object_id = ?", c.id); err != nil {
			return nil, fmt.Errorf("deleting storage_objects record %s: %w", c.id, err)
		}

		// 3. Remove associated cas_entries
		if _, err := d.db.ExecContext(ctx, "DELETE FROM cas_entries WHERE object_id = ?", c.id); err != nil {
			return nil, fmt.Errorf("deleting cas_entries for %s: %w", c.id, err)
		}

		stats.ObjectsDeleted++
		stats.BytesReclaimed += c.byteSize
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}
