package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"tarchive/pkg/models"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database operations for DedupeTar.
type DB struct {
	db *sql.DB
	mu sync.RWMutex
}

// Open initializes SQLite connection and runs schema migrations.
func Open(dataSourceName string) (*DB, error) {
	// Add pragmas for WAL mode and busy timeout for concurrent safety
	dsn := dataSourceName
	if dsn != ":memory:" {
		if !contains(dsn, "_journal_mode") {
			dsn += "?_journal_mode=WAL&_busy_timeout=5000"
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite: %w", err)
	}

	metaDB := &DB{db: db}
	if err := metaDB.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %w", err)
	}

	return metaDB, nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr))
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS archives (
		archive_id VARCHAR(64) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		uncompressed_size BIGINT NOT NULL,
		uncompressed_sha256 CHAR(64) NOT NULL,
		status VARCHAR(16) NOT NULL DEFAULT 'active',
		manifest_key VARCHAR(255) NOT NULL,
		total_entries INTEGER NOT NULL DEFAULT 0,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS archive_blob_refs (
		archive_id VARCHAR(64) REFERENCES archives(archive_id) ON DELETE CASCADE,
		blob_id VARCHAR(64) NOT NULL,
		PRIMARY KEY (archive_id, blob_id)
	);

	CREATE INDEX IF NOT EXISTS idx_blob_refs_blob ON archive_blob_refs(blob_id);

	CREATE TABLE IF NOT EXISTS storage_objects (
		object_id VARCHAR(64) PRIMARY KEY,
		object_type VARCHAR(16) NOT NULL,
		byte_size BIGINT NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS cas_entries (
		hash VARCHAR(64) PRIMARY KEY,
		object_id VARCHAR(64) NOT NULL,
		object_type VARCHAR(16) NOT NULL,
		offset BIGINT NOT NULL,
		length BIGINT NOT NULL,
		uncompressed_length BIGINT NOT NULL DEFAULT 0,
		codec VARCHAR(16) NOT NULL DEFAULT 'none'
	);

	CREATE TABLE IF NOT EXISTS recipe_chunks (
		archive_id VARCHAR(64) NOT NULL,
		chunk_index INTEGER NOT NULL,
		entry_count INTEGER NOT NULL,
		data BLOB NOT NULL,
		PRIMARY KEY (archive_id, chunk_index)
	);

	CREATE TABLE IF NOT EXISTS manifests (
		archive_id VARCHAR(64) PRIMARY KEY,
		data BLOB NOT NULL
	);

	CREATE TABLE IF NOT EXISTS active_packfiles (
		packfile_id VARCHAR(64) PRIMARY KEY,
		current_size BIGINT NOT NULL,
		sealed BOOLEAN NOT NULL DEFAULT 0,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := d.db.Exec(schema)
	return err
}

// FindCASEntry checks if a payload hash is already recorded.
func (d *DB) FindCASEntry(ctx context.Context, hash string) (*models.PayloadLocation, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var objID, objType, codecStr string
	var offset, length, uncompLen int64
	err := d.db.QueryRowContext(ctx,
		"SELECT object_id, object_type, offset, length, uncompressed_length, codec FROM cas_entries WHERE hash = ?",
		hash,
	).Scan(&objID, &objType, &offset, &length, &uncompLen, &codecStr)

	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	codec := models.CompressionCodec(codecStr)
	loc := &models.PayloadLocation{
		Type:           models.StorageType(objType),
		CompressedSize: length,
		Codec:          codec,
	}
	if loc.Type == models.StorageTypeBlob {
		loc.StandaloneBlobHash = objID
	} else {
		loc.PackfileSlice = &models.PackfileSlice{
			PackfileID:         objID,
			Offset:             uint64(offset),
			Length:             uint32(length),
			UncompressedLength: uint32(uncompLen),
			Codec:              codec,
		}
	}
	return loc, true, nil
}

// RecordCASEntry records a deduplicated CAS payload mapping with compression codec.
func (d *DB) RecordCASEntry(ctx context.Context, hash string, loc models.PayloadLocation) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var objID string
	var offset, length, uncompLen int64
	codec := string(loc.Codec)
	if codec == "" {
		codec = string(models.CodecNone)
	}

	if loc.Type == models.StorageTypeBlob {
		objID = loc.StandaloneBlobHash
		offset = 0
		length = loc.CompressedSize
		uncompLen = loc.CompressedSize
	} else if loc.PackfileSlice != nil {
		objID = loc.PackfileSlice.PackfileID
		offset = int64(loc.PackfileSlice.Offset)
		length = int64(loc.PackfileSlice.Length)
		uncompLen = int64(loc.PackfileSlice.UncompressedLength)
		codec = string(loc.PackfileSlice.Codec)
	}

	_, err := d.db.ExecContext(ctx,
		"INSERT OR REPLACE INTO cas_entries (hash, object_id, object_type, offset, length, uncompressed_length, codec) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?)",
		hash, objID, string(loc.Type), offset, length, uncompLen, codec,
	)
	return err
}

// RecordStorageObject records or updates a stored object (blob or packfile).
func (d *DB) RecordStorageObject(ctx context.Context, objectID string, objectType string, byteSize int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.ExecContext(ctx,
		"INSERT INTO storage_objects (object_id, object_type, byte_size, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) "+
			"ON CONFLICT(object_id) DO UPDATE SET byte_size = excluded.byte_size",
		objectID, objectType, byteSize,
	)
	return err
}

// SaveArchiveTransaction saves the archive record, its manifest, recipe chunks, and referenced blob/packfile IDs in a single atomic transaction.
func (d *DB) SaveArchiveTransaction(ctx context.Context, manifest *models.ArchiveManifest, blobIDs []string, chunks []models.RecipeChunk) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	manifestKey := fmt.Sprintf("manifests/%s.json", manifest.ArchiveID)

	// 1. Insert Archive
	_, err = tx.ExecContext(ctx,
		"INSERT INTO archives (archive_id, name, uncompressed_size, uncompressed_sha256, status, manifest_key, total_entries, created_at) "+
			"VALUES (?, ?, ?, ?, 'active', ?, ?, ?)",
		manifest.ArchiveID, manifest.OriginalName, manifest.TotalUncompressedBytes, manifest.UncompressedSHA256,
		manifestKey, manifest.TotalEntries, manifest.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting archive: %w", err)
	}

	// 2. Insert Archive Blob Refs (unique set of blob hash / packfile IDs)
	uniqueBlobIDs := make(map[string]struct{})
	for _, id := range blobIDs {
		if id != "" {
			uniqueBlobIDs[id] = struct{}{}
		}
	}
	stmtRef, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO archive_blob_refs (archive_id, blob_id) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("preparing ref stmt: %w", err)
	}
	defer stmtRef.Close()

	for id := range uniqueBlobIDs {
		if _, err := stmtRef.ExecContext(ctx, manifest.ArchiveID, id); err != nil {
			return fmt.Errorf("inserting blob ref: %w", err)
		}
	}

	// 3. Insert Manifest
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO manifests (archive_id, data) VALUES (?, ?)", manifest.ArchiveID, manifestBytes)
	if err != nil {
		return fmt.Errorf("inserting manifest: %w", err)
	}

	// 4. Insert Recipe Chunks
	stmtChunk, err := tx.PrepareContext(ctx, "INSERT INTO recipe_chunks (archive_id, chunk_index, entry_count, data) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("preparing chunk stmt: %w", err)
	}
	defer stmtChunk.Close()

	for _, chunk := range chunks {
		chunkBytes, err := json.Marshal(chunk)
		if err != nil {
			return fmt.Errorf("marshaling chunk %d: %w", chunk.ChunkIndex, err)
		}
		if _, err := stmtChunk.ExecContext(ctx, manifest.ArchiveID, chunk.ChunkIndex, len(chunk.Entries), chunkBytes); err != nil {
			return fmt.Errorf("inserting chunk %d: %w", chunk.ChunkIndex, err)
		}
	}

	return tx.Commit()
}

func parseTime(s string) time.Time {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Now()
}

// GetArchive fetches metadata for a specific archive.
func (d *DB) GetArchive(ctx context.Context, archiveID string) (*models.ArchiveRecord, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var rec models.ArchiveRecord
	var createdAtStr string
	err := d.db.QueryRowContext(ctx,
		"SELECT archive_id, name, uncompressed_size, uncompressed_sha256, status, manifest_key, total_entries, created_at "+
			"FROM archives WHERE archive_id = ?",
		archiveID,
	).Scan(&rec.ArchiveID, &rec.Name, &rec.UncompressedSize, &rec.UncompressedSHA256, &rec.Status, &rec.ManifestKey, &rec.TotalEntries, &createdAtStr)
	if err != nil {
		return nil, err
	}
	rec.CreatedAt = parseTime(createdAtStr)
	return &rec, nil
}

// ListArchives returns all archives (optionally filtering active ones).
func (d *DB) ListArchives(ctx context.Context, activeOnly bool) ([]models.ArchiveRecord, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := "SELECT archive_id, name, uncompressed_size, uncompressed_sha256, status, manifest_key, total_entries, created_at FROM archives"
	if activeOnly {
		query += " WHERE status = 'active'"
	}
	query += " ORDER BY created_at DESC"

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []models.ArchiveRecord
	for rows.Next() {
		var rec models.ArchiveRecord
		var createdAtStr string
		if err := rows.Scan(&rec.ArchiveID, &rec.Name, &rec.UncompressedSize, &rec.UncompressedSHA256, &rec.Status, &rec.ManifestKey, &rec.TotalEntries, &createdAtStr); err != nil {
			return nil, err
		}
		rec.CreatedAt = parseTime(createdAtStr)
		records = append(records, rec)
	}
	return records, rows.Err()
}

// GetManifest retrieves the manifest for an archive.
func (d *DB) GetManifest(ctx context.Context, archiveID string) (*models.ArchiveManifest, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var data []byte
	err := d.db.QueryRowContext(ctx, "SELECT data FROM manifests WHERE archive_id = ?", archiveID).Scan(&data)
	if err != nil {
		return nil, err
	}

	var manifest models.ArchiveManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshaling manifest: %w", err)
	}
	return &manifest, nil
}

// GetRecipeChunk retrieves a single recipe chunk.
func (d *DB) GetRecipeChunk(ctx context.Context, archiveID string, chunkIndex uint32) (*models.RecipeChunk, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var data []byte
	err := d.db.QueryRowContext(ctx,
		"SELECT data FROM recipe_chunks WHERE archive_id = ? AND chunk_index = ?",
		archiveID, chunkIndex,
	).Scan(&data)
	if err != nil {
		return nil, err
	}

	var chunk models.RecipeChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return nil, fmt.Errorf("unmarshaling recipe chunk: %w", err)
	}
	return &chunk, nil
}

// SoftDeleteArchive marks an archive status as 'deleted'.
func (d *DB) SoftDeleteArchive(ctx context.Context, archiveID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	res, err := d.db.ExecContext(ctx, "UPDATE archives SET status = 'deleted' WHERE archive_id = ?", archiveID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// HardDeleteArchive removes archive records, blobs refs, manifest, and recipe chunks from the DB.
func (d *DB) HardDeleteArchive(ctx context.Context, archiveID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM archive_blob_refs WHERE archive_id = ?", archiveID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM recipe_chunks WHERE archive_id = ?", archiveID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM manifests WHERE archive_id = ?", archiveID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM archives WHERE archive_id = ?", archiveID); err != nil {
		return err
	}

	return tx.Commit()
}

// GetActivePackfile retrieves the latest unsealed active packfile.
func (d *DB) GetActivePackfile(ctx context.Context) (string, int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var packID string
	var size int64
	err := d.db.QueryRowContext(ctx,
		"SELECT packfile_id, current_size FROM active_packfiles WHERE sealed = 0 ORDER BY created_at DESC LIMIT 1",
	).Scan(&packID, &size)
	if err == sql.ErrNoRows {
		return "", 0, nil
	}
	return packID, size, err
}

// UpdateActivePackfile updates the size or sealed status of a packfile.
func (d *DB) UpdateActivePackfile(ctx context.Context, packfileID string, currentSize int64, sealed bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	sealedInt := 0
	if sealed {
		sealedInt = 1
	}

	_, err := d.db.ExecContext(ctx,
		"INSERT INTO active_packfiles (packfile_id, current_size, sealed, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) "+
			"ON CONFLICT(packfile_id) DO UPDATE SET current_size = excluded.current_size, sealed = excluded.sealed",
		packfileID, currentSize, sealedInt,
	)
	return err
}

// SystemStatus aggregates repository-wide storage and deduplication statistics.
type SystemStatus struct {
	TotalArchives           int     `json:"total_archives"`
	ActiveArchives          int     `json:"active_archives"`
	DeletedArchives         int     `json:"deleted_archives"`
	TotalArchiveEntries     int64   `json:"total_archive_entries"`
	RawUncompressedBytes    int64   `json:"raw_uncompressed_bytes"`

	UniquePayloads          int64   `json:"unique_payloads"`
	UniqueUncompressedBytes int64   `json:"unique_uncompressed_bytes"`

	StandaloneBlobsCount    int     `json:"standalone_blobs_count"`
	StandaloneBlobsBytes    int64   `json:"standalone_blobs_bytes"`
	PackfilesCount          int     `json:"packfiles_count"`
	PackfilesBytes          int64   `json:"packfiles_bytes"`
	TotalStoredBytes        int64   `json:"total_stored_bytes"`

	DeduplicationRatio      float64 `json:"deduplication_ratio"`
	CompressionRatio        float64 `json:"compression_ratio"`
	OverallEfficiency       float64 `json:"overall_efficiency"`
	TotalSpaceSavedBytes    int64   `json:"total_space_saved_bytes"`
	TotalSpaceSavedPct      float64 `json:"total_space_saved_pct"`
}

// GetSystemStatus gathers repository-wide metrics across archives, CAS entries, and storage objects.
func (d *DB) GetSystemStatus(ctx context.Context) (*SystemStatus, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	s := &SystemStatus{}

	// 1. Archives overview
	row := d.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			COALESCE(SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'deleted' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'active' THEN total_entries ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'active' THEN uncompressed_size ELSE 0 END), 0)
		FROM archives
	`)
	if err := row.Scan(&s.TotalArchives, &s.ActiveArchives, &s.DeletedArchives, &s.TotalArchiveEntries, &s.RawUncompressedBytes); err != nil {
		return nil, fmt.Errorf("querying archives status: %w", err)
	}

	// 2. CAS unique payloads
	rowCAS := d.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			COALESCE(SUM(uncompressed_length), 0)
		FROM cas_entries
	`)
	if err := rowCAS.Scan(&s.UniquePayloads, &s.UniqueUncompressedBytes); err != nil {
		return nil, fmt.Errorf("querying cas_entries status: %w", err)
	}

	// 3. Storage objects breakdown
	rowsObj, err := d.db.QueryContext(ctx, `
		SELECT object_type, COUNT(*), COALESCE(SUM(byte_size), 0)
		FROM storage_objects
		GROUP BY object_type
	`)
	if err != nil {
		return nil, fmt.Errorf("querying storage_objects status: %w", err)
	}
	defer rowsObj.Close()

	for rowsObj.Next() {
		var objType string
		var count int
		var bSize int64
		if err := rowsObj.Scan(&objType, &count, &bSize); err != nil {
			return nil, err
		}
		if objType == "blob" {
			s.StandaloneBlobsCount = count
			s.StandaloneBlobsBytes = bSize
		} else if objType == "packfile" {
			s.PackfilesCount = count
			s.PackfilesBytes = bSize
		}
	}
	s.TotalStoredBytes = s.StandaloneBlobsBytes + s.PackfilesBytes

	// 4. Calculate efficiency metrics
	s.DeduplicationRatio = 1.0
	if s.RawUncompressedBytes > 0 && s.UniqueUncompressedBytes > 0 {
		s.DeduplicationRatio = float64(s.RawUncompressedBytes) / float64(s.UniqueUncompressedBytes)
	}

	s.CompressionRatio = 1.0
	if s.UniqueUncompressedBytes > 0 && s.TotalStoredBytes > 0 {
		s.CompressionRatio = float64(s.UniqueUncompressedBytes) / float64(s.TotalStoredBytes)
	}

	s.OverallEfficiency = 1.0
	if s.RawUncompressedBytes > 0 && s.TotalStoredBytes > 0 {
		s.OverallEfficiency = float64(s.RawUncompressedBytes) / float64(s.TotalStoredBytes)
	}

	if s.RawUncompressedBytes > s.TotalStoredBytes {
		s.TotalSpaceSavedBytes = s.RawUncompressedBytes - s.TotalStoredBytes
		s.TotalSpaceSavedPct = (float64(s.TotalSpaceSavedBytes) / float64(s.RawUncompressedBytes)) * 100
	}

	return s, nil
}
