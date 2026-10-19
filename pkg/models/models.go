package models

import "time"

// StorageType indicates whether a payload is stored as a standalone CAS blob or packfile slice.
type StorageType string

const (
	StorageTypeBlob     StorageType = "blob"
	StorageTypePackfile StorageType = "packfile"
)

// CompressionCodec represents the algorithm used to compress a payload slice or blob.
type CompressionCodec string

const (
	CodecNone CompressionCodec = "none"
	CodecZSTD CompressionCodec = "zstd"
)

// PackfileSlice identifies the exact location and compression metadata of a payload within a packfile.
type PackfileSlice struct {
	PackfileID         string           `json:"packfile_id"`
	Offset             uint64           `json:"offset"`
	Length             uint32           `json:"length"` // Stored length on disk (compressed or uncompressed)
	UncompressedLength uint32           `json:"uncompressed_length"`
	Codec              CompressionCodec `json:"codec"`
}

// PayloadLocation specifies where a payload is stored.
type PayloadLocation struct {
	Type               StorageType      `json:"type"`
	StandaloneBlobHash string           `json:"standalone_blob_hash,omitempty"`
	CompressedSize     int64            `json:"compressed_size,omitempty"`
	Codec              CompressionCodec `json:"codec,omitempty"`
	PackfileSlice      *PackfileSlice   `json:"packfile_slice,omitempty"`
}

// TarEntry represents a single entry in a reconstructed tarball.
type TarEntry struct {
	// Verbatim 512-byte header + any auxiliary PAX / GNU data blocks
	RawHeaderBlocks []byte `json:"raw_header_blocks"`

	PayloadSize    int64 `json:"payload_size"`
	PayloadPadding uint32 `json:"payload_padding"`

	// Cumulative uncompressed tar byte offset of this entry's start
	TarByteOffset int64 `json:"tar_byte_offset"`

	Location PayloadLocation `json:"location"`
}

// TotalEntryBytes returns the total number of uncompressed tar bytes this entry occupies:
// len(RawHeaderBlocks) + PayloadSize + PayloadPadding.
func (e *TarEntry) TotalEntryBytes() int64 {
	return int64(len(e.RawHeaderBlocks)) + e.PayloadSize + int64(e.PayloadPadding)
}

// RecipeChunk holds a sequential batch of TarEntries.
type RecipeChunk struct {
	ChunkIndex uint32     `json:"chunk_index"`
	Entries    []TarEntry `json:"entries"`
}

// RecipeChunkRef references a stored recipe chunk.
type RecipeChunkRef struct {
	ChunkIndex uint32 `json:"chunk_index"`
	EntryCount uint32 `json:"entry_count"`
	StorageKey string `json:"storage_key"`
}

// ArchiveManifest holds the top-level archive metadata and chunk references.
type ArchiveManifest struct {
	ArchiveID              string           `json:"archive_id"`
	OriginalName           string           `json:"original_name"`
	TotalUncompressedBytes int64            `json:"total_uncompressed_bytes"`
	UncompressedSHA256     string           `json:"uncompressed_sha256"`
	TotalEntries           uint32           `json:"total_entries"`
	TrailingPaddingData    []byte           `json:"trailing_padding_data"`
	RecipeChunks           []RecipeChunkRef `json:"recipe_chunks"`
	SourcePath             string           `json:"source_path,omitempty"`
	SourceSize             int64            `json:"source_size,omitempty"`
	SourceMtime            int64            `json:"source_mtime,omitempty"`
	CreatedAt              time.Time        `json:"created_at"`
}

// ArchiveRecord represents an archive in the metadata catalog.
type ArchiveRecord struct {
	ArchiveID          string    `json:"archive_id"`
	Name               string    `json:"name"`
	UncompressedSize   int64     `json:"uncompressed_size"`
	UncompressedSHA256 string    `json:"uncompressed_sha256"`
	Status             string    `json:"status"` // 'active', 'deleted'
	ManifestKey        string    `json:"manifest_key"`
	TotalEntries       uint32    `json:"total_entries"`
	SourcePath         string    `json:"source_path,omitempty"`
	SourceSize         int64     `json:"source_size,omitempty"`
	SourceMtime        int64     `json:"source_mtime,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
}

// StorageObjectRecord represents a stored blob or packfile container in the catalog.
type StorageObjectRecord struct {
	ObjectID   string    `json:"object_id"`
	ObjectType string    `json:"object_type"` // 'blob', 'packfile'
	ByteSize   int64     `json:"byte_size"`
	CreatedAt  time.Time `json:"created_at"`
}
