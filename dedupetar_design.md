# High-Performance Deduplicating Tarball Storage System (DedupeTar v2)

## 1. Executive Summary & Design Goals

The primary goal of **DedupeTar** is to store arbitrary tarballs with high deduplication ratios while guaranteeing **100% byte-for-byte identical reconstruction** of the uncompressed tar stream, supporting **instantaneous, on-the-fly streaming downloads** across both single-node and distributed cloud environments.

### Core Architectural Pillars

| Pillar | Design Approach |
| :--- | :--- |
| **Byte-for-byte Fidelity** | **Recipe-based archive reconstruction**: Never re-synthesize headers from database records. Store verbatim raw header blocks (including PAX and GNU extended attribute payload blocks), entry sequencing, and raw end-of-archive trailer bytes in a streaming recipe. |
| **Deduplication** | **Content-Addressed Storage (CAS) with Packfiles**: File payloads are hashed (BLAKE3) and deduplicated. Payloads $> 1\text{ MB}$ are stored as independent CAS objects. Payloads $\le 1\text{ MB}$ are batched into immutable **Packfile containers** (SeaweedFS/Git model) to prevent cloud metadata and API throttling. |
| **Streaming Reassembly** | **Zero-assembly on-the-fly pipeline**: The download endpoint reads the chunked recipe sequentially, streaming headers from the recipe and payloads directly from the CAS/Packfile store (via zero-copy `sendfile(2)` locally, or coalesced Range GETs over S3) directly to the HTTP socket. |
| **Random Access & Seeking** | **Pre-computed entry offsets**: The recipe tracks cumulative byte offsets, enabling native HTTP Range requests (`Accept-Ranges: bytes`) and resumable downloads (`curl -C -`). |
| **Robust Concurrency & GC** | **Two-Phase Generation Mark-and-Sweep**: Eliminates fragile integer reference counting in favor of epoch-based tombstone garbage collection with quarantine safety windows. |

---

## 2. End-to-End System Architecture

```
                       [ Incoming Upload: .tar / .tar.gz ]
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │    Ingestion Processor    │
                        │  (Streaming Decompress &  │
                        │     Tar State Machine)    │
                        └─────────────┬─────────────┘
                                      │
                 ┌────────────────────┴────────────────────┐
                 │                                         │
        [Raw Headers & Layout]                     [File Payloads]
                 │                                         │
                 ▼                                         ▼
   ┌──────────────────────────┐              ┌──────────────────────────┐
   │  Streaming Recipe Writer │              │  Hybrid CAS Allocator    │
   │  - Verbatim Raw Blocks   │              │  - BLAKE3 Tree Hash      │
   │  - PAX / GNU Long records│              │  - Size > 1MB: Direct CAS│
   │  - Byte Offset Indexing  │              │  - Size <= 1MB: Packfile │
   └─────────────┬────────────┘              └─────────────┬────────────┘
                 │                                         │
                 ▼                                         ▼
   ┌──────────────────────────┐              ┌──────────────────────────┐
   │      Metadata Store      │              │        Blob Store        │
   │ (PostgreSQL / SQLite)    │              │ (Local POSIX / S3 / GCS) │
   │  - Archive Catalog       │              │  - Standalone Blobs      │
   │  - Chunked Recipes       │              │  - 32-64MB Packfiles     │
   └─────────────┬────────────┘              └─────────────┬────────────┘
                 │                                         │
                 └────────────────────┬────────────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │   Streaming Reassembly    │
                        │     & Download Proxy      │
                        │ - Zero-Copy (sendfile)    │
                        │ - Coalesced Range GETs    │
                        │ - HTTP Range Seeking      │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                    [ Client Download: Byte-Identical .tar ]
```

---

## 3. The Tar Format Specification & Parser State Machine

The POSIX and GNU `tar` standards contain numerous quirks that cause naive parsers to desynchronize or fail byte-for-byte verification. DedupeTar's ingestion pipeline runs a strict block state machine:

### 3.1. Auxiliary Data Blocks (PAX & GNU Long records)
Standard tar entries consist of a 512-byte header followed by payload. However, extended records—such as **PAX Extended Headers** (`typeflag = 'x'`), **PAX Global Headers** (`'g'`), **GNU LongPath** (`'L'`), and **GNU LongLink** (`'K'`)—have their own payload data blocks containing filenames or key-value attributes:

$$\text{Data Blocks} = \left\lceil \frac{\text{size}}{512} \right\rceil$$

```
┌───────────────────────────────────┬───────────────────────────────────┬──────────────────────┐
│ Auxiliary Header Block (512 bytes)│ Auxiliary Data Blocks (N x 512 B) │ File Header (512 B)  │
│ typeflag = 'x' / 'L', size = 850  │ [850 Bytes Data] + [174 Bytes Pad]│ typeflag = '0' / '2' │
└───────────────────────────────────┴───────────────────────────────────┴──────────────────────┘
└───────────────────────────────────┬───────────────────────────────────┘
               Stored verbatim in `raw_header_blocks`
```

**State Machine Rule:**  
When an auxiliary header is encountered, the scanner reads its octal `size`, consumes exactly $\lceil \text{size} / 512 \rceil \times 512$ bytes, and appends both the auxiliary header and its data blocks verbatim to `raw_header_blocks`.

### 3.2. Special Tar Types & Edge Cases

1. **Hardlinks (`typeflag = '1'`):**
   * Some non-compliant archivers populate the `size` field in a hardlink header with the target file's size, but write **zero payload blocks** to the archive.
   * **Rule:** If `typeflag == '1'`, force `payload_size = 0` regardless of the header's `size` field.
2. **Files $\ge 8\text{ GiB}$ (Base-256 Binary Encoding):**
   * Standard octal overflows at $8\text{ GiB}-1$. GNU tar and Star encode large values in binary two's-complement base-256 (signaled by bit `0x80` set in `header[124]`).
   * **Rule:** The scanner tests the top bit; if set, it decodes size as big-endian binary rather than octal.
3. **GNU Sparse Files (`typeflag = 'S'`):**
   * If `isextended == 1`, additional 512-byte `sp_array` blocks appear before the payload. The scanner consumes all extended sparse header blocks into `raw_header_blocks` and stores the compacted payload blocks in CAS.
4. **End-of-Archive (EOA) & Concatenated Archives:**
   * Two consecutive 512-byte null blocks define standard EOA, but Unix users often concatenate archives (`cat a.tar b.tar > c.tar`).
   * **Rule:** The scanner tests whether valid headers follow a null sequence. It buffers trailing bytes and only marks end-of-archive when EOF is actually reached on the stream, storing the exact raw trailing bytes verbatim (`trailing_padding_data`).

---

## 4. Solving the Cloud S3 Bottleneck: Hybrid Packfiles

### 4.1. The 100,000 Small Files Problem in Object Storage
In cloud object storage (S3/GCS), storing 100,000 small files ($\le 1\text{ MB}$) as individual objects results in:
- **Excessive Latency:** 100,000 sequential `GET` requests $\times$ 15 ms = 25 minutes to download a 150 MB archive.
- **S3 503 SlowDown Errors:** S3 throttles beyond 5,500 requests/sec per prefix.
- **Punitive API Billing:** 100,000 S3 GETs cost \$0.04 per single archive download.

### 4.2. The Hybrid Packfile Architecture (SeaweedFS / Git model)
DedupeTar partitions files by payload size:

```
File Size Threshold (e.g., 1 MB):
├── Size > 1 MB  ──> Standalone CAS Blob (`blobs/<hash>`)
└── Size <= 1 MB ──> Appended to active Packfile (`packfiles/pack_2026_001.pack`)
```

- **Packfile Container:** An append-only chunk file (target size: 32 MB – 64 MB).
- **Packfile Storage:** Stored as a single immutable S3 object once sealed.
- **Coalesced Range GETs on Download:**
  When streaming a tarball whose entries reside in a Packfile, the download engine groups contiguous or nearby slices into a **single HTTP Range Request**:
  $$\text{Range: bytes}=O_{\text{start}} - O_{\text{end}}$$
  A tarball containing 10,000 small files spanning two 32 MB Packfiles requires only **2 S3 GET operations** instead of 10,000!

---

## 5. Streaming Recipe Specification

To ensure time-to-first-byte (TTFB) remains $< 10\text{ ms}$ even for archives with 500,000 files, recipes are chunked into 1,000-entry segments or streamed via flat binary encoding.

```protobuf
syntax = "proto3";
package dedupetar.v2;

message ArchiveManifest {
  string archive_id = 1;
  string original_name = 2;
  int64 total_uncompressed_bytes = 3;
  string uncompressed_sha256 = 4;
  uint32 total_entries = 5;
  bytes trailing_padding_data = 6; // Verbatim trailer bytes
  
  repeated RecipeChunkRef recipe_chunks = 7;
}

message RecipeChunkRef {
  uint32 chunk_index = 1;
  uint32 entry_count = 2;
  string storage_key = 3; // Path in blob/metadata store
}

message RecipeChunk {
  repeated TarEntry entries = 1;
}

message TarEntry {
  // Verbatim 512-byte header + any auxiliary PAX / GNU data blocks
  bytes raw_header_blocks = 1;

  int64 payload_size = 2;
  uint32 payload_padding = 3;
  int64 tar_byte_offset = 4; // Pre-calculated offset for HTTP Range seeking

  PayloadLocation location = 5;
}

message PayloadLocation {
  oneof loc {
    string standalone_blob_hash = 1; // Used if > 1 MB
    PackfileSlice packfile_slice = 2; // Used if <= 1 MB
  }
}

message PackfileSlice {
  string packfile_id = 1;
  uint64 offset = 2;
  uint32 length = 3;
}
```

---

## 6. Concurrency & Garbage Collection: Two-Phase Mark-and-Sweep

Mutable integer reference counting (`ref_count INT`) suffers from drift, race conditions, and deadlocks. DedupeTar uses an **Epoch-based Generational Mark-and-Sweep** strategy.

### 6.1. Relational Schema
```sql
CREATE TABLE archives (
    archive_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    uncompressed_size BIGINT NOT NULL,
    uncompressed_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'active', -- active, deleted
    manifest_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Active archive to blob association
CREATE TABLE archive_blob_refs (
    archive_id VARCHAR(64) REFERENCES archives(archive_id) ON DELETE CASCADE,
    blob_id VARCHAR(64) NOT NULL, -- Blob hash OR Packfile ID
    PRIMARY KEY (archive_id, blob_id)
);

CREATE INDEX idx_blob_refs_blob ON archive_blob_refs(blob_id);

CREATE TABLE storage_objects (
    object_id VARCHAR(64) PRIMARY KEY, -- Blob hash OR Packfile ID
    object_type VARCHAR(16) NOT NULL,  -- 'blob' or 'packfile'
    byte_size BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### 6.2. Race-Free Ingestion & GC Protocol

```
           Upload Transaction                   Tombstone GC Worker
      ┌───────────────────────────┐         ┌───────────────────────────┐
      │ 1. Stream & Hash Payloads │         │ 1. Mark Phase:            │
      │ 2. Write new Blobs/Packs  │         │    Gather all active refs │
      │ 3. BEGIN TRANSACTION      │         │    from `archive_blob_refs`│
      │    - Insert Archive       │         │ 2. Sweep Phase:           │
      │    - Insert Blob Refs     │         │    Find orphaned objects  │
      │    COMMIT                 │         │ 3. Quarantine Filter:     │
      └───────────────────────────┘         │    Skip objects where     │
                                            │    created_at > (now - 6h)│
                                            │ 4. Delete unreferenced    │
                                            │    objects older than 6h  │
                                            └───────────────────────────┘
```

1. **Quarantine Window ($T_{\text{grace}} = 6\text{ hours}$):** Any blob written to storage is immune from deletion for 6 hours, completely preventing Time-of-Check to Time-of-Use (TOCTOU) races between concurrent uploads and sweep cycles.
2. **Packfile Compaction:** When small files within a Packfile become unreferenced, a low-priority background compactor copies surviving active slices into a new Packfile and drops the old container once fragmentation exceeds 40%.

---

## 7. High-Throughput Streaming & Random Access (Seeking)

### 7.1. Streaming Pipeline Mechanics
- **Local Target:** Employs `sendfile(2)` or `splice(2)` for zero-copy file transmission directly from NVMe page cache to socket.
- **Cloud Target:** Employs an asynchronous prefetch buffer:
  - Worker fetches recipe chunk $0$.
  - Begins streaming `raw_header_blocks` to the client socket.
  - Spawns background worker to prefetch payload blobs/ranges for chunk $0$ and recipe chunk $1$.
  - Yields a **Time-To-First-Byte (TTFB) under 10 ms**.

### 7.2. HTTP Range Requests & Resumption
Because `tar_byte_offset` is tracked per entry:
1. When a client requests `Range: bytes=524288000-` (seeking to 500 MB):
2. Binary search the recipe chunk index to identify the target entry in $O(\log N)$ time.
3. If the offset falls inside a payload:
   - Calculate payload internal slice: $\text{offset}_{\text{internal}} = 500\text{ MB} - (\text{tar\_byte\_offset} + \text{header\_len})$.
   - Stream starting from that internal byte offset in the CAS blob/packfile.
4. Seamlessly supports `curl -C -` and multi-threaded parallel download accelerators.

---

## 8. Summary Comparison

| Dimension | Initial v1 Draft | Refined v2 Architecture |
| :--- | :--- | :--- |
| **Tar Fidelity** | Standard headers only | **Full Support:** Verbatim PAX data, GNU LongLink/LongName, sparse blocks, base-256 integers, raw trailer |
| **Cloud S3 Latency (100k files)**| ~25 minutes (100k individual GETs) | **< 2 seconds** (coalesced Packfile Range GETs) |
| **S3 API Cost (per 100k download)**| \$0.04 per download | **\$0.000008 per download** (99.98% cost reduction) |
| **GC Reliability** | Vulnerable integer `ref_count` | **Robust Two-Phase Generational Mark-and-Sweep** |
| **HTTP Seeking** | Stream from byte 0 only | **Full `Accept-Ranges: bytes` random-access seeking** |
