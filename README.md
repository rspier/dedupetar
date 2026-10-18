# DedupeTar v2: High-Performance Deduplicating Tarball Storage

A high-performance deduplicating tarball storage system designed for 100% byte-for-byte fidelity, hybrid packfile CAS storage, streaming reassembly, random-access HTTP Range requests, and epoch-based mark-and-sweep garbage collection.

## Core Features

- **Byte-for-Byte Fidelity**: Preserves verbatim 512-byte headers, PAX extended headers, GNU LongName/LongLink records, sparse file headers, base-256 binary sizes, hard links, and raw trailer padding bytes.
- **Hybrid Content-Addressed Storage (CAS)**:
  - Payloads $> 1\text{ MB}$: Standalone CAS blobs (`blobs/<hash>`) hashed with BLAKE3.
  - Payloads $\le 1\text{ MB}$: Batched into append-only Packfile containers (`packfiles/pack_<id>.pack`) to eliminate the 100,000 small files problem in object storage.
- **Adaptive Zstandard Compression**: Slices and standalone blobs are automatically compressed with **Zstandard (zstd)** if compression saves $\ge 5\%$ space. Incompressible or already-compressed media automatically bypasses compression (`codec = none`), while compressible text, code, and configurations achieve massive space reductions (often 3x to 50x) while retaining sub-millisecond seeking and 100% byte fidelity.
- **Cross-Archive Deduplication**: Duplicate file payloads are detected via BLAKE3 hashes and mapped directly to existing packfile slices or blobs, achieving immediate space savings.
- **Zero-Assembly Streaming Reassembly**: Streams byte-identical tarballs on the fly without reconstructing a temporary file on disk.
- **HTTP Range Seeking (`Accept-Ranges: bytes`)**: Pre-computed `TarByteOffset` enables $O(\log N)$ binary-search random access and seamless `curl -C -` resumption.
- **Two-Phase Generational Mark-and-Sweep GC**: Eliminates mutable reference counting in favor of an active-reference mark phase and quarantine safety window ($T_{\text{grace}}$, default 6 hours).
- **Cloud Storage Ready**: Extensible `BlobStore` interface with `LocalBlobStore` implementation and fully-documented `S3BlobStore` placeholder featuring coalesced Range GET logic.

---

## Architecture Overview

```
                      [ Incoming Tar / Tar.gz ]
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │  Tar Block State Machine  │
                    │   (tarparser.Parser)      │
                    └─────────────┬─────────────┘
                                  │
             ┌────────────────────┴────────────────────┐
             │                                         │
    [Verbatim Raw Headers]                     [File Payloads]
             │                                         │
             ▼                                         ▼
┌──────────────────────────┐              ┌──────────────────────────┐
│  Recipe Chunk Writer     │              │    CAS / Packfile Mgr    │
│  - Raw 512B blocks       │              │  - BLAKE3 Hashing        │
│  - PAX / GNU records     │              │  - > 1MB: Standalone     │
│  - TarByteOffset index   │              │  - <= 1MB: Packfile      │
└────────────┬─────────────┘              └────────────┬─────────────┘
             │                                         │
             ▼                                         ▼
┌──────────────────────────┐              ┌──────────────────────────┐
│ SQLite Metadata Store    │              │ Local / Cloud Blob Store │
│ - Archives & Catalog     │              │ - blobs/<hash>           │
│ - Recipe Chunks          │              │ - packfiles/<id>.pack    │
│ - CAS Entries            │              └────────────┬─────────────┘
└────────────┬─────────────┘                           │
             │                                         │
             └────────────────────┬────────────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │   Streaming Reassembly    │
                    │  & HTTP Download Proxy    │
                    │ - On-the-fly streaming    │
                    │ - HTTP Range seeking      │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
               [ Client Download: 100% Byte-Identical Tar ]
```

---

## Building and Testing

### Build Binary
```bash
go build -o dedupetar ./cmd/dedupetar
```

### Run Tests
```bash
go test -v ./...
```

---

## CLI Usage

### Ingest Tarball
```bash
./dedupetar ingest my_archive.tar
# or with gzip compression
./dedupetar ingest my_archive.tar.gz
```

### List Archives
```bash
./dedupetar list
```

### Show Details & Recipe Chunks
```bash
./dedupetar show <archive_id>
```

### Extract & Verify Byte Fidelity
```bash
./dedupetar extract <archive_id> reconstructed.tar
./dedupetar verify <archive_id> original.tar
```

### Repository Status & Efficiency Overview
```bash
./dedupetar status
# or output as machine-readable JSON
./dedupetar status --json
```

### Soft-Delete & Mark-and-Sweep GC
```bash
# Soft-delete archive (marks status = 'deleted')
./dedupetar delete <archive_id>

# Run GC with 6 hour quarantine grace period
./dedupetar gc --grace 6h

# Run immediate GC (for testing)
./dedupetar gc --grace 0s
```

### Start HTTP Server
```bash
./dedupetar serve --port 8080
```

---

## HTTP REST API

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/archives?name=archive.tar` | Stream upload and ingest a tar or tar.gz archive |
| `GET` | `/archives` | List stored archives |
| `GET` | `/archives/:id` | Get archive manifest and metadata |
| `GET` | `/archives/:id/download` | Stream byte-identical tarball (supports `Range: bytes=...`) |
| `GET` | `/status` | Retrieve repository-wide storage, deduplication & compression metrics |
| `DELETE` | `/archives/:id` | Soft-delete archive |
| `POST` | `/gc?grace=6h` | Trigger Two-Phase Mark-and-Sweep GC |

### Resumable Downloads & Seeking Example
```bash
# Resume interrupted download
curl -C - -O http://localhost:8080/archives/<id>/download

# Fetch first 1000 bytes
curl -H "Range: bytes=0-999" http://localhost:8080/archives/<id>/download -o partial.bin
```

---

## Cloud Storage (S3 / GCS) Integration

See [pkg/cas/s3_placeholder.go](file:///home/rspier/oldhome/projects/tarchive/pkg/cas/s3_placeholder.go) for the S3 integration placeholder, including the `CoalesceRequests` algorithm that coalesces thousands of small packfile slice requests into single S3 Range GETs (`Range: bytes=O_start - O_end`).
