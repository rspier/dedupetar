package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"tarchive/pkg/cas"
	"tarchive/pkg/engine"
	"tarchive/pkg/metadata"
	"tarchive/pkg/server"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	dataDir := os.Getenv("DEDUPETAR_DATA_DIR")
	if dataDir == "" {
		dataDir = ".dedupetar_data"
	}

	switch cmd {
	case "ingest":
		runIngest(dataDir, args)
	case "extract":
		runExtract(dataDir, args)
	case "verify":
		runVerify(dataDir, args)
	case "list":
		runList(dataDir, args)
	case "show":
		runShow(dataDir, args)
	case "delete":
		runDelete(dataDir, args)
	case "gc":
		runGC(dataDir, args)
	case "status":
		runStatus(dataDir, args)
	case "serve":
		runServe(dataDir, args)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`DedupeTar v2 - High-Performance Deduplicating Tarball Storage System

Usage:
  dedupetar <command> [arguments]

Commands:
  ingest <archive.tar[.gz]>   Ingest a tar or compressed tarball into DedupeTar
  extract <archive_id> <out>  Reconstruct byte-identical tarball to output path
  verify <archive_id> [orig]  Verify SHA256 integrity of an archive against catalog
  list [--all]                List stored archives
  show <archive_id>           Display archive manifest and recipe chunks details
  status [--json]             Show repository-wide storage, deduplication & compression status
  delete <archive_id>         Soft-delete an archive
  gc [--grace <duration>]     Run Two-Phase Generational Mark-and-Sweep GC
  serve [--port <port>]       Start HTTP REST and streaming download server
  help                        Show this help message

Options can also be controlled with DEDUPETAR_DATA_DIR environment variable.`)
}

func initContext(dataDir string, blobThreshold int64) (*engine.Engine, *metadata.DB, cas.BlobStore, func()) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating data dir: %v\n", err)
		os.Exit(1)
	}

	dbPath := filepath.Join(dataDir, "meta.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening metadata DB: %v\n", err)
		os.Exit(1)
	}

	casDir := filepath.Join(dataDir, "cas")
	blobStore, err := cas.NewLocalBlobStore(casDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening local blob store: %v\n", err)
		os.Exit(1)
	}

	pm := cas.NewPackfileManager(blobStore, cas.Config{
		BlobThreshold: blobThreshold,
	})

	eng := engine.New(db, blobStore, pm, blobThreshold)

	cleanup := func() {
		db.Close()
		blobStore.Close()
	}

	return eng, db, blobStore, cleanup
}

func runIngest(dataDir string, args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	nameFlag := fs.String("name", "", "Custom name for archive")
	thresholdFlag := fs.Int64("threshold", cas.DefaultBlobThreshold, "Blob size threshold in bytes (default 1MB)")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: dedupetar ingest [options] <path-to-archive.tar[.gz]>")
		os.Exit(1)
	}

	archivePath := fs.Arg(0)
	f, err := os.Open(archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", archivePath, err)
		os.Exit(1)
	}
	defer f.Close()

	archiveName := *nameFlag
	if archiveName == "" {
		archiveName = filepath.Base(archivePath)
	}

	eng, _, _, cleanup := initContext(dataDir, *thresholdFlag)
	defer cleanup()

	ctx := context.Background()
	fmt.Printf("Ingesting %s...\n", archiveName)
	res, err := eng.Ingest(ctx, f, engine.IngestOptions{
		Name: archiveName,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ingestion failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nIngestion Completed Successfully:")
	fmt.Printf("  Archive ID:              %s\n", res.ArchiveID)
	fmt.Printf("  Name:                    %s\n", res.Name)
	fmt.Printf("  Total Entries:           %d\n", res.TotalEntries)
	fmt.Printf("  Total Uncompressed Size: %d bytes\n", res.TotalUncompressedBytes)
	fmt.Printf("  Uncompressed SHA256:     %s\n", res.UncompressedSHA256)
	fmt.Printf("  New Bytes Stored:        %d bytes (on-disk compressed)\n", res.NewBytesStored)
	fmt.Printf("  Deduplicated Bytes:      %d bytes\n", res.DeduplicatedBytes)
	fmt.Printf("  Deduplication Ratio:     %.2fx\n", res.DeduplicationRatio)
	fmt.Printf("  Compression Ratio:       %.2fx\n", res.CompressionRatio)
	fmt.Printf("  Duration:                %v\n", res.Duration)
}

func runExtract(dataDir string, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: dedupetar extract <archive_id> <output_path.tar>")
		os.Exit(1)
	}

	archiveID := args[0]
	outPath := args[1]

	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file %s: %v\n", outPath, err)
		os.Exit(1)
	}
	defer outFile.Close()

	ctx := context.Background()
	fmt.Printf("Extracting %s to %s...\n", archiveID, outPath)
	written, calculatedSHA, err := eng.Reconstruct(ctx, archiveID, outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Extraction failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Extraction Verified Successfully:")
	fmt.Printf("  Bytes Reconstructed: %d\n", written)
	fmt.Printf("  SHA256 Verified:     %s\n", calculatedSHA)
}

func runVerify(dataDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: dedupetar verify <archive_id> [original_file.tar]")
		os.Exit(1)
	}

	archiveID := args[0]
	var origPath string
	if len(args) >= 2 {
		origPath = args[1]
	}

	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	manifest, err := eng.GetManifest(ctx, archiveID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching manifest for %s: %v\n", archiveID, err)
		os.Exit(1)
	}

	fmt.Printf("Verifying archive %s (%s)...\n", archiveID, manifest.OriginalName)
	written, calculatedSHA, err := eng.Reconstruct(ctx, archiveID, io.Discard)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Verification failed: %v\n", err)
		os.Exit(1)
	}

	if calculatedSHA != manifest.UncompressedSHA256 {
		fmt.Fprintf(os.Stderr, "FAILED: SHA256 mismatch (got %s, want %s)\n", calculatedSHA, manifest.UncompressedSHA256)
		os.Exit(1)
	}

	fmt.Printf("Catalog Checksum:      %s\n", manifest.UncompressedSHA256)
	fmt.Printf("Reconstructed Stream:  %s\n", calculatedSHA)
	fmt.Printf("Total Bytes Verified:  %d bytes\n", written)

	if origPath != "" {
		origBytes, err := os.ReadFile(origPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading original file: %v\n", err)
			os.Exit(1)
		}
		origSHA := sha256.Sum256(origBytes)
		origSHAStr := fmt.Sprintf("%x", origSHA)
		if origSHAStr != calculatedSHA {
			fmt.Fprintf(os.Stderr, "FAILED: Reconstructed tar SHA256 (%s) does not match original file (%s)\n", calculatedSHA, origSHAStr)
			os.Exit(1)
		}
		fmt.Println("Original File SHA256:  MATCH (100% byte-for-byte identical!)")
	}

	fmt.Println("Result: PASSED (100% byte-for-byte fidelity)")
}

func runList(dataDir string, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	allFlag := fs.Bool("all", false, "Include deleted archives")
	fs.Parse(args)

	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	archives, err := eng.ListArchives(ctx, !*allFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing archives: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ARCHIVE ID\tNAME\tUNCOMPRESSED SIZE\tENTRIES\tSTATUS\tCREATED AT")
	for _, a := range archives {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
			a.ArchiveID, a.Name, a.UncompressedSize, a.TotalEntries, a.Status, a.CreatedAt.Format("2006-01-02 15:04:05"))
	}
	w.Flush()
}

func runShow(dataDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: dedupetar show <archive_id>")
		os.Exit(1)
	}

	archiveID := args[0]
	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	manifest, err := eng.GetManifest(ctx, archiveID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting manifest: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Archive ID:              %s\n", manifest.ArchiveID)
	fmt.Printf("Original Name:           %s\n", manifest.OriginalName)
	fmt.Printf("Total Uncompressed Size: %d bytes\n", manifest.TotalUncompressedBytes)
	fmt.Printf("Uncompressed SHA256:     %s\n", manifest.UncompressedSHA256)
	fmt.Printf("Total Entries:           %d\n", manifest.TotalEntries)
	fmt.Printf("Trailing Padding Bytes:  %d bytes\n", len(manifest.TrailingPaddingData))
	fmt.Printf("Created At:              %s\n", manifest.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Recipe Chunks (%d):\n", len(manifest.RecipeChunks))
	for _, chunk := range manifest.RecipeChunks {
		fmt.Printf("  - Chunk #%d: %d entries (key: %s)\n", chunk.ChunkIndex, chunk.EntryCount, chunk.StorageKey)
	}
}

func runDelete(dataDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: dedupetar delete <archive_id>")
		os.Exit(1)
	}

	archiveID := args[0]
	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	if err := eng.DeleteArchive(ctx, archiveID); err != nil {
		fmt.Fprintf(os.Stderr, "Error deleting archive: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Archive %s marked as deleted (will be swept on next GC cycle).\n", archiveID)
}

func runGC(dataDir string, args []string) {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	graceFlag := fs.String("grace", "6h", "Quarantine grace period (e.g. 6h, 30m, 0s)")
	fs.Parse(args)

	grace, err := time.ParseDuration(*graceFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid grace duration %q: %v\n", *graceFlag, err)
		os.Exit(1)
	}

	_, db, blobStore, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	fmt.Printf("Running Two-Phase Generational Mark-and-Sweep GC (grace period: %v)...\n", grace)
	stats, err := db.RunTwoPhaseGC(ctx, blobStore, grace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GC failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nGC Completed Successfully:")
	fmt.Printf("  Total Objects Scanned:       %d\n", stats.TotalObjectsScanned)
	fmt.Printf("  Active Objects Marked:       %d\n", stats.ActiveObjectsMarked)
	fmt.Printf("  Quarantined Objects Skipped: %d\n", stats.QuarantinedObjectsSkipped)
	fmt.Printf("  Objects Deleted:             %d\n", stats.ObjectsDeleted)
	fmt.Printf("  Bytes Reclaimed:             %d bytes\n", stats.BytesReclaimed)
	fmt.Printf("  Duration:                    %v\n", stats.Duration)
}

func runServe(dataDir string, args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	portFlag := fs.Int("port", 8080, "Port to listen on")
	fs.Parse(args)

	eng, db, blobStore, cleanup := initContext(dataDir, 0)
	defer cleanup()

	srv := server.NewServer(eng, db, blobStore)
	addr := fmt.Sprintf(":%d", *portFlag)

	fmt.Printf("DedupeTar server listening on http://localhost%s\n", addr)
	fmt.Printf("  - POST   /archives             Upload / Ingest tarball\n")
	fmt.Printf("  - GET    /archives             List archives\n")
	fmt.Printf("  - GET    /archives/:id         Get archive manifest\n")
	fmt.Printf("  - GET    /archives/:id/download Streaming download (supports Range requests)\n")
	fmt.Printf("  - DELETE /archives/:id         Soft-delete archive\n")
	fmt.Printf("  - POST   /gc?grace=6h          Trigger mark-and-sweep GC\n")

	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		fmt.Fprintf(os.Stderr, "Server failed: %v\n", err)
		os.Exit(1)
	}
}

func runStatus(dataDir string, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	jsonFlag := fs.Bool("json", false, "Output status as JSON")
	fs.Parse(args)

	eng, _, _, cleanup := initContext(dataDir, 0)
	defer cleanup()

	ctx := context.Background()
	status, err := eng.GetStatus(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting status: %v\n", err)
		os.Exit(1)
	}

	if *jsonFlag {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(status)
		return
	}

	fmt.Println("================================================================================")
	fmt.Println("DedupeTar Repository Overview & Storage Efficiency Status")
	fmt.Println("================================================================================")
	fmt.Println("ARCHIVES OVERVIEW")
	fmt.Printf("  Total Archives:               %d (%d active, %d deleted)\n",
		status.TotalArchives, status.ActiveArchives, status.DeletedArchives)
	fmt.Printf("  Total Archive File Entries:   %s entries\n", formatNumber(status.TotalArchiveEntries))
	fmt.Printf("  Raw Uncompressed Archive:     %s\n", formatBytes(status.RawUncompressedBytes))
	fmt.Println()
	fmt.Println("CONTENT-ADDRESSED STORAGE (CAS)")
	fmt.Printf("  Unique Payloads:              %s unique files\n", formatNumber(status.UniquePayloads))
	fmt.Printf("  Unique Uncompressed Payloads: %s\n", formatBytes(status.UniqueUncompressedBytes))
	fmt.Printf("  Standalone Blobs (> 1MB):     %d blobs (%s)\n",
		status.StandaloneBlobsCount, formatBytes(status.StandaloneBlobsBytes))
	fmt.Printf("  Packfiles (<= 1MB):           %d packfiles (%s)\n",
		status.PackfilesCount, formatBytes(status.PackfilesBytes))
	fmt.Printf("  Total Actual Stored Bytes:    %s\n", formatBytes(status.TotalStoredBytes))
	fmt.Println()
	fmt.Println("EFFICIENCY & SAVINGS BREAKDOWN")
	fmt.Printf("  Deduplication Factor:         %.2fx\n", status.DeduplicationRatio)
	fmt.Printf("  Compression Factor (zstd):    %.2fx\n", status.CompressionRatio)
	fmt.Printf("  Overall Storage Efficiency:   %.2fx\n", status.OverallEfficiency)
	if status.TotalSpaceSavedBytes > 0 {
		fmt.Printf("  Total Space Reclaimed:        %s (%.1f%% reduction!)\n",
			formatBytes(status.TotalSpaceSavedBytes), status.TotalSpaceSavedPct)
	} else {
		fmt.Printf("  Total Space Reclaimed:        0 bytes (0.0%%)\n")
	}
	fmt.Println("================================================================================")
}

func formatBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB (%s bytes)", float64(b)/float64(div), "KMGTPE"[exp], formatNumber(b))
}

func formatNumber(n int64) string {
	in := fmt.Sprintf("%d", n)
	out := ""
	for i, c := range in {
		if i > 0 && (len(in)-i)%3 == 0 {
			out += ","
		}
		out += string(c)
	}
	return out
}
