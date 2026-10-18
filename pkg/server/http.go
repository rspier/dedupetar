package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"tarchive/pkg/cas"
	"tarchive/pkg/engine"
	"tarchive/pkg/metadata"
)

// Server provides the HTTP REST and streaming download service.
type Server struct {
	engine    *engine.Engine
	db        *metadata.DB
	blobStore cas.BlobStore
	mux       *http.ServeMux
}

// NewServer creates a new HTTP server instance.
func NewServer(eng *engine.Engine, db *metadata.DB, blobStore cas.BlobStore) *Server {
	s := &Server{
		engine:    eng,
		db:        db,
		blobStore: blobStore,
		mux:       http.NewServeMux(),
	}
	s.routes()
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/archives", s.handleArchives)
	s.mux.HandleFunc("/archives/", s.handleArchiveByID)
	s.mux.HandleFunc("/gc", s.handleGC)
	s.mux.HandleFunc("/status", s.handleStatus)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status, err := s.engine.GetStatus(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("getting system status: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleArchives(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		archives, err := s.engine.ListArchives(r.Context(), false)
		if err != nil {
			http.Error(w, fmt.Sprintf("listing archives: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(archives)

	case http.MethodPost:
		// Stream upload from request body
		filename := r.URL.Query().Get("name")
		if filename == "" {
			filename = "upload.tar"
		}
		res, err := s.engine.Ingest(r.Context(), r.Body, engine.IngestOptions{
			Name: filename,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("ingesting archive: %v", err), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(res)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleArchiveByID(w http.ResponseWriter, r *http.Request) {
	// Path format: /archives/{id} or /archives/{id}/download
	path := strings.TrimPrefix(r.URL.Path, "/archives/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	archiveID := parts[0]

	if len(parts) == 2 && parts[1] == "download" {
		s.handleDownload(w, r, archiveID)
		return
	}

	switch r.Method {
	case http.MethodGet:
		manifest, err := s.engine.GetManifest(r.Context(), archiveID)
		if err != nil {
			http.Error(w, fmt.Sprintf("archive not found: %v", err), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(manifest)

	case http.MethodDelete:
		if err := s.engine.DeleteArchive(r.Context(), archiveID); err != nil {
			http.Error(w, fmt.Sprintf("deleting archive: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request, archiveID string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rec, err := s.engine.GetArchive(r.Context(), archiveID)
	if err != nil {
		http.Error(w, "archive not found", http.StatusNotFound)
		return
	}
	if rec.Status != "active" {
		http.Error(w, "archive has been deleted", http.StatusGone)
		return
	}

	seeker, err := s.engine.NewTarReadSeeker(r.Context(), archiveID)
	if err != nil {
		http.Error(w, fmt.Sprintf("initializing archive reader: %v", err), http.StatusInternalServerError)
		return
	}
	defer seeker.Close()

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, rec.Name))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, rec.UncompressedSHA256))

	// http.ServeContent handles:
	// - Full download
	// - Range headers (bytes=start-end, bytes=start-)
	// - 206 Partial Content
	// - Resumable downloads (curl -C -)
	http.ServeContent(w, r, rec.Name, rec.CreatedAt, seeker)
}

func (s *Server) handleGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	graceStr := r.URL.Query().Get("grace")
	grace := 6 * time.Hour
	if graceStr != "" {
		parsed, err := time.ParseDuration(graceStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid grace duration: %v", err), http.StatusBadRequest)
			return
		}
		grace = parsed
	}

	stats, err := s.db.RunTwoPhaseGC(r.Context(), s.blobStore, grace)
	if err != nil {
		http.Error(w, fmt.Sprintf("running GC: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
