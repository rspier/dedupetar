package server

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"tarchive/pkg/cas"
	"tarchive/pkg/engine"
	"tarchive/pkg/metadata"
	"tarchive/pkg/models"
)

func createServerTestTar() []byte {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	hdr := &tar.Header{
		Name: "test.txt",
		Mode: 0644,
		Size: 12,
	}
	tw.WriteHeader(hdr)
	tw.Write([]byte("Hello Server"))
	tw.Close()
	return buf.Bytes()
}

func TestHTTPServer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := metadata.Open(filepath.Join(tmpDir, "meta.db"))
	if err != nil {
		t.Fatalf("Open DB failed: %v", err)
	}
	defer db.Close()

	blobStore, err := cas.NewLocalBlobStore(filepath.Join(tmpDir, "cas"))
	if err != nil {
		t.Fatalf("NewLocalBlobStore failed: %v", err)
	}

	pm := cas.NewPackfileManager(blobStore, cas.Config{BlobThreshold: 1024})
	eng := engine.New(db, blobStore, pm, 1024)
	srv := NewServer(eng, db, blobStore)

	testTar := createServerTestTar()

	// 1. POST /archives (Upload)
	req := httptest.NewRequest(http.MethodPost, "/archives?name=my_archive.tar", bytes.NewReader(testTar))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("POST /archives status = %d, body: %s", w.Code, w.Body.String())
	}

	var ingestRes engine.IngestResult
	if err := json.Unmarshal(w.Body.Bytes(), &ingestRes); err != nil {
		t.Fatalf("Unmarshal ingest response failed: %v", err)
	}
	if ingestRes.ArchiveID == "" {
		t.Fatalf("ArchiveID is empty")
	}

	archiveID := ingestRes.ArchiveID

	// 2. GET /archives (List)
	reqList := httptest.NewRequest(http.MethodGet, "/archives", nil)
	wList := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wList, reqList)

	if wList.Code != http.StatusOK {
		t.Fatalf("GET /archives status = %d", wList.Code)
	}
	var archives []models.ArchiveRecord
	if err := json.Unmarshal(wList.Body.Bytes(), &archives); err != nil {
		t.Fatalf("Unmarshal list failed: %v", err)
	}
	if len(archives) != 1 || archives[0].ArchiveID != archiveID {
		t.Errorf("Unexpected archives list: %+v", archives)
	}

	// 3. GET /archives/{id} (Manifest)
	reqGet := httptest.NewRequest(http.MethodGet, "/archives/"+archiveID, nil)
	wGet := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wGet, reqGet)

	if wGet.Code != http.StatusOK {
		t.Fatalf("GET /archives/%s status = %d", archiveID, wGet.Code)
	}

	// 3b. GET /status (System Overview)
	reqStatus := httptest.NewRequest(http.MethodGet, "/status", nil)
	wStatus := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wStatus, reqStatus)
	if wStatus.Code != http.StatusOK {
		t.Fatalf("GET /status status = %d", wStatus.Code)
	}
	var sysStatus metadata.SystemStatus
	if err := json.Unmarshal(wStatus.Body.Bytes(), &sysStatus); err != nil {
		t.Fatalf("Unmarshal status failed: %v", err)
	}
	if sysStatus.TotalArchives != 1 || sysStatus.ActiveArchives != 1 {
		t.Errorf("Unexpected system status: %+v", sysStatus)
	}

	// 4. GET /archives/{id}/download (Full Download)
	reqDown := httptest.NewRequest(http.MethodGet, "/archives/"+archiveID+"/download", nil)
	wDown := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wDown, reqDown)

	if wDown.Code != http.StatusOK {
		t.Fatalf("GET download status = %d", wDown.Code)
	}
	if !bytes.Equal(wDown.Body.Bytes(), testTar) {
		t.Fatalf("Full download byte fidelity failed!")
	}

	// 5. GET /archives/{id}/download with Range header (HTTP 206 Partial Content)
	reqRange := httptest.NewRequest(http.MethodGet, "/archives/"+archiveID+"/download", nil)
	reqRange.Header.Set("Range", "bytes=0-99")
	wRange := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wRange, reqRange)

	if wRange.Code != http.StatusPartialContent {
		t.Fatalf("Expected 206 Partial Content, got %d", wRange.Code)
	}
	if wRange.Body.Len() != 100 {
		t.Fatalf("Expected 100 bytes returned for Range 0-99, got %d", wRange.Body.Len())
	}
	if !bytes.Equal(wRange.Body.Bytes(), testTar[0:100]) {
		t.Fatalf("Range bytes mismatch against original tar slice!")
	}

	// 6. DELETE /archives/{id}
	reqDel := httptest.NewRequest(http.MethodDelete, "/archives/"+archiveID, nil)
	wDel := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wDel, reqDel)

	if wDel.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", wDel.Code)
	}

	// 7. POST /gc?grace=0s
	reqGC := httptest.NewRequest(http.MethodPost, "/gc?grace=0s", nil)
	wGC := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wGC, reqGC)

	if wGC.Code != http.StatusOK {
		t.Fatalf("POST /gc status = %d, body: %s", wGC.Code, wGC.Body.String())
	}
	var gcStats metadata.GCStats
	if err := json.Unmarshal(wGC.Body.Bytes(), &gcStats); err != nil {
		t.Fatalf("Unmarshal GCStats failed: %v", err)
	}
	if gcStats.ObjectsDeleted != 1 {
		t.Errorf("Expected 1 object deleted in GC, got %d", gcStats.ObjectsDeleted)
	}
}
