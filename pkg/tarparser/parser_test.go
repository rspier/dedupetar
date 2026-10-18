package tarparser

import (
	"archive/tar"
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestStandardTarParsing(t *testing.T) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	files := []struct {
		Name string
		Body string
	}{
		{"hello.txt", "Hello, DedupeTar!"},
		{"dir/sub.txt", "Some content in a subdirectory."},
	}

	for _, f := range files {
		hdr := &tar.Header{
			Name: f.Name,
			Mode: 0644,
			Size: int64(len(f.Body)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if _, err := tw.Write([]byte(f.Body)); err != nil {
			t.Fatalf("Write body failed: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	rawTarBytes := buf.Bytes()
	parser := NewParser(bytes.NewReader(rawTarBytes))

	for idx, expected := range files {
		hdr, r, err := parser.Next()
		if err != nil {
			t.Fatalf("Next[%d] failed: %v", idx, err)
		}
		if hdr.Name != expected.Name {
			t.Errorf("Next[%d] name = %q, want %q", idx, hdr.Name, expected.Name)
		}
		if hdr.PayloadSize != int64(len(expected.Body)) {
			t.Errorf("Next[%d] payload size = %d, want %d", idx, hdr.PayloadSize, len(expected.Body))
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll body[%d] failed: %v", idx, err)
		}
		if string(body) != expected.Body {
			t.Errorf("Next[%d] body = %q, want %q", idx, string(body), expected.Body)
		}
	}

	// Next should return io.EOF
	_, _, err := parser.Next()
	if err != io.EOF {
		t.Fatalf("Expected io.EOF, got: %v", err)
	}

	trailer := parser.TrailingPaddingData()
	if len(trailer) < 1024 {
		t.Errorf("Expected at least 1024 trailer bytes, got %d", len(trailer))
	}
}

func TestPAXAndGNULongNameParsing(t *testing.T) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	longName := strings.Repeat("very_long_path_component/", 15) + "file.txt"
	body := "Long file content"
	hdr := &tar.Header{
		Name:     longName,
		Mode:     0644,
		Size:     int64(len(body)),
		PAXRecords: map[string]string{
			"comment": "DedupeTar PAX test record",
		},
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader PAX failed: %v", err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatalf("Write body failed: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	parser := NewParser(bytes.NewReader(buf.Bytes()))
	entryHdr, r, err := parser.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	// Raw header blocks should contain auxiliary PAX header blocks + file header block (multiple 512-byte blocks)
	if len(entryHdr.RawHeaderBlocks) <= 512 {
		t.Errorf("Expected raw header blocks > 512 due to PAX auxiliary blocks, got %d", len(entryHdr.RawHeaderBlocks))
	}
	readBody, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(readBody) != body {
		t.Errorf("Body = %q, want %q", string(readBody), body)
	}
}

func TestHardLinkPayloadSizeZero(t *testing.T) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	hdr := &tar.Header{
		Typeflag: tar.TypeLink,
		Name:     "hardlink_target",
		Linkname: "original_file",
		Size:     1024, // Non-compliant archivers might write 1024 here
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	parser := NewParser(bytes.NewReader(buf.Bytes()))
	entryHdr, _, err := parser.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if entryHdr.PayloadSize != 0 {
		t.Errorf("Hardlink payload size should be 0, got %d", entryHdr.PayloadSize)
	}
}

func TestBase256SizeParsing(t *testing.T) {
	// Base-256 for 10 GiB = 10 * 1024 * 1024 * 1024 = 10737418240
	val := int64(10737418240)
	field := make([]byte, 12)
	field[0] = 0x80 // bit 0x80 signals base-256
	field[7] = byte(val >> 32)
	field[8] = byte(val >> 24)
	field[9] = byte(val >> 16)
	field[10] = byte(val >> 8)
	field[11] = byte(val)

	parsed := parseNumericField(field)
	if parsed != val {
		t.Errorf("parseNumericField base-256 = %d, want %d", parsed, val)
	}
}

func TestConcatenatedArchives(t *testing.T) {
	// Archive 1
	buf1 := new(bytes.Buffer)
	tw1 := tar.NewWriter(buf1)
	tw1.WriteHeader(&tar.Header{Name: "a.txt", Size: 4, Mode: 0644})
	tw1.Write([]byte("aaaa"))
	tw1.Close()

	// Archive 2
	buf2 := new(bytes.Buffer)
	tw2 := tar.NewWriter(buf2)
	tw2.WriteHeader(&tar.Header{Name: "b.txt", Size: 4, Mode: 0644})
	tw2.Write([]byte("bbbb"))
	tw2.Close()

	// Concatenated archive: a.tar + b.tar
	combined := append(buf1.Bytes(), buf2.Bytes()...)

	parser := NewParser(bytes.NewReader(combined))
	h1, r1, err := parser.Next()
	if err != nil {
		t.Fatalf("Next 1 failed: %v", err)
	}
	if h1.Name != "a.txt" {
		t.Errorf("h1.Name = %q, want a.txt", h1.Name)
	}
	b1, _ := io.ReadAll(r1)
	if string(b1) != "aaaa" {
		t.Errorf("b1 = %q, want aaaa", string(b1))
	}

	h2, r2, err := parser.Next()
	if err != nil {
		t.Fatalf("Next 2 failed: %v", err)
	}
	if h2.Name != "b.txt" {
		t.Errorf("h2.Name = %q, want b.txt", h2.Name)
	}
	b2, _ := io.ReadAll(r2)
	if string(b2) != "bbbb" {
		t.Errorf("b2 = %q, want bbbb", string(b2))
	}

	// b.txt's raw header blocks should include the 1024 zero bytes trailer of a.tar
	if len(h2.RawHeaderBlocks) < 1024+512 {
		t.Errorf("Expected h2 raw header blocks to contain concatenated separator null blocks, got len %d", len(h2.RawHeaderBlocks))
	}

	_, _, err = parser.Next()
	if err != io.EOF {
		t.Fatalf("Expected io.EOF, got %v", err)
	}
}
