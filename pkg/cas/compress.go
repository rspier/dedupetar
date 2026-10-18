package cas

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"tarchive/pkg/models"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
	initOnce    sync.Once
	initErr     error
)

func initZstd() error {
	initOnce.Do(func() {
		zstdEncoder, initErr = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if initErr != nil {
			return
		}
		zstdDecoder, initErr = zstd.NewReader(nil)
	})
	return initErr
}

// CompressAdaptive attempts to compress data with Zstandard.
// If compression saves at least 5% of space, the compressed bytes and CodecZSTD are returned.
// Otherwise, the original uncompressed bytes and CodecNone are returned.
func CompressAdaptive(raw []byte) ([]byte, models.CompressionCodec, error) {
	if err := initZstd(); err != nil {
		return nil, models.CodecNone, fmt.Errorf("initializing zstd: %w", err)
	}

	// Tiny payloads (< 32 bytes) usually expand under compression
	if len(raw) < 32 {
		return raw, models.CodecNone, nil
	}

	compressed := zstdEncoder.EncodeAll(raw, make([]byte, 0, len(raw)))
	// Require at least a 5% savings threshold
	if len(compressed) < int(float64(len(raw))*0.95) {
		return compressed, models.CodecZSTD, nil
	}

	return raw, models.CodecNone, nil
}

// Decompress decompresses payload bytes according to the specified codec.
func Decompress(data []byte, codec models.CompressionCodec) ([]byte, error) {
	switch codec {
	case models.CodecNone, "":
		return data, nil
	case models.CodecZSTD:
		if err := initZstd(); err != nil {
			return nil, fmt.Errorf("initializing zstd: %w", err)
		}
		decompressed, err := zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompression failed: %w", err)
		}
		return decompressed, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}
}

// DecompressReader returns an io.ReadCloser that decompresses the stream if needed.
func DecompressReader(r io.Reader, codec models.CompressionCodec) (io.ReadCloser, error) {
	switch codec {
	case models.CodecNone, "":
		if rc, ok := r.(io.ReadCloser); ok {
			return rc, nil
		}
		return io.NopCloser(r), nil
	case models.CodecZSTD:
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("initializing zstd reader: %w", err)
		}
		return zr.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}
}

// NewBufferSectionReader returns an io.ReadCloser over a slice range of an in-memory byte buffer.
func NewBufferSectionReader(buf []byte, offset int64, length int64) io.ReadCloser {
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(buf)) {
		offset = int64(len(buf))
	}
	end := offset + length
	if end > int64(len(buf)) {
		end = int64(len(buf))
	}
	return io.NopCloser(bytes.NewReader(buf[offset:end]))
}
