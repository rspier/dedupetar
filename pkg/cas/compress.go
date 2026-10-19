package cas

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"

	"tarchive/pkg/models"

	"github.com/klauspost/compress/zstd"
)

var (
	encoderPool = sync.Pool{
		New: func() interface{} {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				panic(err)
			}
			return enc
		},
	}

	decoderPool = sync.Pool{
		New: func() interface{} {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				panic(err)
			}
			return dec
		},
	}
)

// CompressAdaptive attempts to compress data with Zstandard using pooled encoders.
// If compression saves at least 5% of space, the compressed bytes and CodecZSTD are returned.
// Otherwise, the original uncompressed bytes and CodecNone are returned.
func CompressAdaptive(raw []byte) ([]byte, models.CompressionCodec, error) {
	// Tiny payloads (< 32 bytes) usually expand under compression
	if len(raw) < 32 {
		return raw, models.CodecNone, nil
	}

	enc := encoderPool.Get().(*zstd.Encoder)
	defer encoderPool.Put(enc)

	compressed := enc.EncodeAll(raw, make([]byte, 0, len(raw)))
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
		dec := decoderPool.Get().(*zstd.Decoder)
		defer decoderPool.Put(dec)

		decompressed, err := dec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompression failed: %w", err)
		}
		return decompressed, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}
}

// NewStreamingCompressor creates a streaming multi-threaded Zstandard encoder utilizing all CPU cores.
func NewStreamingCompressor(w io.Writer) (*zstd.Encoder, error) {
	concurrency := runtime.GOMAXPROCS(0)
	if concurrency < 1 {
		concurrency = 1
	}
	return zstd.NewWriter(w,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(concurrency),
	)
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
