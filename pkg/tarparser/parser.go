package tarparser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	BlockSize = 512

	// Tar type flags
	TypeReg       = '0'
	TypeRegA      = '\000'
	TypeLink      = '1' // Hard link
	TypeSymlink   = '2' // Symbolic link
	TypeChar      = '3' // Character device
	TypeBlock     = '4' // Block device
	TypeDir       = '5' // Directory
	TypeFifo      = '6' // FIFO
	TypeCont      = '7' // Contiguous file
	TypeXHeader   = 'x' // Extended header (PAX)
	TypeXGlobal   = 'g' // Global extended header (PAX)
	TypeGNULongName = 'L' // GNU tar long pathname
	TypeGNULongLink = 'K' // GNU tar long linkname
	TypeGNUSparse = 'S' // GNU tar sparse file
)

// HeaderInfo contains information about a parsed tar header.
type HeaderInfo struct {
	Name            string
	TypeFlag        byte
	PayloadSize     int64
	PayloadPadding  uint32
	RawHeaderBlocks []byte
}

// Parser is a streaming parser for uncompressed tar archives.
type Parser struct {
	r              io.Reader
	pendingNull    []byte // buffered null blocks (or gap bytes)
	rawAuxBlocks   []byte // accumulated auxiliary blocks (PAX, GNU Long records)
	currentPayload *limitedPayloadReader
	eofReached     bool
	totalBytesRead int64
}

// NewParser creates a new tar streaming parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		r: r,
	}
}

// Next reads the next entry in the tar archive.
// It returns (HeaderInfo, io.Reader for payload, error).
// When the archive ends, it returns io.EOF.
func (p *Parser) Next() (*HeaderInfo, io.Reader, error) {
	// If previous entry had unconsumed payload bytes, finish them
	if p.currentPayload != nil {
		if err := p.currentPayload.discardRemaining(); err != nil {
			return nil, nil, err
		}
		p.currentPayload = nil
	}

	block := make([]byte, BlockSize)

	for {
		// Read 512-byte block
		n, err := io.ReadFull(p.r, block)
		if n > 0 {
			p.totalBytesRead += int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// We reached EOF. If we read a partial block, buffer it in pendingNull.
				if n > 0 {
					p.pendingNull = append(p.pendingNull, block[:n]...)
				}
				p.eofReached = true
				return nil, nil, io.EOF
			}
			return nil, nil, err
		}

		// Check if block is all zeros
		if isAllZeros(block) {
			p.pendingNull = append(p.pendingNull, block...)
			continue
		}

		// We have a non-zero block. Check if it is a valid tar header.
		if !isValidChecksum(block) {
			// Not a valid checksum; could be garbage or corrupt header
			return nil, nil, fmt.Errorf("invalid tar header checksum at offset %d", p.totalBytesRead-BlockSize)
		}

		// Valid header found. Any pendingNull bytes prior to this block are part of
		// raw stream padding / concatenation. Prepend them to auxiliary header blocks.
		if len(p.pendingNull) > 0 {
			p.rawAuxBlocks = append(p.rawAuxBlocks, p.pendingNull...)
			p.pendingNull = nil
		}

		typeFlag := block[156]
		size := parseNumericField(block[124:136])

		// Handle Auxiliary headers: PAX ('x', 'g') and GNU Long records ('L', 'K')
		if isAuxiliaryType(typeFlag) {
			p.rawAuxBlocks = append(p.rawAuxBlocks, block...)

			// Auxiliary data blocks: ceil(size / 512) * 512
			dataBlocksCount := (size + BlockSize - 1) / BlockSize
			if dataBlocksCount > 0 {
				dataBytes := make([]byte, dataBlocksCount*BlockSize)
				if _, err := io.ReadFull(p.r, dataBytes); err != nil {
					return nil, nil, fmt.Errorf("reading auxiliary data blocks: %w", err)
				}
				p.totalBytesRead += int64(len(dataBytes))
				p.rawAuxBlocks = append(p.rawAuxBlocks, dataBytes...)
			}
			continue
		}

		// Handle GNU Sparse files: typeflag 'S'
		if typeFlag == TypeGNUSparse {
			// Check if extended sparse header blocks exist: block[482] == 1
			// In GNU tar sparse format:
			// Header contains up to 4 sparse entries, and byte 482 is `isextended`.
			isExtended := block[482]
			sparseHeaderBlocks := make([]byte, 0)

			for isExtended != 0 {
				extBlock := make([]byte, BlockSize)
				if _, err := io.ReadFull(p.r, extBlock); err != nil {
					return nil, nil, fmt.Errorf("reading GNU sparse extended header block: %w", err)
				}
				p.totalBytesRead += BlockSize
				sparseHeaderBlocks = append(sparseHeaderBlocks, extBlock...)
				// Offset 504 in extended header contains next isextended flag
				isExtended = extBlock[504]
			}

			// Assemble raw header blocks: auxiliary + file header + sparse header blocks
			rawBlocks := make([]byte, 0, len(p.rawAuxBlocks)+BlockSize+len(sparseHeaderBlocks))
			rawBlocks = append(rawBlocks, p.rawAuxBlocks...)
			rawBlocks = append(rawBlocks, block...)
			rawBlocks = append(rawBlocks, sparseHeaderBlocks...)
			p.rawAuxBlocks = nil

			payloadSize := size
			pad := uint32(0)
			if payloadSize > 0 {
				pad = uint32((BlockSize - (payloadSize % BlockSize)) % BlockSize)
			}

			p.currentPayload = &limitedPayloadReader{
				r:       p.r,
				remain:  payloadSize,
				pad:     pad,
				pParser: p,
			}

			name := parseStringField(block[0:100])
			info := &HeaderInfo{
				Name:            name,
				TypeFlag:        typeFlag,
				PayloadSize:     payloadSize,
				PayloadPadding:  pad,
				RawHeaderBlocks: rawBlocks,
			}
			return info, p.currentPayload, nil
		}

		// Normal file entry header (regular file, dir, symlink, hardlink, etc.)
		rawBlocks := make([]byte, 0, len(p.rawAuxBlocks)+BlockSize)
		rawBlocks = append(rawBlocks, p.rawAuxBlocks...)
		rawBlocks = append(rawBlocks, block...)
		p.rawAuxBlocks = nil

		var payloadSize int64
		switch typeFlag {
		case TypeLink:
			// Section 3.2.1: Hardlinks: Force payload_size = 0 regardless of header's size field
			payloadSize = 0
		case TypeSymlink, TypeChar, TypeBlock, TypeDir, TypeFifo:
			payloadSize = 0
		default:
			payloadSize = size
		}

		pad := uint32(0)
		if payloadSize > 0 {
			pad = uint32((BlockSize - (payloadSize % BlockSize)) % BlockSize)
		}

		p.currentPayload = &limitedPayloadReader{
			r:       p.r,
			remain:  payloadSize,
			pad:     pad,
			pParser: p,
		}

		name := parseStringField(block[0:100])
		info := &HeaderInfo{
			Name:            name,
			TypeFlag:        typeFlag,
			PayloadSize:     payloadSize,
			PayloadPadding:  pad,
			RawHeaderBlocks: rawBlocks,
		}

		return info, p.currentPayload, nil
	}
}

// TrailingPaddingData returns any trailing raw bytes read after the last entry (e.g. standard 1024-byte EOA or concatenated zeroes).
func (p *Parser) TrailingPaddingData() []byte {
	return p.pendingNull
}

func isAuxiliaryType(typeFlag byte) bool {
	return typeFlag == TypeXHeader ||
		typeFlag == TypeXGlobal ||
		typeFlag == TypeGNULongName ||
		typeFlag == TypeGNULongLink
}

func isAllZeros(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func parseStringField(b []byte) string {
	n := bytes.IndexByte(b, 0)
	if n < 0 {
		n = len(b)
	}
	return string(b[:n])
}

// parseNumericField decodes standard ASCII octal or GNU/Star base-256 binary.
func parseNumericField(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	// Check for binary base-256 encoding (Section 3.2.2: bit 0x80 in header[124])
	if (b[0] & 0x80) != 0 {
		// Positive base-256 encoded integer
		// 12 bytes total, bit 0x80 indicates positive base-256.
		var val int64
		// If length > 8, the upper bytes (beyond 8) must be 0 for int64
		start := len(b) - 8
		if start < 0 {
			start = 0
		}
		for i := 1; i < start; i++ {
			if b[i] != 0 {
				// Value exceeds int64 max
				return 1<<63 - 1
			}
		}
		valBuf := make([]byte, 8)
		copy(valBuf[8-(len(b)-start):], b[start:])
		// Clear sign bit if start == 0
		if start == 0 {
			valBuf[0] &= 0x7f
		}
		val = int64(binary.BigEndian.Uint64(valBuf))
		return val
	}

	// ASCII octal string
	s := strings.Trim(string(b), " \x00")
	if s == "" {
		return 0
	}
	val, err := strconv.ParseInt(s, 8, 64)
	if err != nil {
		return 0
	}
	return val
}

func isValidChecksum(block []byte) bool {
	if len(block) < BlockSize {
		return false
	}
	chksumField := block[148:156]
	s := strings.Trim(string(chksumField), " \x00")
	if s == "" {
		return false
	}
	recorded, err := strconv.ParseInt(s, 8, 64)
	if err != nil {
		return false
	}

	var unsignedSum, signedSum int64
	for i := 0; i < BlockSize; i++ {
		if i >= 148 && i < 156 {
			unsignedSum += ' '
			signedSum += ' '
		} else {
			unsignedSum += int64(block[i])
			signedSum += int64(int8(block[i]))
		}
	}

	return recorded == unsignedSum || recorded == signedSum
}

type limitedPayloadReader struct {
	r       io.Reader
	remain  int64
	pad     uint32
	pParser *Parser
}

func (lr *limitedPayloadReader) Read(p []byte) (int, error) {
	if lr.remain <= 0 {
		return 0, io.EOF
	}
	toRead := int64(len(p))
	if toRead > lr.remain {
		toRead = lr.remain
	}
	n, err := lr.r.Read(p[:toRead])
	if n > 0 {
		lr.remain -= int64(n)
		lr.pParser.totalBytesRead += int64(n)
	}
	if lr.remain == 0 {
		// When payload finishes, consume padding blocks from stream
		if padErr := lr.consumePadding(); padErr != nil && err == nil {
			err = padErr
		}
		if err == nil {
			err = io.EOF
		}
	}
	return n, err
}

func (lr *limitedPayloadReader) consumePadding() error {
	if lr.pad > 0 {
		padBuf := make([]byte, lr.pad)
		if _, err := io.ReadFull(lr.r, padBuf); err != nil {
			return fmt.Errorf("reading payload padding: %w", err)
		}
		lr.pParser.totalBytesRead += int64(lr.pad)
		lr.pad = 0
	}
	return nil
}

func (lr *limitedPayloadReader) discardRemaining() error {
	if lr.remain > 0 {
		if _, err := io.CopyN(io.Discard, lr.r, lr.remain); err != nil {
			return err
		}
		lr.pParser.totalBytesRead += lr.remain
		lr.remain = 0
	}
	return lr.consumePadding()
}
