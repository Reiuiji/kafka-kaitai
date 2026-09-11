package services

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	internalKaitai "github.com/okuu/kafka_kaitai/internal/kaitai"
	"github.com/okuu/kafka_kaitai/models"
)

type KaitaiService struct {
	mu      sync.RWMutex
	builder *internalKaitai.Builder
	runner  *internalKaitai.Runner
	formats map[string]models.KaitaiFormat
	dataDir string
}

func NewKaitaiService() (*KaitaiService, error) {
	home, err := os.UserHomeDir()
	var baseDir string
	if err != nil {
		baseDir = filepath.Join(os.TempDir(), "kafka_kaitai")
	} else {
		baseDir = filepath.Join(home, ".kafka_kaitai")
	}

	parsersDir := filepath.Join(baseDir, "parsers")
	builder, err := internalKaitai.NewBuilder(parsersDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init builder: %w", err)
	}

	ks := &KaitaiService{
		builder: builder,
		runner:  internalKaitai.NewRunner(),
		formats: make(map[string]models.KaitaiFormat),
		dataDir: baseDir,
	}

	// Register built-in sample format: IPv4 packet header
	ks.registerBuiltins()

	return ks, nil
}

func (ks *KaitaiService) registerBuiltins() {
	sampleKSY := `meta:
  id: ipv4_packet
  title: IPv4 network packet
  endian: be
seq:
  - id: b1
    type: u1
  - id: b2
    type: u1
  - id: total_length
    type: u2
  - id: identification
    type: u2
  - id: b6_and_7
    type: u2
  - id: ttl
    type: u1
  - id: protocol
    type: u1
  - id: header_checksum
    type: u2
  - id: src_ip_addr
    size: 4
  - id: dst_ip_addr
    size: 4
`
	formatID := "builtin-ipv4"
	ks.formats[formatID] = models.KaitaiFormat{
		ID:          formatID,
		Name:        "IPv4 Packet Header",
		Version:     "1.0",
		Description: "Built-in sample format for IPv4 network packet headers",
		Extension:   "bin",
		KSYSrc:      sampleKSY,
		Compiled:    false,
		LastBuilt:   time.Now(),
		StructName:  "Ipv4Packet",
		Checksum:    ks.builder.ComputeChecksum(sampleKSY),
	}
}

// ImportFormat creates a new format from KSY source
func (ks *KaitaiService) ImportFormat(name string, ksyContent string) (*models.KaitaiFormat, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	id := uuid.New().String()[:8]
	structName := ks.builder.ExtractStructName(ksyContent)
	checksum := ks.builder.ComputeChecksum(ksyContent)

	fmtObj := models.KaitaiFormat{
		ID:          id,
		Name:        name,
		Version:     "1.0",
		Description: fmt.Sprintf("Imported format: %s", structName),
		Extension:   "bin",
		KSYSrc:      ksyContent,
		Compiled:    false,
		StructName:  structName,
		Checksum:    checksum,
		LastBuilt:   time.Now(),
	}

	ks.formats[id] = fmtObj
	return &fmtObj, nil
}

// ListFormats returns all registered Kaitai formats
func (ks *KaitaiService) ListFormats() []models.KaitaiFormat {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	var result []models.KaitaiFormat
	for _, f := range ks.formats {
		result = append(result, f)
	}
	return result
}

// GetFormat retrieves a single format
func (ks *KaitaiService) GetFormat(id string) (*models.KaitaiFormat, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	f, exists := ks.formats[id]
	if !exists {
		return nil, fmt.Errorf("format %s not found", id)
	}
	return &f, nil
}

// DeleteFormat removes a format
func (ks *KaitaiService) DeleteFormat(id string) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if _, exists := ks.formats[id]; !exists {
		return fmt.Errorf("format %s not found", id)
	}

	delete(ks.formats, id)
	return nil
}

// BuildFormat compiles the format into a standalone binary
func (ks *KaitaiService) BuildFormat(id string) (*models.KaitaiFormat, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	fmtObj, exists := ks.formats[id]
	if !exists {
		return nil, fmt.Errorf("format %s not found", id)
	}

	binPath, structName, err := ks.builder.Build(id, fmtObj.KSYSrc)
	if err != nil {
		fmtObj.BuildError = err.Error()
		ks.formats[id] = fmtObj
		return &fmtObj, err
	}

	fmtObj.Compiled = true
	fmtObj.BinaryPath = binPath
	fmtObj.StructName = structName
	fmtObj.BuildError = ""
	fmtObj.LastBuilt = time.Now()
	ks.formats[id] = fmtObj

	return &fmtObj, nil
}

// ParseBinary decodes binary data using a compiled Kaitai format
func (ks *KaitaiService) ParseBinary(req models.ParseRequest) (*models.ParseResult, error) {
	ks.mu.RLock()
	fmtObj, exists := ks.formats[req.FormatID]
	ks.mu.RUnlock()

	if !exists {
		return &models.ParseResult{
			Success: false,
			Error:   fmt.Sprintf("format %s not found", req.FormatID),
		}, errors.New("format not found")
	}

	// Auto-build if not yet compiled
	if !fmtObj.Compiled || fmtObj.BinaryPath == "" {
		built, err := ks.BuildFormat(req.FormatID)
		if err != nil {
			return &models.ParseResult{
				Success: false,
				Error:   fmt.Sprintf("auto-build failed: %v", err),
			}, err
		}
		fmtObj = *built
	}

	var data []byte
	var err error

	if req.DataBase64 != "" {
		data, err = base64.StdEncoding.DecodeString(req.DataBase64)
		if err != nil {
			return &models.ParseResult{
				Success: false,
				Error:   fmt.Sprintf("invalid base64 payload: %v", err),
			}, err
		}
	} else if req.DataHex != "" {
		data, err = hex.DecodeString(req.DataHex)
		if err != nil {
			return &models.ParseResult{
				Success: false,
				Error:   fmt.Sprintf("invalid hex payload: %v", err),
			}, err
		}
	} else {
		return &models.ParseResult{
			Success: false,
			Error:   "no data provided (base64 or hex required)",
		}, errors.New("empty data")
	}

	res, elapsed, err := ks.runner.Run(fmtObj.BinaryPath, data)
	if err != nil {
		return &models.ParseResult{
			Success:   false,
			Error:     err.Error(),
			ElapsedMs: elapsed,
		}, err
	}

	return &models.ParseResult{
		Success:   true,
		Result:    res,
		ElapsedMs: elapsed,
	}, nil
}
