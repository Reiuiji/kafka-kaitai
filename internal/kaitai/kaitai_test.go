package kaitai

import (
	"encoding/hex"
	"os"
	"testing"
)

func TestKaitaiBuildAndParse(t *testing.T) {
	builder, err := NewBuilder("")
	if err != nil {
		t.Fatalf("failed to create builder: %v", err)
	}

	ksyContent := `meta:
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

	// 1. Test compilation and Go building into standalone binary
	binaryPath, structName, err := builder.Build("ipv4_test", ksyContent)
	if err != nil {
		t.Fatalf("builder.Build failed: %v", err)
	}

	if structName != "Ipv4Packet" {
		t.Errorf("expected struct name 'Ipv4Packet', got '%s'", structName)
	}

	fi, err := os.Stat(binaryPath)
	if err != nil || fi.IsDir() {
		t.Fatalf("expected binary at %s, stat error: %v", binaryPath, err)
	}
	t.Logf("Generated parser binary at: %s (size: %d bytes)", binaryPath, fi.Size())

	// 2. Test runner subprocess with actual IPv4 header bytes
	// 45 00 00 3c (len 60) 1c 46 40 00 40 06 (protocol 6=TCP) b1 e6 (checksum) c0 a8 00 01 (192.168.0.1) c0 a8 00 02 (192.168.0.2)
	hexData := "4500003c1c4640004006b1e6c0a80001c0a80002"
	rawBytes, err := hex.DecodeString(hexData)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	runner := NewRunner()
	result, elapsedMs, err := runner.Run(binaryPath, rawBytes)
	if err != nil {
		t.Fatalf("runner.Run failed: %v", err)
	}

	t.Logf("Parsed in %.2f ms! Result: %+v", elapsedMs, result)

	parsedMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any result, got %T: %+v", result, result)
	}

	// Verify fields
	if val, ok := parsedMap["B1"].(float64); !ok || int(val) != 69 {
		t.Errorf("expected B1=69 (0x45), got %v", parsedMap["B1"])
	}
	if val, ok := parsedMap["TotalLength"].(float64); !ok || int(val) != 60 {
		t.Errorf("expected TotalLength=60, got %v", parsedMap["TotalLength"])
	}
	if val, ok := parsedMap["Protocol"].(float64); !ok || int(val) != 6 {
		t.Errorf("expected Protocol=6 (TCP), got %v", parsedMap["Protocol"])
	}

	t.Log("All Kaitai fields decoded successfully!")
}
