package kaitai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

type Runner struct{}

func NewRunner() *Runner {
	return &Runner{}
}

// Run executes the binary with binaryData on stdin, returning parsed JSON
func (r *Runner) Run(binaryPath string, binaryData []byte) (any, float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(ctx, binaryPath)
	cmd.Stdin = bytes.NewReader(binaryData)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		elapsed := float64(time.Since(start).Microseconds()) / 1000.0
		return nil, elapsed, fmt.Errorf("parser execution failed: %w (stderr: %s)", err, stderr.String())
	}

	elapsed := float64(time.Since(start).Microseconds()) / 1000.0

	var result any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		// Fallback: return as raw string if not JSON
		return stdout.String(), elapsed, nil
	}

	return result, elapsed, nil
}
