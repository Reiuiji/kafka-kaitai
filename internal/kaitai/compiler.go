package kaitai

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type Compiler struct {
	kscPath string
}

func NewCompiler() *Compiler {
	path, err := exec.LookPath("kaitai-struct-compiler")
	if err != nil {
		path, err = exec.LookPath("ksc")
		if err != nil {
			if _, statErr := os.Stat("/usr/local/bin/kaitai-struct-compiler"); statErr == nil {
				path = "/usr/local/bin/kaitai-struct-compiler"
			}
		}
	}
	return &Compiler{kscPath: path}
}

func (c *Compiler) IsAvailable() bool {
	return c.kscPath != ""
}

// Compile compiles a .ksy file into Go source code within outputDir
func (c *Compiler) Compile(ksyPath string, outputDir string) error {
	if !c.IsAvailable() {
		return fmt.Errorf("kaitai-struct-compiler not found in PATH or /usr/local/bin")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.kscPath, "-t", "go", "--outdir", outputDir, ksyPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kaitai compilation failed: %w (stderr: %s)", err, stderr.String())
	}

	// Verify that at least one .go file was created
	files, err := filepath.Glob(filepath.Join(outputDir, "*.go"))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no Go source generated in %s", outputDir)
	}

	return nil
}
