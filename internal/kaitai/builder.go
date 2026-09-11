package kaitai

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"
)

type Builder struct {
	cacheDir string
	compiler *Compiler
}

func NewBuilder(cacheDir string) (*Builder, error) {
	if cacheDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			cacheDir = filepath.Join(os.TempDir(), "kafka_kaitai_cache")
		} else {
			cacheDir = filepath.Join(home, ".kafka_kaitai", "parsers")
		}
	}

	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %w", err)
	}

	return &Builder{
		cacheDir: cacheDir,
		compiler: NewCompiler(),
	}, nil
}

func (b *Builder) CacheDir() string {
	return b.cacheDir
}

// ComputeChecksum generates a SHA256 of the KSY source
func (b *Builder) ComputeChecksum(ksyContent string) string {
	h := sha256.Sum256([]byte(ksyContent))
	return hex.EncodeToString(h[:])
}

// ExtractStructName extracts or guesses the root struct name from KSY content
func (b *Builder) ExtractStructName(ksyContent string) string {
	re := regexp.MustCompile(`(?m)^\s*id:\s*([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(ksyContent)
	if len(matches) > 1 {
		raw := matches[1]
		parts := strings.Split(raw, "_")
		var capitalized []string
		for _, p := range parts {
			if len(p) > 0 {
				capitalized = append(capitalized, strings.ToUpper(p[:1])+p[1:])
			}
		}
		return strings.Join(capitalized, "")
	}
	return "Parser"
}

// Build compiles KSY to Go, generates main shim, and builds binary
func (b *Builder) Build(formatID string, ksyContent string) (string, string, error) {
	checksum := b.ComputeChecksum(ksyContent)
	binaryName := fmt.Sprintf("parser_%s_%s", formatID, checksum[:12])
	targetBinary := filepath.Join(b.cacheDir, binaryName)

	// Check if already cached and exists
	if fi, err := os.Stat(targetBinary); err == nil && !fi.IsDir() {
		structName := b.ExtractStructName(ksyContent)
		return targetBinary, structName, nil
	}

	// Create a temporary build workspace
	buildDir, err := os.MkdirTemp("", "ksc_build_*")
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp build dir: %w", err)
	}
	defer os.RemoveAll(buildDir)

	// Write .ksy file
	ksyFile := filepath.Join(buildDir, "format.ksy")
	if err := os.WriteFile(ksyFile, []byte(ksyContent), 0644); err != nil {
		return "", "", fmt.Errorf("failed to write ksy file: %w", err)
	}

	// Compile to Go
	if err := b.compiler.Compile(ksyFile, buildDir); err != nil {
		return "", "", err
	}

	// Fix package declaration in generated Go files (ensure 'package main' is present)
	goFiles, err := filepath.Glob(filepath.Join(buildDir, "*.go"))
	if err != nil {
		return "", "", fmt.Errorf("failed to list generated Go files: %w", err)
	}

	for _, f := range goFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		content := string(data)
		if !strings.Contains(content, "package main") {
			newContent := "package main\n\n" + content
			_ = os.WriteFile(f, []byte(newContent), 0644)
		}
	}

	// Detect struct name from generated Go file
	structName := b.detectStructNameFromDir(buildDir)
	if structName == "" {
		structName = b.ExtractStructName(ksyContent)
	}

	// Generate main.go
	tmplContent := `package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/kaitai-io/kaitai_struct_go_runtime/kaitai"
)

func main() {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("failed to read stdin: %v", err)
	}

	stream := kaitai.NewStream(bytes.NewReader(raw))
	parser := New{{.StructName}}()
	if err := parser.Read(stream, nil, parser); err != nil {
		log.Fatalf("kaitai parse error: %v", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(parser); err != nil {
		log.Fatalf("json encode error: %v", err)
	}
}
`
	tmpl, err := template.New("main").Parse(tmplContent)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse template: %w", err)
	}

	var mainBuf bytes.Buffer
	if err := tmpl.Execute(&mainBuf, map[string]string{"StructName": structName}); err != nil {
		return "", "", fmt.Errorf("failed to execute template: %w", err)
	}

	mainFile := filepath.Join(buildDir, "main.go")
	if err := os.WriteFile(mainFile, mainBuf.Bytes(), 0644); err != nil {
		return "", "", fmt.Errorf("failed to write main.go: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Initialize go module and fetch dependencies
	initCmd := exec.CommandContext(ctx, "go", "mod", "init", "ksc_parser")
	initCmd.Dir = buildDir
	_ = initCmd.Run()

	tidyCmd := exec.CommandContext(ctx, "go", "mod", "tidy")
	tidyCmd.Dir = buildDir
	var tidyStderr bytes.Buffer
	tidyCmd.Stderr = &tidyStderr
	if err := tidyCmd.Run(); err != nil {
		return "", "", fmt.Errorf("go mod tidy failed: %w (stderr: %s)", err, tidyStderr.String())
	}

	// Build standalone binary
	cmd := exec.CommandContext(ctx, "go", "build", "-o", targetBinary, ".")
	cmd.Dir = buildDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", "", fmt.Errorf("go build failed: %w (stderr: %s)", err, stderr.String())
	}

	return targetBinary, structName, nil
}

func (b *Builder) detectStructNameFromDir(dir string) string {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return ""
	}

	re := regexp.MustCompile(`func New([a-zA-Z0-9_]+)\(\)`)
	for _, f := range files {
		if filepath.Base(f) == "main.go" {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		matches := re.FindStringSubmatch(string(data))
		if len(matches) > 1 {
			return matches[1]
		}
	}
	return ""
}
