package models

import "time"

type KaitaiFormat struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	Description string    `json:"description"`
	Extension   string    `json:"extension"`
	KSYSrc      string    `json:"ksySrc"`
	Compiled    bool      `json:"compiled"`
	BinaryPath  string    `json:"binaryPath"`
	LastBuilt   time.Time `json:"lastBuilt"`
	StructName  string    `json:"structName"`
	Checksum    string    `json:"checksum"`
	BuildError  string    `json:"buildError,omitempty"`
}

type ParseRequest struct {
	FormatID   string `json:"formatId"`
	DataBase64 string `json:"dataBase64"`
	DataHex    string `json:"dataHex"`
}

type ParseResult struct {
	Success   bool    `json:"success"`
	Error     string  `json:"error,omitempty"`
	Result    any     `json:"result,omitempty"`
	ElapsedMs float64 `json:"elapsedMs"`
}
