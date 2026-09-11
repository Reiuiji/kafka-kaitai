package models

import "time"

type KafkaRecord struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Offset      int64             `json:"offset"`
	Key         string            `json:"key"`
	KeyBase64   string            `json:"keyBase64"`
	Value       string            `json:"value"`
	ValueBase64 string            `json:"valueBase64"`
	ValueHex    string            `json:"valueHex"`
	Headers     map[string]string `json:"headers"`
	Timestamp   time.Time         `json:"timestamp"`
	Format      string            `json:"format"`     // "json", "binary", "text", "avro", "protobuf"
	ParsedJSON  string            `json:"parsedJson"` // Populated if parsed via Kaitai or Schema Registry
}

type ProduceRequest struct {
	Topic          string            `json:"topic"`
	Key            string            `json:"key"`
	KeyFormat      string            `json:"keyFormat"` // "string", "hex", "base64"
	Value          string            `json:"value"`
	ValueFormat    string            `json:"valueFormat"` // "string", "json", "base64", "hex", "binary"
	Headers        map[string]string `json:"headers"`
	Partition      int32             `json:"partition"` // -1 for auto-hash/round-robin
	KaitaiFormatID string            `json:"kaitaiFormatId,omitempty"`
}

type ProduceResponse struct {
	Success   bool      `json:"success"`
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error,omitempty"`
}

type TailRequest struct {
	Topic          string `json:"topic"`
	Partition      int32  `json:"partition"` // -1 for all partitions
	Count          int    `json:"count"`     // e.g. last 10, 50, 100 messages
	KaitaiFormatID string `json:"kaitaiFormatId,omitempty"`
}
