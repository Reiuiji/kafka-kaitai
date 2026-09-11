package models

type StressTestConfig struct {
	Topic            string `json:"topic"`
	Concurrency      int    `json:"concurrency"`       // goroutine worker pool count
	TargetRate       int    `json:"targetRate"`        // messages/second across all workers (0 for unbounded max speed)
	DurationSeconds  int    `json:"durationSeconds"`   // 0 for until stopped
	MessageSizeBytes int    `json:"messageSizeBytes"`  // size for random payload
	PayloadType      string `json:"payloadType"`       // "random", "counter", "custom", "kaitai_template"
	CustomPayload    string `json:"customPayload"`     // Used if payloadType == "custom"
	KaitaiFormatID   string `json:"kaitaiFormatId"`    // Used if payloadType == "kaitai_template"
	KeyPattern       string `json:"keyPattern"`        // "none", "uuid", "counter", "fixed"
	Compression      string `json:"compression"`       // "none", "gzip", "snappy", "lz4", "zstd"
}

type StressTestMetrics struct {
	Active         bool    `json:"active"`
	Topic          string  `json:"topic"`
	ElapsedSeconds float64 `json:"elapsedSeconds"`
	SentMessages   int64   `json:"sentMessages"`
	SentBytes      int64   `json:"sentBytes"`
	CurrentMsgRate float64 `json:"currentMsgRate"`
	CurrentByteRate float64 `json:"currentByteRate"`
	ErrorsCount    int64   `json:"errorsCount"`
	LastError      string  `json:"lastError,omitempty"`
	LatencyP50     float64 `json:"latencyP50"` // ms
	LatencyP95     float64 `json:"latencyP95"` // ms
	LatencyP99     float64 `json:"latencyP99"` // ms
	LatencyAvg     float64 `json:"latencyAvg"` // ms
}

type ConsumerBenchmarkConfig struct {
	Topic          string `json:"topic"`
	GroupID        string `json:"groupId"`
	Mode           string `json:"mode"` // "drop", "store", "inspect"
	StoreFilePath  string `json:"storeFilePath"`
	StoreFormat    string `json:"storeFormat"` // "raw", "jsonl", "csv_hex", "pcap_like"
	AutoOffsetReset string `json:"autoOffsetReset"` // "latest", "earliest"
	KaitaiFormatID string `json:"kaitaiFormatId,omitempty"`
}

type ConsumerBenchmarkMetrics struct {
	Active          bool    `json:"active"`
	Topic           string  `json:"topic"`
	Mode            string  `json:"mode"`
	ElapsedSeconds  float64 `json:"elapsedSeconds"`
	ConsumedMessages int64  `json:"consumedMessages"`
	ConsumedBytes   int64   `json:"consumedBytes"`
	CurrentMsgRate  float64 `json:"currentMsgRate"`
	CurrentByteRate float64 `json:"currentByteRate"`
	DroppedMessages int64   `json:"droppedMessages"`
	StoredMessages  int64   `json:"storedMessages"`
	ErrorsCount     int64   `json:"errorsCount"`
}
