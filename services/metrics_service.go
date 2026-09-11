package services

import (
	"runtime"
	"time"
)

type SystemTelemetry struct {
	Timestamp      time.Time `json:"timestamp"`
	Goroutines     int       `json:"goroutines"`
	AllocMB        float64   `json:"allocMb"`
	TotalAllocMB   float64   `json:"totalAllocMb"`
	SysMB          float64   `json:"sysMb"`
	NumGC          uint32    `json:"numGc"`
	NumCPU         int       `json:"numCpu"`
}

type MetricsService struct{}

func NewMetricsService() *MetricsService {
	return &MetricsService{}
}

func (m *MetricsService) GetSystemTelemetry() SystemTelemetry {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return SystemTelemetry{
		Timestamp:    time.Now(),
		Goroutines:   runtime.NumGoroutine(),
		AllocMB:      float64(ms.Alloc) / (1024 * 1024),
		TotalAllocMB: float64(ms.TotalAlloc) / (1024 * 1024),
		SysMB:        float64(ms.Sys) / (1024 * 1024),
		NumGC:        ms.NumGC,
		NumCPU:       runtime.NumCPU(),
	}
}
