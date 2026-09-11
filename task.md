# KafkaKaitai — Task Tracker

## Phase 1: Project Scaffolding
- [x] Install Wails v3 CLI
- [x] Initialize Wails v3 project with vanilla template
- [x] Set up Go module with dependencies (franz-go, kaitai runtime)
- [x] Create project directory structure (services/, models/, internal/, frontend/)
- [x] Create main.go with service registration stubs
- [x] Create frontend HTML shell with dark theme + sidebar nav
- [x] Create CSS design system (tokens, dark theme, glassmorphism)
- [x] Create SPA router (vanilla JS/TS)
- [x] Verify project builds successfully

## Phase 2: Kafka Core + Full Authentication
- [x] Implement ConnectionConfig model (all auth modes)
- [x] Implement KafkaService.Connect() with PLAIN/SSL/mTLS/SASL support
- [x] Implement KafkaService.Disconnect()
- [x] Implement KafkaService.TestConnection()
- [x] Implement KafkaService.ListTopics()
- [x] Implement KafkaService.ListConsumerGroups()
- [x] Implement Connection Manager UI (frontend)
- [x] Implement SchemaRegistryService skeleton
- [x] Wire up frontend ↔ backend bindings for connection

## Phase 3: Kaitai Build Pipeline
- [x] Install/verify kaitai-struct-compiler availability
- [x] Implement internal/kaitai/compiler.go (ksc subprocess wrapper)
- [x] Implement internal/kaitai/builder.go (go build for parser binaries)
- [x] Create parser_main.go.tmpl template
- [x] Implement internal/kaitai/runner.go (subprocess invocation)
- [x] Implement KaitaiService (import, list, build, parse)
- [x] Implement Kaitai Format Manager UI
- [x] Test with sample .ksy file (verified with end-to-end integration test)

## Phase 4: Producer + Stress Testing
- [x] Implement KafkaService.Produce() (single message, all formats)
- [x] Implement KafkaService.StartStressTest() (goroutine pool)
- [x] Implement KafkaService.StopStressTest()
- [x] Implement rate limiter (golang.org/x/time/rate)
- [x] Implement latency histogram tracking
- [x] Implement Producer UI (single message mode)
- [x] Implement Stress Test UI (concurrency, duration, rate controls)
- [x] Implement live performance dashboard in producer view
- [x] Wire up stress test metrics → frontend gauges

## Phase 5: Consumer + Benchmark/Drop/Tail
- [x] Implement KafkaService.StartConsumer() (inspect mode)
- [x] Implement consumer benchmark/drop mode
- [x] Implement consumer store mode + file dump
- [x] Implement KafkaService.TailTopic() (last N messages)
- [x] Implement Consumer UI (mode selector, message list)
- [x] Implement Benchmark mode UI (gauges, charts)
- [x] Implement Tail/Peek UI with Kaitai parse integration
- [x] Implement hex viewer component
- [x] Implement tree view component for parsed structs
- [x] Implement file export formats (JSONL, CSV Hex, Raw)

## Phase 6: Dashboard & Polish
- [x] Implement MetricsService with real-time telemetry
- [x] Implement Dashboard view (produce/consume throughput)
- [x] Add toast notifications
- [x] UI polish: transitions, glassmorphism, responsive layout
- [x] Build verification (`bin/kafka-kaitai`)
