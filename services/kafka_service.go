package services

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/okuu/kafka_kaitai/models"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/time/rate"
)

type KafkaService struct {
	mu           sync.RWMutex
	client       *kgo.Client
	config       models.ConnectionConfig
	status       models.ConnectionStatus
	statusMu     sync.RWMutex
	cancelFunc   context.CancelFunc

	// Stress test state
	stressMu      sync.RWMutex
	stressRunning bool
	stressCancel  context.CancelFunc
	stressMetrics models.StressTestMetrics
	stressLatHist []float64

	// Consumer benchmark state
	consumerMu      sync.RWMutex
	consumerRunning bool
	consumerCancel  context.CancelFunc
	consumerMetrics models.ConsumerBenchmarkMetrics
	consumerFile    *os.File

	// Optional pointer to KaitaiService for auto-parsing
	kaitaiService *KaitaiService
}

func NewKafkaService(ks *KaitaiService) *KafkaService {
	return &KafkaService{
		status: models.ConnectionStatus{
			Connected: false,
		},
		kaitaiService: ks,
	}
}

// buildClientOptions translates ConnectionConfig into franz-go kgo.Opt slice
func (s *KafkaService) buildClientOptions(cfg models.ConnectionConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.ProducerBatchMaxBytes(4 * 1024 * 1024),
		kgo.BrokerMaxReadBytes(100 * 1024 * 1024),
		kgo.BrokerMaxWriteBytes(100 * 1024 * 1024),
	}

	// TLS Configuration
	var tlsConfig *tls.Config
	if cfg.TLSEnabled || cfg.AuthType == "ssl" || cfg.AuthType == "mtls" {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		}

		if cfg.CACertPath != "" {
			caCert, err := os.ReadFile(cfg.CACertPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA cert: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, errors.New("failed to parse CA certificate PEM")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if (cfg.AuthType == "mtls" || cfg.ClientCertPath != "") && cfg.ClientKeyPath != "" {
			cert, err := tls.LoadX509KeyPair(cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, fmt.Errorf("failed to load client keypair: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// SASL Configuration
	var saslMechanism sasl.Mechanism
	switch cfg.AuthType {
	case "plain":
		saslMechanism = plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()
	case "scram-sha-256":
		saslMechanism = scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha256Mechanism()
	case "scram-sha-512":
		saslMechanism = scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha512Mechanism()
	case "oauthbearer":
		saslMechanism = oauth.Auth{
			Token: cfg.Token,
		}.AsMechanism()
	}

	if saslMechanism != nil {
		opts = append(opts, kgo.SASL(saslMechanism))
	}

	return opts, nil
}

// Connect establishes a connection to the Kafka cluster
func (s *KafkaService) Connect(cfg models.ConnectionConfig) (*models.ConnectionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		s.client.Close()
		s.client = nil
	}

	opts, err := s.buildClientOptions(cfg)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("cluster ping failed: %w", err)
	}

	latency := float64(time.Since(start).Milliseconds())

	// Fetch cluster metadata
	req := kmsg.NewMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		cl.Close()
		return nil, fmt.Errorf("metadata request failed: %w", err)
	}

	var brokers []string
	for _, b := range resp.Brokers {
		brokers = append(brokers, fmt.Sprintf("%s:%d (node %d)", b.Host, b.Port, b.NodeID))
	}

	clusterID := ""
	if resp.ClusterID != nil {
		clusterID = *resp.ClusterID
	}

	status := models.ConnectionStatus{
		Connected:    true,
		ClusterID:    clusterID,
		ControllerID: resp.ControllerID,
		Brokers:      brokers,
		TopicsCount:  len(resp.Topics),
		LatencyMs:    latency,
	}

	s.client = cl
	s.config = cfg
	s.statusMu.Lock()
	s.status = status
	s.statusMu.Unlock()

	return &status, nil
}

// Disconnect terminates the Kafka connection
func (s *KafkaService) Disconnect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop any active stress test or consumer
	_ = s.StopStressTest()
	_ = s.StopConsumer()

	if s.client != nil {
		s.client.Close()
		s.client = nil
	}

	s.statusMu.Lock()
	s.status = models.ConnectionStatus{Connected: false}
	s.statusMu.Unlock()

	return nil
}

// TestConnection checks connection without persisting it
func (s *KafkaService) TestConnection(cfg models.ConnectionConfig) (*models.ConnectionStatus, error) {
	opts, err := s.buildClientOptions(cfg)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if err := cl.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	latency := float64(time.Since(start).Milliseconds())

	req := kmsg.NewMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("metadata fetch failed: %w", err)
	}

	var brokers []string
	for _, b := range resp.Brokers {
		brokers = append(brokers, fmt.Sprintf("%s:%d", b.Host, b.Port))
	}

	clusterID := ""
	if resp.ClusterID != nil {
		clusterID = *resp.ClusterID
	}

	return &models.ConnectionStatus{
		Connected:    true,
		ClusterID:    clusterID,
		ControllerID: resp.ControllerID,
		Brokers:      brokers,
		TopicsCount:  len(resp.Topics),
		LatencyMs:    latency,
	}, nil
}

// GetStatus returns the current connection status
func (s *KafkaService) GetStatus() models.ConnectionStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

// ListTopics returns summaries of all topics in the cluster
func (s *KafkaService) ListTopics() ([]models.TopicSummary, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return nil, errors.New("not connected to kafka")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := kmsg.NewMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	var summaries []models.TopicSummary
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		topicName := *t.Topic

		var partitions []models.PartitionInfo
		rf := 0
		for _, p := range t.Partitions {
			if len(p.Replicas) > rf {
				rf = len(p.Replicas)
			}
			partitions = append(partitions, models.PartitionInfo{
				ID:       p.Partition,
				Leader:   p.Leader,
				Replicas: p.Replicas,
				ISR:      p.ISR,
			})
		}

		summaries = append(summaries, models.TopicSummary{
			Name:              topicName,
			PartitionsCount:   len(t.Partitions),
			ReplicationFactor: rf,
			Partitions:        partitions,
		})
	}

	return summaries, nil
}

// CreateTopic creates a new topic with partitions, replication factor, and optional configs
func (s *KafkaService) CreateTopic(req models.CreateTopicRequest) error {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return errors.New("not connected to kafka")
	}

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	partitions := req.Partitions
	if partitions <= 0 {
		partitions = 1
	}

	rf := req.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	configs := make(map[string]*string)
	for k, v := range req.Configs {
		val := v
		configs[k] = &val
	}

	resp, err := adm.CreateTopics(ctx, partitions, rf, configs, req.Name)
	if err != nil {
		return fmt.Errorf("create topic request failed: %w", err)
	}

	if tResp, ok := resp[req.Name]; ok {
		if tResp.Err != nil {
			return fmt.Errorf("failed to create topic '%s': %w", req.Name, tResp.Err)
		}
	}

	return nil
}

// DeleteTopic deletes a topic from the cluster
func (s *KafkaService) DeleteTopic(name string) error {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return errors.New("not connected to kafka")
	}

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := adm.DeleteTopics(ctx, name)
	if err != nil {
		return fmt.Errorf("delete topic request failed: %w", err)
	}

	if tResp, ok := resp[name]; ok {
		if tResp.Err != nil {
			return fmt.Errorf("failed to delete topic '%s': %w", name, tResp.Err)
		}
	}

	return nil
}

// GetTopicDetail retrieves partition metadata and broker topic configurations
func (s *KafkaService) GetTopicDetail(name string) (*models.TopicDetail, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return nil, errors.New("not connected to kafka")
	}

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topDetails, err := adm.ListTopics(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to list topic: %w", err)
	}

	top, exists := topDetails[name]
	if !exists || top.Err != nil {
		if top.Err != nil {
			return nil, top.Err
		}
		return nil, fmt.Errorf("topic '%s' not found", name)
	}

	var partitions []models.PartitionInfo
	rf := 0
	for _, p := range top.Partitions {
		if len(p.Replicas) > rf {
			rf = len(p.Replicas)
		}
		partitions = append(partitions, models.PartitionInfo{
			ID:       p.Partition,
			Leader:   p.Leader,
			Replicas: p.Replicas,
			ISR:      p.ISR,
		})
	}

	var configEntries []models.TopicConfigEntry
	cfgResp, err := adm.DescribeTopicConfigs(ctx, name)
	if err == nil {
		for _, resource := range cfgResp {
			if resource.Name == name && resource.Err == nil {
				for _, c := range resource.Configs {
					configEntries = append(configEntries, models.TopicConfigEntry{
						Name:      c.Key,
						Value:     c.MaybeValue(),
						IsDefault: c.Source == kmsg.ConfigSourceDefaultConfig,
						Source:    c.Source.String(),
					})
				}
			}
		}
	}

	sort.Slice(configEntries, func(i, j int) bool {
		return configEntries[i].Name < configEntries[j].Name
	})

	return &models.TopicDetail{
		Name:              name,
		PartitionsCount:   len(top.Partitions),
		ReplicationFactor: rf,
		Partitions:        partitions,
		Configs:           configEntries,
	}, nil
}

// AlterTopicConfig sets or deletes a topic configuration override
func (s *KafkaService) AlterTopicConfig(topic string, key string, value string) error {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return errors.New("not connected to kafka")
	}

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var op kadm.AlterConfig
	if value == "" {
		op = kadm.AlterConfig{
			Op:   kadm.DeleteConfig,
			Name: key,
		}
	} else {
		val := value
		op = kadm.AlterConfig{
			Op:    kadm.SetConfig,
			Name:  key,
			Value: &val,
		}
	}

	resps, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{op}, topic)
	if err != nil {
		return fmt.Errorf("alter topic config failed: %w", err)
	}

	for _, resp := range resps {
		if resp.Name == topic && resp.Err != nil {
			if resp.ErrMessage != "" {
				return fmt.Errorf("failed to alter '%s': %s (%w)", key, resp.ErrMessage, resp.Err)
			}
			return fmt.Errorf("failed to alter '%s': %w", key, resp.Err)
		}
	}

	return nil
}

// ListConsumerGroups returns active consumer groups
func (s *KafkaService) ListConsumerGroups() ([]models.ConsumerGroupInfo, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return nil, errors.New("not connected to kafka")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := kmsg.NewListGroupsRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	var groups []models.ConsumerGroupInfo
	for _, g := range resp.Groups {
		groups = append(groups, models.ConsumerGroupInfo{
			GroupID:      g.Group,
			ProtocolType: g.ProtocolType,
		})
	}

	return groups, nil
}

// Produce sends a single record to Kafka
func (s *KafkaService) Produce(req models.ProduceRequest) (*models.ProduceResponse, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return nil, errors.New("not connected to kafka")
	}

	// Decode key
	var keyBytes []byte
	switch req.KeyFormat {
	case "hex":
		var err error
		keyBytes, err = hex.DecodeString(req.Key)
		if err != nil {
			return nil, fmt.Errorf("invalid hex key: %w", err)
		}
	case "base64":
		var err error
		keyBytes, err = base64.StdEncoding.DecodeString(req.Key)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 key: %w", err)
		}
	default:
		keyBytes = []byte(req.Key)
	}

	// Decode value
	var valBytes []byte
	switch req.ValueFormat {
	case "hex":
		var err error
		valBytes, err = hex.DecodeString(req.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid hex value: %w", err)
		}
	case "base64":
		var err error
		valBytes, err = base64.StdEncoding.DecodeString(req.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value: %w", err)
		}
	default:
		valBytes = []byte(req.Value)
	}

	record := &kgo.Record{
		Topic: req.Topic,
		Key:   keyBytes,
		Value: valBytes,
	}

	if req.Partition >= 0 {
		record.Partition = req.Partition
	}

	for k, v := range req.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var produceErr error
	var resRecord *kgo.Record

	res := cl.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return &models.ProduceResponse{
			Success: false,
			Topic:   req.Topic,
			Error:   err.Error(),
		}, err
	}
	resRecord = res[0].Record
	produceErr = res[0].Err

	if produceErr != nil {
		return &models.ProduceResponse{
			Success: false,
			Topic:   req.Topic,
			Error:   produceErr.Error(),
		}, produceErr
	}

	return &models.ProduceResponse{
		Success:   true,
		Topic:     resRecord.Topic,
		Partition: resRecord.Partition,
		Offset:    resRecord.Offset,
		Timestamp: resRecord.Timestamp,
	}, nil
}

// StartStressTest launches a high-throughput producer worker pool
func (s *KafkaService) StartStressTest(cfg models.StressTestConfig) error {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return errors.New("not connected to kafka")
	}

	s.stressMu.Lock()
	if s.stressRunning {
		s.stressMu.Unlock()
		return errors.New("stress test is already running")
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.stressRunning = true
	s.stressCancel = cancel
	s.stressMetrics = models.StressTestMetrics{
		Active: true,
		Topic:  cfg.Topic,
	}
	s.stressLatHist = make([]float64, 0, 10000)
	s.stressMu.Unlock()

	var sentMsgs int64
	var sentBytes int64
	var errCount int64

	startTime := time.Now()

	// Rate limiter (if targetRate > 0)
	var limiter *rate.Limiter
	if cfg.TargetRate > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.TargetRate), cfg.TargetRate)
	}

	// Payload generator
	payloadSize := cfg.MessageSizeBytes
	if payloadSize <= 0 {
		payloadSize = 256
	}

	genPayload := func(workerID int, counter int64) []byte {
		if cfg.PayloadType == "custom" && len(cfg.CustomPayload) > 0 {
			return []byte(cfg.CustomPayload)
		}
		data := make([]byte, payloadSize)
		rand.Read(data)
		return data
	}

	genKey := func(counter int64) []byte {
		switch cfg.KeyPattern {
		case "uuid":
			return []byte(uuid.New().String())
		case "counter":
			return []byte(fmt.Sprintf("key-%d", counter))
		default:
			return nil
		}
	}

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var localCounter int64

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if cfg.DurationSeconds > 0 && time.Since(startTime).Seconds() >= float64(cfg.DurationSeconds) {
					return
				}

				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						return
					}
				}

				c := atomic.AddInt64(&localCounter, 1)
				val := genPayload(workerID, c)
				key := genKey(c)

				rec := &kgo.Record{
					Topic: cfg.Topic,
					Key:   key,
					Value: val,
				}

				recStart := time.Now()
				cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
					if err != nil {
						atomic.AddInt64(&errCount, 1)
						return
					}
					lat := float64(time.Since(recStart).Microseconds()) / 1000.0 // ms
					atomic.AddInt64(&sentMsgs, 1)
					atomic.AddInt64(&sentBytes, int64(len(r.Value)+len(r.Key)))

					s.stressMu.Lock()
					if len(s.stressLatHist) < 50000 {
						s.stressLatHist = append(s.stressLatHist, lat)
					}
					s.stressMu.Unlock()
				})
			}
		}(i)
	}

	// Metrics update goroutine
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		var lastSent int64
		var lastBytes int64
		var lastTime = time.Now()

		for {
			select {
			case <-ctx.Done():
				s.stressMu.Lock()
				s.stressRunning = false
				s.stressMetrics.Active = false
				s.stressMu.Unlock()
				return
			case now := <-ticker.C:
				dt := now.Sub(lastTime).Seconds()
				if dt <= 0 {
					continue
				}

				currSent := atomic.LoadInt64(&sentMsgs)
				currBytes := atomic.LoadInt64(&sentBytes)
				currErr := atomic.LoadInt64(&errCount)

				msgRate := float64(currSent-lastSent) / dt
				byteRate := float64(currBytes-lastBytes) / dt

				lastSent = currSent
				lastBytes = currBytes
				lastTime = now

				s.stressMu.Lock()
				s.stressMetrics.ElapsedSeconds = now.Sub(startTime).Seconds()
				s.stressMetrics.SentMessages = currSent
				s.stressMetrics.SentBytes = currBytes
				s.stressMetrics.CurrentMsgRate = msgRate
				s.stressMetrics.CurrentByteRate = byteRate
				s.stressMetrics.ErrorsCount = currErr

				// Calculate percentiles
				if n := len(s.stressLatHist); n > 0 {
					sum := 0.0
					for _, l := range s.stressLatHist {
						sum += l
					}
					s.stressMetrics.LatencyAvg = sum / float64(n)
					p50Idx := int(float64(n) * 0.50)
					p95Idx := int(float64(n) * 0.95)
					p99Idx := int(float64(n) * 0.99)
					if p50Idx < n {
						s.stressMetrics.LatencyP50 = s.stressLatHist[p50Idx]
					}
					if p95Idx < n {
						s.stressMetrics.LatencyP95 = s.stressLatHist[p95Idx]
					}
					if p99Idx < n {
						s.stressMetrics.LatencyP99 = s.stressLatHist[p99Idx]
					}
				}
				s.stressMu.Unlock()

				if cfg.DurationSeconds > 0 && now.Sub(startTime).Seconds() >= float64(cfg.DurationSeconds) {
					cancel()
					return
				}
			}
		}
	}()

	return nil
}

// StopStressTest terminates an active producer stress test
func (s *KafkaService) StopStressTest() error {
	s.stressMu.Lock()
	defer s.stressMu.Unlock()

	if s.stressCancel != nil {
		s.stressCancel()
		s.stressCancel = nil
	}
	s.stressRunning = false
	s.stressMetrics.Active = false

	return nil
}

// GetStressTestMetrics returns real-time stress testing stats
func (s *KafkaService) GetStressTestMetrics() models.StressTestMetrics {
	s.stressMu.RLock()
	defer s.stressMu.RUnlock()
	return s.stressMetrics
}

// StartConsumer runs a consumer in drop/store/inspect mode
func (s *KafkaService) StartConsumer(cfg models.ConsumerBenchmarkConfig) error {
	s.mu.RLock()
	parentCfg := s.config
	s.mu.RUnlock()

	s.consumerMu.Lock()
	if s.consumerRunning {
		s.consumerMu.Unlock()
		return errors.New("consumer is already running")
	}

	opts, err := s.buildClientOptions(parentCfg)
	if err != nil {
		s.consumerMu.Unlock()
		return err
	}

	opts = append(opts, kgo.ConsumeTopics(cfg.Topic))
	if cfg.GroupID != "" {
		opts = append(opts, kgo.ConsumerGroup(cfg.GroupID))
	}

	if cfg.AutoOffsetReset == "earliest" {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	// Prepare store file if mode is store
	var storeFile *os.File
	if cfg.Mode == "store" && cfg.StoreFilePath != "" {
		f, err := os.OpenFile(cfg.StoreFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			s.consumerMu.Unlock()
			return fmt.Errorf("failed to open store file: %w", err)
		}
		storeFile = f
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		if storeFile != nil {
			storeFile.Close()
		}
		s.consumerMu.Unlock()
		return fmt.Errorf("failed to create consumer client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.consumerRunning = true
	s.consumerCancel = cancel
	s.consumerFile = storeFile
	s.consumerMetrics = models.ConsumerBenchmarkMetrics{
		Active: true,
		Topic:  cfg.Topic,
		Mode:   cfg.Mode,
	}
	s.consumerMu.Unlock()

	var consumedMsgs int64
	var consumedBytes int64
	var droppedMsgs int64
	var storedMsgs int64
	var errCount int64
	startTime := time.Now()

	// Consumer loop
	go func() {
		defer cl.Close()
		if storeFile != nil {
			defer storeFile.Close()
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				atomic.AddInt64(&errCount, int64(len(errs)))
				continue
			}

			fetches.EachRecord(func(r *kgo.Record) {
				sz := int64(len(r.Value) + len(r.Key))
				atomic.AddInt64(&consumedMsgs, 1)
				atomic.AddInt64(&consumedBytes, sz)

				switch cfg.Mode {
				case "drop":
					atomic.AddInt64(&droppedMsgs, 1)
				case "store":
					if storeFile != nil {
						switch cfg.StoreFormat {
						case "jsonl":
							fmt.Fprintf(storeFile, "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}\n",
								r.Topic, r.Partition, r.Offset,
								base64.StdEncoding.EncodeToString(r.Key),
								base64.StdEncoding.EncodeToString(r.Value))
						case "csv_hex":
							fmt.Fprintf(storeFile, "%s,%d,%d,%s,%s\n",
								r.Topic, r.Partition, r.Offset,
								hex.EncodeToString(r.Key),
								hex.EncodeToString(r.Value))
						default: // raw binary chunk
							storeFile.Write(r.Value)
						}
					}
					atomic.AddInt64(&storedMsgs, 1)
				case "inspect":
					// In inspect mode, records are tracked in memory / emitted
					atomic.AddInt64(&droppedMsgs, 1)
				}
			})
		}
	}()

	// Metrics update goroutine
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		var lastConsumed int64
		var lastBytes int64
		var lastTime = time.Now()

		for {
			select {
			case <-ctx.Done():
				s.consumerMu.Lock()
				s.consumerRunning = false
				s.consumerMetrics.Active = false
				s.consumerMu.Unlock()
				return
			case now := <-ticker.C:
				dt := now.Sub(lastTime).Seconds()
				if dt <= 0 {
					continue
				}

				currConsumed := atomic.LoadInt64(&consumedMsgs)
				currBytes := atomic.LoadInt64(&consumedBytes)
				currDropped := atomic.LoadInt64(&droppedMsgs)
				currStored := atomic.LoadInt64(&storedMsgs)
				currErr := atomic.LoadInt64(&errCount)

				msgRate := float64(currConsumed-lastConsumed) / dt
				byteRate := float64(currBytes-lastBytes) / dt

				lastConsumed = currConsumed
				lastBytes = currBytes
				lastTime = now

				s.consumerMu.Lock()
				s.consumerMetrics.ElapsedSeconds = now.Sub(startTime).Seconds()
				s.consumerMetrics.ConsumedMessages = currConsumed
				s.consumerMetrics.ConsumedBytes = currBytes
				s.consumerMetrics.CurrentMsgRate = msgRate
				s.consumerMetrics.CurrentByteRate = byteRate
				s.consumerMetrics.DroppedMessages = currDropped
				s.consumerMetrics.StoredMessages = currStored
				s.consumerMetrics.ErrorsCount = currErr
				s.consumerMu.Unlock()
			}
		}
	}()

	return nil
}

// StopConsumer halts the active consumer
func (s *KafkaService) StopConsumer() error {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()

	if s.consumerCancel != nil {
		s.consumerCancel()
		s.consumerCancel = nil
	}
	s.consumerRunning = false
	s.consumerMetrics.Active = false

	return nil
}

// GetConsumerMetrics returns consumer throughput metrics
func (s *KafkaService) GetConsumerMetrics() models.ConsumerBenchmarkMetrics {
	s.consumerMu.RLock()
	defer s.consumerMu.RUnlock()
	return s.consumerMetrics
}

// TailTopic fetches the last N records from a topic
func (s *KafkaService) TailTopic(req models.TailRequest) ([]models.KafkaRecord, error) {
	s.mu.RLock()
	parentCfg := s.config
	s.mu.RUnlock()

	opts, err := s.buildClientOptions(parentCfg)
	if err != nil {
		return nil, err
	}

	opts = append(opts, kgo.ConsumeTopics(req.Topic))

	count := req.Count
	if count <= 0 {
		count = 20
	}
	if count > 500 {
		count = 500
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create tail client: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var records []models.KafkaRecord
	fetches := cl.PollFetches(ctx)
	fetches.EachRecord(func(r *kgo.Record) {
		rec := s.formatRecord(r)
		if req.KaitaiFormatID != "" && s.kaitaiService != nil {
			parseRes, pErr := s.kaitaiService.ParseBinary(models.ParseRequest{
				FormatID:   req.KaitaiFormatID,
				DataBase64: rec.ValueBase64,
			})
			if pErr == nil && parseRes.Success {
				rec.ParsedJSON = fmt.Sprintf("%v", parseRes.Result)
			}
		}
		records = append(records, rec)
	})

	if len(records) > count {
		records = records[len(records)-count:]
	}

	return records, nil
}

func (s *KafkaService) formatRecord(r *kgo.Record) models.KafkaRecord {
	headers := make(map[string]string)
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
	}

	rec := models.KafkaRecord{
		Topic:       r.Topic,
		Partition:   r.Partition,
		Offset:      r.Offset,
		Key:         string(r.Key),
		KeyBase64:   base64.StdEncoding.EncodeToString(r.Key),
		Value:       string(r.Value),
		ValueBase64: base64.StdEncoding.EncodeToString(r.Value),
		ValueHex:    hex.EncodeToString(r.Value),
		Headers:     headers,
		Timestamp:   r.Timestamp,
		Format:      "binary",
	}

	return rec
}
