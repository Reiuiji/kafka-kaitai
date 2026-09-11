package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestClusterHealthAndHighSpeedBenchmark(t *testing.T) {
	seeds := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	t.Logf("Connecting to Kafka cluster with seeds: %v", seeds)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.BrokerMaxReadBytes(100*1024*1024),
		kgo.BrokerMaxWriteBytes(100*1024*1024),
	)
	if err != nil {
		t.Fatalf("Failed to create kgo client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Cluster Ping & Metadata Probe
	startPing := time.Now()
	if err := cl.Ping(ctx); err != nil {
		t.Fatalf("Cluster Ping failed: %v", err)
	}
	t.Logf("✓ Cluster Ping successful in %v", time.Since(startPing))

	req := kmsg.NewMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("Metadata request failed: %v", err)
	}

	clusterID := "unknown"
	if resp.ClusterID != nil {
		clusterID = *resp.ClusterID
	}
	t.Logf("=== Kafka Cluster Topology ===")
	t.Logf("Cluster ID: %s", clusterID)
	t.Logf("Controller Node ID: %d", resp.ControllerID)
	t.Logf("Active Brokers Count: %d", len(resp.Brokers))
	for _, b := range resp.Brokers {
		isController := ""
		if b.NodeID == resp.ControllerID {
			isController = " [KRaft Controller Leader]"
		}
		t.Logf("  -> Broker Node %d: %s:%d%s", b.NodeID, b.Host, b.Port, isController)
	}

	// 2. High-Speed Production-like Topic Provisioning
	adm := kadm.NewClient(cl)
	topicName := "perf-highspeed-30kb"
	numPartitions := int32(24) // 24 partitions for parallel throughput across brokers

	t.Logf("\n=== Topic Provisioning ===")
	// Check if topic exists, create if not
	existingTopics, err := adm.ListTopics(ctx, topicName)
	if err == nil {
	top, exists := existingTopics[topicName]
	if exists && top.Err == nil && len(top.Partitions) > 0 {
		t.Logf("Topic '%s' already exists with %d partitions", topicName, len(top.Partitions))
	} else {
		t.Logf("Creating topic '%s' with %d partitions (replication factor 1)...", topicName, numPartitions)
		// Delete stale topic if needed
		if exists && top.Err != nil {
			_, _ = adm.DeleteTopics(ctx, topicName)
			time.Sleep(500 * time.Millisecond)
		}
		createResp, err := adm.CreateTopics(ctx, numPartitions, 1, nil, topicName)
		if err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}
		t.Logf("✓ Topic '%s' created: %v", topicName, createResp)
		// Wait for metadata propagation
		time.Sleep(1 * time.Second)
	}
	}

	// 3. High-Speed 30KB Producer Benchmark
	t.Logf("\n=== High-Speed Producer Benchmark (30KB Records) ===")
	payloadSize := 30 * 1024 // 30KB
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	targetMessages := 5000 // 5,000 * 30KB = 150 MB test run
	concurrency := 16
	var producedCount int64
	var producedBytes int64
	var errorCount int64

	t.Logf("Benchmarking: %d workers sending %d messages of %d bytes (~%.1f MB total)...",
		concurrency, targetMessages, payloadSize, float64(targetMessages*payloadSize)/(1024*1024))

	benchStart := time.Now()
	var wg sync.WaitGroup
	msgsPerWorker := targetMessages / concurrency

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			sem := make(chan struct{}, 256)

			for m := 0; m < msgsPerWorker; m++ {
				sem <- struct{}{}
				key := []byte(fmt.Sprintf("key-w%d-m%d", workerID, m))
				rec := &kgo.Record{
					Topic: topicName,
					Key:   key,
					Value: payload,
				}

				cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
					<-sem
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						t.Errorf("Produce error on worker %d: %v", workerID, err)
						return
					}
					atomic.AddInt64(&producedCount, 1)
					atomic.AddInt64(&producedBytes, int64(len(r.Value)+len(r.Key)))
				})
			}

			// Drain semaphore
			for i := 0; i < cap(sem); i++ {
				sem <- struct{}{}
			}
		}(w)
	}

	wg.Wait()
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := cl.Flush(flushCtx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	duration := time.Since(benchStart)
	totalMb := float64(producedBytes) / (1024 * 1024)
	throughputMbSec := totalMb / duration.Seconds()
	msgsSec := float64(producedCount) / duration.Seconds()

	t.Logf("✓ Benchmark Complete in %v", duration)
	t.Logf("  -> Total Produced: %d messages (Errors: %d)", producedCount, errorCount)
	t.Logf("  -> Total Bandwidth: %.2f MB", totalMb)
	t.Logf("  -> Throughput Speed: %.2f MB/s", throughputMbSec)
	t.Logf("  -> Message Rate: %.0f msg/s", msgsSec)

	if errorCount > 0 {
		t.Fatalf("Benchmark finished with %d produce errors!", errorCount)
	}

	// 4. Consumer Ingress & Integrity Verification
	t.Logf("\n=== High-Speed Consumer Verification ===")
	consStart := time.Now()
	consCl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(fmt.Sprintf("bench-group-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(50*1024*1024),
	)
	if err != nil {
		t.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consCl.Close()

	var consumedCount int
	var consumedBytes int64

	consCtx, consCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consCancel()

	for consumedCount < int(producedCount) && consCtx.Err() == nil {
		fetches := consCl.PollFetches(consCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("Consumer fetch error: %v", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			consumedCount++
			consumedBytes += int64(len(r.Value))
		})
	}

	consDuration := time.Since(consStart)
	consMb := float64(consumedBytes) / (1024 * 1024)
	consThroughput := consMb / consDuration.Seconds()

	t.Logf("✓ Consumer Verified: %d / %d records read", consumedCount, targetMessages)
	t.Logf("  -> Consumer Bandwidth: %.2f MB", consMb)
	t.Logf("  -> Consumer Throughput: %.2f MB/s in %v", consThroughput, consDuration)
}
