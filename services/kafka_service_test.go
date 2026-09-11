package services

import (
	"testing"

	"github.com/okuu/kafka_kaitai/models"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildClientOptions(t *testing.T) {
	ks, err := NewKaitaiService()
	if err != nil {
		t.Fatalf("NewKaitaiService failed: %v", err)
	}
	svc := NewKafkaService(ks)

	cfg := models.ConnectionConfig{
		Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
	}

	opts, err := svc.buildClientOptions(cfg)
	if err != nil {
		t.Fatalf("buildClientOptions failed: %v", err)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("kgo.NewClient failed with options: %v", err)
	}
	defer cl.Close()
}
