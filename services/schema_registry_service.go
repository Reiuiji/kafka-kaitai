package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
)

type SchemaRegistryService struct {
	mu     sync.RWMutex
	client *sr.Client
	url    string
}

func NewSchemaRegistryService() *SchemaRegistryService {
	return &SchemaRegistryService{}
}

func (s *SchemaRegistryService) Configure(url, user, pass string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if url == "" {
		s.client = nil
		s.url = ""
		return nil
	}

	opts := []sr.ClientOpt{
		sr.URLs(url),
	}
	if user != "" || pass != "" {
		opts = append(opts, sr.BasicAuth(user, pass))
	}

	cl, err := sr.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create schema registry client: %w", err)
	}

	s.client = cl
	s.url = url
	return nil
}

func (s *SchemaRegistryService) ListSubjects() ([]string, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	return cl.Subjects(ctx)
}

func (s *SchemaRegistryService) GetSchema(subject string, version int) (string, error) {
	s.mu.RLock()
	cl := s.client
	s.mu.RUnlock()

	if cl == nil {
		return "", fmt.Errorf("schema registry not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	schema, err := cl.SchemaByVersion(ctx, subject, version)
	if err != nil {
		return "", err
	}

	return schema.Schema.Schema, nil
}
