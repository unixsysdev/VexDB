// Package storage provides the core storage engine functionality for VexDB
package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/metrics"
	"vexdb/internal/storage/buffer"
	"vexdb/internal/storage/clusterrange"
	"vexdb/internal/storage/compression"
	"vexdb/internal/storage/hashing"
	"vexdb/internal/storage/search"
	"vexdb/internal/storage/segment"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

// Storage represents the main storage engine
type Storage struct {
	config  config.Config
	logger  *zap.Logger
	metrics *metrics.StorageMetrics

	buffer     *buffer.Manager
	compressor *compression.Compressor
	hasher     *hashing.Hasher
	ranges     *clusterrange.RangeManager
	segments   *segment.Manager
	search     *search.Engine

	mu      sync.RWMutex
	started bool
}

// NewStorage creates a new storage engine instance
func NewStorage(cfg config.Config, logger *zap.Logger, metrics *metrics.StorageMetrics) (*Storage, error) {
	s := &Storage{
		config:  cfg,
		logger:  logger,
		metrics: metrics,
	}

	// Initialize components
	if err := s.initializeComponents(); err != nil {
		return nil, err
	}

	return s, nil
}

// initializeComponents initializes all storage components
func (s *Storage) initializeComponents() error {
	var err error

	// Initialize buffer manager
	s.buffer, err = buffer.NewBuffer(s.config, *s.logger, s.metrics, nil)
	if err != nil {
		return err
	}

	// Initialize compressor
	s.compressor, err = compression.NewCompressor(s.config, *s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize hasher
	s.hasher, err = hashing.NewHasher(s.config, *s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize cluster range manager
	s.ranges, err = clusterrange.NewRangeManager(s.config, *s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize segment manager
	s.segments, err = segment.NewManager(&s.config, s.logger, nil, s.compressor)
	if err != nil {
		return err
	}

	// Initialize search engine
	s.search, err = search.NewEngine(s.config, s.logger, s.metrics, s.segments, s.buffer)
	if err != nil {
		return err
	}

	return nil
}

// Start starts the storage engine
func (s *Storage) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	s.logger.Info("Starting storage engine")

	s.started = true
	s.logger.Info("Storage engine started successfully")
	return nil
}

// Stop stops the storage engine
func (s *Storage) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	s.logger.Info("Stopping storage engine")

	s.started = false
	s.logger.Info("Storage engine stopped successfully")
	return nil
}

// StoreVector stores a vector in the storage engine
func (s *Storage) StoreVector(ctx context.Context, vector *types.Vector) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return types.ErrStorageNotStarted
	}

	// Hash the vector to determine cluster assignment
	clusterID, err := s.hasher.HashVector(vector)
	if err != nil {
		return err
	}

	// Get the cluster range for the cluster ID
	_, err = s.ranges.GetRange(uint32(clusterID))
	if err != nil {
		return err
	}

	start := time.Now()
	if err := s.search.AddVectors([]*types.Vector{vector}); err != nil {
		if s.metrics != nil {
			s.metrics.Errors.Inc("store")
		}
		return err
	}
	if s.metrics != nil {
		s.metrics.WriteOperations.Inc("storage", "store")
		s.metrics.WriteLatency.Observe(time.Since(start).Seconds(), "storage", "store")
		s.metrics.VectorsTotal.Inc("storage", fmt.Sprintf("%d", clusterID))
	}
	return nil
}

// SearchVectors performs a similarity search for vectors
func (s *Storage) SearchVectors(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return nil, types.ErrStorageNotStarted
	}

	// Perform the search using the search engine
	searchQuery := &search.SearchQuery{QueryVector: query, Limit: k}
	return s.search.Search(ctx, searchQuery)
}

// GetStatus returns the current status of the storage engine
func (s *Storage) GetStatus() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"started": s.started,
	}
}
