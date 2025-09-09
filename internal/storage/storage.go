// Package storage provides the core storage engine functionality for VexDB
package storage

import (
	"context"
	"sync"

	"vexdb/internal/config"
	"vexdb/internal/logging"
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
	config      *config.Config
	logger      *zap.Logger
	metrics     *metrics.Collector
	
	buffer      *buffer.Manager
	compressor  *compression.Compressor
	hasher      *hashing.Hasher
	ranges      *clusterrange.Manager
	segments    *segment.Manager
	search      *search.Engine
	
	mu          sync.RWMutex
	started     bool
}

// NewStorage creates a new storage engine instance
func NewStorage(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector) (*Storage, error) {
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
	s.buffer, err = buffer.NewManager(s.config, s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize compressor
	s.compressor, err = compression.NewCompressor(s.config, s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize hasher
	s.hasher, err = hashing.NewHasher(s.config, s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize cluster range manager
	s.ranges, err = clusterrange.NewManager(s.config, s.logger, s.metrics)
	if err != nil {
		return err
	}

	// Initialize segment manager
	s.segments, err = segment.NewManager(s.config, s.logger, s.metrics, s.compressor)
	if err != nil {
		return err
	}

	// Initialize search engine
	s.search, err = search.NewEngine(s.config, s.logger, s.metrics, s.buffer, s.segments)
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

	// Start components
	if err := s.buffer.Start(ctx); err != nil {
		return err
	}

	if err := s.compressor.Start(ctx); err != nil {
		return err
	}

	if err := s.hasher.Start(ctx); err != nil {
		return err
	}

	if err := s.ranges.Start(ctx); err != nil {
		return err
	}

	if err := s.segments.Start(ctx); err != nil {
		return err
	}

	if err := s.search.Start(ctx); err != nil {
		return err
	}

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

	// Stop components in reverse order
	if err := s.search.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop search engine", zap.Error(err))
	}

	if err := s.segments.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop segment manager", zap.Error(err))
	}

	if err := s.ranges.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop cluster range manager", zap.Error(err))
	}

	if err := s.hasher.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop hasher", zap.Error(err))
	}

	if err := s.compressor.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop compressor", zap.Error(err))
	}

	if err := s.buffer.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop buffer manager", zap.Error(err))
	}

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
	rangeInfo, err := s.ranges.GetRange(clusterID)
	if err != nil {
		return err
	}

	// Store the vector in the buffer
	return s.buffer.StoreVector(ctx, vector, clusterID, rangeInfo)
}

// SearchVectors performs a similarity search for vectors
func (s *Storage) SearchVectors(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return nil, types.ErrStorageNotStarted
	}

	// Perform the search using the search engine
	return s.search.Search(ctx, query, k)
}

// GetStatus returns the current status of the storage engine
func (s *Storage) GetStatus() *types.StorageStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &types.StorageStatus{
		Started: s.started,
		Buffer:  s.buffer.GetStatus(),
		Segments: s.segments.GetStatus(),
		Search:  s.search.GetStatus(),
	}
}