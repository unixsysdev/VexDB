// Package search provides search functionality for VexDB
package search

import (
	"context"
	"sync"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/storage/buffer"
	"vexdb/internal/storage/segment"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

// Engine provides search functionality across all storage components
type Engine struct {
	config      *config.Config
	logger      *zap.Logger
	metrics     *metrics.Collector
	buffer      *buffer.Manager
	segments    *segment.Manager
	
	linear      *LinearSearch
	dual        *DualSearch
	merger      *ResultMerger
	filter      *MetadataFilter
	topk        *TopKSelector
	
	mu          sync.RWMutex
	started     bool
}

// NewEngine creates a new search engine
func NewEngine(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector, buffer *buffer.Manager, segments *segment.Manager) (*Engine, error) {
	e := &Engine{
		config:   cfg,
		logger:   logger,
		metrics:  metrics,
		buffer:   buffer,
		segments: segments,
	}

	// Initialize search components
	if err := e.initializeComponents(); err != nil {
		return nil, err
	}

	return e, nil
}

// initializeComponents initializes all search components
func (e *Engine) initializeComponents() error {
	var err error

	// Initialize linear search
	e.linear, err = NewLinearSearch(e.config, e.logger, e.metrics)
	if err != nil {
		return err
	}

	// Initialize dual search
	e.dual, err = NewDualSearch(e.config, e.logger, e.metrics, e.buffer, e.segments)
	if err != nil {
		return err
	}

	// Initialize result merger
	e.merger, err = NewResultMerger(e.config, e.logger, e.metrics)
	if err != nil {
		return err
	}

	// Initialize metadata filter
	e.filter, err = NewMetadataFilter(e.config, e.logger, e.metrics)
	if err != nil {
		return err
	}

	// Initialize top-k selector
	e.topk, err = NewTopKSelector(e.config, e.logger, e.metrics)
	if err != nil {
		return err
	}

	return nil
}

// Start starts the search engine
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	e.logger.Info("Starting search engine")

	// Start components
	if err := e.linear.Start(ctx); err != nil {
		return err
	}

	if err := e.dual.Start(ctx); err != nil {
		return err
	}

	if err := e.merger.Start(ctx); err != nil {
		return err
	}

	if err := e.filter.Start(ctx); err != nil {
		return err
	}

	if err := e.topk.Start(ctx); err != nil {
		return err
	}

	e.started = true
	e.logger.Info("Search engine started successfully")
	return nil
}

// Stop stops the search engine
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return nil
	}

	e.logger.Info("Stopping search engine")

	// Stop components in reverse order
	if err := e.topk.Stop(ctx); err != nil {
		e.logger.Error("Failed to stop top-k selector", zap.Error(err))
	}

	if err := e.filter.Stop(ctx); err != nil {
		e.logger.Error("Failed to stop metadata filter", zap.Error(err))
	}

	if err := e.merger.Stop(ctx); err != nil {
		e.logger.Error("Failed to stop result merger", zap.Error(err))
	}

	if err := e.dual.Stop(ctx); err != nil {
		e.logger.Error("Failed to stop dual search", zap.Error(err))
	}

	if err := e.linear.Stop(ctx); err != nil {
		e.logger.Error("Failed to stop linear search", zap.Error(err))
	}

	e.started = false
	e.logger.Info("Search engine stopped successfully")
	return nil
}

// Search performs a similarity search
func (e *Engine) Search(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return nil, types.ErrStorageNotStarted
	}

	// Validate query vector
	if err := query.Validate(); err != nil {
		return nil, err
	}

	// Validate k parameter
	if k <= 0 {
		return nil, types.ErrInvalidKValue
	}

	// Perform dual search (buffer + segments)
	results, err := e.dual.Search(ctx, query, k)
	if err != nil {
		return nil, err
	}

	// Apply metadata filtering if needed
	if query.MetadataFilter != nil {
		results, err = e.filter.Filter(ctx, results, query.MetadataFilter)
		if err != nil {
			return nil, err
		}
	}

	// Select top-k results
	finalResults, err := e.topk.Select(ctx, results, k)
	if err != nil {
		return nil, err
	}

	return finalResults, nil
}

// SearchWithMetadata performs a similarity search with metadata filtering
func (e *Engine) SearchWithMetadata(ctx context.Context, query *types.Vector, k int, filter *types.MetadataFilter) ([]*types.SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return nil, types.ErrStorageNotStarted
	}

	// Validate query vector
	if err := query.Validate(); err != nil {
		return nil, err
	}

	// Validate k parameter
	if k <= 0 {
		return nil, types.ErrInvalidKValue
	}

	// Validate metadata filter
	if filter != nil {
		if err := filter.Validate(); err != nil {
			return nil, err
		}
	}

	// Perform dual search (buffer + segments)
	results, err := e.dual.Search(ctx, query, k)
	if err != nil {
		return nil, err
	}

	// Apply metadata filtering
	if filter != nil {
		results, err = e.filter.Filter(ctx, results, filter)
		if err != nil {
			return nil, err
		}
	}

	// Select top-k results
	finalResults, err := e.topk.Select(ctx, results, k)
	if err != nil {
		return nil, err
	}

	return finalResults, nil
}

// GetStatus returns the current status of the search engine
func (e *Engine) GetStatus() *types.SearchEngineStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &types.SearchEngineStatus{
		Started: e.started,
		Linear:  e.linear.GetStatus(),
		Dual:    e.dual.GetStatus(),
		Merger:  e.merger.GetStatus(),
		Filter:  e.filter.GetStatus(),
		TopK:    e.topk.GetStatus(),
	}
}