// Package search provides dual search functionality for VexDB
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

// DualSearch provides search functionality across both buffer and segments
type DualSearch struct {
	config      *config.Config
	logger      *zap.Logger
	metrics     *metrics.Collector
	buffer      *buffer.Manager
	segments    *segment.Manager
	
	linear      *LinearSearch
	parallel    *ParallelSearch
	
	mu          sync.RWMutex
	started     bool
}

// NewDualSearch creates a new dual search instance
func NewDualSearch(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector, buffer *buffer.Manager, segments *segment.Manager) (*DualSearch, error) {
	d := &DualSearch{
		config:   cfg,
		logger:   logger,
		metrics:  metrics,
		buffer:   buffer,
		segments: segments,
	}

	// Initialize search components
	if err := d.initializeComponents(); err != nil {
		return nil, err
	}

	return d, nil
}

// initializeComponents initializes all search components
func (d *DualSearch) initializeComponents() error {
	var err error

	// Initialize linear search
	d.linear, err = NewLinearSearch(d.config, d.logger, d.metrics)
	if err != nil {
		return err
	}

	// Initialize parallel search
	d.parallel, err = NewParallelSearch(d.config, d.logger, d.metrics)
	if err != nil {
		return err
	}

	return nil
}

// Start starts the dual search
func (d *DualSearch) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return nil
	}

	d.logger.Info("Starting dual search")

	// Start components
	if err := d.linear.Start(ctx); err != nil {
		return err
	}

	if err := d.parallel.Start(ctx); err != nil {
		return err
	}

	d.started = true
	d.logger.Info("Dual search started successfully")
	return nil
}

// Stop stops the dual search
func (d *DualSearch) Stop(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.started {
		return nil
	}

	d.logger.Info("Stopping dual search")

	// Stop components in reverse order
	if err := d.parallel.Stop(ctx); err != nil {
		d.logger.Error("Failed to stop parallel search", zap.Error(err))
	}

	if err := d.linear.Stop(ctx); err != nil {
		d.logger.Error("Failed to stop linear search", zap.Error(err))
	}

	d.started = false
	d.logger.Info("Dual search stopped successfully")
	return nil
}

// Search performs a dual search across both buffer and segments
func (d *DualSearch) Search(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.started {
		return nil, types.ErrStorageNotStarted
	}

	// Create channels for results
	bufferResults := make(chan []*types.SearchResult, 1)
	segmentResults := make(chan []*types.SearchResult, 1)
	bufferErr := make(chan error, 1)
	segmentErr := make(chan error, 1)

	// Search buffer in goroutine
	go func() {
		results, err := d.searchBuffer(ctx, query, k)
		if err != nil {
			bufferErr <- err
			return
		}
		bufferResults <- results
	}()

	// Search segments in goroutine
	go func() {
		results, err := d.searchSegments(ctx, query, k)
		if err != nil {
			segmentErr <- err
			return
		}
		segmentResults <- results
	}()

	// Wait for results
	var bufferRes, segmentRes []*types.SearchResult
	var bufferErrVal, segmentErrVal error
	
	completed := 0
	for completed < 2 {
		select {
		case res := <-bufferResults:
			bufferRes = res
			completed++
		case res := <-segmentResults:
			segmentRes = res
			completed++
		case err := <-bufferErr:
			bufferErrVal = err
			completed++
		case err := <-segmentErr:
			segmentErrVal = err
			completed++
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Check for errors
	if bufferErrVal != nil {
		d.logger.Error("Buffer search failed", zap.Error(bufferErrVal))
	}
	if segmentErrVal != nil {
		d.logger.Error("Segment search failed", zap.Error(segmentErrVal))
	}

	// If both searches failed, return error
	if bufferErrVal != nil && segmentErrVal != nil {
		return nil, types.ErrSearchFailed
	}

	// Combine results
	var allResults []*types.SearchResult
	if bufferRes != nil {
		allResults = append(allResults, bufferRes...)
	}
	if segmentRes != nil {
		allResults = append(allResults, segmentRes...)
	}

	// If no results found, return empty slice
	if len(allResults) == 0 {
		return []*types.SearchResult{}, nil
	}

	return allResults, nil
}

// searchBuffer performs search in the buffer
func (d *DualSearch) searchBuffer(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	// Get buffer status
	bufferStatus := d.buffer.GetStatus()
	if bufferStatus.VectorCount == 0 {
		return []*types.SearchResult{}, nil
	}

	// Get all vectors from buffer
	vectors, err := d.buffer.GetAllVectors(ctx)
	if err != nil {
		return nil, err
	}

	// Perform linear search on buffer vectors
	results := d.linear.SearchVectors(query, vectors, k)

	return results, nil
}

// searchSegments performs search in segments
func (d *DualSearch) searchSegments(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	// Get all segments
	segments := d.segments.GetAllSegments()
	if len(segments) == 0 {
		return []*types.SearchResult{}, nil
	}

	// Prepare segment data for parallel search
	segmentData := make([]ParallelSearchSegment, len(segments))
	for i, segment := range segments {
		segmentData[i] = ParallelSearchSegment{
			Segment:    segment,
			Query:      query,
			K:          k,
		}
	}

	// Perform parallel search
	results, err := d.parallel.SearchSegments(ctx, segmentData)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// GetStatus returns the current status of the dual search
func (d *DualSearch) GetStatus() *types.DualSearchStatus {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return &types.DualSearchStatus{
		Started: d.started,
		Linear:  d.linear.GetStatus(),
		Parallel: d.parallel.GetStatus(),
	}
}