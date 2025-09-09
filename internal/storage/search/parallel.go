// Package search provides parallel search functionality for VexDB
package search

import (
	"context"
	"sync"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

// ParallelSearchSegment represents a segment to be searched in parallel
type ParallelSearchSegment struct {
	Segment *Segment
	Query   *types.Vector
	K       int
}

// ParallelSearch provides parallel search functionality across multiple segments
type ParallelSearch struct {
	config      *config.Config
	logger      *zap.Logger
	metrics     *metrics.Collector
	
	linear      *LinearSearch
	
	mu          sync.RWMutex
	started     bool
}

// NewParallelSearch creates a new parallel search instance
func NewParallelSearch(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector) (*ParallelSearch, error) {
	p := &ParallelSearch{
		config:  cfg,
		logger:  logger,
		metrics: metrics,
	}

	// Initialize linear search
	var err error
	p.linear, err = NewLinearSearch(p.config, p.logger, p.metrics)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Start starts the parallel search
func (p *ParallelSearch) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return nil
	}

	p.logger.Info("Starting parallel search")

	// Start linear search
	if err := p.linear.Start(ctx); err != nil {
		return err
	}

	p.started = true
	p.logger.Info("Parallel search started successfully")
	return nil
}

// Stop stops the parallel search
func (p *ParallelSearch) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	p.logger.Info("Stopping parallel search")

	// Stop linear search
	if err := p.linear.Stop(ctx); err != nil {
		p.logger.Error("Failed to stop linear search", zap.Error(err))
	}

	p.started = false
	p.logger.Info("Parallel search stopped successfully")
	return nil
}

// SearchSegments performs parallel search across multiple segments
func (p *ParallelSearch) SearchSegments(ctx context.Context, segments []ParallelSearchSegment) ([]*types.SearchResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.started {
		return nil, types.ErrStorageNotStarted
	}

	if len(segments) == 0 {
		return []*types.SearchResult{}, nil
	}

	// Create channels for results and errors
	resultsChan := make(chan []*types.SearchResult, len(segments))
	errorsChan := make(chan error, len(segments))

	// Use WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	wg.Add(len(segments))

	// Launch goroutines for each segment search
	for _, segment := range segments {
		go func(s ParallelSearchSegment) {
			defer wg.Done()
			
			results, err := p.searchSegment(ctx, s)
			if err != nil {
				errorsChan <- err
				return
			}
			resultsChan <- results
		}(segment)
	}

	// Wait for all goroutines to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorsChan)
	}()

	// Collect results and errors
	var allResults []*types.SearchResult
	var searchErrors []error

	// Process results and errors as they come in
	for resultsChan != nil || errorsChan != nil {
		select {
		case results, ok := <-resultsChan:
			if !ok {
				resultsChan = nil
				continue
			}
			allResults = append(allResults, results...)
			
		case err, ok := <-errorsChan:
			if !ok {
				errorsChan = nil
				continue
			}
			searchErrors = append(searchErrors, err)
			
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Log any search errors
	for _, err := range searchErrors {
		p.logger.Error("Segment search failed", zap.Error(err))
	}

	// If all searches failed, return error
	if len(searchErrors) == len(segments) && len(segments) > 0 {
		return nil, types.ErrSearchFailed
	}

	// If no results found, return empty slice
	if len(allResults) == 0 {
		return []*types.SearchResult{}, nil
	}

	return allResults, nil
}

// searchSegment performs search on a single segment
func (p *ParallelSearch) searchSegment(ctx context.Context, segment ParallelSearchSegment) ([]*types.SearchResult, error) {
	// Get vectors from segment
	vectors, err := segment.Segment.GetVectors(ctx)
	if err != nil {
		return nil, err
	}

	// If no vectors in segment, return empty results
	if len(vectors) == 0 {
		return []*types.SearchResult{}, nil
	}

	// Perform linear search on segment vectors
	results := p.linear.SearchVectors(segment.Query, vectors, segment.K)

	return results, nil
}

// GetStatus returns the current status of the parallel search
func (p *ParallelSearch) GetStatus() *types.ParallelSearchStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return &types.ParallelSearchStatus{
		Started: p.started,
		Linear:  p.linear.GetStatus(),
	}
}