package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/storage/buffer"
	"vexdb/internal/storage/segment"
	"vexdb/internal/types"
)

var (
	ErrInvalidStrategy    = errors.New("invalid search strategy")
	ErrBufferSearchFailed = errors.New("buffer search failed")
	ErrSegmentSearchFailed = errors.New("segment search failed")
	ErrMergeFailed        = errors.New("result merge failed")
	ErrNoSegments         = errors.New("no segments available")
	ErrBufferUnavailable  = errors.New("buffer unavailable")
)

// SearchStrategy represents a search strategy
type SearchStrategy string

const (
	StrategyBufferFirst  SearchStrategy = "buffer_first"
	StrategySegmentFirst SearchStrategy = "segment_first"
	StrategyParallel     SearchStrategy = "parallel"
	StrategyAdaptive     SearchStrategy = "adaptive"
)

// DualSearchConfig represents the dual search configuration
type DualSearchConfig struct {
	Strategy          SearchStrategy `yaml:"strategy" json:"strategy"`
	BufferWeight      float32        `yaml:"buffer_weight" json:"buffer_weight"`
	SegmentWeight     float32        `yaml:"segment_weight" json:"segment_weight"`
	MaxBufferResults  int            `yaml:"max_buffer_results" json:"max_buffer_results"`
	MaxSegmentResults int            `yaml:"max_segment_results" json:"max_segment_results"`
	EnableAdaptive    bool           `yaml:"enable_adaptive" json:"enable_adaptive"`
	AdaptiveThreshold float32        `yaml:"adaptive_threshold" json:"adaptive_threshold"`
	EnableCache       bool           `yaml:"enable_cache" json:"enable_cache"`
	CacheSize         int            `yaml:"cache_size" json:"cache_size"`
	EnableValidation  bool           `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultDualSearchConfig returns the default dual search configuration
func DefaultDualSearchConfig() *DualSearchConfig {
	return &DualSearchConfig{
		Strategy:          StrategyBufferFirst,
		BufferWeight:      1.0,
		SegmentWeight:     1.0,
		MaxBufferResults:  1000,
		MaxSegmentResults: 1000,
		EnableAdaptive:    true,
		AdaptiveThreshold: 0.8,
		EnableCache:       true,
		CacheSize:         5000,
		EnableValidation:  true,
	}
}

// DualSearch represents a dual search engine that searches both memory buffer and segments
type DualSearch struct {
	config        *DualSearchConfig
	linearSearch  *LinearSearch
	bufferManager *buffer.Manager
	segmentReader *segment.Reader
	cache         map[string][]*SearchResult
	mu            sync.RWMutex
	logger        logging.Logger
	metrics       *metrics.StorageMetrics
}

// DualSearchStats represents dual search statistics
type DualSearchStats struct {
	TotalSearches      int64     `json:"total_searches"`
	BufferSearches     int64     `json:"buffer_searches"`
	SegmentSearches    int64     `json:"segment_searches"`
	ParallelSearches   int64     `json:"parallel_searches"`
	AdaptiveSearches   int64     `json:"adaptive_searches"`
	CacheHits          int64     `json:"cache_hits"`
	CacheMisses        int64     `json:"cache_misses"`
	AverageLatency     float64   `json:"average_latency"`
	BufferResults      int64     `json:"buffer_results"`
	SegmentResults     int64     `json:"segment_results"`
	MergedResults      int64     `json:"merged_results"`
	LastSearchAt       time.Time `json:"last_search_at"`
	FailureCount       int64     `json:"failure_count"`
}

// NewDualSearch creates a new dual search engine
func NewDualSearch(cfg *config.Config, linearSearch *LinearSearch, bufferManager *buffer.Manager, segmentReader *segment.Reader, logger logging.Logger, metrics *metrics.StorageMetrics) (*DualSearch, error) {
	searchConfig := DefaultDualSearchConfig()
	
	if cfg != nil {
		if searchCfg, ok := cfg.Get("dual_search"); ok {
			if cfgMap, ok := searchCfg.(map[string]interface{}); ok {
				if strategy, ok := cfgMap["strategy"].(string); ok {
					searchConfig.Strategy = SearchStrategy(strategy)
				}
				if bufferWeight, ok := cfgMap["buffer_weight"].(float64); ok {
					searchConfig.BufferWeight = float32(bufferWeight)
				}
				if segmentWeight, ok := cfgMap["segment_weight"].(float64); ok {
					searchConfig.SegmentWeight = float32(segmentWeight)
				}
				if maxBufferResults, ok := cfgMap["max_buffer_results"].(int); ok {
					searchConfig.MaxBufferResults = maxBufferResults
				}
				if maxSegmentResults, ok := cfgMap["max_segment_results"].(int); ok {
					searchConfig.MaxSegmentResults = maxSegmentResults
				}
				if enableAdaptive, ok := cfgMap["enable_adaptive"].(bool); ok {
					searchConfig.EnableAdaptive = enableAdaptive
				}
				if adaptiveThreshold, ok := cfgMap["adaptive_threshold"].(float64); ok {
					searchConfig.AdaptiveThreshold = float32(adaptiveThreshold)
				}
				if enableCache, ok := cfgMap["enable_cache"].(bool); ok {
					searchConfig.EnableCache = enableCache
				}
				if cacheSize, ok := cfgMap["cache_size"].(int); ok {
					searchConfig.CacheSize = cacheSize
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					searchConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateDualSearchConfig(searchConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidStrategy, err)
	}
	
	search := &DualSearch{
		config:        searchConfig,
		linearSearch:  linearSearch,
		bufferManager: bufferManager,
		segmentReader: segmentReader,
		cache:         make(map[string][]*SearchResult),
		logger:        logger,
		metrics:       metrics,
	}
	
	// Pre-allocate cache if enabled
	if searchConfig.EnableCache && searchConfig.CacheSize > 0 {
		search.cache = make(map[string][]*SearchResult, searchConfig.CacheSize)
	}
	
	search.logger.Info("Created dual search engine",
		"strategy", searchConfig.Strategy,
		"buffer_weight", searchConfig.BufferWeight,
		"segment_weight", searchConfig.SegmentWeight,
		"max_buffer_results", searchConfig.MaxBufferResults,
		"max_segment_results", searchConfig.MaxSegmentResults,
		"enable_adaptive", searchConfig.EnableAdaptive,
		"adaptive_threshold", searchConfig.AdaptiveThreshold,
		"enable_cache", searchConfig.EnableCache,
		"cache_size", searchConfig.CacheSize)
	
	return search, nil
}

// validateDualSearchConfig validates the dual search configuration
func validateDualSearchConfig(cfg *DualSearchConfig) error {
	if cfg.Strategy != StrategyBufferFirst &&
		cfg.Strategy != StrategySegmentFirst &&
		cfg.Strategy != StrategyParallel &&
		cfg.Strategy != StrategyAdaptive {
		return fmt.Errorf("unsupported strategy: %s", cfg.Strategy)
	}
	
	if cfg.BufferWeight < 0 || cfg.BufferWeight > 1 {
		return errors.New("buffer weight must be between 0 and 1")
	}
	
	if cfg.SegmentWeight < 0 || cfg.SegmentWeight > 1 {
		return errors.New("segment weight must be between 0 and 1")
	}
	
	if cfg.MaxBufferResults <= 0 {
		return errors.New("max buffer results must be positive")
	}
	
	if cfg.MaxSegmentResults <= 0 {
		return errors.New("max segment results must be positive")
	}
	
	if cfg.AdaptiveThreshold < 0 || cfg.AdaptiveThreshold > 1 {
		return errors.New("adaptive threshold must be between 0 and 1")
	}
	
	if cfg.CacheSize < 0 {
		return errors.New("cache size must be non-negative")
	}
	
	return nil
}

// Search performs a dual search on both memory buffer and segments
func (s *DualSearch) Search(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	if s.config.EnableValidation {
		if err := query.Validate(); err != nil {
			s.metrics.Errors.Inc("dual_search", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
	}
	
	// Check cache first
	if s.config.EnableCache {
		cacheKey := s.getCacheKey(query)
		if results, exists := s.getFromCache(cacheKey); exists {
			s.metrics.CacheHits.Inc("dual_search", "cache_hit")
			return results, nil
		}
		s.metrics.CacheMisses.Inc("dual_search", "cache_miss")
	}
	
	start := time.Now()
	
	// Perform search based on strategy
	var results []*SearchResult
	var err error
	
	switch s.config.Strategy {
	case StrategyBufferFirst:
		results, err = s.bufferFirstSearch(ctx, query)
	case StrategySegmentFirst:
		results, err = s.segmentFirstSearch(ctx, query)
	case StrategyParallel:
		results, err = s.parallelSearch(ctx, query)
	case StrategyAdaptive:
		results, err = s.adaptiveSearch(ctx, query)
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidStrategy, s.config.Strategy)
	}
	
	if err != nil {
		s.metrics.Errors.Inc("dual_search", "search_failed")
		return nil, fmt.Errorf("%w: %v", ErrSearchFailed, err)
	}
	
	duration := time.Since(start)
	
	// Update metrics
	s.metrics.SearchOperations.Inc("dual_search", "search")
	s.metrics.SearchLatency.Observe(duration.Seconds())
	
	// Update cache
	if s.config.EnableCache {
		cacheKey := s.getCacheKey(query)
		s.addToCache(cacheKey, results)
	}
	
	return results, nil
}

// bufferFirstSearch performs buffer-first search
func (s *DualSearch) bufferFirstSearch(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	s.metrics.BufferSearches.Inc("dual_search", "buffer_first")
	
	// Search buffer first
	bufferResults, err := s.searchBuffer(ctx, query)
	if err != nil {
		s.logger.Error("Buffer search failed", "error", err)
		bufferResults = []*SearchResult{}
	}
	
	// If we have enough results from buffer, return them
	if len(bufferResults) >= query.Limit {
		return bufferResults[:query.Limit], nil
	}
	
	// Search segments for remaining results
	remainingLimit := query.Limit - len(bufferResults)
	segmentQuery := *query
	segmentQuery.Limit = remainingLimit
	
	segmentResults, err := s.searchSegments(ctx, &segmentQuery)
	if err != nil {
		s.logger.Error("Segment search failed", "error", err)
		segmentResults = []*SearchResult{}
	}
	
	// Merge results
	mergedResults, err := s.mergeResults(bufferResults, segmentResults, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMergeFailed, err)
	}
	
	return mergedResults, nil
}

// segmentFirstSearch performs segment-first search
func (s *DualSearch) segmentFirstSearch(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	s.metrics.SegmentSearches.Inc("dual_search", "segment_first")
	
	// Search segments first
	segmentResults, err := s.searchSegments(ctx, query)
	if err != nil {
		s.logger.Error("Segment search failed", "error", err)
		segmentResults = []*SearchResult{}
	}
	
	// If we have enough results from segments, return them
	if len(segmentResults) >= query.Limit {
		return segmentResults[:query.Limit], nil
	}
	
	// Search buffer for remaining results
	remainingLimit := query.Limit - len(segmentResults)
	bufferQuery := *query
	bufferQuery.Limit = remainingLimit
	
	bufferResults, err := s.searchBuffer(ctx, &bufferQuery)
	if err != nil {
		s.logger.Error("Buffer search failed", "error", err)
		bufferResults = []*SearchResult{}
	}
	
	// Merge results
	mergedResults, err := s.mergeResults(segmentResults, bufferResults, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMergeFailed, err)
	}
	
	return mergedResults, nil
}

// parallelSearch performs parallel search
func (s *DualSearch) parallelSearch(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	s.metrics.ParallelSearches.Inc("dual_search", "parallel")
	
	var wg sync.WaitGroup
	var bufferResults, segmentResults []*SearchResult
	var bufferErr, segmentErr error
	
	// Search buffer in parallel
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferResults, bufferErr = s.searchBuffer(ctx, query)
		if bufferErr != nil {
			s.logger.Error("Buffer search failed", "error", bufferErr)
			bufferResults = []*SearchResult{}
		}
	}()
	
	// Search segments in parallel
	wg.Add(1)
	go func() {
		defer wg.Done()
		segmentResults, segmentErr = s.searchSegments(ctx, query)
		if segmentErr != nil {
			s.logger.Error("Segment search failed", "error", segmentErr)
			segmentResults = []*SearchResult{}
		}
	}()
	
	// Wait for both searches to complete
	wg.Wait()
	
	// Merge results
	mergedResults, err := s.mergeResults(bufferResults, segmentResults, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMergeFailed, err)
	}
	
	return mergedResults, nil
}

// adaptiveSearch performs adaptive search based on buffer fill level
func (s *DualSearch) adaptiveSearch(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	s.metrics.AdaptiveSearches.Inc("dual_search", "adaptive")
	
	// Get buffer stats
	bufferStats := s.bufferManager.GetStats()
	bufferFillRatio := float32(bufferStats.CurrentSize) / float32(bufferStats.MaxSize)
	
	// Choose strategy based on buffer fill level
	if bufferFillRatio > s.config.AdaptiveThreshold {
		// Buffer is relatively full, search it first
		return s.bufferFirstSearch(ctx, query)
	} else {
		// Buffer is relatively empty, search segments first
		return s.segmentFirstSearch(ctx, query)
	}
}

// searchBuffer searches the memory buffer
func (s *DualSearch) searchBuffer(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	// Get vectors from buffer
	vectors, err := s.bufferManager.GetVectors()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBufferSearchFailed, err)
	}
	
	if len(vectors) == 0 {
		return []*SearchResult{}, nil
	}
	
	// Limit buffer results
	if len(vectors) > s.config.MaxBufferResults {
		vectors = vectors[:s.config.MaxBufferResults]
	}
	
	// Perform linear search on buffer vectors
	bufferQuery := *query
	bufferQuery.Limit = s.config.MaxBufferResults
	
	results, err := s.linearSearch.Search(&bufferQuery, vectors)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBufferSearchFailed, err)
	}
	
	// Apply buffer weight
	for _, result := range results {
		result.Score *= s.config.BufferWeight
	}
	
	s.metrics.BufferResults.Add("dual_search", "buffer_results", int64(len(results)))
	
	return results, nil
}

// searchSegments searches the segments
func (s *DualSearch) searchSegments(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	// Get segment manifests
	manifests, err := s.segmentReader.GetManifests()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSegmentSearchFailed, err)
	}
	
	if len(manifests) == 0 {
		return []*SearchResult{}, nil
	}
	
	var allResults []*SearchResult
	
	// Search each segment
	for _, manifest := range manifests {
		// Get vectors from segment
		vectors, err := s.segmentReader.GetVectors(manifest.ID)
		if err != nil {
			s.logger.Error("Failed to get vectors from segment", "segment_id", manifest.ID, "error", err)
			continue
		}
		
		if len(vectors) == 0 {
			continue
		}
		
		// Limit segment results
		if len(vectors) > s.config.MaxSegmentResults {
			vectors = vectors[:s.config.MaxSegmentResults]
		}
		
		// Perform linear search on segment vectors
		segmentQuery := *query
		segmentQuery.Limit = s.config.MaxSegmentResults
		
		results, err := s.linearSearch.Search(&segmentQuery, vectors)
		if err != nil {
			s.logger.Error("Failed to search segment", "segment_id", manifest.ID, "error", err)
			continue
		}
		
		// Apply segment weight
		for _, result := range results {
			result.Score *= s.config.SegmentWeight
		}
		
		allResults = append(allResults, results...)
	}
	
	s.metrics.SegmentResults.Add("dual_search", "segment_results", int64(len(allResults)))
	
	return allResults, nil
}

// mergeResults merges results from buffer and segments
func (s *DualSearch) mergeResults(bufferResults, segmentResults []*SearchResult, limit int) ([]*SearchResult, error) {
	// Combine all results
	allResults := append(bufferResults, segmentResults...)
	
	// Sort by score (descending)
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Score > allResults[j].Score
	})
	
	// Apply limit
	if limit > 0 && len(allResults) > limit {
		allResults = allResults[:limit]
	}
	
	// Update ranks
	for i, result := range allResults {
		result.Rank = i + 1
	}
	
	s.metrics.MergedResults.Add("dual_search", "merged_results", int64(len(allResults)))
	
	return allResults, nil
}

// getCacheKey generates a cache key for a query
func (s *DualSearch) getCacheKey(query *SearchQuery) string {
	// Include strategy in cache key
	key := fmt.Sprintf("%s_%v_%s_%d_%f", s.config.Strategy, query.QueryVector.ID, query.DistanceMetric, query.Limit, query.Threshold)
	return key
}

// getFromCache retrieves results from cache
func (s *DualSearch) getFromCache(key string) ([]*SearchResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	results, exists := s.cache[key]
	if !exists {
		return nil, false
	}
	
	// Return a copy of results
	copy := make([]*SearchResult, len(results))
	for i, result := range results {
		copy[i] = &SearchResult{
			Vector:   result.Vector,
			Distance: result.Distance,
			Score:    result.Score,
			Rank:     result.Rank,
			Metadata: result.Metadata,
		}
	}
	
	return copy, true
}

// addToCache adds results to cache
func (s *DualSearch) addToCache(key string, results []*SearchResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Simple cache management - evict oldest if cache is full
	if len(s.cache) >= s.config.CacheSize {
		// Evict a random entry (simple approach)
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}
	
	// Store a copy of results
	copy := make([]*SearchResult, len(results))
	for i, result := range results {
		copy[i] = &SearchResult{
			Vector:   result.Vector,
			Distance: result.Distance,
			Score:    result.Score,
			Rank:     result.Rank,
			Metadata: result.Metadata,
		}
	}
	
	s.cache[key] = copy
}

// GetStats returns dual search statistics
func (s *DualSearch) GetStats() *DualSearchStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return &DualSearchStats{
		TotalSearches:    s.metrics.SearchOperations.Get("dual_search", "search"),
		BufferSearches:   s.metrics.BufferSearches.Get("dual_search", "buffer_first"),
		SegmentSearches:  s.metrics.SegmentSearches.Get("dual_search", "segment_first"),
		ParallelSearches: s.metrics.ParallelSearches.Get("dual_search", "parallel"),
		AdaptiveSearches: s.metrics.AdaptiveSearches.Get("dual_search", "adaptive"),
		CacheHits:        s.metrics.CacheHits.Get("dual_search", "cache_hit"),
		CacheMisses:      s.metrics.CacheMisses.Get("dual_search", "cache_miss"),
		AverageLatency:   s.metrics.SearchLatency.Get("dual_search", "search"),
		BufferResults:    s.metrics.BufferResults.Get("dual_search", "buffer_results"),
		SegmentResults:   s.metrics.SegmentResults.Get("dual_search", "segment_results"),
		MergedResults:    s.metrics.MergedResults.Get("dual_search", "merged_results"),
		FailureCount:     s.metrics.Errors.Get("dual_search", "search_failed"),
	}
}

// GetConfig returns the dual search configuration
func (s *DualSearch) GetConfig() *DualSearchConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy of config
	config := *s.config
	return &config
}

// UpdateConfig updates the dual search configuration
func (s *DualSearch) UpdateConfig(config *DualSearchConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Validate new configuration
	if err := validateDualSearchConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStrategy, err)
	}
	
	// Clear cache if configuration changed significantly
	if config.Strategy != s.config.Strategy ||
		config.EnableCache != s.config.EnableCache ||
		config.CacheSize != s.config.CacheSize {
		s.cache = make(map[string][]*SearchResult)
		if config.EnableCache && config.CacheSize > 0 {
			s.cache = make(map[string][]*SearchResult, config.CacheSize)
		}
	}
	
	s.config = config
	
	s.logger.Info("Updated dual search configuration", "config", config)
	
	return nil
}

// ClearCache clears the search cache
func (s *DualSearch) ClearCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.cache = make(map[string][]*SearchResult)
	if s.config.EnableCache && s.config.CacheSize > 0 {
		s.cache = make(map[string][]*SearchResult, s.config.CacheSize)
	}
	
	s.logger.Info("Cleared dual search cache")
}

// GetCacheInfo returns cache information
func (s *DualSearch) GetCacheInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["size"] = len(s.cache)
	info["max_size"] = s.config.CacheSize
	info["enabled"] = s.config.EnableCache
	info["usage"] = float64(len(s.cache)) / float64(s.config.CacheSize) * 100
	
	return info
}

// Validate validates the dual search state
func (s *DualSearch) Validate() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Validate configuration
	if err := validateDualSearchConfig(s.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate cache consistency
	if s.config.EnableCache {
		for key, results := range s.cache {
			if len(results) == 0 {
				return fmt.Errorf("empty results in cache for key: %s", key)
			}
			for _, result := range results {
				if result.Vector == nil && result.Metadata == nil {
					return fmt.Errorf("invalid result in cache for key: %s", key)
				}
			}
		}
	}
	
	// Validate dependencies
	if s.linearSearch == nil {
		return errors.New("linear search is nil")
	}
	if s.bufferManager == nil {
		return errors.New("buffer manager is nil")
	}
	if s.segmentReader == nil {
		return errors.New("segment reader is nil")
	}
	
	return nil
}

// BenchmarkSearch benchmarks dual search performance
func (s *DualSearch) BenchmarkSearch(ctx context.Context, query *SearchQuery) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	
	// Test all strategies
	strategies := []SearchStrategy{
		StrategyBufferFirst,
		StrategySegmentFirst,
		StrategyParallel,
		StrategyAdaptive,
	}
	
	for _, strategy := range strategies {
		// Temporarily set strategy
		oldStrategy := s.config.Strategy
		s.config.Strategy = strategy
		
		start := time.Now()
		searchResults, err := s.Search(ctx, query)
		duration := time.Since(start)
		
		if err != nil {
			results[string(strategy)] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		
		results[string(strategy)] = map[string]interface{}{
			"duration": duration.String(),
			"results_found": len(searchResults),
		}
		
		// Restore original strategy
		s.config.Strategy = oldStrategy
	}
	
	return results, nil
}

// GetSupportedStrategies returns a list of supported search strategies
func (s *DualSearch) GetSupportedStrategies() []SearchStrategy {
	return []SearchStrategy{
		StrategyBufferFirst,
		StrategySegmentFirst,
		StrategyParallel,
		StrategyAdaptive,
	}
}

// GetStrategyInfo returns information about a search strategy
func (s *DualSearch) GetStrategyInfo(strategy SearchStrategy) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch strategy {
	case StrategyBufferFirst:
		info["name"] = "Buffer First"
		info["description"] = "Search memory buffer first, then segments"
		info["best_for"] = "Recent data, low latency queries"
		info["performance"] = "Fast for recent data"
		
	case StrategySegmentFirst:
		info["name"] = "Segment First"
		info["description"] = "Search segments first, then memory buffer"
		info["best_for"] = "Historical data, comprehensive searches"
		info["performance"] = "Consistent performance"
		
	case StrategyParallel:
		info["name"] = "Parallel"
		info["description"] = "Search buffer and segments in parallel"
		info["best_for"] = "High throughput, balanced workload"
		info["performance"] = "Highest throughput"
		
	case StrategyAdaptive:
		info["name"] = "Adaptive"
		info["description"] = "Adaptively choose strategy based on buffer fill level"
		info["best_for"] = "Dynamic workloads, varying data patterns"
		info["performance"] = "Optimized for current conditions"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown search strategy"
		info["best_for"] = "Unknown"
		info["performance"] = "Unknown"
	}
	
	return info
}

// sort is used for result sorting
import "sort"