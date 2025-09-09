
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
	"vexdb/internal/types"
)

var (
	ErrInvalidSearchInterface = errors.New("invalid search interface")
	ErrSearchNotImplemented   = errors.New("search not implemented")
	ErrIndexNotReady          = errors.New("index not ready")
	ErrInvalidIndexType       = errors.New("invalid index type")
	ErrIndexCreationFailed    = errors.New("index creation failed")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrIndexQueryFailed       = errors.New("index query failed")
)

// IndexType represents the type of search index
type IndexType string

const (
	IndexTypeLinear IndexType = "linear"
	IndexTypeIVF    IndexType = "ivf"
	IndexTypeHNSW   IndexType = "hnsw"
	IndexTypePQ     IndexType = "pq"
	IndexTypeLSH    IndexType = "lsh"
)

// IndexStatus represents the status of a search index
type IndexStatus string

const (
	IndexStatusNotReady IndexStatus = "not_ready"
	IndexStatusBuilding IndexStatus = "building"
	IndexStatusReady    IndexStatus = "ready"
	IndexStatusUpdating IndexStatus = "updating"
	IndexStatusError    IndexStatus = "error"
)

// IndexConfig represents the configuration for a search index
type IndexConfig struct {
	Type           IndexType `yaml:"type" json:"type"`
	Dimensions     int       `yaml:"dimensions" json:"dimensions"`
	Metric         string    `yaml:"metric" json:"metric"`
	BuildParams    map[string]interface{} `yaml:"build_params" json:"build_params"`
	QueryParams    map[string]interface{} `yaml:"query_params" json:"query_params"`
	EnableCache    bool      `yaml:"enable_cache" json:"enable_cache"`
	CacheSize      int       `yaml:"cache_size" json:"cache_size"`
	AutoRebuild    bool      `yaml:"auto_rebuild" json:"auto_rebuild"`
	RebuildTrigger string    `yaml:"rebuild_trigger" json:"rebuild_trigger"`
}

// IndexStats represents statistics for a search index
type IndexStats struct {
	Type           IndexType `json:"type"`
	Status         IndexStatus `json:"status"`
	VectorsCount   int64     `json:"vectors_count"`
	IndexSize      int64     `json:"index_size"`
	MemoryUsage    int64     `json:"memory_usage"`
	BuildTime      time.Duration `json:"build_time"`
	LastBuildAt    time.Time `json:"last_build_at"`
	LastQueryAt    time.Time `json:"last_query_at"`
	QueryCount     int64     `json:"query_count"`
	AverageLatency float64   `json:"average_latency"`
	ErrorCount     int64     `json:"error_count"`
}

// SearchIndex represents a search index interface
type SearchIndex interface {
	// Basic operations
	GetType() IndexType
	GetStatus() IndexStatus
	GetConfig() *IndexConfig
	GetStats() *IndexStats
	
	// Index management
	Build(vectors []*types.Vector) error
	Update(vectors []*types.Vector) error
	Delete(vectorIDs []string) error
	Clear() error
	
	// Search operations
	Search(ctx context.Context, query *SearchQuery) ([]*SearchResult, error)
	BatchSearch(ctx context.Context, queries []*SearchQuery) ([][]*SearchResult, error)
	
	// Maintenance
	Rebuild() error
	Optimize() error
	Validate() error
	
	// Lifecycle
	Start() error
	Stop() error
	IsReady() bool
}

// SearchEngine represents the main search engine
type SearchEngine struct {
	config         *config.Config
	logger         logging.Logger
	metrics        *metrics.StorageMetrics
	linearSearch   *LinearSearch
	dualSearch     *DualSearch
	resultMerger   *ResultMerger
	metadataFilter *Filter
	topKSelector   *TopKSelector
	
	// Index management
	indices        map[IndexType]SearchIndex
	activeIndex    IndexType
	mu             sync.RWMutex
	
	// Configuration
	searchConfig   *SearchConfig
}

// SearchConfig represents the search engine configuration
type SearchConfig struct {
	DefaultIndexType   IndexType `yaml:"default_index_type" json:"default_index_type"`
	EnableDualSearch   bool      `yaml:"enable_dual_search" json:"enable_dual_search"`
	EnableMetadataFilter bool    `yaml:"enable_metadata_filter" json:"enable_metadata_filter"`
	EnableTopK         bool      `yaml:"enable_top_k" json:"enable_top_k"`
	EnableParallel     bool      `yaml:"enable_parallel" json:"enable_parallel"`
	MaxConcurrentSearches int    `yaml:"max_concurrent_searches" json:"max_concurrent_searches"`
	QueryTimeout       time.Duration `yaml:"query_timeout" json:"query_timeout"`
	EnableValidation   bool      `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultSearchConfig returns the default search configuration
func DefaultSearchConfig() *SearchConfig {
	return &SearchConfig{
		DefaultIndexType:    IndexTypeLinear,
		EnableDualSearch:    true,
		EnableMetadataFilter: true,
		EnableTopK:          true,
		EnableParallel:      true,
		MaxConcurrentSearches: 100,
		QueryTimeout:        30 * time.Second,
		EnableValidation:    true,
	}
}

// NewSearchEngine creates a new search engine
func NewSearchEngine(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*SearchEngine, error) {
	searchConfig := DefaultSearchConfig()
	
	if cfg != nil {
		if searchCfg, ok := cfg.Get("search"); ok {
			if cfgMap, ok := searchCfg.(map[string]interface{}); ok {
				if defaultIndexType, ok := cfgMap["default_index_type"].(string); ok {
					searchConfig.DefaultIndexType = IndexType(defaultIndexType)
				}
				if enableDualSearch, ok := cfgMap["enable_dual_search"].(bool); ok {
					searchConfig.EnableDualSearch = enableDualSearch
				}
				if enableMetadataFilter, ok := cfgMap["enable_metadata_filter"].(bool); ok {
					searchConfig.EnableMetadataFilter = enableMetadataFilter
				}
				if enableTopK, ok := cfgMap["enable_top_k"].(bool); ok {
					searchConfig.EnableTopK = enableTopK
				}
				if enableParallel, ok := cfgMap["enable_parallel"].(bool); ok {
					searchConfig.EnableParallel = enableParallel
				}
				if maxConcurrentSearches, ok := cfgMap["max_concurrent_searches"].(int); ok {
					searchConfig.MaxConcurrentSearches = maxConcurrentSearches
				}
				if queryTimeout, ok := cfgMap["query_timeout"].(string); ok {
					if timeout, err := time.ParseDuration(queryTimeout); err == nil {
						searchConfig.QueryTimeout = timeout
					}
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					searchConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateSearchConfig(searchConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSearchInterface, err)
	}
	
	engine := &SearchEngine{
		config:       cfg,
		logger:       logger,
		metrics:      metrics,
		indices:      make(map[IndexType]SearchIndex),
		activeIndex:  searchConfig.DefaultIndexType,
		searchConfig: searchConfig,
	}
	
	// Initialize components
	if err := engine.initializeComponents(); err != nil {
		return nil, fmt.Errorf("failed to initialize search components: %w", err)
	}
	
	// Initialize indices
	if err := engine.initializeIndices(); err != nil {
		return nil, fmt.Errorf("failed to initialize search indices: %w", err)
	}
	
	engine.logger.Info("Created search engine",
		"default_index_type", searchConfig.DefaultIndexType,
		"enable_dual_search", searchConfig.EnableDualSearch,
		"enable_metadata_filter", searchConfig.EnableMetadataFilter,
		"enable_top_k", searchConfig.EnableTopK,
		"enable_parallel", searchConfig.EnableParallel,
		"max_concurrent_searches", searchConfig.MaxConcurrentSearches,
		"query_timeout", searchConfig.QueryTimeout)
	
	return engine, nil
}

// validateSearchConfig validates the search configuration
func validateSearchConfig(cfg *SearchConfig) error {
	if cfg.DefaultIndexType != IndexTypeLinear &&
		cfg.DefaultIndexType != IndexTypeIVF &&
		cfg.DefaultIndexType != IndexTypeHNSW &&
		cfg.DefaultIndexType != IndexTypePQ &&
		cfg.DefaultIndexType != IndexTypeLSH {
		return fmt.Errorf("unsupported index type: %s", cfg.DefaultIndexType)
	}
	
	if cfg.MaxConcurrentSearches <= 0 {
		return errors.New("max concurrent searches must be positive")
	}
	
	if cfg.QueryTimeout <= 0 {
		return errors.New("query timeout must be positive")
	}
	
	return nil
}

// initializeComponents initializes search components
func (e *SearchEngine) initializeComponents() error {
	var err error
	
	// Initialize linear search
	e.linearSearch, err = NewLinearSearch(e.config, e.logger, e.metrics)
	if err != nil {
		return fmt.Errorf("failed to create linear search: %w", err)
	}
	
	// Initialize dual search if enabled
	if e.searchConfig.EnableDualSearch {
		// Note: dual search requires buffer manager and segment reader
		// These will be set later when the storage engine is initialized
		e.dualSearch = nil
	}
	
	// Initialize result merger
	e.resultMerger, err = NewResultMerger(e.config, e.logger, e.metrics)
	if err != nil {
		return fmt.Errorf("failed to create result merger: %w", err)
	}
	
	// Initialize metadata filter if enabled
	if e.searchConfig.EnableMetadataFilter {
		e.metadataFilter, err = NewFilter(e.config, e.logger, e.metrics)
		if err != nil {
			return fmt.Errorf("failed to create metadata filter: %w", err)
		}
	}
	
	// Initialize top-k selector if enabled
	if e.searchConfig.EnableTopK {
		e.topKSelector, err = NewTopKSelector(e.config, e.logger, e.metrics)
		if err != nil {
			return fmt.Errorf("failed to create top-k selector: %w", err)
		}
	}
	
	return nil
}

// initializeIndices initializes search indices
func (e *SearchEngine) initializeIndices() error {
	// Always initialize linear search as the basic index
	e.indices[IndexTypeLinear] = e.linearSearch
	
	// Initialize other indices based on configuration
	// For now, we only have linear search implemented
	// Other indices (IVF, HNSW, PQ, LSH) will be added in future iterations
	
	return nil
}

// SetDualSearchComponents sets the components required for dual search
func (e *SearchEngine) SetDualSearchComponents(bufferManager interface{}, segmentReader interface{}) error {
	if !e.searchConfig.EnableDualSearch {
		return nil
	}
	
	// Type assertion for buffer manager and segment reader
	// This is a simplified approach - in a real implementation, you would have proper interfaces
	bm, ok := bufferManager.(interface{})
	if !ok {
		return errors.New("invalid buffer manager type")
	}
	
	sr, ok := segmentReader.(interface{})
	if !ok {
		return errors.New("invalid segment reader type")
	}
	
	// Create dual search with the provided components
	// Note: This is a placeholder - actual implementation would depend on the specific interfaces
	var err error
	e.dualSearch, err = NewDualSearch(e.config, e.linearSearch, bm, sr, e.logger, e.metrics)
	if err != nil {
		return fmt.Errorf("failed to create dual search: %w", err)
	}
	
	return nil
}

// Search performs a search query
func (e *SearchEngine) Search(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	if e.searchConfig.EnableValidation {
		if err := query.Validate(); err != nil {
			e.metrics.Errors.Inc("search", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidSearchQuery, err)
		}
	}
	
	start := time.Now()
	
	// Apply query timeout if set
	if e.searchConfig.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.searchConfig.QueryTimeout)
		defer cancel()
	}
	
	// Get active index
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	// Perform search
	var results []*SearchResult
	var err error
	
	if e.searchConfig.EnableDualSearch && e.dualSearch != nil {
		// Use dual search
		results, err = e.dualSearch.Search(ctx, query)
	} else {
		// Use active index
		results, err = index.Search(ctx, query)
	}
	
	if err != nil {
		e.metrics.Errors.Inc("search", "search_failed")
		return nil, fmt.Errorf("%w: %v", ErrIndexQueryFailed, err)
	}
	
	// Apply metadata filtering if enabled
	if e.searchConfig.EnableMetadataFilter && e.metadataFilter != nil && query.MetadataFilter != nil {
		// Convert search results to vectors for filtering
		vectors := make([]*types.Vector, 0, len(results))
		for _, result := range results {
			if result.Vector != nil {
				vectors = append(vectors, result.Vector)
			}
		}
		
		filteredVectors, err := e.metadataFilter.FilterVectors(vectors, query.MetadataFilter)
		if err != nil {
			e.metrics.Errors.Inc("search", "filter_failed")
			return nil, fmt.Errorf("metadata filtering failed: %w", err)
		}
		
		// Rebuild results with filtered vectors
		filteredResults := make([]*SearchResult, 0, len(filteredVectors))
		for _, vector := range filteredVectors {
			// Find corresponding result
			for _, result := range results {
				if result.Vector != nil && result.Vector.ID == vector.ID {
					filteredResults = append(filteredResults, result)
					break
				}
			}
		}
		results = filteredResults
	}
	
	// Apply top-k selection if enabled
	if e.searchConfig.EnableTopK && e.topKSelector != nil {
		results, err = e.topKSelector.SelectTopK(results, query.Limit)
		if err != nil {
			e.metrics.Errors.Inc("search", "topk_failed")
			return nil, fmt.Errorf("top-k selection failed: %w", err)
		}
	}
	
	duration := time.Since(start)
	
	// Update metrics
	e.metrics.SearchOperations.Inc("search", "search")
	e.metrics.SearchLatency.Observe(duration.Seconds())
	e.metrics.ResultsReturned.Add("search", "results_returned", int64(len(results)))
	
	return results, nil
}

// BatchSearch performs multiple search queries
func (e *SearchEngine) BatchSearch(ctx context.Context, queries []*SearchQuery) ([][]*SearchResult, error) {
	if e.searchConfig.EnableValidation {
		for i, query := range queries {
			if err := query.Validate(); err != nil {
				e.metrics.Errors.Inc("search", "validation_failed")
				return nil, fmt.Errorf("%w: query %d: %v", ErrInvalidSearchQuery, i, err)
			}
		}
	}
	
	if !e.searchConfig.EnableParallel {
		// Sequential processing
		results := make([][]*SearchResult, len(queries))
		for i, query := range queries {
			result, err := e.Search(ctx, query)
			if err != nil {
				return nil, fmt.Errorf("batch search failed at query %d: %w", i, err)
			}
			results[i] = result
		}
		return results, nil
	}
	
	// Parallel processing with concurrency limit
	semaphore := make(chan struct{}, e.searchConfig.MaxConcurrentSearches)
	results := make([][]*SearchResult, len(queries))
	errors := make([]error, len(queries))
	var wg sync.WaitGroup
	
	for i, query := range queries {
		wg.Add(1)
		go func(idx int, q *SearchQuery) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			result, err := e.Search(ctx, q)
			if err != nil {
				errors[idx] = err
				return
			}
			results[idx] = result
		}(i, query)
	}
	
	wg.Wait()
	
	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("batch search failed at query %d: %w", i, err)
		}
	}
	
	return results, nil
}

// AddVectors adds vectors to the search index
func (e *SearchEngine) AddVectors(vectors []*types.Vector) error {
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	return index.Update(vectors)
}

// DeleteVectors deletes vectors from the search index
func (e *SearchEngine) DeleteVectors(vectorIDs []string) error {
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	return index.Delete(vectorIDs)
}

// ClearIndex clears the search index
func (e *SearchEngine) ClearIndex() error {
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	return index.Clear()
}

// RebuildIndex rebuilds the search index
func (e *SearchEngine) RebuildIndex() error {
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	return index.Rebuild()
}

// OptimizeIndex optimizes the search index
func (e *SearchEngine) OptimizeIndex() error {
	e.mu.RLock()
	activeIndex := e.activeIndex
	index, exists := e.indices[activeIndex]
	e.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("%w: %s", ErrIndexNotReady, activeIndex)
	}
	
	return index.Optimize()
}

// SetActiveIndex sets the active search index
func (e *SearchEngine) SetActiveIndex(indexType IndexType) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if _, exists := e.indices[indexType]; !exists {
		return fmt.Errorf("%w: %s", ErrInvalidIndexType, indexType)
	}
	
	e.activeIndex = indexType
	e.logger.Info("Set active search index", "index_type", indexType)
	
	return nil
}

// GetActiveIndex returns the active search index type
func (e *SearchEngine) GetActiveIndex() IndexType {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	return e.activeIndex
}

// GetIndex returns a search index by type
func (e *SearchEngine) GetIndex(indexType IndexType) (SearchIndex, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	index, exists := e.indices[indexType]
	return index, exists
}

// GetAvailableIndices returns available search index types
func (e *SearchEngine) GetAvailableIndices() []IndexType {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	indices := make([]IndexType, 0, len(e.indices))
	for indexType := range e.indices {
		indices = append(indices, indexType)
	}
	
	return indices
}

// GetIndexStats returns statistics for all indices
func (e *SearchEngine) GetIndexStats() map[IndexType]*IndexStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	stats := make(map[IndexType]*IndexStats)
	for indexType, index := range e.indices {
		stats[indexType] = index.GetStats()
	}
	
	return stats
}

// GetConfig returns the search engine configuration
func (e *SearchEngine) GetConfig() *SearchConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// Return a copy of config
	config := *e.searchConfig
	return &config
}

// UpdateConfig updates the search engine configuration
func (e *SearchEngine) UpdateConfig(config *SearchConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// Validate new configuration
	if err := validateSearchConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSearchInterface, err)
	}
	
	e.searchConfig = config
	
	e.logger.Info("Updated search engine configuration", "config", config)
	
	return nil
}

// Start starts the search engine
func (e *SearchEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	for _, index := range e.indices {
		if err := index.Start(); err != nil {
			return fmt.Errorf("failed to start index %s: %w", index.GetType(), err)
		}
	}
	
	e.logger.Info("Started search engine")
	
	return nil
}

// Stop stops the search engine
func (e *SearchEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	for _, index := range e.indices {
		if err := index.Stop(); err != nil {
			e.logger.Error("Failed to stop index", "index_type", index.GetType(), "error", err)
		}
	}
	
	e.logger.Info("Stopped search engine")
	
	return nil
}

// IsReady checks if the search engine is ready
func (e *SearchEngine) IsReady() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	for _, index := range e.indices {
		if !index.IsReady() {
			return false
		}
	}
	
	return true
}

// Validate validates the search engine state
func (e *SearchEngine) Validate() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// Validate configuration
	if err := validateSearchConfig(e.searchConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate indices
	for indexType, index := range e.indices {
		if err := index.Validate(); err != nil {
			return fmt.Errorf("index %s validation failed: %w", indexType, err)
		}
	}
	
	return nil
}

// GetSupportedIndexTypes returns supported index types
func (e *SearchEngine) GetSupportedIndexTypes() []IndexType {
	return []IndexType{
		IndexTypeLinear,
		IndexTypeIVF,
		IndexTypeHNSW,
		IndexTypePQ,
		IndexTypeLSH,
	}
}

// GetIndexTypeInfo returns information about an index type
func (e *SearchEngine) GetIndexTypeInfo(indexType IndexType) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch indexType {
	case IndexTypeLinear:
		info["name"] = "Linear Search"
		info["description"] = "Brute-force linear search through all vectors"
		info["best_for"] = "Small datasets, exact search"
		info["performance"] = "O(n) time complexity"
		info["memory_usage"] = "Low"
		info["build_time"] = "Fast"
		info["implemented"] = true
		
	case IndexTypeIVF:
		info["name"] = "IVF (Inverted File Index)"
		info["description"]