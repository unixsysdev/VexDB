package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/metrics"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

var (
	ErrInvalidQuery     = errors.New("invalid query")
	ErrInvalidDistance  = errors.New("invalid distance metric")
	ErrInvalidLimit     = errors.New("invalid limit")
	ErrSearchFailed     = errors.New("search failed")
	ErrNoResults        = errors.New("no results found")
	ErrInvalidThreshold = errors.New("invalid threshold")
)

// types DistanceMetric, SearchResult, SearchQuery, MetadataFilter are defined in interface.go/filter.go

// LinearSearchConfig represents the linear search configuration
type LinearSearchConfig struct {
	EnableSIMD       bool           `yaml:"enable_simd" json:"enable_simd"`
	BatchSize        int            `yaml:"batch_size" json:"batch_size"`
	MaxConcurrency   int            `yaml:"max_concurrency" json:"max_concurrency"`
	DistanceMetric   DistanceMetric `yaml:"distance_metric" json:"distance_metric"`
	EnableCache      bool           `yaml:"enable_cache" json:"enable_cache"`
	CacheSize        int            `yaml:"cache_size" json:"cache_size"`
	EnableValidation bool           `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultLinearSearchConfig returns the default linear search configuration
func DefaultLinearSearchConfig() *LinearSearchConfig {
	return &LinearSearchConfig{
		EnableSIMD:       true,
		BatchSize:        1000,
		MaxConcurrency:   4,
		DistanceMetric:   DistanceCosine,
		EnableCache:      true,
		CacheSize:        10000,
		EnableValidation: true,
	}
}

// LinearSearch represents a linear search engine
type LinearSearch struct {
	config  *LinearSearchConfig
	cache   map[string][]*SearchResult
	vectors []*types.Vector
	mu      sync.RWMutex
	logger  *zap.Logger
	metrics *metrics.StorageMetrics
}

func (s *LinearSearch) Start() error { return nil }

func (s *LinearSearch) Stop() error { return nil }

func (s *LinearSearch) GetType() IndexType { return IndexTypeLinear }

func (s *LinearSearch) GetStatus() IndexStatus { return IndexStatusReady }

func (s *LinearSearch) GetConfig() *IndexConfig { return nil }

func (s *LinearSearch) GetStats() *IndexStats {
	return &IndexStats{Type: IndexTypeLinear, Status: IndexStatusReady}
}

func (s *LinearSearch) IsReady() bool { return true }

func (s *LinearSearch) Rebuild() error { return nil }

func (s *LinearSearch) Optimize() error { return nil }

// NewLinearSearch creates a new linear search engine
func NewLinearSearch(cfg config.Config, logger *zap.Logger, metrics *metrics.StorageMetrics) (*LinearSearch, error) {
	searchConfig := DefaultLinearSearchConfig()

	if cfg != nil {
		if searchCfg, ok := cfg.Get("linear_search"); ok {
			if cfgMap, ok := searchCfg.(map[string]interface{}); ok {
				if enableSIMD, ok := cfgMap["enable_simd"].(bool); ok {
					searchConfig.EnableSIMD = enableSIMD
				}
				if batchSize, ok := cfgMap["batch_size"].(int); ok {
					searchConfig.BatchSize = batchSize
				}
				if maxConcurrency, ok := cfgMap["max_concurrency"].(int); ok {
					searchConfig.MaxConcurrency = maxConcurrency
				}
				if distanceMetric, ok := cfgMap["distance_metric"].(string); ok {
					searchConfig.DistanceMetric = DistanceMetric(distanceMetric)
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
	if err := validateLinearSearchConfig(searchConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDistance, err)
	}

	search := &LinearSearch{
		config:  searchConfig,
		cache:   make(map[string][]*SearchResult),
		logger:  logger,
		metrics: metrics,
	}

	// Pre-allocate cache if enabled
	if searchConfig.EnableCache && searchConfig.CacheSize > 0 {
		search.cache = make(map[string][]*SearchResult, searchConfig.CacheSize)
	}

	search.logger.Info("Created linear search engine",
		zap.Bool("enable_simd", searchConfig.EnableSIMD),
		zap.Int("batch_size", searchConfig.BatchSize),
		zap.Int("max_concurrency", searchConfig.MaxConcurrency),
		zap.String("distance_metric", string(searchConfig.DistanceMetric)),
		zap.Bool("enable_cache", searchConfig.EnableCache),
		zap.Int("cache_size", searchConfig.CacheSize))

	return search, nil
}

// validateLinearSearchConfig validates the linear search configuration
func validateLinearSearchConfig(cfg *LinearSearchConfig) error {
	if cfg.DistanceMetric != DistanceEuclidean &&
		cfg.DistanceMetric != DistanceCosine &&
		cfg.DistanceMetric != DistanceManhattan &&
		cfg.DistanceMetric != DistanceDotProduct &&
		cfg.DistanceMetric != DistanceHamming &&
		cfg.DistanceMetric != DistanceJaccard {
		return fmt.Errorf("unsupported distance metric: %s", cfg.DistanceMetric)
	}

	if cfg.BatchSize <= 0 {
		return errors.New("batch size must be positive")
	}

	if cfg.MaxConcurrency <= 0 {
		return errors.New("max concurrency must be positive")
	}

	if cfg.CacheSize < 0 {
		return errors.New("cache size must be non-negative")
	}

	return nil
}

// Search performs a linear search on the given vectors
func (s *LinearSearch) searchVectors(query *SearchQuery, vectors []*types.Vector) ([]*SearchResult, error) {
	if s.config.EnableValidation {
		if err := query.Validate(); err != nil {
			s.metrics.Errors.Inc("search", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
	}

	// Check cache first
	if s.config.EnableCache {
		cacheKey := s.getCacheKey(query)
		if results, exists := s.getFromCache(cacheKey); exists {
			s.metrics.CacheHits.Inc("search", "linear_search")
			return results, nil
		}
		s.metrics.CacheMisses.Inc("search", "linear_search")
	}

	start := time.Now()

	// Perform search
	results, err := s.performSearch(query, vectors)
	if err != nil {
		s.metrics.Errors.Inc("search", "search_failed")
		return nil, fmt.Errorf("%w: %v", ErrSearchFailed, err)
	}

	duration := time.Since(start)

	// Update metrics
	s.metrics.SearchOperations.Inc("search", "linear_search")
	s.metrics.SearchLatency.Observe(duration.Seconds(), "search", "linear_search")

	// Update cache
	if s.config.EnableCache {
		cacheKey := s.getCacheKey(query)
		s.addToCache(cacheKey, results)
	}

	return results, nil
}

// performSearch performs the actual search (internal)
func (s *LinearSearch) performSearch(query *SearchQuery, vectors []*types.Vector) ([]*SearchResult, error) {
	if len(vectors) == 0 {
		return nil, ErrNoResults
	}

	// Calculate distances
	results := make([]*SearchResult, 0, len(vectors))

	if s.config.EnableSIMD {
		// Use SIMD-optimized search if available
		simdResults, err := s.simdSearch(query, vectors)
		if err == nil {
			results = simdResults
		} else {
			// Fall back to regular search if SIMD fails
			s.logger.Warn("SIMD search failed, falling back to regular search", zap.Error(err))
			results = s.regularSearch(query, vectors)
		}
	} else {
		// Use regular search
		results = s.regularSearch(query, vectors)
	}

	// Sort results by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	// Apply limit
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	// Apply threshold
	if query.Threshold > 0 {
		filtered := make([]*SearchResult, 0, len(results))
		for _, result := range results {
			if result.Distance <= float64(query.Threshold) {
				filtered = append(filtered, result)
			}
		}
		results = filtered
	}

	// Set ranks
	for i, result := range results {
		result.Rank = i + 1
	}

	return results, nil
}

// regularSearch performs regular (non-SIMD) search
func (s *LinearSearch) regularSearch(query *SearchQuery, vectors []*types.Vector) []*SearchResult {
	results := make([]*SearchResult, 0, len(vectors))

	for _, vector := range vectors {
		distance, err := s.calculateDistance(query.QueryVector, vector, query.DistanceMetric)
		if err != nil {
			s.logger.Error("Failed to calculate distance", zap.String("vector_id", vector.ID), zap.Error(err))
			continue
		}

		score := s.calculateScore(distance, query.DistanceMetric)

		result := &SearchResult{
			Vector:   vector,
			Distance: float64(distance),
			Score:    score,
			Metadata: vector.Metadata,
		}

		if !query.IncludeVector {
			result.Vector = nil
		}

		results = append(results, result)
	}

	return results
}

// simdSearch performs SIMD-optimized search
func (s *LinearSearch) simdSearch(query *SearchQuery, vectors []*types.Vector) ([]*SearchResult, error) {
	// This would implement SIMD-optimized distance calculations
	// For now, we'll fall back to regular search
	// In a real implementation, you would use a library like github.com/klauspost/cpuid/v2
	// and implement SIMD instructions for distance calculations
	return s.regularSearch(query, vectors), nil
}

// calculateDistance calculates the distance between two vectors
func (s *LinearSearch) calculateDistance(v1, v2 *types.Vector, metric DistanceMetric) (float32, error) {
	if len(v1.Data) != len(v2.Data) {
		return 0, ErrInvalidVector
	}

	switch metric {
	case DistanceEuclidean:
		return s.euclideanDistance(v1.Data, v2.Data), nil
	case DistanceCosine:
		return s.cosineDistance(v1.Data, v2.Data), nil
	case DistanceManhattan:
		return s.manhattanDistance(v1.Data, v2.Data), nil
	case DistanceDotProduct:
		return s.dotProductDistance(v1.Data, v2.Data), nil
	case DistanceHamming:
		return s.hammingDistance(v1.Data, v2.Data), nil
	case DistanceJaccard:
		return s.jaccardDistance(v1.Data, v2.Data), nil
	default:
		return 0, fmt.Errorf("%w: %s", ErrInvalidDistance, metric)
	}
}

// euclideanDistance calculates Euclidean distance between two vectors
func (s *LinearSearch) euclideanDistance(v1, v2 []float32) float32 {
	var sum float32
	for i := 0; i < len(v1); i++ {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// cosineDistance calculates cosine distance between two vectors
func (s *LinearSearch) cosineDistance(v1, v2 []float32) float32 {
	var dotProduct float32
	var norm1 float32
	var norm2 float32

	for i := 0; i < len(v1); i++ {
		dotProduct += v1[i] * v2[i]
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 1.0
	}

	cosine := dotProduct / (float32(math.Sqrt(float64(norm1))) * float32(math.Sqrt(float64(norm2))))
	return 1.0 - cosine
}

// manhattanDistance calculates Manhattan distance between two vectors
func (s *LinearSearch) manhattanDistance(v1, v2 []float32) float32 {
	var sum float32
	for i := 0; i < len(v1); i++ {
		sum += float32(math.Abs(float64(v1[i] - v2[i])))
	}
	return sum
}

// dotProductDistance calculates dot product distance between two vectors
func (s *LinearSearch) dotProductDistance(v1, v2 []float32) float32 {
	var dotProduct float32
	for i := 0; i < len(v1); i++ {
		dotProduct += v1[i] * v2[i]
	}
	return -dotProduct // Negative because higher dot product means more similar
}

// hammingDistance calculates Hamming distance between two vectors
func (s *LinearSearch) hammingDistance(v1, v2 []float32) float32 {
	var distance float32
	for i := 0; i < len(v1); i++ {
		if v1[i] != v2[i] {
			distance++
		}
	}
	return distance
}

// jaccardDistance calculates Jaccard distance between two vectors
func (s *LinearSearch) jaccardDistance(v1, v2 []float32) float32 {
	var intersection float32
	var union float32

	for i := 0; i < len(v1); i++ {
		if v1[i] > 0 && v2[i] > 0 {
			intersection++
		}
		if v1[i] > 0 || v2[i] > 0 {
			union++
		}
	}

	if union == 0 {
		return 0.0
	}

	return 1.0 - (intersection / union)
}

// calculateScore calculates a similarity score from distance
func (s *LinearSearch) calculateScore(distance float32, metric DistanceMetric) float32 {
	switch metric {
	case DistanceEuclidean, DistanceManhattan, DistanceHamming, DistanceJaccard:
		// For these metrics, lower distance means higher similarity
		return 1.0 / (1.0 + distance)
	case DistanceCosine:
		// For cosine distance, lower distance means higher similarity
		return 1.0 - distance
	case DistanceDotProduct:
		// For dot product distance, higher (less negative) distance means higher similarity
		return 1.0 + distance
	default:
		return 0.0
	}
}

// getCacheKey generates a cache key for a query
func (s *LinearSearch) getCacheKey(query *SearchQuery) string {
	// Simple cache key generation
	// In a real implementation, you would use a more sophisticated approach
	key := fmt.Sprintf("%v_%s_%d_%f", query.QueryVector.ID, query.DistanceMetric, query.Limit, query.Threshold)
	return key
}

// getFromCache retrieves results from cache
func (s *LinearSearch) getFromCache(key string) ([]*SearchResult, bool) {
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
func (s *LinearSearch) addToCache(key string, results []*SearchResult) {
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

// UpdateConfig updates the linear search configuration
func (s *LinearSearch) UpdateConfig(config *LinearSearchConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate new configuration
	if err := validateLinearSearchConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDistance, err)
	}

	// Clear cache if configuration changed significantly
	if config.DistanceMetric != s.config.DistanceMetric ||
		config.EnableCache != s.config.EnableCache ||
		config.CacheSize != s.config.CacheSize {
		s.cache = make(map[string][]*SearchResult)
		if config.EnableCache && config.CacheSize > 0 {
			s.cache = make(map[string][]*SearchResult, config.CacheSize)
		}
	}

	s.config = config

	s.logger.Info("Updated linear search configuration", zap.Any("config", config))

	return nil
}

// ClearCache clears the search cache
func (s *LinearSearch) ClearCache() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache = make(map[string][]*SearchResult)
	if s.config.EnableCache && s.config.CacheSize > 0 {
		s.cache = make(map[string][]*SearchResult, s.config.CacheSize)
	}

	s.logger.Info("Cleared linear search cache")
}

// Build replaces the current vector set
func (s *LinearSearch) Build(vectors []*types.Vector) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = append([]*types.Vector(nil), vectors...)
	return nil
}

// Update adds new vectors to the search set
func (s *LinearSearch) Update(vectors []*types.Vector) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = append(s.vectors, vectors...)
	return nil
}

// Delete removes vectors with the given IDs
func (s *LinearSearch) Delete(ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	idset := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idset[id] = struct{}{}
	}
	filtered := s.vectors[:0]
	for _, v := range s.vectors {
		if _, ok := idset[v.ID]; !ok {
			filtered = append(filtered, v)
		}
	}
	s.vectors = filtered
	return nil
}

// Clear removes all vectors
func (s *LinearSearch) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = nil
	s.cache = make(map[string][]*SearchResult)
	return nil
}

// Search implements the SearchIndex interface.
func (s *LinearSearch) Search(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	s.mu.RLock()
	vectors := make([]*types.Vector, len(s.vectors))
	copy(vectors, s.vectors)
	s.mu.RUnlock()
	return s.searchVectors(query, vectors)
}

// BatchSearch implements the SearchIndex interface.
func (s *LinearSearch) BatchSearch(ctx context.Context, queries []*SearchQuery) ([][]*SearchResult, error) {
	results := make([][]*SearchResult, len(queries))
	for i, q := range queries {
		r, err := s.Search(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("batch search failed at query %d: %w", i, err)
		}
		results[i] = r
	}
	return results, nil
}

// GetCacheInfo returns cache information
func (s *LinearSearch) GetCacheInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := make(map[string]interface{})
	info["size"] = len(s.cache)
	info["max_size"] = s.config.CacheSize
	info["enabled"] = s.config.EnableCache
	info["usage"] = float64(len(s.cache)) / float64(s.config.CacheSize) * 100

	return info
}

// Validate validates the linear search state
func (s *LinearSearch) Validate() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Validate configuration
	if err := validateLinearSearchConfig(s.config); err != nil {
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

	return nil
}

// BenchmarkSearch benchmarks search performance
func (s *LinearSearch) BenchmarkSearch(query *SearchQuery, vectors []*types.Vector) (map[string]interface{}, error) {
	results := make(map[string]interface{})

	if len(vectors) == 0 {
		return results, nil
	}

	// Test all distance metrics
	metrics := []DistanceMetric{
		DistanceEuclidean,
		DistanceCosine,
		DistanceManhattan,
		DistanceDotProduct,
		DistanceHamming,
		DistanceJaccard,
	}

	for _, metric := range metrics {
		// Temporarily set metric
		oldMetric := query.DistanceMetric
		query.DistanceMetric = metric

		start := time.Now()
		searchResults, err := s.searchVectors(query, vectors)
		duration := time.Since(start)

		if err != nil {
			results[string(metric)] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}

		results[string(metric)] = map[string]interface{}{
			"duration":           duration.String(),
			"vectors_per_second": float64(len(vectors)) / duration.Seconds(),
			"results_found":      len(searchResults),
		}

		// Restore original metric
		query.DistanceMetric = oldMetric
	}

	return results, nil
}

// GetSupportedMetrics returns a list of supported distance metrics
func (s *LinearSearch) GetSupportedMetrics() []DistanceMetric {
	return []DistanceMetric{
		DistanceEuclidean,
		DistanceCosine,
		DistanceManhattan,
		DistanceDotProduct,
		DistanceHamming,
		DistanceJaccard,
	}
}

// GetMetricInfo returns information about a distance metric
func (s *LinearSearch) GetMetricInfo(metric DistanceMetric) map[string]interface{} {
	info := make(map[string]interface{})

	switch metric {
	case DistanceEuclidean:
		info["name"] = "Euclidean"
		info["description"] = "Euclidean distance (L2 norm)"
		info["range"] = "[0, +∞)"
		info["lower_is_better"] = true
		info["best_for"] = "Geometric similarity, continuous data"

	case DistanceCosine:
		info["name"] = "Cosine"
		info["description"] = "Cosine similarity distance"
		info["range"] = "[0, 2]"
		info["lower_is_better"] = true
		info["best_for"] = "Text similarity, direction-based comparison"

	case DistanceManhattan:
		info["name"] = "Manhattan"
		info["description"] = "Manhattan distance (L1 norm)"
		info["range"] = "[0, +∞)"
		info["lower_is_better"] = true
		info["best_for"] = "Grid-based data, discrete features"

	case DistanceDotProduct:
		info["name"] = "Dot Product"
		info["description"] = "Dot product distance"
		info["range"] = "(-∞, +∞)"
		info["lower_is_better"] = false
		info["best_for"] = "Neural network embeddings, magnitude matters"

	case DistanceHamming:
		info["name"] = "Hamming"
		info["description"] = "Hamming distance"
		info["range"] = "[0, +∞)"
		info["lower_is_better"] = true
		info["best_for"] = "Binary data, categorical features"

	case DistanceJaccard:
		info["name"] = "Jaccard"
		info["description"] = "Jaccard distance"
		info["range"] = "[0, 1]"
		info["lower_is_better"] = true
		info["best_for"] = "Set similarity, binary data"

	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown distance metric"
		info["range"] = "Unknown"
		info["lower_is_better"] = true
		info["best_for"] = "Unknown"
	}

	return info
}

// Validate validates the search query
