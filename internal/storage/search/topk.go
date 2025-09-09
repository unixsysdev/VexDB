package search

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrInvalidK          = errors.New("invalid k value")
	ErrInvalidResults    = errors.New("invalid results")
	ErrTopKFailed        = errors.New("top-k selection failed")
	ErrNoResults         = errors.New("no results to select from")
	ErrInvalidStrategy   = errors.New("invalid selection strategy")
	ErrInvalidThreshold  = errors.New("invalid threshold")
)

// SelectionStrategy represents a top-k selection strategy
type SelectionStrategy string

const (
	StrategyTopK         SelectionStrategy = "topk"
	StrategyThreshold    SelectionStrategy = "threshold"
	StrategyHybrid       SelectionStrategy = "hybrid"
	StrategyDiverse      SelectionStrategy = "diverse"
	StrategyAdaptive     SelectionStrategy = "adaptive"
)

// TopKConfig represents the top-k selection configuration
type TopKConfig struct {
	Strategy           SelectionStrategy `yaml:"strategy" json:"strategy"`
	DefaultK           int               `yaml:"default_k" json:"default_k"`
	MaxK               int               `yaml:"max_k" json:"max_k"`
	MinK               int               `yaml:"min_k" json:"min_k"`
	DefaultThreshold   float32           `yaml:"default_threshold" json:"default_threshold"`
	EnableDiversity    bool              `yaml:"enable_diversity" json:"enable_diversity"`
	DiversityMethod    string            `yaml:"diversity_method" json:"diversity_method"`
	DiversityThreshold float32           `yaml:"diversity_threshold" json:"diversity_threshold"`
	EnableAdaptive     bool              `yaml:"enable_adaptive" json:"enable_adaptive"`
	AdaptiveWindow     int               `yaml:"adaptive_window" json:"adaptive_window"`
	EnableValidation   bool              `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultTopKConfig returns the default top-k configuration
func DefaultTopKConfig() *TopKConfig {
	return &TopKConfig{
		Strategy:           StrategyTopK,
		DefaultK:           10,
		MaxK:               1000,
		MinK:               1,
		DefaultThreshold:   0.5,
		EnableDiversity:    true,
		DiversityMethod:    "angular",
		DiversityThreshold: 0.8,
		EnableAdaptive:     true,
		AdaptiveWindow:     100,
		EnableValidation:   true,
	}
}

// TopKSelector represents a top-k selector
type TopKSelector struct {
	config *TopKConfig
	logger logging.Logger
	metrics *metrics.StorageMetrics
}

// TopKStats represents top-k selection statistics
type TopKStats struct {
	TotalSelections    int64     `json:"total_selections"`
	ResultsSelected    int64     `json:"results_selected"`
	ResultsRejected    int64     `json:"results_rejected"`
	DiversityApplied   int64     `json:"diversity_applied"`
	AdaptiveSelections int64     `json:"adaptive_selections"`
	AverageLatency     float64   `json:"average_latency"`
	LastSelectionAt    time.Time `json:"last_selection_at"`
	FailureCount       int64     `json:"failure_count"`
}

// NewTopKSelector creates a new top-k selector
func NewTopKSelector(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*TopKSelector, error) {
	topKConfig := DefaultTopKConfig()
	
	if cfg != nil {
		if topKCfg, ok := cfg.Get("topk"); ok {
			if cfgMap, ok := topKCfg.(map[string]interface{}); ok {
				if strategy, ok := cfgMap["strategy"].(string); ok {
					topKConfig.Strategy = SelectionStrategy(strategy)
				}
				if defaultK, ok := cfgMap["default_k"].(int); ok {
					topKConfig.DefaultK = defaultK
				}
				if maxK, ok := cfgMap["max_k"].(int); ok {
					topKConfig.MaxK = maxK
				}
				if minK, ok := cfgMap["min_k"].(int); ok {
					topKConfig.MinK = minK
				}
				if defaultThreshold, ok := cfgMap["default_threshold"].(float64); ok {
					topKConfig.DefaultThreshold = float32(defaultThreshold)
				}
				if enableDiversity, ok := cfgMap["enable_diversity"].(bool); ok {
					topKConfig.EnableDiversity = enableDiversity
				}
				if diversityMethod, ok := cfgMap["diversity_method"].(string); ok {
					topKConfig.DiversityMethod = diversityMethod
				}
				if diversityThreshold, ok := cfgMap["diversity_threshold"].(float64); ok {
					topKConfig.DiversityThreshold = float32(diversityThreshold)
				}
				if enableAdaptive, ok := cfgMap["enable_adaptive"].(bool); ok {
					topKConfig.EnableAdaptive = enableAdaptive
				}
				if adaptiveWindow, ok := cfgMap["adaptive_window"].(int); ok {
					topKConfig.AdaptiveWindow = adaptiveWindow
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					topKConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateTopKConfig(topKConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidStrategy, err)
	}
	
	selector := &TopKSelector{
		config: topKConfig,
		logger: logger,
		metrics: metrics,
	}
	
	selector.logger.Info("Created top-k selector",
		"strategy", topKConfig.Strategy,
		"default_k", topKConfig.DefaultK,
		"max_k", topKConfig.MaxK,
		"min_k", topKConfig.MinK,
		"default_threshold", topKConfig.DefaultThreshold,
		"enable_diversity", topKConfig.EnableDiversity,
		"diversity_method", topKConfig.DiversityMethod,
		"diversity_threshold", topKConfig.DiversityThreshold,
		"enable_adaptive", topKConfig.EnableAdaptive,
		"adaptive_window", topKConfig.AdaptiveWindow)
	
	return selector, nil
}

// validateTopKConfig validates the top-k configuration
func validateTopKConfig(cfg *TopKConfig) error {
	if cfg.Strategy != StrategyTopK &&
		cfg.Strategy != StrategyThreshold &&
		cfg.Strategy != StrategyHybrid &&
		cfg.Strategy != StrategyDiverse &&
		cfg.Strategy != StrategyAdaptive {
		return fmt.Errorf("unsupported selection strategy: %s", cfg.Strategy)
	}
	
	if cfg.DefaultK <= 0 {
		return errors.New("default k must be positive")
	}
	
	if cfg.MaxK <= 0 {
		return errors.New("max k must be positive")
	}
	
	if cfg.MinK <= 0 {
		return errors.New("min k must be positive")
	}
	
	if cfg.MinK > cfg.MaxK {
		return errors.New("min k cannot be greater than max k")
	}
	
	if cfg.DefaultK < cfg.MinK || cfg.DefaultK > cfg.MaxK {
		return errors.New("default k must be between min k and max k")
	}
	
	if cfg.DefaultThreshold < 0 || cfg.DefaultThreshold > 1 {
		return errors.New("default threshold must be between 0 and 1")
	}
	
	if cfg.DiversityThreshold < 0 || cfg.DiversityThreshold > 1 {
		return errors.New("diversity threshold must be between 0 and 1")
	}
	
	if cfg.AdaptiveWindow <= 0 {
		return errors.New("adaptive window must be positive")
	}
	
	return nil
}

// SelectTopK selects top-k results from a list of search results
func (s *TopKSelector) SelectTopK(results []*SearchResult, k int) ([]*SearchResult, error) {
	if s.config.EnableValidation {
		if err := s.validateResults(results); err != nil {
			s.metrics.Errors.Inc("topk", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidResults, err)
		}
	}
	
	if len(results) == 0 {
		return []*SearchResult{}, nil
	}
	
	// Validate and adjust k
	k = s.validateK(k)
	
	start := time.Now()
	
	// Apply selection strategy
	var selected []*SearchResult
	var err error
	
	switch s.config.Strategy {
	case StrategyTopK:
		selected, err = s.selectTopK(results, k)
	case StrategyThreshold:
		selected, err = s.selectThreshold(results, s.config.DefaultThreshold)
	case StrategyHybrid:
		selected, err = s.selectHybrid(results, k, s.config.DefaultThreshold)
	case StrategyDiverse:
		selected, err = s.selectDiverse(results, k)
	case StrategyAdaptive:
		selected, err = s.selectAdaptive(results, k)
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidStrategy, s.config.Strategy)
	}
	
	if err != nil {
		s.metrics.Errors.Inc("topk", "selection_failed")
		return nil, fmt.Errorf("%w: %v", ErrTopKFailed, err)
	}
	
	duration := time.Since(start)
	
	// Update metrics
	s.metrics.TopKOperations.Inc("topk", "select_topk")
	s.metrics.TopKLatency.Observe(duration.Seconds())
	s.metrics.ResultsSelected.Add("topk", "results_selected", int64(len(selected)))
	s.metrics.ResultsRejected.Add("topk", "results_rejected", int64(len(results)-len(selected)))
	
	return selected, nil
}

// SelectWithThreshold selects results based on threshold
func (s *TopKSelector) SelectWithThreshold(results []*SearchResult, threshold float32) ([]*SearchResult, error) {
	if s.config.EnableValidation {
		if err := s.validateResults(results); err != nil {
			s.metrics.Errors.Inc("topk", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidResults, err)
		}
	}
	
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("%w: threshold must be between 0 and 1", ErrInvalidThreshold)
	}
	
	if len(results) == 0 {
		return []*SearchResult{}, nil
	}
	
	start := time.Now()
	
	selected, err := s.selectThreshold(results, threshold)
	if err != nil {
		s.metrics.Errors.Inc("topk", "selection_failed")
		return nil, fmt.Errorf("%w: %v", ErrTopKFailed, err)
	}
	
	duration := time.Since(start)
	
	// Update metrics
	s.metrics.TopKOperations.Inc("topk", "select_threshold")
	s.metrics.TopKLatency.Observe(duration.Seconds())
	s.metrics.ResultsSelected.Add("topk", "results_selected", int64(len(selected)))
	s.metrics.ResultsRejected.Add("topk", "results_rejected", int64(len(results)-len(selected)))
	
	return selected, nil
}

// SelectHybrid selects results using hybrid approach
func (s *TopKSelector) SelectHybrid(results []*SearchResult, k int, threshold float32) ([]*SearchResult, error) {
	if s.config.EnableValidation {
		if err := s.validateResults(results); err != nil {
			s.metrics.Errors.Inc("topk", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidResults, err)
		}
	}
	
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("%w: threshold must be between 0 and 1", ErrInvalidThreshold)
	}
	
	if len(results) == 0 {
		return []*SearchResult{}, nil
	}
	
	// Validate and adjust k
	k = s.validateK(k)
	
	start := time.Now()
	
	selected, err := s.selectHybrid(results, k, threshold)
	if err != nil {
		s.metrics.Errors.Inc("topk", "selection_failed")
		return nil, fmt.Errorf("%w: %v", ErrTopKFailed, err)
	}
	
	duration := time.Since(start)
	
	// Update metrics
	s.metrics.TopKOperations.Inc("topk", "select_hybrid")
	s.metrics.TopKLatency.Observe(duration.Seconds())
	s.metrics.ResultsSelected.Add("topk", "results_selected", int64(len(selected)))
	s.metrics.ResultsRejected.Add("topk", "results_rejected", int64(len(results)-len(selected)))
	
	return selected, nil
}

// validateResults validates search results
func (s *TopKSelector) validateResults(results []*SearchResult) error {
	for i, result := range results {
		if result == nil {
			return fmt.Errorf("result %d is nil", i)
		}
		if result.Vector == nil && result.Metadata == nil {
			return fmt.Errorf("result %d has no vector or metadata", i)
		}
	}
	return nil
}

// validateK validates and adjusts k value
func (s *TopKSelector) validateK(k int) int {
	if k <= 0 {
		return s.config.DefaultK
	}
	if k < s.config.MinK {
		return s.config.MinK
	}
	if k > s.config.MaxK {
		return s.config.MaxK
	}
	return k
}

// selectTopK selects top-k results using min-heap approach
func (s *TopKSelector) selectTopK(results []*SearchResult, k int) ([]*SearchResult, error) {
	if k >= len(results) {
		return results, nil
	}
	
	// Use min-heap for efficient top-k selection
	h := &SearchResultHeap{}
	heap.Init(h)
	
	for _, result := range results {
		if h.Len() < k {
			heap.Push(h, result)
		} else {
			if result.Score > (*h)[0].Score {
				heap.Pop(h)
				heap.Push(h, result)
			}
		}
	}
	
	// Extract results from heap (in reverse order for descending score)
	selected := make([]*SearchResult, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		selected[i] = heap.Pop(h).(*SearchResult)
	}
	
	// Update ranks
	for i, result := range selected {
		result.Rank = i + 1
	}
	
	return selected, nil
}

// selectThreshold selects results based on threshold
func (s *TopKSelector) selectThreshold(results []*SearchResult, threshold float32) ([]*SearchResult, error) {
	var selected []*SearchResult
	
	for _, result := range results {
		if result.Score >= threshold {
			selected = append(selected, result)
		}
	}
	
	// Sort by score descending
	s.sortByScore(selected)
	
	// Update ranks
	for i, result := range selected {
		result.Rank = i + 1
	}
	
	return selected, nil
}

// selectHybrid selects results using hybrid approach (top-k + threshold)
func (s *TopKSelector) selectHybrid(results []*SearchResult, k int, threshold float32) ([]*SearchResult, error) {
	// First filter by threshold
	thresholdResults := make([]*SearchResult, 0)
	for _, result := range results {
		if result.Score >= threshold {
			thresholdResults = append(thresholdResults, result)
		}
	}
	
	// Then select top-k from threshold results
	return s.selectTopK(thresholdResults, k)
}

// selectDiverse selects results with diversity consideration
func (s *TopKSelector) selectDiverse(results []*SearchResult, k int) ([]*SearchResult, error) {
	if !s.config.EnableDiversity {
		return s.selectTopK(results, k)
	}
	
	if k >= len(results) {
		return results, nil
	}
	
	// Sort by score descending
	sorted := make([]*SearchResult, len(results))
	copy(sorted, results)
	s.sortByScore(sorted)
	
	// Select diverse results
	selected := make([]*SearchResult, 0, k)
	selected = append(selected, sorted[0]) // Always include the top result
	
	for i := 1; i < len(sorted) && len(selected) < k; i++ {
		candidate := sorted[i]
		isDiverse := true
		
		// Check diversity against already selected results
		for _, selectedResult := range selected {
			similarity := s.calculateSimilarity(candidate, selectedResult)
			if similarity > s.config.DiversityThreshold {
				isDiverse = false
				break
			}
		}
		
		if isDiverse {
			selected = append(selected, candidate)
		}
	}
	
	// Update ranks
	for i, result := range selected {
		result.Rank = i + 1
	}
	
	s.metrics.DiversityApplied.Inc("topk", "diversity_applied")
	
	return selected, nil
}

// selectAdaptive selects results using adaptive approach
func (s *TopKSelector) selectAdaptive(results []*SearchResult, k int) ([]*SearchResult, error) {
	if !s.config.EnableAdaptive {
		return s.selectTopK(results, k)
	}
	
	if len(results) <= s.config.AdaptiveWindow {
		return s.selectTopK(results, k)
	}
	
	// Analyze score distribution in the adaptive window
	window := s.config.AdaptiveWindow
	if window > len(results) {
		window = len(results)
	}
	
	// Calculate score statistics for the window
	var sum, sumSquares, minScore, maxScore float32
	for i := 0; i < window; i++ {
		score := results[i].Score
		sum += score
		sumSquares += score * score
		if i == 0 {
			minScore = score
			maxScore = score
		} else {
			if score < minScore {
				minScore = score
			}
			if score > maxScore {
				maxScore = score
			}
		}
	}
	
	mean := sum / float32(window)
	variance := sumSquares/float32(window) - mean*mean
	stdDev := float32(0.0)
	if variance > 0 {
		stdDev = float32(math.Sqrt(float64(variance)))
	}
	
	// Adaptive threshold based on score distribution
	adaptiveThreshold := mean - stdDev*0.5
	
	// Use hybrid selection with adaptive threshold
	return s.selectHybrid(results, k, adaptiveThreshold)
}

// calculateSimilarity calculates similarity between two results
func (s *TopKSelector) calculateSimilarity(result1, result2 *SearchResult) float32 {
	switch s.config.DiversityMethod {
	case "angular":
		return s.angularSimilarity(result1, result2)
	case "euclidean":
		return s.euclideanSimilarity(result1, result2)
	case "manhattan":
		return s.manhattanSimilarity(result1, result2)
	case "cosine":
		return s.cosineSimilarity(result1, result2)
	default:
		return s.angularSimilarity(result1, result2)
	}
}

// angularSimilarity calculates angular similarity between two results
func (s *TopKSelector) angularSimilarity(result1, result2 *SearchResult) float32 {
	if result1.Vector == nil || result2.Vector == nil {
		return 0.0
	}
	
	v1 := result1.Vector.Data
	v2 := result2.Vector.Data
	
	if len(v1) != len(v2) {
		return 0.0
	}
	
	var dotProduct float32
	var norm1, norm2 float32
	
	for i := 0; i < len(v1); i++ {
		dotProduct += v1[i] * v2[i]
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
	}
	
	if norm1 == 0 || norm2 == 0 {
		return 0.0
	}
	
	return dotProduct / (float32(math.Sqrt(float64(norm1))) * float32(math.Sqrt(float64(norm2))))
}

// euclideanSimilarity calculates euclidean similarity between two results
func (s *TopKSelector) euclideanSimilarity(result1, result2 *SearchResult) float32 {
	if result1.Vector == nil || result2.Vector == nil {
		return 0.0
	}
	
	v1 := result1.Vector.Data
	v2 := result2.Vector.Data
	
	if len(v1) != len(v2) {
		return 0.0
	}
	
	var distance float32
	for i := 0; i < len(v1); i++ {
		diff := v1[i] - v2[i]
		distance += diff * diff
	}
	
	distance = float32(math.Sqrt(float64(distance)))
	
	// Convert distance to similarity (lower distance = higher similarity)
	return 1.0 / (1.0 + distance)
}

// manhattanSimilarity calculates manhattan similarity between two results
func (s *TopKSelector) manhattanSimilarity(result1, result2 *SearchResult) float32 {
	if result1.Vector == nil || result2.Vector == nil {
		return 0.0
	}
	
	v1 := result1.Vector.Data
	v2 := result2.Vector.Data
	
	if len(v1) != len(v2) {
		return 0.0
	}
	
	var distance float32
	for i := 0; i < len(v1); i++ {
		distance += float32(math.Abs(float64(v1[i] - v2[i])))
	}
	
	// Convert distance to similarity (lower distance = higher similarity)
	return 1.0 / (1.0 + distance)
}

// cosineSimilarity calculates cosine similarity between two results
func (s *TopKSelector) cosineSimilarity(result1, result2 *SearchResult) float32 {
	// Cosine similarity is the same as angular similarity for normalized vectors
	return s.angularSimilarity(result1, result2)
}

// sortByScore sorts results by score in descending order
func (s *TopKSelector) sortByScore(results []*SearchResult) {
	// Use heap sort for better performance with large datasets
	h := &SearchResultHeap{}
	heap.Init(h)
	
	for _, result := range results {
		heap.Push(h, result)
	}
	
	// Extract in descending order
	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(h).(*SearchResult)
	}
}

// GetStats returns top-k selection statistics
func (s *TopKSelector) GetStats() *TopKStats {
	return &TopKStats{
		TotalSelections:    s.metrics.TopKOperations.Get("topk", "select_topk"),
		ResultsSelected:    s.metrics.ResultsSelected.Get("topk", "results_selected"),
		ResultsRejected:    s.metrics.ResultsRejected.Get("topk", "results_rejected"),
		DiversityApplied:   s.metrics.DiversityApplied.Get("topk", "diversity_applied"),
		AdaptiveSelections: s.metrics.AdaptiveSelections.Get("topk", "adaptive_selections"),
		AverageLatency:     s.metrics.TopKLatency.Get("topk", "select_topk"),
		FailureCount:       s.metrics.Errors.Get("topk", "selection_failed"),
	}
}

// GetConfig returns the top-k configuration
func (s *TopKSelector) GetConfig() *TopKConfig {
	// Return a copy of config
	config := *s.config
	return &config
}

// UpdateConfig updates the top-k configuration
func (s *TopKSelector) UpdateConfig(config *TopKConfig) error {
	// Validate new configuration
	if err := validateTopKConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStrategy, err)
	}
	
	s.config = config
	
	s.logger.Info("Updated top-k configuration", "config", config)
	
	return nil
}

// GetSupportedStrategies returns a list of supported selection strategies
func (s *TopKSelector) GetSupportedStrategies() []SelectionStrategy {
	return []SelectionStrategy{
		StrategyTopK,
		StrategyThreshold,
		StrategyHybrid,
		StrategyDiverse,
		StrategyAdaptive,
	}
}

// GetStrategyInfo returns information about a selection strategy
func (s *TopKSelector) GetStrategyInfo(strategy SelectionStrategy) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch strategy {
	case StrategyTopK:
		info["name"] = "Top-K"
		info["description"] = "Select top-k results by score"
		info["best_for"] = "Fixed result size, best matches"
		info["performance"] = "Fast, heap-based"
		
	case StrategyThreshold:
		info["name"] = "Threshold"
		info["description"] = "Select results above threshold"
		info["best_for"] = "Quality-based selection, variable results"
		info["performance"] = "Fast, linear scan"
		
	case StrategyHybrid:
		info["name"] = "Hybrid"
		info["description"] = "Combine top-k and threshold selection"
		info["best_for"] = "Balanced quality and quantity"
		info["performance"] = "Moderate, two-pass"
		
	case StrategyDiverse:
		info["name"] = "Diverse"
		info["description"] = "Select diverse results with similarity constraints"
		info["best_for"] = "Varied results, avoiding duplicates"
		info["performance"] = "Slower, similarity calculations"
		
	case StrategyAdaptive:
		info["name"] = "Adaptive"
		info["description"] = "Adaptively select based on score distribution"
		info["best_for"] = "Dynamic workloads, varying data quality"
		info["performance"] = "Moderate, statistical analysis"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown selection strategy"
		info["best_for"] = "Unknown"
		info["performance"] = "Unknown"
	}
	
	return info
}

// BenchmarkSelection benchmarks selection performance
func (s *TopKSelector) BenchmarkSelection(results []*SearchResult) (map[string]interface{}, error) {
	benchmarkResults := make(map[string]interface{})
	
	if len(results) == 0 {
		return benchmarkResults, nil
	}
	
	// Test all strategies
	strategies := []SelectionStrategy{
		StrategyTopK,
		StrategyThreshold,
		StrategyHybrid,
		StrategyDiverse,
		StrategyAdaptive,
	}
	
	k := 10 // Default k for benchmarking
	threshold := float32(0.5) // Default threshold for benchmarking
	
	for _, strategy := range strategies {
		// Temporarily set strategy
		oldStrategy := s.config.Strategy
		s.config.Strategy = strategy
		
		start := time.Now()
		var selected []*SearchResult
		var err error
		
		switch strategy {
		case StrategyTopK:
			selected, err = s.SelectTopK(results, k)
		case StrategyThreshold:
			selected, err = s.SelectWithThreshold(results, threshold)
		case StrategyHybrid:
			selected, err = s.SelectHybrid(results, k, threshold)
		case StrategyDiverse:
			selected, err = s.SelectTopK(results, k) // Diverse is applied internally
		case StrategyAdaptive:
			selected, err = s.SelectTopK(results, k) // Adaptive is applied internally
		}
		
		duration := time.Since(start)
		
		if err != nil {
			benchmarkResults[string(strategy)] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		
		benchmarkResults[string(strategy)] = map[string]interface{}{
			"duration": duration.String(),
			"results_selected": len(selected),
			"results_per_second": float64(len(selected)) / duration.Seconds(),
		}
		
		// Restore original strategy
		s.config.Strategy = oldStrategy
	}
	
	return benchmarkResults, nil
}

// Validate validates the top-k selector state
func (s *TopKSelector) Validate() error {
	// Validate configuration
	if err := validateTopKConfig(s.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// SearchResultHeap implements heap.Interface for search results
type SearchResultHeap []*SearchResult

func (h SearchResultHeap) Len() int           { return len(h) }
func (h SearchResultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h SearchResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *SearchResultHeap) Push(x interface{}) {
	*h = append(*h, x.(*SearchResult))
}

func (h *SearchResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}