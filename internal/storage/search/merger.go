package search

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrInvalidMergeStrategy = errors.New("invalid merge strategy")
	ErrInvalidResults       = errors.New("invalid results")
	ErrMergeFailed          = errors.New("merge failed")
	ErrNoResultsToMerge     = errors.New("no results to merge")
	ErrDuplicateDetection   = errors.New("duplicate detection failed")
)

// MergeStrategy represents a result merge strategy
type MergeStrategy string

const (
	MergeStrategyRank      MergeStrategy = "rank"
	MergeStrategyDistance  MergeStrategy = "distance"
	MergeStrategyScore     MergeStrategy = "score"
	MergeStrategyWeighted  MergeStrategy = "weighted"
	MergeStrategyReciprocal MergeStrategy = "reciprocal"
)

// MergeConfig represents the merge configuration
type MergeConfig struct {
	Strategy           MergeStrategy `yaml:"strategy" json:"strategy"`
	EnableDeduplication bool         `yaml:"enable_deduplication" json:"enable_deduplication"`
	DeduplicationKey   string        `yaml:"deduplication_key" json:"deduplication_key"`
	MaxResults         int           `yaml:"max_results" json:"max_results"`
	EnableNormalization bool         `yaml:"enable_normalization" json:"enable_normalization"`
	NormalizationMethod string       `yaml:"normalization_method" json:"normalization_method"`
	EnableValidation    bool         `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultMergeConfig returns the default merge configuration
func DefaultMergeConfig() *MergeConfig {
	return &MergeConfig{
		Strategy:           MergeStrategyScore,
		EnableDeduplication: true,
		DeduplicationKey:   "id",
		MaxResults:         1000,
		EnableNormalization: true,
		NormalizationMethod: "min_max",
		EnableValidation:    true,
	}
}

// ResultMerger represents a result merger
type ResultMerger struct {
	config *MergeConfig
	logger logging.Logger
	metrics *metrics.StorageMetrics
}

// MergeStats represents merge statistics
type MergeStats struct {
	TotalMerges      int64     `json:"total_merges"`
	ResultsMerged    int64     `json:"results_merged"`
	DuplicatesFound  int64     `json:"duplicates_found"`
	ResultsDeduped   int64     `json:"results_deduped"`
	AverageLatency   float64   `json:"average_latency"`
	LastMergeAt      time.Time `json:"last_merge_at"`
	FailureCount     int64     `json:"failure_count"`
}

// NewResultMerger creates a new result merger
func NewResultMerger(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*ResultMerger, error) {
	mergeConfig := DefaultMergeConfig()
	
	if cfg != nil {
		if mergeCfg, ok := cfg.Get("merge"); ok {
			if cfgMap, ok := mergeCfg.(map[string]interface{}); ok {
				if strategy, ok := cfgMap["strategy"].(string); ok {
					mergeConfig.Strategy = MergeStrategy(strategy)
				}
				if enableDeduplication, ok := cfgMap["enable_deduplication"].(bool); ok {
					mergeConfig.EnableDeduplication = enableDeduplication
				}
				if deduplicationKey, ok := cfgMap["deduplication_key"].(string); ok {
					mergeConfig.DeduplicationKey = deduplicationKey
				}
				if maxResults, ok := cfgMap["max_results"].(int); ok {
					mergeConfig.MaxResults = maxResults
				}
				if enableNormalization, ok := cfgMap["enable_normalization"].(bool); ok {
					mergeConfig.EnableNormalization = enableNormalization
				}
				if normalizationMethod, ok := cfgMap["normalization_method"].(string); ok {
					mergeConfig.NormalizationMethod = normalizationMethod
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					mergeConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateMergeConfig(mergeConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMergeStrategy, err)
	}
	
	merger := &ResultMerger{
		config: mergeConfig,
		logger: logger,
		metrics: metrics,
	}
	
	merger.logger.Info("Created result merger",
		"strategy", mergeConfig.Strategy,
		"enable_deduplication", mergeConfig.EnableDeduplication,
		"deduplication_key", mergeConfig.DeduplicationKey,
		"max_results", mergeConfig.MaxResults,
		"enable_normalization", mergeConfig.EnableNormalization,
		"normalization_method", mergeConfig.NormalizationMethod)
	
	return merger, nil
}

// validateMergeConfig validates the merge configuration
func validateMergeConfig(cfg *MergeConfig) error {
	if cfg.Strategy != MergeStrategyRank &&
		cfg.Strategy != MergeStrategyDistance &&
		cfg.Strategy != MergeStrategyScore &&
		cfg.Strategy != MergeStrategyWeighted &&
		cfg.Strategy != MergeStrategyReciprocal {
		return fmt.Errorf("unsupported merge strategy: %s", cfg.Strategy)
	}
	
	if cfg.MaxResults <= 0 {
		return errors.New("max results must be positive")
	}
	
	if cfg.EnableDeduplication && cfg.DeduplicationKey == "" {
		return errors.New("deduplication key is required when deduplication is enabled")
	}
	
	if cfg.EnableNormalization {
		if cfg.NormalizationMethod != "min_max" &&
			cfg.NormalizationMethod != "z_score" &&
			cfg.NormalizationMethod != "unit_vector" {
			return fmt.Errorf("unsupported normalization method: %s", cfg.NormalizationMethod)
		}
	}
	
	return nil
}

// Merge merges multiple result sets
func (m *ResultMerger) Merge(resultSets [][]*SearchResult) ([]*SearchResult, error) {
	if m.config.EnableValidation {
		if err := m.validateResultSets(resultSets); err != nil {
			m.metrics.Errors.Inc("merge", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidResults, err)
		}
	}
	
	if len(resultSets) == 0 {
		return nil, ErrNoResultsToMerge
	}
	
	start := time.Now()
	
	// Flatten all results
	var allResults []*SearchResult
	for _, resultSet := range resultSets {
		allResults = append(allResults, resultSet...)
	}
	
	if len(allResults) == 0 {
		return []*SearchResult{}, nil
	}
	
	// Deduplicate if enabled
	if m.config.EnableDeduplication {
		var err error
		allResults, err = m.deduplicateResults(allResults)
		if err != nil {
			m.metrics.Errors.Inc("merge", "deduplication_failed")
			return nil, fmt.Errorf("%w: %v", ErrDuplicateDetection, err)
		}
	}
	
	// Normalize if enabled
	if m.config.EnableNormalization {
		allResults = m.normalizeResults(allResults)
	}
	
	// Sort results based on strategy
	switch m.config.Strategy {
	case MergeStrategyRank:
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].Rank < allResults[j].Rank
		})
	case MergeStrategyDistance:
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].Distance < allResults[j].Distance
		})
	case MergeStrategyScore:
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].Score > allResults[j].Score
		})
	case MergeStrategyWeighted:
		allResults = m.weightedMerge(allResults)
	case MergeStrategyReciprocal:
		allResults = m.reciprocalMerge(allResults)
	}
	
	// Apply max results limit
	if m.config.MaxResults > 0 && len(allResults) > m.config.MaxResults {
		allResults = allResults[:m.config.MaxResults]
	}
	
	// Update ranks
	for i, result := range allResults {
		result.Rank = i + 1
	}
	
	duration := time.Since(start)
	
	// Update metrics
	m.metrics.MergeOperations.Inc("merge", "merge_results")
	m.metrics.MergeLatency.Observe(duration.Seconds())
	metrics.ResultsMerged.Add("merge", "results_merged", int64(len(allResults)))
	
	return allResults, nil
}

// MergeWithWeights merges results with weights
func (m *ResultMerger) MergeWithWeights(resultSets [][]*SearchResult, weights []float32) ([]*SearchResult, error) {
	if len(resultSets) != len(weights) {
		return nil, fmt.Errorf("%w: result sets and weights count mismatch", ErrInvalidResults)
	}
	
	// Apply weights to each result set
	for i, resultSet := range resultSets {
		for _, result := range resultSet {
			result.Score *= weights[i]
		}
	}
	
	return m.Merge(resultSets)
}

// MergeDistanceBased merges results based on distance
func (m *ResultMerger) MergeDistanceBased(resultSets [][]*SearchResult) ([]*SearchResult, error) {
	// Temporarily set strategy to distance
	oldStrategy := m.config.Strategy
	m.config.Strategy = MergeStrategyDistance
	
	results, err := m.Merge(resultSets)
	if err != nil {
		m.config.Strategy = oldStrategy
		return nil, err
	}
	
	// Restore original strategy
	m.config.Strategy = oldStrategy
	
	return results, nil
}

// MergeScoreBased merges results based on score
func (m *ResultMerger) MergeScoreBased(resultSets [][]*SearchResult) ([]*SearchResult, error) {
	// Temporarily set strategy to score
	oldStrategy := m.config.Strategy
	m.config.Strategy = MergeStrategyScore
	
	results, err := m.Merge(resultSets)
	if err != nil {
		m.config.Strategy = oldStrategy
		return nil, err
	}
	
	// Restore original strategy
	m.config.Strategy = oldStrategy
	
	return results, nil
}

// MergeHybrid merges results using hybrid approach
func (m *ResultMerger) MergeHybrid(resultSets [][]*SearchResult, distanceWeight, scoreWeight float32) ([]*SearchResult, error) {
	// Combine distance and score with weights
	var allResults []*SearchResult
	for _, resultSet := range resultSets {
		for _, result := range resultSet {
			// Normalize distance and score to [0, 1] range
			normalizedDistance := result.Distance / (result.Distance + 1.0)
			normalizedScore := result.Score / (result.Score + 1.0)
			
			// Calculate hybrid score
			hybridScore := distanceWeight*normalizedDistance + scoreWeight*normalizedScore
			result.Score = hybridScore
		}
		allResults = append(allResults, resultSet...)
	}
	
	// Sort by hybrid score
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Score > allResults[j].Score
	})
	
	// Apply max results limit
	if m.config.MaxResults > 0 && len(allResults) > m.config.MaxResults {
		allResults = allResults[:m.config.MaxResults]
	}
	
	// Update ranks
	for i, result := range allResults {
		result.Rank = i + 1
	}
	
	return allResults, nil
}

// validateResultSets validates result sets
func (m *ResultMerger) validateResultSets(resultSets [][]*SearchResult) error {
	for i, resultSet := range resultSets {
		for j, result := range resultSet {
			if result == nil {
				return fmt.Errorf("result %d in set %d is nil", j, i)
			}
			if result.Vector == nil && result.Metadata == nil {
				return fmt.Errorf("result %d in set %d has no vector or metadata", j, i)
			}
		}
	}
	return nil
}

// deduplicateResults removes duplicate results
func (m *ResultMerger) deduplicateResults(results []*SearchResult) ([]*SearchResult, error) {
	seen := make(map[string]bool)
	deduped := make([]*SearchResult, 0, len(results))
	duplicatesFound := 0
	
	for _, result := range results {
		key := m.getDeduplicationKey(result)
		if seen[key] {
			duplicatesFound++
			continue
		}
		
		seen[key] = true
		deduped = append(deduped, result)
	}
	
	m.metrics.DuplicatesFound.Add("merge", "duplicates_found", int64(duplicatesFound))
	m.metrics.ResultsDeduped.Add("merge", "results_deduped", int64(len(deduped)))
	
	return deduped, nil
}

// getDeduplicationKey returns the deduplication key for a result
func (m *ResultMerger) getDeduplicationKey(result *SearchResult) string {
	switch m.config.DeduplicationKey {
	case "id":
		if result.Vector != nil {
			return result.Vector.ID
		}
		return fmt.Sprintf("%v", result.Metadata)
	case "metadata":
		return fmt.Sprintf("%v", result.Metadata)
	default:
		// Default to vector ID if available, otherwise use metadata
		if result.Vector != nil {
			return result.Vector.ID
		}
		return fmt.Sprintf("%v", result.Metadata)
	}
}

// normalizeResults normalizes result scores
func (m *ResultMerger) normalizeResults(results []*SearchResult) []*SearchResult {
	if len(results) == 0 {
		return results
	}
	
	switch m.config.NormalizationMethod {
	case "min_max":
		return m.minMaxNormalization(results)
	case "z_score":
		return m.zScoreNormalization(results)
	case "unit_vector":
		return m.unitVectorNormalization(results)
	default:
		return results
	}
}

// minMaxNormalization performs min-max normalization
func (m *ResultMerger) minMaxNormalization(results []*SearchResult) []*SearchResult {
	// Find min and max scores
	var minScore, maxScore float32
	if len(results) > 0 {
		minScore = results[0].Score
		maxScore = results[0].Score
	}
	
	for _, result := range results {
		if result.Score < minScore {
			minScore = result.Score
		}
		if result.Score > maxScore {
			maxScore = result.Score
		}
	}
	
	// Normalize scores
	if maxScore != minScore {
		for _, result := range results {
			result.Score = (result.Score - minScore) / (maxScore - minScore)
		}
	} else {
		// All scores are the same
		for _, result := range results {
			result.Score = 0.5
		}
	}
	
	return results
}

// zScoreNormalization performs z-score normalization
func (m *ResultMerger) zScoreNormalization(results []*SearchResult) []*SearchResult {
	if len(results) == 0 {
		return results
	}
	
	// Calculate mean and standard deviation
	var sum, sumSquares float32
	for _, result := range results {
		sum += result.Score
		sumSquares += result.Score * result.Score
	}
	
	mean := sum / float32(len(results))
	variance := sumSquares/float32(len(results)) - mean*mean
	stdDev := float32(0.0)
	if variance > 0 {
		stdDev = float32(math.Sqrt(float64(variance)))
	}
	
	// Normalize scores
	if stdDev > 0 {
		for _, result := range results {
			result.Score = (result.Score - mean) / stdDev
		}
	} else {
		// All scores are the same
		for _, result := range results {
			result.Score = 0.0
		}
	}
	
	return results
}

// unitVectorNormalization performs unit vector normalization
func (m *ResultMerger) unitVectorNormalization(results []*SearchResult) []*SearchResult {
	if len(results) == 0 {
		return results
	}
	
	// Calculate magnitude
	var magnitude float32
	for _, result := range results {
		magnitude += result.Score * result.Score
	}
	magnitude = float32(math.Sqrt(float64(magnitude)))
	
	// Normalize scores
	if magnitude > 0 {
		for _, result := range results {
			result.Score = result.Score / magnitude
		}
	} else {
		// All scores are zero
		for _, result := range results {
			result.Score = 0.0
		}
	}
	
	return results
}

// weightedMerge performs weighted merge
func (m *ResultMerger) weightedMerge(results []*SearchResult) []*SearchResult {
	// Calculate weights based on rank
	weights := make([]float32, len(results))
	for i, result := range results {
		// Higher rank gets higher weight
		weights[i] = 1.0 / float32(result.Rank)
	}
	
	// Apply weights
	for i, result := range results {
		result.Score *= weights[i]
	}
	
	// Sort by weighted score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	return results
}

// reciprocalMerge performs reciprocal rank fusion
func (m *ResultMerger) reciprocalMerge(results []*SearchResult) []*SearchResult {
	// Group results by deduplication key
	groups := make(map[string][]*SearchResult)
	for _, result := range results {
		key := m.getDeduplicationKey(result)
		groups[key] = append(groups[key], result)
	}
	
	// Calculate reciprocal rank fusion scores
	var fusedResults []*SearchResult
	for key, group := range groups {
		var rrfScore float32
		for _, result := range group {
			rrfScore += 1.0 / float32(result.Rank+60) // +60 to avoid division by zero
		}
		
		// Use the best result from the group
		bestResult := group[0]
		for _, result := range group {
			if result.Rank < bestResult.Rank {
				bestResult = result
			}
		}
		
		bestResult.Score = rrfScore
		fusedResults = append(fusedResults, bestResult)
	}
	
	// Sort by RRF score
	sort.Slice(fusedResults, func(i, j int) bool {
		return fusedResults[i].Score > fusedResults[j].Score
	})
	
	return fusedResults
}

// GetStats returns merge statistics
func (m *ResultMerger) GetStats() *MergeStats {
	return &MergeStats{
		TotalMerges:     m.metrics.MergeOperations.Get("merge", "merge_results"),
		ResultsMerged:   m.metrics.ResultsMerged.Get("merge", "results_merged"),
		DuplicatesFound: m.metrics.DuplicatesFound.Get("merge", "duplicates_found"),
		ResultsDeduped:  m.metrics.ResultsDeduped.Get("merge", "results_deduped"),
		AverageLatency:  m.metrics.MergeLatency.Get("merge", "merge_results"),
		FailureCount:    m.metrics.Errors.Get("merge", "merge_failed"),
	}
}

// GetConfig returns the merge configuration
func (m *ResultMerger) GetConfig() *MergeConfig {
	// Return a copy of config
	config := *m.config
	return &config
}

// UpdateConfig updates the merge configuration
func (m *ResultMerger) UpdateConfig(config *MergeConfig) error {
	// Validate new configuration
	if err := validateMergeConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMergeStrategy, err)
	}
	
	m.config = config
	
	m.logger.Info("Updated merge configuration", "config", config)
	
	return nil
}

// GetSupportedStrategies returns a list of supported merge strategies
func (m *ResultMerger) GetSupportedStrategies() []MergeStrategy {
	return []MergeStrategy{
		MergeStrategyRank,
		MergeStrategyDistance,
		MergeStrategyScore,
		MergeStrategyWeighted,
		MergeStrategyReciprocal,
	}
}

// GetStrategyInfo returns information about a merge strategy
func (m *ResultMerger) GetStrategyInfo(strategy MergeStrategy) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch strategy {
	case MergeStrategyRank:
		info["name"] = "Rank"
		info["description"] = "Merge based on result rank"
		info["best_for"] = "Preserving original ranking order"
		info["performance"] = "Fast, simple"
		
	case MergeStrategyDistance:
		info["name"] = "Distance"
		info["description"] = "Merge based on distance metric"
		info["best_for"] = "Distance-based similarity search"
		info["performance"] = "Fast, distance-based"
		
	case MergeStrategyScore:
		info["name"] = "Score"
		info["description"] = "Merge based on similarity score"
		info["best_for"] = "Score-based similarity search"
		info["performance"] = "Fast, score-based"
		
	case MergeStrategyWeighted:
		info["name"] = "Weighted"
		info["description"] = "Merge using rank-based weights"
		info["best_for"] = "Balanced result ranking"
		info["performance"] = "Moderate, weighted"
		
	case MergeStrategyReciprocal:
		info["name"] = "Reciprocal"
		info["description"] = "Merge using reciprocal rank fusion"
		info["best_for"] = "Combining multiple ranked lists"
		info["performance"] = "Complex, fusion-based"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown merge strategy"
		info["best_for"] = "Unknown"
		info["performance"] = "Unknown"
	}
	
	return info
}

// BenchmarkMerge benchmarks merge performance
func (m *ResultMerger) BenchmarkMerge(resultSets [][]*SearchResult) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	
	if len(resultSets) == 0 {
		return results, nil
	}
	
	// Test all strategies
	strategies := []MergeStrategy{
		MergeStrategyRank,
		MergeStrategyDistance,
		MergeStrategyScore,
		MergeStrategyWeighted,
		MergeStrategyReciprocal,
	}
	
	for _, strategy := range strategies {
		// Temporarily set strategy
		oldStrategy := m.config.Strategy
		m.config.Strategy = strategy
		
		start := time.Now()
		mergedResults, err := m.Merge(resultSets)
		duration := time.Since(start)
		
		if err != nil {
			results[string(strategy)] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		
		results[string(strategy)] = map[string]interface{}{
			"duration": duration.String(),
			"results_merged": len(mergedResults),
			"results_per_second": float64(len(mergedResults)) / duration.Seconds(),
		}
		
		// Restore original strategy
		m.config.Strategy = oldStrategy
	}
	
	return results, nil
}

// Validate validates the merger state
func (m *ResultMerger) Validate() error {
	// Validate configuration
	if err := validateMergeConfig(m.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// math is used for normalization
import "math"