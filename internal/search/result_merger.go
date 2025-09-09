package search

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

// ResultMerger handles merging search results from multiple nodes
type ResultMerger struct {
	logger  logging.Logger
	metrics *metrics.SearchMetrics
	mu      sync.RWMutex
}

// ResultHeap implements a heap for search results
type ResultHeap []*types.SearchResult

// Len returns the length of the heap
func (h ResultHeap) Len() int { return len(h) }

// Less compares two results in the heap
func (h ResultHeap) Less(i, j int) bool {
	// We want a max-heap based on distance (lower distance = higher rank)
	return h[i].Distance < h[j].Distance
}

// Swap swaps two elements in the heap
func (h ResultHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap
func (h *ResultHeap) Push(x interface{}) {
	*h = append(*h, x.(*types.SearchResult))
}

// Pop removes and returns the element at the top of the heap
func (h *ResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeResult represents a merged search result with metadata
type MergeResult struct {
	Result      *types.SearchResult
	SourceNodes []string
	Rank        int
	Score       float64
}

// MergeConfig represents the configuration for result merging
type MergeConfig struct {
	EnableDeduplication bool    `yaml:"enable_deduplication" json:"enable_deduplication"`
	DistanceThreshold   float64 `yaml:"distance_threshold" json:"distance_threshold"`
	MaxResults          int     `yaml:"max_results" json:"max_results"`
	EnableRanking       bool    `yaml:"enable_ranking" json:"enable_ranking"`
	EnableScoring       bool    `yaml:"enable_scoring" json:"enable_scoring"`
}

// DefaultMergeConfig returns the default merge configuration
func DefaultMergeConfig() *MergeConfig {
	return &MergeConfig{
		EnableDeduplication: true,
		DistanceThreshold:   0.001,
		MaxResults:          1000,
		EnableRanking:       true,
		EnableScoring:       true,
	}
}

// NewResultMerger creates a new result merger
func NewResultMerger(logger logging.Logger, metrics *metrics.SearchMetrics) *ResultMerger {
	return &ResultMerger{
		logger:  logger,
		metrics: metrics,
	}
}

// MergeResults merges search results from multiple nodes
func (m *ResultMerger) MergeResults(results []*types.SearchResult, topK int) ([]*types.SearchResult, error) {
	if len(results) == 0 {
		return []*types.SearchResult{}, nil
	}

	startTime := time.Now()

	// Deduplicate results if enabled
	deduplicatedResults := results
	if DefaultMergeConfig().EnableDeduplication {
		deduplicatedResults = m.deduplicateResults(results)
	}

	// Sort results by distance
	sort.Slice(deduplicatedResults, func(i, j int) bool {
		return deduplicatedResults[i].Distance < deduplicatedResults[j].Distance
	})

	// Apply top-K limit
	if topK > 0 && len(deduplicatedResults) > topK {
		deduplicatedResults = deduplicatedResults[:topK]
	}

	// Apply ranking and scoring if enabled
	if DefaultMergeConfig().EnableRanking || DefaultMergeConfig().EnableScoring {
		deduplicatedResults = m.applyRankingAndScoring(deduplicatedResults)
	}

	duration := time.Since(startTime)

	// Update metrics
	if m.metrics != nil {
		m.metrics.ObserveMergeLatency(duration)
		m.metrics.ObserveMergeResultCount(len(deduplicatedResults))
	}

	m.logger.Debug("Merged search results",
		zap.Int("input_count", len(results)),
		zap.Int("output_count", len(deduplicatedResults)),
		zap.Int("top_k", topK),
		zap.Duration("duration", duration))

	return deduplicatedResults, nil
}

// MergeResultsWithMetadata merges search results with detailed metadata
func (m *ResultMerger) MergeResultsWithMetadata(results []*types.SearchResult, topK int) ([]*MergeResult, error) {
	if len(results) == 0 {
		return []*MergeResult{}, nil
	}

	// First, merge the basic results
	mergedResults, err := m.MergeResults(results, topK)
	if err != nil {
		return nil, err
	}

	// Create merge results with metadata
	mergeResults := make([]*MergeResult, len(mergedResults))
	for i, result := range mergedResults {
		mergeResults[i] = &MergeResult{
			Result:      result,
			SourceNodes: m.getSourceNodes(result, results),
			Rank:        i + 1,
			Score:       m.calculateScore(result, i),
		}
	}

	return mergeResults, nil
}

// deduplicateResults removes duplicate results based on vector ID and distance threshold
func (m *ResultMerger) deduplicateResults(results []*types.SearchResult) []*types.SearchResult {
	if len(results) == 0 {
		return results
	}

	seen := make(map[string]bool)
	deduplicated := make([]*types.SearchResult, 0)
	threshold := DefaultMergeConfig().DistanceThreshold

	for _, result := range results {
		// Check if we've seen this vector ID
		if seen[result.Vector.ID] {
			continue
		}

		// Check for near-duplicates based on distance threshold
		isDuplicate := false
		for _, existing := range deduplicated {
			if math.Abs(result.Distance-existing.Distance) < threshold {
				isDuplicate = true
				break
			}
		}

		if !isDuplicate {
			seen[result.Vector.ID] = true
			deduplicated = append(deduplicated, result)
		}
	}

	return deduplicated
}

// applyRankingAndScoring applies ranking and scoring to results
func (m *ResultMerger) applyRankingAndScoring(results []*types.SearchResult) []*types.SearchResult {
	if len(results) == 0 {
		return results
	}

	// Apply scoring if enabled
	if DefaultMergeConfig().EnableScoring {
		for i, result := range results {
			result.Score = float32(m.calculateScore(result, i))
		}
	}

	// Ranking is implicitly handled by the distance-based sorting
	return results
}

// calculateScore calculates a normalized score for a result
func (m *ResultMerger) calculateScore(result *types.SearchResult, rank int) float64 {
	// Normalize distance to a score (0-1, where 1 is best)
	maxDistance := 2.0 // Assuming cosine distance, max is 2.0
	distanceScore := 1.0 - (result.Distance / maxDistance)

	// Apply rank-based scoring
	rankScore := 1.0 / float64(rank+1)

	// Combine scores (weighted average)
	finalScore := 0.7*distanceScore + 0.3*rankScore

	return finalScore
}

// getSourceNodes determines which nodes contributed to a result
func (m *ResultMerger) getSourceNodes(target *types.SearchResult, allResults []*types.SearchResult) []string {
	nodes := make([]string, 0)
	threshold := DefaultMergeConfig().DistanceThreshold

	for _, result := range allResults {
		if result.Vector.ID == target.Vector.ID &&
			math.Abs(result.Distance-target.Distance) < threshold {
			if result.NodeID != "" {
				nodes = append(nodes, result.NodeID)
			}
		}
	}

	return nodes
}

// MergeBatches merges multiple batches of search results
func (m *ResultMerger) MergeBatches(batches [][]*types.SearchResult, topK int) ([]*types.SearchResult, error) {
	if len(batches) == 0 {
		return []*types.SearchResult{}, nil
	}

	// Flatten all batches
	allResults := make([]*types.SearchResult, 0)
	for _, batch := range batches {
		allResults = append(allResults, batch...)
	}

	// Merge all results
	return m.MergeResults(allResults, topK)
}

// MergeWithHeap merges results using a heap-based approach for better performance
func (m *ResultMerger) MergeWithHeap(results []*types.SearchResult, topK int) ([]*types.SearchResult, error) {
	if len(results) == 0 {
		return []*types.SearchResult{}, nil
	}

	// Create a max-heap
	h := &ResultHeap{}
	heap.Init(h)

	// Push all results into the heap
	for _, result := range results {
		heap.Push(h, result)
	}

	// Extract top-K results
	mergedResults := make([]*types.SearchResult, 0, min(topK, len(results)))
	for i := 0; i < topK && h.Len() > 0; i++ {
		result := heap.Pop(h).(*types.SearchResult)
		mergedResults = append(mergedResults, result)
	}

	return mergedResults, nil
}

// MergeStreaming merges results in a streaming fashion
func (m *ResultMerger) MergeStreaming(resultChan <-chan *types.SearchResult, topK int) ([]*types.SearchResult, error) {
	// Create a max-heap for streaming merge
	h := &ResultHeap{}
	heap.Init(h)

	// Process results as they arrive
	for result := range resultChan {
		heap.Push(h, result)

		// Keep only top-K results in memory
		if h.Len() > topK {
			heap.Pop(h)
		}
	}

	// Extract final results
	mergedResults := make([]*types.SearchResult, 0, h.Len())
	for h.Len() > 0 {
		result := heap.Pop(h).(*types.SearchResult)
		mergedResults = append(mergedResults, result)
	}

	// Reverse to get correct order (closest first)
	for i, j := 0, len(mergedResults)-1; i < j; i, j = i+1, j-1 {
		mergedResults[i], mergedResults[j] = mergedResults[j], mergedResults[i]
	}

	return mergedResults, nil
}

// GetMergeStats returns statistics about the merge process
func (m *ResultMerger) GetMergeStats() map[string]interface{} {
	return map[string]interface{}{
		"enable_deduplication": DefaultMergeConfig().EnableDeduplication,
		"distance_threshold":   DefaultMergeConfig().DistanceThreshold,
		"max_results":          DefaultMergeConfig().MaxResults,
		"enable_ranking":       DefaultMergeConfig().EnableRanking,
		"enable_scoring":       DefaultMergeConfig().EnableScoring,
	}
}

// ValidateResults validates search results before merging
func (m *ResultMerger) ValidateResults(results []*types.SearchResult) error {
	for i, result := range results {
		if result == nil {
			return fmt.Errorf("result at index %d is nil", i)
		}

		if result.Vector == nil {
			return fmt.Errorf("result at index %d has nil vector", i)
		}

		if result.Vector.ID == "" {
			return fmt.Errorf("result at index %d has empty vector ID", i)
		}

		if result.Distance < 0 || result.Distance > 2.0 {
			return fmt.Errorf("result at index %d has invalid distance: %f", i, result.Distance)
		}
	}

	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
