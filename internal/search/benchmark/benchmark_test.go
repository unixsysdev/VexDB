package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"vexdb/internal/search/config"
	"vexdb/internal/search/response"
	"vexdb/internal/search/validation"
	"vexdb/internal/types"
)

// BenchmarkSearchValidation benchmarks the search validation performance
func BenchmarkSearchValidation(b *testing.B) {
	validator := validation.NewSearchRequestValidator(1000, 100, 1000, 50)
	
	// Create test vectors of different dimensions
	testVectors := map[string]*types.Vector{
		"small":  createTestVector(10),
		"medium": createTestVector(100),
		"large":  createTestVector(500),
		"xlarge": createTestVector(1000),
	}
	
	for name, vector := range testVectors {
		b.Run(fmt.Sprintf("VectorDimension_%s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := validator.ValidateSearchRequest(vector, 10, nil)
				if err != nil {
					b.Fatalf("Validation failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSearchValidationWithMetadata benchmarks validation with metadata filters
func BenchmarkSearchValidationWithMetadata(b *testing.B) {
	validator := validation.NewSearchRequestValidator(1000, 100, 1000, 50)
	
	query := createTestVector(100)
	
	// Create metadata filters of different complexities
	filters := map[string]*types.MetadataFilter{
		"simple": createSimpleMetadataFilter(),
		"medium": createMediumMetadataFilter(),
		"complex": createComplexMetadataFilter(),
	}
	
	for name, filter := range filters {
		b.Run(fmt.Sprintf("MetadataFilter_%s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := validator.ValidateSearchRequest(query, 10, filter)
				if err != nil {
					b.Fatalf("Validation failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkResponseFormatting benchmarks response formatting performance
func BenchmarkResponseFormatting(b *testing.B) {
	formatter := response.NewFormatter(response.DefaultResponseConfig())
	
	// Create result sets of different sizes
	resultSets := map[string][]*types.SearchResult{
		"small":  createSearchResults(10),
		"medium": createSearchResults(100),
		"large":  createSearchResults(1000),
	}
	
	query := createTestVector(100)
	stats := &response.SearchStats{
		TotalResults:    1000,
		ReturnedResults: 100,
		ExecutionTime:   50 * time.Millisecond,
		SearchTime:      40 * time.Millisecond,
		NetworkTime:     5 * time.Millisecond,
		MergeTime:       3 * time.Millisecond,
		NodesQueried:    3,
		ClustersQueried: 5,
	}
	
	for name, results := range resultSets {
		b.Run(fmt.Sprintf("ResultSet_%s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				response := formatter.FormatSearchResponse(results, query, 100, "benchmark-query", stats)
				if response == nil {
					b.Fatal("Expected response to be created")
				}
			}
		})
	}
}

// BenchmarkResponseFormattingWithMetadata benchmarks formatting with metadata
func BenchmarkResponseFormattingWithMetadata(b *testing.B) {
	configs := map[string]*response.ResponseConfig{
		"no_metadata": {
			IncludeQueryVector: false,
			IncludeMetadata:    false,
			IncludeTimestamps:  true,
			PrettyPrint:        false,
		},
		"with_metadata": {
			IncludeQueryVector: false,
			IncludeMetadata:    true,
			IncludeTimestamps:  true,
			PrettyPrint:        false,
		},
		"with_vector": {
			IncludeQueryVector: true,
			IncludeMetadata:    true,
			IncludeTimestamps:  true,
			PrettyPrint:        false,
		},
		"pretty_print": {
			IncludeQueryVector: false,
			IncludeMetadata:    true,
			IncludeTimestamps:  true,
			PrettyPrint:        true,
		},
	}
	
	results := createSearchResults(100)
	query := createTestVector(100)
	stats := &response.SearchStats{
		TotalResults:    100,
		ReturnedResults: 100,
		ExecutionTime:   50 * time.Millisecond,
	}
	
	for name, config := range configs {
		b.Run(fmt.Sprintf("Config_%s", name), func(b *testing.B) {
			formatter := response.NewFormatter(config)
			for i := 0; i < b.N; i++ {
				response := formatter.FormatSearchResponse(results, query, 100, "benchmark-query", stats)
				if response == nil {
					b.Fatal("Expected response to be created")
				}
			}
		})
	}
}

// BenchmarkBatchResponseFormatting benchmarks batch response formatting
func BenchmarkBatchResponseFormatting(b *testing.B) {
	formatter := response.NewFormatter(response.DefaultResponseConfig())
	
	// Create batch results of different sizes
	batchSizes := []int{10, 50, 100}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			// Create batch results
			allResults := make([][]*types.SearchResult, batchSize)
			queries := make([]*types.Vector, batchSize)
			queryIDs := make([]string, batchSize)
			stats := make([]*response.SearchStats, batchSize)
			
			for i := 0; i < batchSize; i++ {
				allResults[i] = createSearchResults(10)
				queries[i] = createTestVector(100)
				queryIDs[i] = fmt.Sprintf("benchmark-query-%d", i)
				stats[i] = &response.SearchStats{
					TotalResults:    10,
					ReturnedResults: 10,
					ExecutionTime:   50 * time.Millisecond,
				}
			}
			
			for i := 0; i < b.N; i++ {
				response := formatter.FormatBatchResponse(allResults, queries, 10, queryIDs, stats)
				if response == nil {
					b.Fatal("Expected batch response to be created")
				}
			}
		})
	}
}

// BenchmarkJSONSerialization benchmarks JSON serialization performance
func BenchmarkJSONSerialization(b *testing.B) {
	formatter := response.NewFormatter(&response.ResponseConfig{PrettyPrint: false})
	
	results := createSearchResults(100)
	query := createTestVector(100)
	stats := &response.SearchStats{
		TotalResults:    100,
		ReturnedResults: 100,
		ExecutionTime:   50 * time.Millisecond,
	}
	
	response := formatter.FormatSearchResponse(results, query, 100, "benchmark-query", stats)
	
	b.Run("CompactJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonData, err := formatter.ToJSON(response)
			if err != nil {
				b.Fatalf("JSON serialization failed: %v", err)
			}
			if len(jsonData) == 0 {
				b.Fatal("Expected non-empty JSON data")
			}
		}
	})
	
	prettyFormatter := response.NewFormatter(&response.ResponseConfig{PrettyPrint: true})
	
	b.Run("PrettyJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonData, err := prettyFormatter.ToJSON(response)
			if err != nil {
				b.Fatalf("JSON serialization failed: %v", err)
			}
			if len(jsonData) == 0 {
				b.Fatal("Expected non-empty JSON data")
			}
		}
	})
}

// BenchmarkConfigurationValidation benchmarks configuration validation performance
func BenchmarkConfigurationValidation(b *testing.B) {
	config := config.DefaultSearchServiceConfig()
	
	b.Run("DefaultConfig", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := config.Validate()
			if err != nil {
				b.Fatalf("Configuration validation failed: %v", err)
			}
		}
	})
	
	// Test with various modifications
	invalidConfigs := map[string]*config.SearchServiceConfig{
		"invalid_host": func() *config.SearchServiceConfig {
			c := *config
			c.Server.Host = ""
			return &c
		}(),
		"invalid_port": func() *config.SearchServiceConfig {
			c := *config
			c.Server.Port = 0
			return &c
		}(),
		"invalid_engine": func() *config.SearchServiceConfig {
			c := *config
			c.Engine.MaxResults = -1
			return &c
		}(),
	}
	
	for name, invalidConfig := range invalidConfigs {
		b.Run(fmt.Sprintf("InvalidConfig_%s", name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := invalidConfig.Validate()
				if err == nil {
					b.Fatal("Expected validation to fail for invalid config")
				}
			}
		})
	}
}

// BenchmarkVectorGeneration benchmarks vector generation performance
func BenchmarkVectorGeneration(b *testing.B) {
	dimensions := []int{10, 50, 100, 500, 1000}
	
	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("Dimension_%d", dim), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				vector := createTestVector(dim)
				if vector == nil {
					b.Fatal("Expected vector to be created")
				}
				if len(vector.Data) != dim {
					b.Fatalf("Expected vector dimension %d, got %d", dim, len(vector.Data))
				}
			}
		})
	}
}

// BenchmarkDistanceCalculation benchmarks distance calculation performance
func BenchmarkDistanceCalculation(b *testing.B) {
	dimensions := []int{10, 50, 100, 500, 1000}
	
	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("Dimension_%d", dim), func(b *testing.B) {
			query := createTestVector(dim)
			target := createTestVector(dim)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				distance := calculateEuclideanDistance(query.Data, target.Data)
				if distance < 0 {
					b.Fatal("Expected non-negative distance")
				}
			}
		})
	}
}

// Helper functions

func createTestVector(dimension int) *types.Vector {
	data := make([]float32, dimension)
	for i := 0; i < dimension; i++ {
		data[i] = rand.Float32()
	}
	
	return &types.Vector{
		ID:   fmt.Sprintf("test-vector-%d", dimension),
		Data: data,
		Metadata: map[string]interface{}{
			"dimension": dimension,
			"source":    "benchmark",
		},
		ClusterID: uint32(rand.Intn(10)),
	}
}

func createSearchResults(count int) []*types.SearchResult {
	results := make([]*types.SearchResult, count)
	
	for i := 0; i < count; i++ {
		vector := createTestVector(100)
		results[i] = &types.SearchResult{
			Vector:   vector,
			Distance: float64(i) * 0.1,
		}
	}
	
	return results
}

func createSimpleMetadataFilter() *types.MetadataFilter {
	return &types.MetadataFilter{
		Conditions: []interface{}{
			map[string]interface{}{
				"key":      "category",
				"operator": "=",
				"value":    "test",
			},
		},
	}
}

func createMediumMetadataFilter() *types.MetadataFilter {
	return &types.MetadataFilter{
		Conditions: []interface{}{
			map[string]interface{}{
				"key":      "category",
				"operator": "=",
				"value":    "test",
			},
			map[string]interface{}{
				"key":      "source",
				"operator": "!=",
				"value":    "invalid",
			},
			map[string]interface{}{
				"key":      "score",
				"operator": ">",
				"value":    "0.5",
			},
		},
	}
}

func createComplexMetadataFilter() *types.MetadataFilter {
	return &types.MetadataFilter{
		Conditions: []interface{}{
			map[string]interface{}{
				"key":      "category",
				"operator": "in",
				"values":   []string{"test", "validation", "benchmark"},
			},
			map[string]interface{}{
				"key":      "source",
				"operator": "contains",
				"value":    "benchmark",
			},
			map[string]interface{}{
				"key":      "score",
				"operator": ">=",
				"value":    "0.8",
			},
			map[string]interface{}{
				"key":      "tags",
				"operator": "not_in",
				"values":   []string{"deprecated", "invalid"},
			},
		},
	}
}

func calculateEuclideanDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return -1 // Error case
	}
	
	sum := 0.0
	for i := 0; i < len(a); i++ {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	
	return math.Sqrt(sum)
}