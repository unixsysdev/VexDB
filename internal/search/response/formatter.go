package response

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"vxdb/internal/types"
)

// Formatter handles formatting of search responses
type Formatter struct {
	config *ResponseConfig
}

// ResponseConfig represents response formatting configuration
type ResponseConfig struct {
	IncludeQueryVector bool          `json:"include_query_vector" yaml:"include_query_vector"`
	IncludeMetadata    bool          `json:"include_metadata" yaml:"include_metadata"`
	IncludeTimestamps  bool          `json:"include_timestamps" yaml:"include_timestamps"`
	PrettyPrint        bool          `json:"pretty_print" yaml:"pretty_print"`
	MaxResults         int           `json:"max_results" yaml:"max_results"`
	ResponseTimeout    time.Duration `json:"response_timeout" yaml:"response_timeout"`
}

// SearchResponse represents a formatted search response
type SearchResponse struct {
	Success   bool                   `json:"success"`
	Message   string                 `json:"message,omitempty"`
	QueryID   string                 `json:"query_id,omitempty"`
	Results   []SearchResult         `json:"results,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Stats     *SearchStats           `json:"stats,omitempty"`
}

// SearchResult represents a formatted search result
type SearchResult struct {
	ID       string            `json:"id"`
	Vector   []float64         `json:"vector,omitempty"`
	Distance float64           `json:"distance"`
	Score    float64           `json:"score"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Cluster  string            `json:"cluster,omitempty"`
	Node     string            `json:"node,omitempty"`
}

// SearchStats represents search execution statistics
type SearchStats struct {
	TotalResults    int           `json:"total_results"`
	ReturnedResults int           `json:"returned_results"`
	ExecutionTime   time.Duration `json:"execution_time"`
	SearchTime      time.Duration `json:"search_time"`
	NetworkTime     time.Duration `json:"network_time"`
	MergeTime       time.Duration `json:"merge_time"`
	NodesQueried    int           `json:"nodes_queried"`
	ClustersQueried int           `json:"clusters_queried"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Success   bool                   `json:"success"`
	Error     string                 `json:"error"`
	Message   string                 `json:"message"`
	Code      string                 `json:"code,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	QueryID   string                 `json:"query_id,omitempty"`
}

// NewFormatter creates a new response formatter
func NewFormatter(config *ResponseConfig) *Formatter {
	if config == nil {
		config = DefaultResponseConfig()
	}
	return &Formatter{config: config}
}

// DefaultResponseConfig returns the default response configuration
func DefaultResponseConfig() *ResponseConfig {
	return &ResponseConfig{
		IncludeQueryVector: false,
		IncludeMetadata:    true,
		IncludeTimestamps:  true,
		PrettyPrint:        false,
		MaxResults:         1000,
		ResponseTimeout:    30 * time.Second,
	}
}

// FormatSearchResponse formats a search response
func (f *Formatter) FormatSearchResponse(results []*types.SearchResult, query *types.Vector, k int, queryID string, stats *SearchStats) *SearchResponse {
	response := &SearchResponse{
		Success:   true,
		QueryID:   queryID,
		Timestamp: time.Now(),
		Stats:     stats,
	}

	// Format results
	response.Results = f.formatSearchResults(results)

	// Add metadata
	response.Metadata = f.buildMetadata(results, query, k)

	return response
}

// FormatMultiClusterSearchResponse formats a multi-cluster search response
func (f *Formatter) FormatMultiClusterSearchResponse(clusterResults map[string][]*types.SearchResult, query *types.Vector, k int, queryID string, stats *SearchStats) *SearchResponse {
	response := &SearchResponse{
		Success:   true,
		QueryID:   queryID,
		Timestamp: time.Now(),
		Stats:     stats,
	}

	// Merge and format results from all clusters
	var allResults []*types.SearchResult
	for _, results := range clusterResults {
		allResults = append(allResults, results...)
	}

	response.Results = f.formatSearchResults(allResults)

	// Add metadata
	response.Metadata = f.buildMultiClusterMetadata(clusterResults, query, k)

	return response
}

// FormatErrorResponse formats an error response
func (f *Formatter) FormatErrorResponse(err error, queryID string, code string, details map[string]interface{}) *ErrorResponse {
	return &ErrorResponse{
		Success:   false,
		Error:     err.Error(),
		Message:   getErrorMessage(err),
		Code:      code,
		Details:   details,
		Timestamp: time.Now(),
		QueryID:   queryID,
	}
}

// FormatValidationErrorResponse formats a validation error response
func (f *Formatter) FormatValidationErrorResponse(errors []ValidationError, queryID string) *ErrorResponse {
	details := map[string]interface{}{
		"validation_errors": errors,
	}

	return &ErrorResponse{
		Success:   false,
		Error:     "validation_failed",
		Message:   "Request validation failed",
		Code:      "VALIDATION_ERROR",
		Details:   details,
		Timestamp: time.Now(),
		QueryID:   queryID,
	}
}

// formatSearchResults formats search results
func (f *Formatter) formatSearchResults(results []*types.SearchResult) []SearchResult {
	if len(results) == 0 {
		return []SearchResult{}
	}

	formatted := make([]SearchResult, 0, len(results))
	for _, result := range results {
		formatted = append(formatted, f.formatSearchResult(result))
	}

	return formatted
}

// formatSearchResult formats a single search result
func (f *Formatter) formatSearchResult(result *types.SearchResult) SearchResult {
	formatted := SearchResult{
		ID:       result.Vector.ID,
		Distance: result.Distance,
		Score:    calculateScore(result.Distance),
	}

	// Include vector data if configured
	if f.config.IncludeQueryVector && result.Vector != nil {
		// Convert float32 to float64 for JSON compatibility
		formatted.Vector = make([]float64, len(result.Vector.Data))
		for i, val := range result.Vector.Data {
			formatted.Vector[i] = float64(val)
		}
	}

	// Include metadata if configured
	if f.config.IncludeMetadata && result.Vector != nil && result.Vector.Metadata != nil {
		// Convert metadata to string map
		formatted.Metadata = make(map[string]string)
		for k, v := range result.Vector.Metadata {
			if str, ok := v.(string); ok {
				formatted.Metadata[k] = str
			}
		}
	}

	// Include cluster and node information if available
	if result.Vector != nil && result.Vector.ClusterID != 0 {
		formatted.Cluster = fmt.Sprintf("%d", result.Vector.ClusterID)
	}

	return formatted
}

// buildMetadata builds response metadata
func (f *Formatter) buildMetadata(results []*types.SearchResult, query *types.Vector, k int) map[string]interface{} {
	metadata := map[string]interface{}{
		"total_results":    len(results),
		"requested_k":      k,
		"response_version": "1.0",
	}

	if f.config.IncludeTimestamps {
		metadata["timestamp"] = time.Now().Format(time.RFC3339)
	}

	if f.config.IncludeQueryVector && query != nil {
		metadata["query_vector"] = map[string]interface{}{
			"dimension": len(query.Data),
			"id":        query.ID,
		}
	}

	return metadata
}

// buildMultiClusterMetadata builds metadata for multi-cluster search
func (f *Formatter) buildMultiClusterMetadata(clusterResults map[string][]*types.SearchResult, query *types.Vector, k int) map[string]interface{} {
	metadata := f.buildMetadata(nil, query, k)

	// Add cluster-specific information
	clusterInfo := make(map[string]interface{})
	for clusterID, results := range clusterResults {
		clusterInfo[clusterID] = map[string]interface{}{
			"result_count": len(results),
		}
	}

	metadata["clusters"] = clusterInfo
	metadata["total_clusters"] = len(clusterResults)

	return metadata
}

// ToJSON converts the response to JSON
func (f *Formatter) ToJSON(response interface{}) ([]byte, error) {
	if f.config.PrettyPrint {
		return json.MarshalIndent(response, "", "  ")
	}
	return json.Marshal(response)
}

// ToJSONString converts the response to a JSON string
func (f *Formatter) ToJSONString(response interface{}) (string, error) {
	data, err := f.ToJSON(response)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// calculateScore calculates a normalized score from distance (0-1, where 1 is best)
func calculateScore(distance float64) float64 {
	// Simple inverse relationship - can be customized based on distance type
	if distance <= 0 {
		return 1.0
	}

	// Use exponential decay for score calculation
	score := math.Exp(-distance / 10.0)
	if score > 1.0 {
		score = 1.0
	}
	return score
}

// getErrorMessage returns a user-friendly error message
func getErrorMessage(err error) string {
	if err == nil {
		return "Unknown error"
	}

	// Map common errors to user-friendly messages
	errMsg := err.Error()

	switch {
	case contains(errMsg, "validation"):
		return "Invalid request parameters"
	case contains(errMsg, "timeout"):
		return "Request timed out"
	case contains(errMsg, "connection"):
		return "Connection error"
	case contains(errMsg, "not found"):
		return "Resource not found"
	case contains(errMsg, "unauthorized"):
		return "Authentication required"
	case contains(errMsg, "rate limit"):
		return "Rate limit exceeded"
	default:
		return "An error occurred while processing your request"
	}
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string      `json:"field"`
	Message string      `json:"message"`
	Value   interface{} `json:"value,omitempty"`
}

// FormatBatchResponse formats a batch search response
func (f *Formatter) FormatBatchResponse(results [][]*types.SearchResult, queries []*types.Vector, k int, queryIDs []string, stats []*SearchStats) *BatchSearchResponse {
	response := &BatchSearchResponse{
		Success:   true,
		Timestamp: time.Now(),
		BatchSize: len(results),
	}

	// Format individual search responses
	for i, resultSet := range results {
		var (
			query   *types.Vector
			queryID string
			stat    *SearchStats
		)

		if len(queries) > i {
			query = queries[i]
		}

		if len(queryIDs) > i {
			queryID = queryIDs[i]
		}

		if len(stats) > i {
			stat = stats[i]
		}

		searchResponse := f.FormatSearchResponse(resultSet, query, k, queryID, stat)
		response.Results = append(response.Results, *searchResponse)
	}

	return response
}

// BatchSearchResponse represents a batch search response
type BatchSearchResponse struct {
	Success   bool             `json:"success"`
	Timestamp time.Time        `json:"timestamp"`
	BatchSize int              `json:"batch_size"`
	Results   []SearchResponse `json:"results"`
}

// FormatHealthResponse formats a health check response
func (f *Formatter) FormatHealthResponse(healthy bool, status string, details map[string]interface{}) *HealthResponse {
	return &HealthResponse{
		Success:   healthy,
		Status:    status,
		Details:   details,
		Timestamp: time.Now(),
	}
}

// HealthResponse represents a health check response
type HealthResponse struct {
	Success   bool                   `json:"success"`
	Status    string                 `json:"status"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// FormatMetricsResponse formats a metrics response
func (f *Formatter) FormatMetricsResponse(metrics map[string]interface{}) *MetricsResponse {
	return &MetricsResponse{
		Success:   true,
		Metrics:   metrics,
		Timestamp: time.Now(),
	}
}

// MetricsResponse represents a metrics response
type MetricsResponse struct {
	Success   bool                   `json:"success"`
	Metrics   map[string]interface{} `json:"metrics"`
	Timestamp time.Time              `json:"timestamp"`
}
