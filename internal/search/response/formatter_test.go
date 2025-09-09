package response

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"vexdb/internal/types"
)

func TestNewFormatter(t *testing.T) {
	config := &ResponseConfig{
		IncludeQueryVector: true,
		IncludeMetadata:    true,
		IncludeTimestamps:  true,
		PrettyPrint:        false,
		MaxResults:         100,
		ResponseTimeout:    30 * time.Second,
	}
	
	formatter := NewFormatter(config)
	
	if formatter == nil {
		t.Fatal("Expected formatter to be created, got nil")
	}
	
	if formatter.config != config {
		t.Error("Expected formatter to use provided config")
	}
}

func TestNewFormatter_DefaultConfig(t *testing.T) {
	formatter := NewFormatter(nil)
	
	if formatter == nil {
		t.Fatal("Expected formatter to be created with default config, got nil")
	}
	
	if formatter.config == nil {
		t.Fatal("Expected formatter to have default config, got nil")
	}
	
	// Check default values
	if !formatter.config.IncludeMetadata {
		t.Error("Expected default IncludeMetadata to be true")
	}
	
	if formatter.config.IncludeQueryVector {
		t.Error("Expected default IncludeQueryVector to be false")
	}
	
	if !formatter.config.IncludeTimestamps {
		t.Error("Expected default IncludeTimestamps to be true")
	}
}

func TestFormatSearchResponse_ValidResults(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	// Create test results
	results := []*types.SearchResult{
		{
			Vector: &types.Vector{
				ID:   "vector1",
				Data: []float32{1.0, 2.0, 3.0},
				Metadata: map[string]interface{}{
					"category": "test",
					"source":   "unit_test",
				},
				ClusterID: 1,
			},
			Distance: 0.5,
		},
		{
			Vector: &types.Vector{
				ID:   "vector2",
				Data: []float32{4.0, 5.0, 6.0},
				Metadata: map[string]interface{}{
					"category": "test",
					"source":   "unit_test",
				},
				ClusterID: 2,
			},
			Distance: 1.2,
		},
	}
	
	query := &types.Vector{
		ID:   "query-vector",
		Data: []float32{1.0, 2.0, 3.0},
	}
	
	stats := &SearchStats{
		TotalResults:    2,
		ReturnedResults: 2,
		ExecutionTime:   100 * time.Millisecond,
		SearchTime:      80 * time.Millisecond,
		NetworkTime:     10 * time.Millisecond,
		MergeTime:       5 * time.Millisecond,
		NodesQueried:    1,
		ClustersQueried: 2,
	}
	
	response := formatter.FormatSearchResponse(results, query, 10, "test-query-123", stats)
	
	if response == nil {
		t.Fatal("Expected response to be created, got nil")
	}
	
	if !response.Success {
		t.Error("Expected response to be successful")
	}
	
	if response.QueryID != "test-query-123" {
		t.Errorf("Expected QueryID to be 'test-query-123', got '%s'", response.QueryID)
	}
	
	if len(response.Results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(response.Results))
	}
	
	if response.Total != 2 {
		t.Errorf("Expected Total to be 2, got %d", response.Total)
	}
	
	if response.Stats == nil {
		t.Fatal("Expected stats to be included")
	}
	
	if response.Stats.TotalResults != 2 {
		t.Errorf("Expected stats.TotalResults to be 2, got %d", response.Stats.TotalResults)
	}
	
	// Check first result
	if len(response.Results) > 0 {
		firstResult := response.Results[0]
		if firstResult.ID != "vector1" {
			t.Errorf("Expected first result ID to be 'vector1', got '%s'", firstResult.ID)
		}
		
		if firstResult.Distance != 0.5 {
			t.Errorf("Expected first result distance to be 0.5, got %f", firstResult.Distance)
		}
		
		// Check that metadata is included (default config includes metadata)
		if firstResult.Metadata == nil {
			t.Error("Expected metadata to be included")
		} else {
			if firstResult.Metadata["category"] != "test" {
				t.Errorf("Expected category to be 'test', got '%s'", firstResult.Metadata["category"])
			}
		}
	}
}

func TestFormatSearchResponse_EmptyResults(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	results := []*types.SearchResult{}
	query := &types.Vector{
		ID:   "query-vector",
		Data: []float32{1.0, 2.0, 3.0},
	}
	
	stats := &SearchStats{
		TotalResults:    0,
		ReturnedResults: 0,
		ExecutionTime:   50 * time.Millisecond,
	}
	
	response := formatter.FormatSearchResponse(results, query, 10, "test-query-456", stats)
	
	if response == nil {
		t.Fatal("Expected response to be created, got nil")
	}
	
	if !response.Success {
		t.Error("Expected response to be successful even with no results")
	}
	
	if len(response.Results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(response.Results))
	}
	
	if response.Total != 0 {
		t.Errorf("Expected Total to be 0, got %d", response.Total)
	}
}

func TestFormatErrorResponse(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	err := fmt.Errorf("test error message")
	queryID := "test-query-789"
	code := "TEST_ERROR"
	details := map[string]interface{}{
		"detail1": "value1",
		"detail2": 42,
	}
	
	response := formatter.FormatErrorResponse(err, queryID, code, details)
	
	if response == nil {
		t.Fatal("Expected error response to be created, got nil")
	}
	
	if response.Success {
		t.Error("Expected error response to have Success=false")
	}
	
	if response.Error != "test error message" {
		t.Errorf("Expected Error to be 'test error message', got '%s'", response.Error)
	}
	
	if response.Message != "An error occurred while processing your request" {
		t.Errorf("Expected default message, got '%s'", response.Message)
	}
	
	if response.Code != code {
		t.Errorf("Expected Code to be '%s', got '%s'", code, response.Code)
	}
	
	if response.QueryID != queryID {
		t.Errorf("Expected QueryID to be '%s', got '%s'", queryID, response.QueryID)
	}
	
	if response.Details == nil {
		t.Error("Expected details to be included")
	}
}

func TestFormatValidationErrorResponse(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	errors := []ValidationError{
		{
			Field:   "vector",
			Message: "vector cannot be empty",
			Value:   []float32{},
		},
		{
			Field:   "k",
			Message: "k must be positive",
			Value:   -1,
		},
	}
	
	queryID := "test-query-999"
	
	response := formatter.FormatValidationErrorResponse(errors, queryID)
	
	if response == nil {
		t.Fatal("Expected validation error response to be created, got nil")
	}
	
	if response.Success {
		t.Error("Expected validation error response to have Success=false")
	}
	
	if response.Error != "validation_failed" {
		t.Errorf("Expected Error to be 'validation_failed', got '%s'", response.Error)
	}
	
	if response.Message != "Request validation failed" {
		t.Errorf("Expected Message to be 'Request validation failed', got '%s'", response.Message)
	}
	
	if response.Code != "VALIDATION_ERROR" {
		t.Errorf("Expected Code to be 'VALIDATION_ERROR', got '%s'", response.Code)
	}
	
	if response.Details == nil {
		t.Fatal("Expected details to be included")
	}
	
	validationErrors, ok := response.Details["validation_errors"].([]ValidationError)
	if !ok {
		t.Fatal("Expected validation_errors in details")
	}
	
	if len(validationErrors) != 2 {
		t.Errorf("Expected 2 validation errors, got %d", len(validationErrors))
	}
}

func TestCalculateScore(t *testing.T) {
	tests := []struct {
		name     string
		distance float64
		expected float64
	}{
		{
			name:     "zero distance",
			distance: 0.0,
			expected: 1.0,
		},
		{
			name:     "small distance",
			distance: 1.0,
			expected: 0.9048374180359595, // exp(-1/10)
		},
		{
			name:     "large distance",
			distance: 100.0,
			expected: 0.00004539992976248485, // exp(-100/10)
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := calculateScore(tt.distance)
			
			if math.Abs(score-tt.expected) > 0.0001 {
				t.Errorf("Expected score %f, got %f", tt.expected, score)
			}
			
			if score < 0 || score > 1 {
				t.Errorf("Expected score to be between 0 and 1, got %f", score)
			}
		})
	}
}

func TestGetErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "validation error",
			err:      fmt.Errorf("validation failed for field"),
			expected: "Invalid request parameters",
		},
		{
			name:     "timeout error",
			err:      fmt.Errorf("request timeout exceeded"),
			expected: "Request timed out",
		},
		{
			name:     "connection error",
			err:      fmt.Errorf("connection refused"),
			expected: "Connection error",
		},
		{
			name:     "not found error",
			err:      fmt.Errorf("resource not found"),
			expected: "Resource not found",
		},
		{
			name:     "unauthorized error",
			err:      fmt.Errorf("unauthorized access"),
			expected: "Authentication required",
		},
		{
			name:     "rate limit error",
			err:      fmt.Errorf("rate limit exceeded"),
			expected: "Rate limit exceeded",
		},
		{
			name:     "unknown error",
			err:      fmt.Errorf("something went wrong"),
			expected: "An error occurred while processing your request",
		},
		{
			name:     "nil error",
			err:      nil,
			expected: "Unknown error",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := getErrorMessage(tt.err)
			
			if message != tt.expected {
				t.Errorf("Expected message '%s', got '%s'", tt.expected, message)
			}
		})
	}
}

func TestToJSON(t *testing.T) {
	formatter := NewFormatter(&ResponseConfig{PrettyPrint: false})
	
	response := &SearchResponse{
		Success:   true,
		QueryID:   "test-query",
		Results:   []SearchResult{},
		Timestamp: time.Now(),
	}
	
	jsonData, err := formatter.ToJSON(response)
	
	if err != nil {
		t.Errorf("Expected no error converting to JSON, got: %v", err)
	}
	
	if len(jsonData) == 0 {
		t.Error("Expected non-empty JSON data")
	}
	
	// Verify it's valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonData, &parsed)
	if err != nil {
		t.Errorf("Expected valid JSON, got parsing error: %v", err)
	}
	
	if parsed["success"] != true {
		t.Error("Expected parsed JSON to have success=true")
	}
}

func TestToJSON_PrettyPrint(t *testing.T) {
	formatter := NewFormatter(&ResponseConfig{PrettyPrint: true})
	
	response := &SearchResponse{
		Success:   true,
		QueryID:   "test-query",
		Results:   []SearchResult{},
		Timestamp: time.Now(),
	}
	
	jsonData, err := formatter.ToJSON(response)
	
	if err != nil {
		t.Errorf("Expected no error converting to JSON, got: %v", err)
	}
	
	// Pretty printed JSON should contain newlines and indentation
	jsonStr := string(jsonData)
	if !contains(jsonStr, "\n") {
		t.Error("Expected pretty-printed JSON to contain newlines")
	}
	
	if !contains(jsonStr, "  ") {
		t.Error("Expected pretty-printed JSON to contain indentation")
	}
}

func TestFormatBatchResponse(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	// Create test batch results
	results := [][]*types.SearchResult{
		{
			{
				Vector: &types.Vector{
					ID:   "vector1",
					Data: []float32{1.0, 2.0},
				},
				Distance: 0.5,
			},
		},
		{
			{
				Vector: &types.Vector{
					ID:   "vector2",
					Data: []float32{3.0, 4.0},
				},
				Distance: 1.0,
			},
		},
	}
	
	queries := []*types.Vector{
		{ID: "query1", Data: []float32{1.0, 2.0}},
		{ID: "query2", Data: []float32{3.0, 4.0}},
	}
	
	queryIDs := []string{"batch-1", "batch-2"}
	stats := []*SearchStats{
		{TotalResults: 1, ReturnedResults: 1, ExecutionTime: 50 * time.Millisecond},
		{TotalResults: 1, ReturnedResults: 1, ExecutionTime: 60 * time.Millisecond},
	}
	
	response := formatter.FormatBatchResponse(results, queries, 10, queryIDs, stats)
	
	if response == nil {
		t.Fatal("Expected batch response to be created, got nil")
	}
	
	if !response.Success {
		t.Error("Expected batch response to be successful")
	}
	
	if response.BatchSize != 2 {
		t.Errorf("Expected BatchSize to be 2, got %d", response.BatchSize)
	}
	
	if len(response.Results) != 2 {
		t.Errorf("Expected 2 individual results, got %d", len(response.Results))
	}
	
	if response.Success != 2 {
		t.Errorf("Expected Success count to be 2, got %d", response.Success)
	}
	
	if response.Failed != 0 {
		t.Errorf("Expected Failed count to be 0, got %d", response.Failed)
	}
}

func TestFormatHealthResponse(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	healthy := true
	status := "healthy"
	details := map[string]interface{}{
		"uptime": "1h30m",
		"version": "1.0.0",
	}
	
	response := formatter.FormatHealthResponse(healthy, status, details)
	
	if response == nil {
		t.Fatal("Expected health response to be created, got nil")
	}
	
	if !response.Success {
		t.Error("Expected health response to be successful")
	}
	
	if response.Status != status {
		t.Errorf("Expected Status to be '%s', got '%s'", status, response.Status)
	}
	
	if response.Details == nil {
		t.Fatal("Expected details to be included")
	}
	
	if response.Details["uptime"] != "1h30m" {
		t.Errorf("Expected uptime detail to be '1h30m', got '%v'", response.Details["uptime"])
	}
}

func TestFormatMetricsResponse(t *testing.T) {
	formatter := NewFormatter(DefaultResponseConfig())
	
	metrics := map[string]interface{}{
		"requests_total": 1000,
		"avg_latency":    0.05,
		"error_rate":     0.01,
	}
	
	response := formatter.FormatMetricsResponse(metrics)
	
	if response == nil {
		t.Fatal("Expected metrics response to be created, got nil")
	}
	
	if !response.Success {
		t.Error("Expected metrics response to be successful")
	}
	
	if response.Metrics == nil {
		t.Fatal("Expected metrics to be included")
	}
	
	if response.Metrics["requests_total"] != 1000 {
		t.Errorf("Expected requests_total to be 1000, got %v", response.Metrics["requests_total"])
	}
}