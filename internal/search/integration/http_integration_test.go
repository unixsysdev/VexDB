package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vexdb/internal/search/api"
	"vexdb/internal/search/config"
	"vexdb/internal/search/response"
	"vexdb/internal/search/testutil"
	"vexdb/internal/types"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// TestSearchHandlerIntegration tests the HTTP search handler integration
func TestSearchHandlerIntegration(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()
	
	// Create test results
	testResults := []*types.SearchResult{
		{
			Vector: &types.Vector{
				ID:   "vector1",
				Data: []float32{1.0, 2.0, 3.0},
				Metadata: map[string]interface{}{
					"category": "test",
					"source":   "integration_test",
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
					"source":   "integration_test",
				},
				ClusterID: 2,
			},
			Distance: 1.2,
		},
	}
	
	mockService.SetSearchResults(testResults)
	
	handler := api.NewSearchHandler(cfg, logger, nil, mockService)
	
	// Create test server
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Test cases
	testCases := []struct {
		name           string
		method         string
		path           string
		body           interface{}
		expectedStatus int
		validateFunc   func(t *testing.T, resp *http.Response)
	}{
		{
			name:   "Basic Search - Valid Request",
			method: "POST",
			path:   "/search",
			body: map[string]interface{}{
				"vector": []float64{1.0, 2.0, 3.0},
				"k":      10,
			},
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.SearchResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful response")
				}
				
				if len(result.Results) != 2 {
					t.Errorf("Expected 2 results, got %d", len(result.Results))
				}
				
				if result.QueryID == "" {
					t.Error("Expected query ID to be set")
				}
			},
		},
		{
			name:   "Basic Search - Invalid Request (Empty Vector)",
			method: "POST",
			path:   "/search",
			body: map[string]interface{}{
				"vector": []float64{},
				"k":      10,
			},
			expectedStatus: http.StatusBadRequest,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.ErrorResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode error response: %v", err)
				}
				
				if result.Success {
					t.Error("Expected error response to have Success=false")
				}
			},
		},
		{
			name:   "Similarity Search - Valid Request",
			method: "POST",
			path:   "/search/similarity",
			body: map[string]interface{}{
				"vector":             []float64{1.0, 2.0, 3.0},
				"k":                  5,
				"distance_threshold": 2.0,
				"filters": []map[string]interface{}{
					{
						"key":      "category",
						"operator": "=",
						"value":    "test",
					},
				},
			},
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.SearchResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful response")
				}
			},
		},
		{
			name:   "Batch Search - Valid Request",
			method: "POST",
			path:   "/search/batch",
			body: map[string]interface{}{
				"queries": []map[string]interface{}{
					{
						"vector": []float64{1.0, 2.0, 3.0},
						"k":      5,
					},
					{
						"vector": []float64{4.0, 5.0, 6.0},
						"k":      3,
					},
				},
			},
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.BatchSearchResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode batch response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful batch response")
				}
				
				if result.BatchSize != 2 {
					t.Errorf("Expected batch size 2, got %d", result.BatchSize)
				}
			},
		},
		{
			name:   "Multi-Cluster Search - Valid Request",
			method: "POST",
			path:   "/search/multi-cluster",
			body: map[string]interface{}{
				"vector":     []float64{1.0, 2.0, 3.0},
				"k":          10,
				"cluster_ids": []string{"cluster1", "cluster2"},
			},
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.SearchResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful response")
				}
			},
		},
		{
			name:   "Health Check - Basic",
			method: "GET",
			path:   "/health",
			body:   nil,
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.HealthResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode health response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful health response")
				}
			},
		},
		{
			name:   "Health Check - Detailed",
			method: "GET",
			path:   "/health/detailed",
			body:   nil,
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.HealthResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode detailed health response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful detailed health response")
				}
			},
		},
		{
			name:   "Metrics - System Metrics",
			method: "GET",
			path:   "/metrics?type=system",
			body:   nil,
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result response.MetricsResponse
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode metrics response: %v", err)
				}
				
				if !result.Success {
					t.Error("Expected successful metrics response")
				}
			},
		},
		{
			name:   "Get Configuration",
			method: "GET",
			path:   "/config",
			body:   nil,
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				// Should return configuration as JSON
				var result map[string]interface{}
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode config response: %v", err)
				}
			},
		},
		{
			name:   "Update Configuration",
			method: "PUT",
			path:   "/config",
			body: map[string]interface{}{
				"search.engine.max_results": "2000",
				"search.engine.default_k":   "20",
			},
			expectedStatus: http.StatusOK,
			validateFunc: func(t *testing.T, resp *http.Response) {
				var result map[string]interface{}
				err := json.NewDecoder(resp.Body).Decode(&result)
				if err != nil {
					t.Fatalf("Failed to decode config update response: %v", err)
				}
				
				if result["success"] != true {
					t.Error("Expected successful config update")
				}
			},
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock service for each test
			mockService.Reset()
			
			// Prepare request
			var req *http.Request
			var err error
			
			if tc.body != nil {
				bodyBytes, err := json.Marshal(tc.body)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req, err = http.NewRequest(tc.method, server.URL+tc.path, bytes.NewBuffer(bodyBytes))
				if err != nil {
					t.Fatalf("Failed to create request: %v", err)
				}
				req.Header.Set("Content-Type", "application/json")
			} else {
				req, err = http.NewRequest(tc.method, server.URL+tc.path, nil)
				if err != nil {
					t.Fatalf("Failed to create request: %v", err)
				}
			}
			
			// Execute request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to execute request: %v", err)
			}
			defer resp.Body.Close()
			
			// Validate status code
			if resp.StatusCode != tc.expectedStatus {
				t.Errorf("Expected status %d, got %d", tc.expectedStatus, resp.StatusCode)
			}
			
			// Validate response content
			if tc.validateFunc != nil {
				tc.validateFunc(t, resp)
			}
		})
	}
}

// TestSearchHandlerErrorScenarios tests error handling scenarios
func TestSearchHandlerErrorScenarios(t *testing.T) {
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()
	
	// Configure service to fail
	mockService.SetShouldFail(true)
	mockService.SetFailureError(fmt.Errorf("search service unavailable"))
	
	handler := api.NewSearchHandler(cfg, logger, nil, mockService)
	
	// Create test server
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Test search with service failure
	reqBody := map[string]interface{}{
		"vector": []float64{1.0, 2.0, 3.0},
		"k":      10,
	}
	
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", server.URL+"/search", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute request: %v", err)
	}
	defer resp.Body.Close()
	
	// Should return 500 Internal Server Error
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", resp.StatusCode)
	}
	
	// Validate error response
	var result response.ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}
	
	if result.Success {
		t.Error("Expected error response to have Success=false")
	}
}

// TestSearchHandlerConcurrentRequests tests concurrent request handling
func TestSearchHandlerConcurrentRequests(t *testing.T) {
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()
	
	// Create test results
	testResults := []*types.SearchResult{
		{
			Vector: &types.Vector{
				ID:   "concurrent-vector",
				Data: []float32{1.0, 2.0, 3.0},
			},
			Distance: 0.5,
		},
	}
	mockService.SetSearchResults(testResults)
	
	handler := api.NewSearchHandler(cfg, logger, nil, mockService)
	
	// Create test server
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Test concurrent requests
	numRequests := 10
	done := make(chan bool, numRequests)
	
	for i := 0; i < numRequests; i++ {
		go func(index int) {
			reqBody := map[string]interface{}{
				"vector": []float64{float64(index), float64(index + 1), float64(index + 2)},
				"k":      5,
			}
			
			bodyBytes, _ := json.Marshal(reqBody)
			req, _ := http.NewRequest("POST", server.URL+"/search", bytes.NewBuffer(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("Request %d failed: %v", index, err)
				done <- false
				return
			}
			defer resp.Body.Close()
			
			if resp.StatusCode != http.StatusOK {
				t.Errorf("Request %d: expected status 200, got %d", index, resp.StatusCode)
				done <- false
				return
			}
			
			done <- true
		}(i)
	}
	
	// Wait for all requests to complete
	successCount := 0
	for i := 0; i < numRequests; i++ {
		if <-done {
			successCount++
		}
	}
	
	if successCount != numRequests {
		t.Errorf("Expected all %d requests to succeed, got %d", numRequests, successCount)
	}
	
	// Verify that all requests were processed
	if mockService.GetSearchCallCount() != numRequests {
		t.Errorf("Expected %d search calls, got %d", numRequests, mockService.GetSearchCallCount())
	}
}

// TestSearchHandlerTimeout tests request timeout handling
func TestSearchHandlerTimeout(t *testing.T) {
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()
	
	// Configure service to have a delay longer than timeout
	mockService.SetSearchDelay(2 * time.Second)
	
	handler := api.NewSearchHandler(cfg, logger, nil, mockService)
	
	// Create test server with short timeout
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Create request with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	reqBody := map[string]interface{}{
		"vector": []float64{1.0, 2.0, 3.0},
		"k":      10,
	}
	
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/search", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	
	// This should timeout
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Error("Expected request to timeout, but it succeeded")
	}
}

// TestSearchHandlerCORS tests CORS functionality
func TestSearchHandlerCORS(t *testing.T) {
	cfg := config.DefaultSearchServiceConfig()
	cfg.API.HTTP.CORS.Enabled = true
	cfg.API.HTTP.CORS.AllowedOrigins = []string{"http://localhost:3000", "https://example.com"}
	
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()
	handler := api.NewSearchHandler(cfg, logger, nil, mockService)
	
	// Create test server
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Test preflight request
	req, _ := http.NewRequest("OPTIONS", server.URL+"/search", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute preflight request: %v", err)
	}
	defer resp.Body.Close()
	
	// Validate CORS headers
	allowedOrigin := resp.Header.Get("Access-Control-Allow-Origin")
	if allowedOrigin != "http://localhost:3000" {
		t.Errorf("Expected allowed origin 'http://localhost:3000', got '%s'", allowedOrigin)
	}
	
	allowedMethods := resp.Header.Get("Access-Control-Allow-Methods")
	if allowedMethods == "" {
		t.Error("Expected Access-Control-Allow-Methods header")
	}
}

// Helper functions for integration tests

func createTestVector(dimension int) []float64 {
	vector := make([]float64, dimension)
	for i := 0; i < dimension; i++ {
		vector[i] = float64(i) * 0.1
	}
	return vector
}

func createTestMetadata() map[string]interface{} {
	return map[string]interface{}{
		"category": "test",
		"source":   "integration_test",
		"version":  "1.0.0",
	}
}

func validateJSONResponse(t *testing.T, resp *http.Response, expectedStatus int) {
	if resp.StatusCode != expectedStatus {
		t.Errorf("Expected status %d, got %d", expectedStatus, resp.StatusCode)
	}
	
	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", contentType)
	}
}