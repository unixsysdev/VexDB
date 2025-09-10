//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"vexdb/internal/search/config"
	searchgrpc "vexdb/internal/search/grpc"
	"vexdb/internal/search/testutil"
	"vexdb/internal/types"

	pb "vexdb/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const bufSize = 1024 * 1024

// TestSearchServerIntegration tests the gRPC search server integration
func TestSearchServerIntegration(t *testing.T) {
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

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test cases
	testCases := []struct {
		name          string
		request       *pb.SearchRequest
		expectedError bool
		expectedCode  codes.Code
		validateFunc  func(t *testing.T, resp *pb.SearchResponse)
		multiCluster  bool
	}{
		{
			name: "Basic Search - Valid Request",
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query1",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K: 10,
			},
			expectedError: false,
			validateFunc: func(t *testing.T, resp *pb.SearchResponse) {
				if !resp.Success {
					t.Error("Expected successful response")
				}

				if len(resp.Results) != 2 {
					t.Errorf("Expected 2 results, got %d", len(resp.Results))
				}

				if resp.TotalResults != 2 {
					t.Errorf("Expected total results 2, got %d", resp.TotalResults)
				}

				if resp.Message != "Search completed successfully" {
					t.Errorf("Expected success message, got '%s'", resp.Message)
				}
			},
		},
		{
			name: "Basic Search - Invalid Request (Empty Vector)",
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query2",
					Data: []float32{},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K: 10,
			},
			expectedError: true,
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Basic Search - Invalid Request (Negative K)",
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query3",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K: -1,
			},
			expectedError: true,
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Basic Search - With Distance Threshold",
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query4",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K:                 10,
				DistanceThreshold: 1.0,
			},
			expectedError: false,
			validateFunc: func(t *testing.T, resp *pb.SearchResponse) {
				if !resp.Success {
					t.Error("Expected successful response")
				}

				// Should filter out results with distance > 1.0
				if len(resp.Results) != 1 {
					t.Errorf("Expected 1 result after distance filtering, got %d", len(resp.Results))
				}
			},
		},
		{
			name: "Basic Search - With Metadata Filter",
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query5",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K: 10,
				MetadataFilter: map[string]string{
					"category": "test",
				},
			},
			expectedError: false,
			validateFunc: func(t *testing.T, resp *pb.SearchResponse) {
				if !resp.Success {
					t.Error("Expected successful response")
				}
			},
		},
		{
			name:         "Multi-Cluster Search - Valid Request",
			multiCluster: true,
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query6",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K:          10,
				ClusterIds: []string{"cluster1", "cluster2", "cluster3"},
			},
			expectedError: false,
			validateFunc: func(t *testing.T, resp *pb.SearchResponse) {
				if !resp.Success {
					t.Error("Expected successful response")
				}

				if len(resp.Results) != 2 {
					t.Errorf("Expected 2 results, got %d", len(resp.Results))
				}

				// Should have clusters_queried in metadata
				if resp.Metadata["clusters_queried"] != "3" {
					t.Errorf("Expected clusters_queried to be '3', got '%s'", resp.Metadata["clusters_queried"])
				}
			},
		},
		{
			name:         "Multi-Cluster Search - Invalid Request (Empty Cluster IDs)",
			multiCluster: true,
			request: &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   "query7",
					Data: []float32{1.0, 2.0, 3.0},
					Metadata: map[string]string{
						"category": "test",
					},
					ClusterId: 1,
				},
				K:          10,
				ClusterIds: []string{},
			},
			expectedError: true,
			expectedCode:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock service for each test
			mockService.Reset()

			// Execute request
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var (
				resp *pb.SearchResponse
				err  error
			)
			if tc.multiCluster {
				resp, err = client.MultiClusterSearch(ctx, tc.request)
			} else {
				resp, err = client.Search(ctx, tc.request)
			}

			// Validate error handling
			if tc.expectedError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Fatal("Expected gRPC status error")
				}

				if st.Code() != tc.expectedCode {
					t.Errorf("Expected error code %v, got %v", tc.expectedCode, st.Code())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				if resp == nil {
					t.Fatal("Expected response, got nil")
				}

				// Validate response content
				if tc.validateFunc != nil {
					tc.validateFunc(t, resp)
				}
			}
		})
	}
}

// TestSearchServerStreaming tests streaming search functionality
func TestSearchServerStreaming(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Create test results
	testResults := []*types.SearchResult{
		{
			Vector: &types.Vector{
				ID:   "stream-vector1",
				Data: []float32{1.0, 2.0, 3.0},
			},
			Distance: 0.5,
		},
		{
			Vector: &types.Vector{
				ID:   "stream-vector2",
				Data: []float32{4.0, 5.0, 6.0},
			},
			Distance: 1.2,
		},
	}

	mockService.SetSearchResults(testResults)

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Create streaming request
	stream, err := client.SearchStream(ctx)
	if err != nil {
		t.Fatalf("Failed to create search stream: %v", err)
	}

	// Send multiple requests
	numRequests := 3
	for i := 0; i < numRequests; i++ {
		req := &pb.SearchRequest{
			QueryVector: &pb.Vector{
				Id:   fmt.Sprintf("stream-query-%d", i),
				Data: []float32{float32(i), float32(i + 1), float32(i + 2)},
			},
			K: 5,
		}

		if err := stream.Send(req); err != nil {
			t.Fatalf("Failed to send request %d: %v", i, err)
		}
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("Failed to close send: %v", err)
	}

	// Receive responses
	responseCount := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			break // Stream ended
		}

		responseCount++

		if !resp.Success {
			t.Errorf("Response %d: expected success, got failure", responseCount)
		}

		if len(resp.Results) != 2 {
			t.Errorf("Response %d: expected 2 results, got %d", responseCount, len(resp.Results))
		}
	}

	if responseCount != numRequests {
		t.Errorf("Expected %d responses, got %d", numRequests, responseCount)
	}
}

// TestSearchServerMultiClusterStreaming tests multi-cluster streaming search
func TestSearchServerMultiClusterStreaming(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Create test results
	testResults := []*types.SearchResult{
		{
			Vector: &types.Vector{
				ID:   "multi-stream-vector1",
				Data: []float32{1.0, 2.0, 3.0},
			},
			Distance: 0.5,
		},
	}

	mockService.SetSearchResults(testResults)

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Create streaming request
	stream, err := client.MultiClusterSearchStream(ctx)
	if err != nil {
		t.Fatalf("Failed to create multi-cluster search stream: %v", err)
	}

	// Send multiple requests
	numRequests := 2
	for i := 0; i < numRequests; i++ {
		req := &pb.SearchRequest{
			QueryVector: &pb.Vector{
				Id:   fmt.Sprintf("multi-stream-query-%d", i),
				Data: []float32{float32(i), float32(i + 1), float32(i + 2)},
			},
			K:          5,
			ClusterIds: []string{"cluster1", "cluster2"},
		}

		if err := stream.Send(req); err != nil {
			t.Fatalf("Failed to send request %d: %v", i, err)
		}
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("Failed to close send: %v", err)
	}

	// Receive responses
	responseCount := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			break // Stream ended
		}

		responseCount++

		if !resp.Success {
			t.Errorf("Response %d: expected success, got failure", responseCount)
		}

		if len(resp.Results) != 1 {
			t.Errorf("Response %d: expected 1 result, got %d", responseCount, len(resp.Results))
		}

		// Should have clusters_queried in metadata
		if resp.Metadata["clusters_queried"] != "2" {
			t.Errorf("Response %d: expected clusters_queried to be '2', got '%s'", responseCount, resp.Metadata["clusters_queried"])
		}
	}

	if responseCount != numRequests {
		t.Errorf("Expected %d responses, got %d", numRequests, responseCount)
	}
}

// TestSearchServerHealthCheck tests health check functionality
func TestSearchServerHealthCheck(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test basic health check
	req := &pb.HealthCheckRequest{Detailed: false}
	resp, err := client.HealthCheck(ctx, req)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if !resp.Healthy {
		t.Error("Expected health check to return healthy")
	}

	if resp.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", resp.Status)
	}

	// Test detailed health check
	req = &pb.HealthCheckRequest{Detailed: true}
	resp, err = client.HealthCheck(ctx, req)
	if err != nil {
		t.Fatalf("Detailed health check failed: %v", err)
	}

	if !resp.Healthy {
		t.Error("Expected detailed health check to return healthy")
	}
}

// TestSearchServerGetMetrics tests metrics retrieval
func TestSearchServerGetMetrics(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test system metrics
	req := &pb.MetricsRequest{
		MetricType: "system",
	}
	resp, err := client.GetMetrics(ctx, req)
	if err != nil {
		t.Fatalf("Get metrics failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected get metrics to return success")
	}

	if len(resp.Metrics) == 0 {
		t.Error("Expected metrics to be returned")
	}

	// Test cluster metrics
	req = &pb.MetricsRequest{
		MetricType: "cluster",
		ClusterId:  "cluster1",
	}
	resp, err = client.GetMetrics(ctx, req)
	if err != nil {
		t.Fatalf("Get cluster metrics failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected get cluster metrics to return success")
	}
}

// TestSearchServerConfiguration tests configuration management
func TestSearchServerConfiguration(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test get configuration
	req := &emptypb.Empty{}
	resp, err := client.GetConfig(ctx, req)
	if err != nil {
		t.Fatalf("Get config failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected get config to return success")
	}

	// Test update configuration
	updateReq := &pb.ConfigUpdateRequest{
		ConfigUpdates: map[string]string{
			"search.engine.max_results": "2000",
			"search.engine.default_k":   "20",
		},
		RestartRequired: false,
	}

	updateResp, err := client.UpdateConfig(ctx, updateReq)
	if err != nil {
		t.Fatalf("Update config failed: %v", err)
	}

	if !updateResp.Success {
		t.Error("Expected update config to return success")
	}

	if len(updateResp.AppliedUpdates) != 2 {
		t.Errorf("Expected 2 applied updates, got %d", len(updateResp.AppliedUpdates))
	}
}

// TestSearchServerErrorHandling tests error handling scenarios
func TestSearchServerErrorHandling(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Configure service to fail
	mockService.SetShouldFail(true)
	mockService.SetFailureError(fmt.Errorf("search service unavailable"))

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test search with service failure
	req := &pb.SearchRequest{
		QueryVector: &pb.Vector{
			Id:   "error-query",
			Data: []float32{1.0, 2.0, 3.0},
		},
		K: 10,
	}

	_, err = client.Search(ctx, req)
	if err == nil {
		t.Fatal("Expected error for failed search, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("Expected gRPC status error")
	}

	if st.Code() != codes.Internal {
		t.Errorf("Expected error code %v, got %v", codes.Internal, st.Code())
	}

	if st.Message() != "Search operation failed" {
		t.Errorf("Expected error message 'Search operation failed', got '%s'", st.Message())
	}
}

// TestSearchServerConcurrentRequests tests concurrent request handling
func TestSearchServerConcurrentRequests(t *testing.T) {
	// Setup
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

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Test concurrent requests
	numRequests := 10
	done := make(chan bool, numRequests)
	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(index int) {
			req := &pb.SearchRequest{
				QueryVector: &pb.Vector{
					Id:   fmt.Sprintf("concurrent-query-%d", index),
					Data: []float32{float32(index), float32(index + 1), float32(index + 2)},
				},
				K: 5,
			}

			resp, err := client.Search(ctx, req)
			if err != nil {
				errors <- fmt.Errorf("request %d failed: %v", index, err)
				done <- false
				return
			}

			if !resp.Success {
				errors <- fmt.Errorf("request %d: expected success, got failure", index)
				done <- false
				return
			}

			done <- true
		}(i)
	}

	// Wait for all requests to complete
	successCount := 0
	for i := 0; i < numRequests; i++ {
		select {
		case success := <-done:
			if success {
				successCount++
			}
		case err := <-errors:
			t.Error(err)
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for concurrent requests")
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

// TestSearchServerTimeout tests request timeout handling
func TestSearchServerTimeout(t *testing.T) {
	// Setup
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	mockService := testutil.NewMockSearchService()

	// Configure service to have a delay longer than timeout
	mockService.SetSearchDelay(2 * time.Second)

	// Create gRPC server
	server := searchgrpc.NewSearchServer(cfg, logger, nil, mockService)

	// Create test listener
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, server)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Fatalf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewSearchServiceClient(conn)

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := &pb.SearchRequest{
		QueryVector: &pb.Vector{
			Id:   "timeout-query",
			Data: []float32{1.0, 2.0, 3.0},
		},
		K: 10,
	}

	// This should timeout
	_, err = client.Search(ctx, req)
	if err == nil {
		t.Error("Expected request to timeout, but it succeeded")
	}

	// Should be a deadline exceeded error
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("Expected gRPC status error")
	}

	if st.Code() != codes.DeadlineExceeded {
		t.Errorf("Expected error code %v, got %v", codes.DeadlineExceeded, st.Code())
	}
}
