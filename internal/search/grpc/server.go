package grpc

import (
	"context"
	"fmt"
	"math"
	"time"

	"vexdb/internal/metrics"
	"vexdb/internal/search/config"
	"vexdb/internal/search/response"
	"vexdb/internal/search/validation"
	"vexdb/internal/service"
	"vexdb/internal/types"

	pb "vexdb/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    emptypb "google.golang.org/protobuf/types/known/emptypb"
    timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// SearchServer implements the gRPC SearchService
type SearchServer struct {
	pb.UnimplementedSearchServiceServer
	config    *config.SearchServiceConfig
	logger    *zap.Logger
	metrics   *metrics.Collector
	validator *validation.SearchRequestValidator
	formatter *response.Formatter
	service   service.SearchService
}

// NewSearchServer creates a new gRPC search server
func NewSearchServer(cfg *config.SearchServiceConfig, logger *zap.Logger, metrics *metrics.Collector, service service.SearchService) *SearchServer {
	validator := validation.NewSearchRequestValidator(
		10000, // maxVectorDim
		cfg.Engine.MaxResults,
		cfg.Engine.MaxResults,
		100, // maxMetadataFilters
	)
	
	formatter := response.NewFormatter(&response.ResponseConfig{
		IncludeQueryVector: cfg.API.Response.IncludeQueryVector,
		IncludeMetadata:    cfg.API.Response.IncludeMetadata,
		IncludeTimestamps:  cfg.API.Response.IncludeTimestamps,
		PrettyPrint:        cfg.API.Response.PrettyPrint,
		MaxResults:         cfg.Engine.MaxResults,
		ResponseTimeout:    cfg.Server.WriteTimeout,
	})

	return &SearchServer{
		config:    cfg,
		logger:    logger,
		metrics:   metrics,
		validator: validator,
		formatter: formatter,
		service:   service,
	}
}

// RegisterServer registers the search server with the gRPC server
func (s *SearchServer) RegisterServer(server *grpc.Server) {
	pb.RegisterSearchServiceServer(server, s)
}

// Search performs a similarity search
func (s *SearchServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_search", time.Since(startTime), nil)
	}()

	// Validate request
	if err := s.validateSearchRequest(req); err != nil {
		s.logger.Warn("Invalid search request", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Convert protobuf request to internal types
    queryVector := &types.Vector{
        ID:        req.QueryVector.Id,
        Data:      req.QueryVector.Data,
        Metadata:  toMetadata(req.QueryVector.Metadata),
        ClusterID: req.QueryVector.ClusterId,
    }

	// Set default k if not provided
	k := int(req.K)
	if k <= 0 {
		k = s.config.Engine.DefaultK
	}

	// Convert metadata filter
	var metadataFilter map[string]string
	if req.MetadataFilter != nil {
		metadataFilter = req.MetadataFilter
	}

	// Perform search
	results, err := s.service.Search(ctx, queryVector, k, metadataFilter)
	if err != nil {
		s.logger.Error("Search failed", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Search operation failed")
	}

	// Apply distance threshold if provided
    if req.DistanceThreshold > 0 {
        results = s.filterByDistance(results, float64(req.DistanceThreshold))
    }

	// Convert results to protobuf format
	pbResults := s.convertToProtoResults(results)

	// Build response metadata
	metadata := map[string]string{
		"query_id":        queryID,
		"execution_time":  time.Since(startTime).String(),
		"distance_type":   s.config.Engine.DistanceType,
	}

	return &pb.SearchResponse{
		Success:      true,
		Message:      "Search completed successfully",
		Results:      pbResults,
		TotalResults: int64(len(pbResults)),
		Metadata:     metadata,
	}, nil
}

// SearchStream handles streaming search requests
func (s *SearchServer) SearchStream(stream pb.SearchService_SearchStreamServer) error {
	ctx := stream.Context()
	queryID := generateQueryID()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			s.logger.Error("Failed to receive stream request", zap.String("query_id", queryID), zap.Error(err))
			return status.Error(codes.Internal, "Failed to receive request")
		}

		// Process individual search request
		resp, err := s.Search(ctx, req)
		if err != nil {
			s.logger.Error("Stream search failed", zap.String("query_id", queryID), zap.Error(err))
			return err
		}

		// Send response
		if err := stream.Send(resp); err != nil {
			s.logger.Error("Failed to send stream response", zap.String("query_id", queryID), zap.Error(err))
			return status.Error(codes.Internal, "Failed to send response")
		}
	}
}

// MultiClusterSearch performs a search across multiple clusters
func (s *SearchServer) MultiClusterSearch(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_multi_cluster_search", time.Since(startTime), nil)
	}()

	// Validate request
	if err := s.validateMultiClusterSearchRequest(req); err != nil {
		s.logger.Warn("Invalid multi-cluster search request", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Convert protobuf request to internal types
    queryVector := &types.Vector{
        ID:        req.QueryVector.Id,
        Data:      req.QueryVector.Data,
        Metadata:  toMetadata(req.QueryVector.Metadata),
        ClusterID: req.QueryVector.ClusterId,
    }

	// Set default k if not provided
	k := int(req.K)
	if k <= 0 {
		k = s.config.Engine.DefaultK
	}

	// Convert cluster IDs
	clusterIDs := make([]string, len(req.ClusterIds))
	for i, clusterID := range req.ClusterIds {
		clusterIDs[i] = clusterID
	}

	// Convert metadata filter
	var metadataFilter map[string]string
	if req.MetadataFilter != nil {
		metadataFilter = req.MetadataFilter
	}

	// Perform multi-cluster search
	results, err := s.service.MultiClusterSearch(ctx, queryVector, k, clusterIDs, metadataFilter)
	if err != nil {
		s.logger.Error("Multi-cluster search failed", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Multi-cluster search operation failed")
	}

	// Apply distance threshold if provided
    if req.DistanceThreshold > 0 {
        results = s.filterByDistance(results, float64(req.DistanceThreshold))
    }

	// Convert results to protobuf format
	pbResults := s.convertToProtoResults(results)

	// Build response metadata
	metadata := map[string]string{
		"query_id":         queryID,
		"execution_time":   time.Since(startTime).String(),
		"distance_type":    s.config.Engine.DistanceType,
		"clusters_queried": fmt.Sprintf("%d", len(clusterIDs)),
	}

	return &pb.SearchResponse{
		Success:      true,
		Message:      "Multi-cluster search completed successfully",
		Results:      pbResults,
		TotalResults: int64(len(pbResults)),
		Metadata:     metadata,
	}, nil
}

// MultiClusterSearchStream handles streaming multi-cluster search requests
func (s *SearchServer) MultiClusterSearchStream(stream pb.SearchService_MultiClusterSearchStreamServer) error {
	ctx := stream.Context()
	queryID := generateQueryID()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			s.logger.Error("Failed to receive multi-cluster stream request", zap.String("query_id", queryID), zap.Error(err))
			return status.Error(codes.Internal, "Failed to receive request")
		}

		// Process individual multi-cluster search request
		resp, err := s.MultiClusterSearch(ctx, req)
		if err != nil {
			s.logger.Error("Stream multi-cluster search failed", zap.String("query_id", queryID), zap.Error(err))
			return err
		}

		// Send response
		if err := stream.Send(resp); err != nil {
			s.logger.Error("Failed to send multi-cluster stream response", zap.String("query_id", queryID), zap.Error(err))
			return status.Error(codes.Internal, "Failed to send response")
		}
	}
}

// GetClusterInfo returns information about a cluster
func (s *SearchServer) GetClusterInfo(ctx context.Context, req *pb.ClusterInfo) (*pb.ClusterInfo, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_get_cluster_info", time.Since(startTime), nil)
	}()

	clusterInfo, err := s.service.GetClusterInfo(ctx, req.Id)
	if err != nil {
		s.logger.Error("Failed to get cluster info", zap.String("query_id", queryID), zap.String("cluster_id", req.Id), zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to get cluster information")
	}

	// Convert to protobuf format
    pbClusterInfo := &pb.ClusterInfo{
        Id:       clusterInfo.ID,
        Metadata: clusterInfo.Metadata,
    }

	return pbClusterInfo, nil
}

// GetClusterStatus returns the status of all clusters
func (s *SearchServer) GetClusterStatus(ctx context.Context, req *emptypb.Empty) (*pb.ClusterStatus, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_get_cluster_status", time.Since(startTime), nil)
	}()

    if _, err := s.service.GetClusterStatus(ctx); err != nil {
        s.logger.Error("Failed to get cluster status", zap.String("query_id", queryID), zap.Error(err))
        return nil, status.Error(codes.Internal, "Failed to get cluster status")
    }

    // Convert to protobuf format (limited internal fields available)
    pbClusterStatus := &pb.ClusterStatus{}

    return pbClusterStatus, nil
}

// HealthCheck performs a health check
func (s *SearchServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_health_check", time.Since(startTime), nil)
	}()

	healthStatus, err := s.service.HealthCheck(ctx, req.Detailed)
	if err != nil {
		s.logger.Error("Health check failed", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Health check failed")
	}

	// Convert to protobuf format
    pbHealthResponse := &pb.HealthCheckResponse{
        Healthy:   healthStatus.Status == "healthy",
        Status:    healthStatus.Status,
        Message:   healthStatus.Message,
        Details:   healthStatus.Details,
        Timestamp: timestamppb.Now(),
    }

	return pbHealthResponse, nil
}

// GetMetrics returns metrics information
func (s *SearchServer) GetMetrics(ctx context.Context, req *pb.MetricsRequest) (*pb.MetricsResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_get_metrics", time.Since(startTime), nil)
	}()

	metrics, err := s.service.GetMetrics(ctx, req.MetricType, req.ClusterId, req.NodeId)
	if err != nil {
		s.logger.Error("Failed to get metrics", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to get metrics")
	}

	// Convert metrics to protobuf format
	pbMetrics := make(map[string]float64)
	for key, value := range metrics {
		pbMetrics[key] = value
	}

    pbMetricsResponse := &pb.MetricsResponse{
        Success:   true,
        Message:   "Metrics retrieved successfully",
        Metrics:   pbMetrics,
        Timestamp: timestamppb.Now(),
    }

	return pbMetricsResponse, nil
}

// UpdateConfig updates the service configuration
func (s *SearchServer) UpdateConfig(ctx context.Context, req *pb.ConfigUpdateRequest) (*pb.ConfigUpdateResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_update_config", time.Since(startTime), nil)
	}()

	if err := s.service.UpdateConfig(ctx, req.ConfigUpdates); err != nil {
		s.logger.Error("Failed to update configuration", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to update configuration")
	}

	pbConfigResponse := &pb.ConfigUpdateResponse{
		Success:        true,
		Message:        "Configuration updated successfully",
		AppliedUpdates: req.ConfigUpdates,
	}

	return pbConfigResponse, nil
}

// GetConfig returns the current configuration
func (s *SearchServer) GetConfig(ctx context.Context, req *emptypb.Empty) (*pb.ConfigUpdateResponse, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	defer func() {
		s.recordMetrics("grpc_get_config", time.Since(startTime), nil)
	}()

	config, err := s.service.GetConfig(ctx)
	if err != nil {
		s.logger.Error("Failed to get configuration", zap.String("query_id", queryID), zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to get configuration")
	}

	pbConfigResponse := &pb.ConfigUpdateResponse{
		Success: true,
		Message: "Configuration retrieved successfully",
		AppliedUpdates: config,
	}

	return pbConfigResponse, nil
}

// Validation methods

func (s *SearchServer) validateSearchRequest(req *pb.SearchRequest) error {
	if req.QueryVector == nil {
		return fmt.Errorf("query vector is required")
	}

	if len(req.QueryVector.Data) == 0 {
		return fmt.Errorf("query vector data cannot be empty")
	}

	if req.K < 0 {
		return fmt.Errorf("k must be non-negative")
	}

	if req.K > int32(s.config.Engine.MaxResults) {
		return fmt.Errorf("k exceeds maximum allowed value of %d", s.config.Engine.MaxResults)
	}

	return nil
}

func (s *SearchServer) validateMultiClusterSearchRequest(req *pb.SearchRequest) error {
	if err := s.validateSearchRequest(req); err != nil {
		return err
	}

	if len(req.ClusterIds) == 0 {
		return fmt.Errorf("cluster IDs cannot be empty for multi-cluster search")
	}

	return nil
}

// Helper methods

func (s *SearchServer) convertToProtoResults(results []*types.SearchResult) []*pb.SearchResult {
	pbResults := make([]*pb.SearchResult, 0, len(results))

	for _, result := range results {
		pbResult := &pb.SearchResult{
            Vector: &pb.Vector{
                Id:        result.Vector.ID,
                Data:      result.Vector.Data,
                Metadata:  toStringMetadata(result.Vector.Metadata),
                ClusterId: result.Vector.ClusterID,
            },
			Distance:  float32(result.Distance),
			Score:     float32(calculateScore(result.Distance)),
			ClusterId: fmt.Sprintf("%d", result.Vector.ClusterID),
		}

		pbResults = append(pbResults, pbResult)
	}

	return pbResults
}

func (s *SearchServer) filterByDistance(results []*types.SearchResult, threshold float64) []*types.SearchResult {
    filtered := make([]*types.SearchResult, 0, len(results))
    for _, result := range results {
        if result.Distance <= threshold {
            filtered = append(filtered, result)
        }
    }
    return filtered
}

func (s *SearchServer) recordMetrics(operation string, duration time.Duration, err error) {
	s.metrics.RecordHistogram("grpc_search_request_duration", duration.Seconds(), map[string]string{
		"operation": operation,
		"status":    getStatusLabel(err),
	})
}

func generateQueryID() string {
	return fmt.Sprintf("query_%d", time.Now().UnixNano())
}

func getStatusLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

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

func toMetadata(in map[string]string) map[string]interface{} {
    if in == nil {
        return nil
    }
    out := make(map[string]interface{}, len(in))
    for k, v := range in {
        out[k] = v
    }
    return out
}

func toStringMetadata(in map[string]interface{}) map[string]string {
    if in == nil {
        return nil
    }
    out := make(map[string]string, len(in))
    for k, v := range in {
        out[k] = fmt.Sprint(v)
    }
    return out
}
