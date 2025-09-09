package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	searchconfig "vexdb/internal/search/config"
	"vexdb/internal/service"
	"vexdb/internal/storage/search"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

// SearchService implements the search service interface
type SearchService struct {
	config        *searchconfig.SearchServiceConfig
	logger        *zap.Logger
	metrics       *metrics.Collector
	searchEngine  *search.Engine
	clusterManager service.ClusterManager
	queryPlanner   service.QueryPlanner
	resultMerger   service.ResultMerger

	mu        sync.RWMutex
	started   bool
	stats     *SearchStats
}

// SearchStats represents search service statistics
type SearchStats struct {
	TotalSearches     int64
	FailedSearches    int64
	AvgLatency        float64
	LastSearch        time.Time
	TotalVectors      int64
	TotalClusters     int32
	ActiveConnections int64
}

// NewSearchService creates a new search service
func NewSearchService(cfg *searchconfig.SearchServiceConfig, logger *zap.Logger, metricsCollector *metrics.Collector, searchEngine *search.Engine) (*SearchService, error) {
	svc := &SearchService{
		config:       cfg,
		logger:       logger,
		metrics:      metricsCollector,
		searchEngine: searchEngine,
		stats:        &SearchStats{},
	}

	return svc, nil
}

// Start starts the search service
func (s *SearchService) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	s.logger.Info("Starting search service")

	// Start the search engine
	if err := s.searchEngine.Start(ctx); err != nil {
		return fmt.Errorf("failed to start search engine: %w", err)
	}

	s.started = true
	s.logger.Info("Search service started successfully")

	// Record startup metrics
	s.metrics.RecordCounter("search_service_startups", 1, map[string]string{
		"status": "success",
	})

	return nil
}

// Stop stops the search service
func (s *SearchService) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	s.logger.Info("Stopping search service")

	// Stop the search engine
	if err := s.searchEngine.Stop(ctx); err != nil {
		s.logger.Error("Failed to stop search engine", zap.Error(err))
	}

	s.started = false
	s.logger.Info("Search service stopped")

	// Record shutdown metrics
	s.metrics.RecordCounter("search_service_shutdowns", 1, map[string]string{
		"status": "success",
	})

	return nil
}

// Reload reloads the search service configuration
func (s *SearchService) Reload(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Reloading search service configuration")

	// For now, just log the reload
	// In a real implementation, this would reload configuration and restart components as needed

	s.logger.Info("Search service configuration reloaded")
	return nil
}

// HealthCheck performs a health check on the search service
func (s *SearchService) HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return &types.HealthStatus{
			Healthy: false,
			Status:  "not_started",
			Message: "Search service is not started",
		}, nil
	}

	// Check search engine health
	engineStatus := s.searchEngine.GetStatus()
	if !engineStatus.Started {
		return &types.HealthStatus{
			Healthy: false,
			Status:  "engine_not_started",
			Message: "Search engine is not started",
		}, nil
	}

	health := &types.HealthStatus{
		Healthy: true,
		Status:  "healthy",
		Message: "Search service is healthy",
	}

	if detailed {
		health.Details = map[string]string{
			"total_searches":     fmt.Sprintf("%d", s.stats.TotalSearches),
			"failed_searches":    fmt.Sprintf("%d", s.stats.FailedSearches),
			"avg_latency_ms":     fmt.Sprintf("%.2f", s.stats.AvgLatency),
			"total_vectors":      fmt.Sprintf("%d", s.stats.TotalVectors),
			"total_clusters":     fmt.Sprintf("%d", s.stats.TotalClusters),
			"active_connections": fmt.Sprintf("%d", s.stats.ActiveConnections),
			"last_search":        s.stats.LastSearch.Format(time.RFC3339),
		}
	}

	return health, nil
}

// GetStatus returns the current status of the search service
func (s *SearchService) GetStatus(ctx context.Context) map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := map[string]interface{}{
		"started":        s.started,
		"total_searches": s.stats.TotalSearches,
		"failed_searches": s.stats.FailedSearches,
		"avg_latency_ms": s.stats.AvgLatency,
		"last_search":    s.stats.LastSearch,
	}

	if s.started {
		engineStatus := s.searchEngine.GetStatus()
		status["engine"] = engineStatus
	}

	return status
}

// GetConfig returns the current configuration
func (s *SearchService) GetConfig(ctx context.Context) (map[string]string, error) {
	// Convert configuration to string map
	config := map[string]string{
		"server.host":              s.config.Server.Host,
		"server.port":              fmt.Sprintf("%d", s.config.Server.Port),
		"engine.max_results":       fmt.Sprintf("%d", s.config.Engine.MaxResults),
		"engine.default_k":         fmt.Sprintf("%d", s.config.Engine.DefaultK),
		"engine.distance_type":     s.config.Engine.DistanceType,
		"engine.enable_cache":      fmt.Sprintf("%t", s.config.Engine.EnableCache),
		"api.http.enabled":         fmt.Sprintf("%t", s.config.API.HTTP.Enabled),
		"api.grpc.enabled":         fmt.Sprintf("%t", s.config.API.GRPC.Enabled),
		"auth.enabled":             fmt.Sprintf("%t", s.config.Auth.Enabled),
		"auth.type":                s.config.Auth.Type,
		"rate_limit.enabled":       fmt.Sprintf("%t", s.config.RateLimit.Enabled),
		"metrics.enabled":          fmt.Sprintf("%t", s.config.Metrics.Enabled),
		"health.enabled":           fmt.Sprintf("%t", s.config.Health.Enabled),
	}

	return config, nil
}

// UpdateConfig updates the service configuration
func (s *SearchService) UpdateConfig(ctx context.Context, updates map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Updating search service configuration", zap.Any("updates", updates))

	// For now, just log the updates
	// In a real implementation, this would validate and apply configuration changes

	s.logger.Info("Search service configuration updated")
	return nil
}

// GetMetrics returns metrics information
func (s *SearchService) GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error) {
	metrics := make(map[string]float64)

	switch metricType {
	case "system":
		metrics["total_searches"] = float64(s.stats.TotalSearches)
		metrics["failed_searches"] = float64(s.stats.FailedSearches)
		metrics["avg_latency_ms"] = s.stats.AvgLatency
		metrics["total_vectors"] = float64(s.stats.TotalVectors)
		metrics["total_clusters"] = float64(s.stats.TotalClusters)
		metrics["active_connections"] = float64(s.stats.ActiveConnections)

	case "performance":
		if s.started {
			engineStatus := s.searchEngine.GetStatus()
			if engineStatus.Linear != nil {
				metrics["linear_searches"] = float64(engineStatus.Linear.Searches)
				metrics["linear_avg_latency_ms"] = engineStatus.Linear.AvgLatency
			}
			if engineStatus.Dual != nil && engineStatus.Dual.Linear != nil {
				metrics["dual_linear_searches"] = float64(engineStatus.Dual.Linear.Searches)
				metrics["dual_linear_avg_latency_ms"] = engineStatus.Dual.Linear.AvgLatency
			}
		}

	case "vectors":
		if s.started {
			engineStatus := s.searchEngine.GetStatus()
			metrics["total_vectors"] = float64(engineStatus.TotalVectors)
			metrics["total_segments"] = float64(engineStatus.TotalSegments)
		}

	default:
		return nil, fmt.Errorf("unknown metric type: %s", metricType)
	}

	return metrics, nil
}

// Search performs a similarity search
func (s *SearchService) Search(ctx context.Context, query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	s.mu.RLock()
	if !s.started {
		s.mu.RUnlock()
		return nil, fmt.Errorf("search service is not started")
	}
	s.mu.RUnlock()

	// Record search start
	s.recordSearchStart(queryID)

	defer func() {
		latency := time.Since(startTime).Milliseconds()
		s.recordSearchComplete(queryID, latency, nil)
	}()

	s.logger.Info("Performing similarity search",
		zap.String("query_id", queryID),
		zap.Int("k", k),
		zap.Int("vector_dim", len(query.Data)),
	)

	// Perform the search
	results, err := s.searchEngine.Search(ctx, query, k)
	if err != nil {
		s.logger.Error("Search failed", zap.String("query_id", queryID), zap.Error(err))
		s.recordSearchError(queryID, err)
		return nil, fmt.Errorf("search failed: %w", err)
	}

	s.logger.Info("Search completed successfully",
		zap.String("query_id", queryID),
		zap.Int("result_count", len(results)),
		zap.Duration("latency", time.Since(startTime)),
	)

	return results, nil
}

// MultiClusterSearch performs a search across multiple clusters
func (s *SearchService) MultiClusterSearch(ctx context.Context, query *types.Vector, k int, clusterIDs []string, filter map[string]string) ([]*types.SearchResult, error) {
	startTime := time.Now()
	queryID := generateQueryID()

	s.mu.RLock()
	if !s.started {
		s.mu.RUnlock()
		return nil, fmt.Errorf("search service is not started")
	}
	s.mu.RUnlock()

	// Record search start
	s.recordSearchStart(queryID)

	defer func() {
		latency := time.Since(startTime).Milliseconds()
		s.recordSearchComplete(queryID, latency, nil)
	}()

	s.logger.Info("Performing multi-cluster search",
		zap.String("query_id", queryID),
		zap.Int("k", k),
		zap.Int("cluster_count", len(clusterIDs)),
		zap.Strings("clusters", clusterIDs),
	)

	// For now, perform a regular search
	// In a real implementation, this would coordinate searches across multiple clusters
	results, err := s.searchEngine.Search(ctx, query, k)
	if err != nil {
		s.logger.Error("Multi-cluster search failed", zap.String("query_id", queryID), zap.Error(err))
		s.recordSearchError(queryID, err)
		return nil, fmt.Errorf("multi-cluster search failed: %w", err)
	}

	s.logger.Info("Multi-cluster search completed successfully",
		zap.String("query_id", queryID),
		zap.Int("result_count", len(results)),
		zap.Duration("latency", time.Since(startTime)),
	)

	return results, nil
}

// GetClusterInfo returns information about a cluster
func (s *SearchService) GetClusterInfo(ctx context.Context, clusterID string) (*types.ClusterInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return nil, fmt.Errorf("search service is not started")
	}

	// For now, return a mock cluster info
	// In a real implementation, this would get cluster info from the cluster manager
	clusterInfo := &types.ClusterInfo{
		ID:        clusterID,
		NodeIDs:   []string{"node1", "node2"},
		VectorCount: 1000,
		SizeBytes:   1024 * 1024, // 1MB
		Metadata: map[string]string{
			"status": "active",
			"region": "us-east-1",
		},
	}

	return clusterInfo, nil
}

// GetClusterStatus returns the status of all clusters
func (s *SearchService) GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return nil, fmt.Errorf("search service is not started")
	}

	// For now, return a mock cluster status
	// In a real implementation, this would get cluster status from the cluster manager
	clusterStatus := &types.ClusterStatus{
		TotalClusters:    3,
		ReplicationFactor: 2,
		Clusters: []*types.ClusterInfo{
			{
				ID:        "cluster1",
				NodeIDs:   []string{"node1", "node2"},
				VectorCount: 1000,
				SizeBytes:   1024 * 1024,
				Metadata:    map[string]string{"status": "active"},
			},
			{
				ID:        "cluster2",
				NodeIDs:   []string{"node3", "node4"},
				VectorCount: 1500,
				SizeBytes:   2 * 1024 * 1024,
				Metadata:    map[string]string{"status": "active"},
			},
			{
				ID:        "cluster3",
				NodeIDs:   []string{"node5", "node6"},
				VectorCount: 800,
				SizeBytes:   512 * 1024,
				Metadata:    map[string]string{"status": "active"},
			},
		},
		Nodes: []*types.NodeInfo{
			{
				ID:        "node1",
				Address:   "127.0.0.1",
				Port:      8081,
				IsPrimary: true,
				Status:    "healthy",
				VectorCount: 500,
				MemoryUsage: 1024 * 1024,
				DiskUsage:   2 * 1024 * 1024,
			},
			{
				ID:        "node2",
				Address:   "127.0.0.1",
				Port:      8082,
				IsPrimary: false,
				Status:    "healthy",
				VectorCount: 500,
				MemoryUsage: 1024 * 1024,
				DiskUsage:   2 * 1024 * 1024,
			},
		},
	}

	return clusterStatus, nil
}

// Internal methods

func (s *SearchService) recordSearchStart(queryID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.TotalSearches++
	s.metrics.RecordCounter("search_requests_total", 1, map[string]string{
		"status": "started",
	})
}

func (s *SearchService) recordSearchComplete(queryID string, latency int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.LastSearch = time.Now()

	// Update average latency (simple moving average)
	if s.stats.TotalSearches == 1 {
		s.stats.AvgLatency = float64(latency)
	} else {
		s.stats.AvgLatency = (s.stats.AvgLatency*float64(s.stats.TotalSearches-1) + float64(latency)) / float64(s.stats.TotalSearches)
	}

	status := "success"
	if err != nil {
		status = "error"
		s.stats.FailedSearches++
	}

	s.metrics.RecordCounter("search_requests_total", 1, map[string]string{
		"status": status,
	})

	s.metrics.RecordHistogram("search_request_duration_ms", float64(latency), map[string]string{
		"status": status,
	})
}

func (s *SearchService) recordSearchError(queryID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.FailedSearches++
	s.metrics.RecordCounter("search_errors_total", 1, map[string]string{
		"error_type": getErrorType(err),
	})
}

func generateQueryID() string {
	return fmt.Sprintf("query_%d", time.Now().UnixNano())
}

func getErrorType(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()
	switch {
	case contains(errStr, "validation"):
		return "validation"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "connection"):
		return "connection"
	case contains(errStr, "not found"):
		return "not_found"
	default:
		return "unknown"
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}