package service

import (
	"context"
	"time"

	"vexdb/internal/types"
	pb "vexdb/proto"
)

// StorageService defines the interface for vector storage operations
type StorageService interface {
	// Insert operations
	InsertVector(ctx context.Context, vector *types.Vector) error
	InsertBatch(ctx context.Context, vectors []*types.Vector) error
	
	// Search operations
	Search(ctx context.Context, query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error)
	
	// Delete operations
	DeleteVector(ctx context.Context, vectorID string) error
	DeleteByMetadata(ctx context.Context, filter map[string]string) (int64, error)
	
	// Cluster management
	GetClusterInfo(ctx context.Context, clusterID string) (*types.ClusterInfo, error)
	GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error)
	
	// Health and metrics
	HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error)
	GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error)
	
	// Configuration
	UpdateConfig(ctx context.Context, config map[string]string) error
	GetConfig(ctx context.Context) (map[string]string, error)
}

// InsertService defines the interface for vector insertion operations
type InsertService interface {
	// Insert operations
	InsertVector(ctx context.Context, vector *types.Vector) error
	InsertBatch(ctx context.Context, vectors []*types.Vector) error
	
	// Routing and cluster management
	GetClusterForVector(ctx context.Context, vector *types.Vector) (*types.ClusterInfo, error)
	GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error)
	
	// Health and metrics
	HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error)
	GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error)
	
	// Configuration
	UpdateConfig(ctx context.Context, config map[string]string) error
	GetConfig(ctx context.Context) (map[string]string, error)
}

// SearchService defines the interface for search operations
type SearchService interface {
	// Search operations
	Search(ctx context.Context, query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error)
	MultiClusterSearch(ctx context.Context, query *types.Vector, k int, clusterIDs []string, filter map[string]string) ([]*types.SearchResult, error)
	
	// Cluster management
	GetClusterInfo(ctx context.Context, clusterID string) (*types.ClusterInfo, error)
	GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error)
	
	// Health and metrics
	HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error)
	GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error)
	
	// Configuration
	UpdateConfig(ctx context.Context, config map[string]string) error
	GetConfig(ctx context.Context) (map[string]string, error)
}

// AdminService defines the interface for administrative operations
type AdminService interface {
	// Cluster management
	AddNode(ctx context.Context, node *types.NodeInfo) (*types.NodeInfo, error)
	RemoveNode(ctx context.Context, nodeID string) error
	UpdateNode(ctx context.Context, node *types.NodeInfo) (*types.NodeInfo, error)
	
	// Cluster operations
	CreateCluster(ctx context.Context, cluster *types.ClusterInfo) (*types.ClusterInfo, error)
	DeleteCluster(ctx context.Context, clusterID string) error
	UpdateCluster(ctx context.Context, cluster *types.ClusterInfo) (*types.ClusterInfo, error)
	
	// System operations
	BackupSystem(ctx context.Context) error
	RestoreSystem(ctx context.Context) error
	CompactStorage(ctx context.Context) error
	
	// Health and metrics
	HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error)
	GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error)
	
	// Configuration
	UpdateConfig(ctx context.Context, config map[string]string) error
	GetConfig(ctx context.Context) (map[string]string, error)
}

// StorageServer implements the gRPC StorageService
type StorageServer interface {
	pb.StorageServiceServer
}

// InsertServer implements the gRPC InsertService
type InsertServer interface {
	pb.InsertServiceServer
}

// SearchServer implements the gRPC SearchService
type SearchServer interface {
	pb.SearchServiceServer
}

// AdminServer implements the gRPC AdminService
type AdminServer interface {
	pb.AdminServiceServer
}

// Service represents a generic service interface
type Service interface {
	// Lifecycle management
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Reload(ctx context.Context) error
	
	// Health and status
	HealthCheck(ctx context.Context) bool
	GetStatus(ctx context.Context) map[string]interface{}
	
	// Configuration
	GetConfig() *types.Config
	UpdateConfig(config *types.Config) error
	
	// Metrics
	GetMetrics() map[string]interface{}
}

// ClusterManager manages cluster operations
type ClusterManager interface {
	// Node management
	AddNode(node *types.NodeInfo) error
	RemoveNode(nodeID string) error
	UpdateNode(node *types.NodeInfo) error
	GetNode(nodeID string) (*types.NodeInfo, error)
	ListNodes() ([]*types.NodeInfo, error)
	
	// Cluster management
	CreateCluster(cluster *types.ClusterInfo) error
	DeleteCluster(clusterID string) error
	UpdateCluster(cluster *types.ClusterInfo) error
	GetCluster(clusterID string) (*types.ClusterInfo, error)
	ListClusters() ([]*types.ClusterInfo, error)
	
	// Health monitoring
	CheckNodeHealth(nodeID string) (*types.HealthStatus, error)
	CheckClusterHealth(clusterID string) (*types.HealthStatus, error)
	
	// Routing
	GetNodesForCluster(clusterID string) ([]*types.NodeInfo, error)
	GetClusterForVector(vector *types.Vector) (*types.ClusterInfo, error)
}

// VectorStorage handles vector storage operations
type VectorStorage interface {
	// Vector operations
	Insert(vector *types.Vector) error
	InsertBatch(vectors []*types.Vector) error
	Get(vectorID string) (*types.Vector, error)
	Delete(vectorID string) error
	
	// Search operations
	Search(query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error)
	SearchInCluster(clusterID string, query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error)
	
	// Cluster operations
	GetClusterVectors(clusterID string) ([]*types.Vector, error)
	GetClusterCount(clusterID string) (int64, error)
	
	// Maintenance
	Compact() error
	Backup() error
	Restore() error
	
	// Statistics
	GetStats() map[string]interface{}
}

// ProtocolAdapter defines the interface for protocol adapters
type ProtocolAdapter interface {
	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	
	// Protocol handling
	HandleRequest(ctx context.Context, request interface{}) (interface{}, error)
	ValidateRequest(request interface{}) error
	
	// Configuration
	GetConfig() map[string]interface{}
	UpdateConfig(config map[string]interface{}) error
	
	// Metrics
	GetMetrics() map[string]interface{}
}

// Router handles vector routing and replication
type Router interface {
	// Routing
	RouteVector(vector *types.Vector) ([]*types.NodeInfo, error)
	RouteToCluster(clusterID string, vector *types.Vector) ([]*types.NodeInfo, error)
	
	// Replication
	ReplicateVector(ctx context.Context, vector *types.Vector, nodes []*types.NodeInfo) error
	ReplicateBatch(ctx context.Context, vectors []*types.Vector, nodes []*types.NodeInfo) error
	
	// Load balancing
	GetLeastLoadedNode(nodes []*types.NodeInfo) (*types.NodeInfo, error)
	GetNodesForReplication(clusterID string, replicationFactor int) ([]*types.NodeInfo, error)
	
	// Health monitoring
	CheckNodeHealth(node *types.NodeInfo) bool
	GetHealthyNodes(clusterID string) ([]*types.NodeInfo, error)
}

// QueryPlanner handles query planning and optimization
type QueryPlanner interface {
	// Query planning
	PlanSearch(query *types.Vector, k int, filter map[string]string) (*types.QueryPlan, error)
	PlanMultiClusterSearch(query *types.Vector, k int, clusterIDs []string, filter map[string]string) (*types.QueryPlan, error)
	
	// Query optimization
	OptimizeQuery(plan *types.QueryPlan) (*types.QueryPlan, error)
	EstimateQueryCost(plan *types.QueryPlan) (float64, error)
	
	// Query execution
	ExecutePlan(ctx context.Context, plan *types.QueryPlan) ([]*types.SearchResult, error)
	CancelQuery(ctx context.Context, queryID string) error
	
	// Query statistics
	GetQueryStats(queryID string) (*types.QueryStats, error)
	GetQueryHistory() ([]*types.QueryStats, error)
}

// ResultMerger handles result merging and ranking
type ResultMerger interface {
	// Result merging
	MergeResults(results [][]*types.SearchResult, k int) ([]*types.SearchResult, error)
	MergeMultiClusterResults(clusterResults map[string][]*types.SearchResult, k int) ([]*types.SearchResult, error)
	
	// Result ranking
	RankResults(results []*types.SearchResult) ([]*types.SearchResult, error)
	FilterResults(results []*types.SearchResult, filter map[string]string) ([]*types.SearchResult, error)
	
	// Deduplication
	DeduplicateResults(results []*types.SearchResult) ([]*types.SearchResult, error)
	
	// Result aggregation
	AggregateResults(results []*types.SearchResult) (*types.AggregatedResult, error)
}

// HealthChecker handles health checking operations
type HealthChecker interface {
	// Health checks
	CheckServiceHealth(serviceID string) (*types.HealthStatus, error)
	CheckNodeHealth(nodeID string) (*types.HealthStatus, error)
	CheckClusterHealth(clusterID string) (*types.HealthStatus, error)
	CheckSystemHealth() (*types.SystemHealth, error)
	
	// Health monitoring
	StartHealthMonitoring(ctx context.Context, interval time.Duration) error
	StopHealthMonitoring() error
	
	// Health status
	GetHealthStatus() map[string]*types.HealthStatus
	GetHealthHistory(serviceID string) ([]*types.HealthStatus, error)
}

// MetricsCollector handles metrics collection
type MetricsCollector interface {
	// Metrics collection
	CollectMetrics() (map[string]interface{}, error)
	CollectServiceMetrics(serviceID string) (map[string]interface{}, error)
	CollectNodeMetrics(nodeID string) (map[string]interface{}, error)
	CollectClusterMetrics(clusterID string) (map[string]interface{}, error)
	
	// Metrics aggregation
	AggregateMetrics(metrics []map[string]interface{}) (map[string]interface{}, error)
	GetAggregatedMetrics(timeRange time.Duration) (map[string]interface{}, error)
	
	// Metrics export
	ExportMetrics(format string) ([]byte, error)
	ExportMetricsToPrometheus() ([]byte, error)
	
	// Metrics history
	GetMetricsHistory(metricName string, timeRange time.Duration) ([]*types.MetricPoint, error)
}

// ConfigManager handles configuration management
type ConfigManager interface {
	// Configuration management
	GetConfig() (*types.Config, error)
	UpdateConfig(config *types.Config) error
	ReloadConfig() error
	
	// Configuration validation
	ValidateConfig(config *types.Config) error
	GetConfigSchema() map[string]interface{}
	
	// Configuration persistence
	SaveConfig(config *types.Config) error
	LoadConfig() (*types.Config, error)
	
	// Configuration versioning
	GetConfigVersions() ([]*types.ConfigVersion, error)
	RollbackConfig(version string) error
}

// Logger defines the logging interface
type Logger interface {
	// Logging levels
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Fatal(msg string, fields ...interface{})
	
	// Contextual logging
	With(fields ...interface{}) Logger
	WithContext(ctx context.Context) Logger
	
	// Performance logging
	Measure(operation string, duration time.Duration, fields ...interface{})
	
	// Structured logging
	Structured(level string, msg string, fields map[string]interface{})
}