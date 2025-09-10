package types

import (
	"time"
)

// Config represents the system configuration
type Config struct {
	Service ServiceConfig `json:"service" yaml:"service"`
	Storage StorageConfig `json:"storage" yaml:"storage"`
	Cluster ClusterConfig `json:"cluster" yaml:"cluster"`
	Search  SearchConfig  `json:"search" yaml:"search"`
	Network NetworkConfig `json:"network" yaml:"network"`
	Metrics MetricsConfig `json:"metrics" yaml:"metrics"`
	Logging LoggingConfig `json:"logging" yaml:"logging"`
	Health  HealthConfig  `json:"health" yaml:"health"`
}

// ServiceConfig represents service-specific configuration
type ServiceConfig struct {
	Name        string         `json:"name" yaml:"name"`
	Version     string         `json:"version" yaml:"version"`
	Environment string         `json:"environment" yaml:"environment"`
	Host        string         `json:"host" yaml:"host"`
	Port        int            `json:"port" yaml:"port"`
	Shutdown    ShutdownConfig `json:"shutdown" yaml:"shutdown"`
}

// StorageConfig represents storage-specific configuration
type StorageConfig struct {
	DataDir        string            `json:"data_dir" yaml:"data_dir"`
	MaxSegmentSize int64             `json:"max_segment_size" yaml:"max_segment_size"`
	Compression    CompressionConfig `json:"compression" yaml:"compression"`
	Buffer         BufferConfig      `json:"buffer" yaml:"buffer"`
	Hashing        HashingConfig     `json:"hashing" yaml:"hashing"`
}

// SearchConfig represents search-specific configuration
type SearchConfig struct {
	Engine       string         `json:"engine" yaml:"engine"`
	MaxResults   int            `json:"max_results" yaml:"max_results"`
	DefaultK     int            `json:"default_k" yaml:"default_k"`
	DistanceType string         `json:"distance_type" yaml:"distance_type"`
	Parallel     ParallelConfig `json:"parallel" yaml:"parallel"`
}

// NetworkConfig represents network-specific configuration
type NetworkConfig struct {
	GRPC      GRPCConfig      `json:"grpc" yaml:"grpc"`
	HTTP      HTTPConfig      `json:"http" yaml:"http"`
	WebSocket WebSocketConfig `json:"websocket" yaml:"websocket"`
}

// MetricsConfig represents metrics-specific configuration
type MetricsConfig struct {
	Enabled   bool      `json:"enabled" yaml:"enabled"`
	Namespace string    `json:"namespace" yaml:"namespace"`
	Subsystem string    `json:"subsystem" yaml:"subsystem"`
	Address   string    `json:"address" yaml:"address"`
	Path      string    `json:"path" yaml:"path"`
	Buckets   []float64 `json:"buckets" yaml:"buckets"`
}

// LoggingConfig represents logging-specific configuration
type LoggingConfig struct {
	Enabled    bool   `json:"enabled" yaml:"enabled"`
	Level      string `json:"level" yaml:"level"`
	Format     string `json:"format" yaml:"format"`
	Output     string `json:"output" yaml:"output"`
	MaxSize    int    `json:"max_size" yaml:"max_size"`
	MaxAge     int    `json:"max_age" yaml:"max_age"`
	MaxBackups int    `json:"max_backups" yaml:"max_backups"`
	Compress   bool   `json:"compress" yaml:"compress"`
}

// HealthConfig represents health-specific configuration
type HealthConfig struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	CheckInterval time.Duration `json:"check_interval" yaml:"check_interval"`
	Timeout       time.Duration `json:"timeout" yaml:"timeout"`
}

// ShutdownConfig represents shutdown-specific configuration
type ShutdownConfig struct {
	Enabled     bool          `json:"enabled" yaml:"enabled"`
	Timeout     time.Duration `json:"timeout" yaml:"timeout"`
	GracePeriod time.Duration `json:"grace_period" yaml:"grace_period"`
}

// CompressionConfig represents compression-specific configuration
type CompressionConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	Algorithm string `json:"algorithm" yaml:"algorithm"`
	Level     int    `json:"level" yaml:"level"`
	Threshold int    `json:"threshold" yaml:"threshold"`
}

// BufferConfig represents buffer-specific configuration
type BufferConfig struct {
	MaxSize        int     `json:"max_size" yaml:"max_size"`
	MaxMemoryMB    int     `json:"max_memory_mb" yaml:"max_memory_mb"`
	FlushThreshold float64 `json:"flush_threshold" yaml:"flush_threshold"`
	EvictionPolicy string  `json:"eviction_policy" yaml:"eviction_policy"`
}

// HashingConfig represents hashing-specific configuration
type HashingConfig struct {
	Algorithm    string `json:"algorithm" yaml:"algorithm"`
	Strategy     string `json:"strategy" yaml:"strategy"`
	ClusterCount uint32 `json:"cluster_count" yaml:"cluster_count"`
	Seed         uint64 `json:"seed" yaml:"seed"`
	EnableCache  bool   `json:"enable_cache" yaml:"enable_cache"`
}

// ParallelConfig represents parallel search configuration
type ParallelConfig struct {
	Enabled    bool `json:"enabled" yaml:"enabled"`
	Workers    int  `json:"workers" yaml:"workers"`
	MaxWorkers int  `json:"max_workers" yaml:"max_workers"`
}

// GRPCConfig represents gRPC-specific configuration
type GRPCConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Host    string `json:"host" yaml:"host"`
	Port    int    `json:"port" yaml:"port"`
	MaxConn int    `json:"max_conn" yaml:"max_conn"`
	Timeout int    `json:"timeout" yaml:"timeout"`
}

// HTTPConfig represents HTTP-specific configuration
type HTTPConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Host    string `json:"host" yaml:"host"`
	Port    int    `json:"port" yaml:"port"`
	MaxConn int    `json:"max_conn" yaml:"max_conn"`
	Timeout int    `json:"timeout" yaml:"timeout"`
}

// WebSocketConfig represents WebSocket-specific configuration
type WebSocketConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Host    string `json:"host" yaml:"host"`
	Port    int    `json:"port" yaml:"port"`
	MaxConn int    `json:"max_conn" yaml:"max_conn"`
	Timeout int    `json:"timeout" yaml:"timeout"`
}

// QueryPlan represents a query execution plan
type QueryPlan struct {
	ID          string                 `json:"id" yaml:"id"`
	QueryVector *Vector                `json:"query_vector" yaml:"query_vector"`
	K           int                    `json:"k" yaml:"k"`
	Filters     []MetadataFilter       `json:"filters" yaml:"filters"`
	Nodes       []QueryNode            `json:"nodes" yaml:"nodes"`
	Strategy    string                 `json:"strategy" yaml:"strategy"`
	Options     map[string]interface{} `json:"options" yaml:"options"`
	CreatedAt   time.Time              `json:"created_at" yaml:"created_at"`
}

// QueryNode represents a node in the query plan
type QueryNode struct {
	ID        string          `json:"id" yaml:"id"`
	Address   string          `json:"address" yaml:"address"`
	Port      int             `json:"port" yaml:"port"`
	Role      string          `json:"role" yaml:"role"`
	Clusters  []uint32        `json:"clusters" yaml:"clusters"`
	Status    string          `json:"status" yaml:"status"`
	StartTime time.Time       `json:"start_time" yaml:"start_time"`
	EndTime   *time.Time      `json:"end_time,omitempty" yaml:"end_time,omitempty"`
	Error     string          `json:"error,omitempty" yaml:"error,omitempty"`
	Results   []*SearchResult `json:"results,omitempty" yaml:"results,omitempty"`
}

// QueryStats represents query execution statistics
type QueryStats struct {
	QueryID         string        `json:"query_id" yaml:"query_id"`
	TotalNodes      int           `json:"total_nodes" yaml:"total_nodes"`
	SuccessfulNodes int           `json:"successful_nodes" yaml:"successful_nodes"`
	FailedNodes     int           `json:"failed_nodes" yaml:"failed_nodes"`
	TotalResults    int           `json:"total_results" yaml:"total_results"`
	ExecutionTime   time.Duration `json:"execution_time" yaml:"execution_time"`
	NetworkTime     time.Duration `json:"network_time" yaml:"network_time"`
	SearchTime      time.Duration `json:"search_time" yaml:"search_time"`
	MergeTime       time.Duration `json:"merge_time" yaml:"merge_time"`
	StartTime       time.Time     `json:"start_time" yaml:"start_time"`
	EndTime         time.Time     `json:"end_time" yaml:"end_time"`
	Errors          []string      `json:"errors,omitempty" yaml:"errors,omitempty"`
}

// AggregatedResult represents aggregated search results from multiple nodes
type AggregatedResult struct {
	QueryID string          `json:"query_id" yaml:"query_id"`
	Results []*SearchResult `json:"results" yaml:"results"`
	Total   int             `json:"total" yaml:"total"`
	K       int             `json:"k" yaml:"k"`
	Stats   *QueryStats     `json:"stats,omitempty" yaml:"stats,omitempty"`
	Success bool            `json:"success" yaml:"success"`
	Error   string          `json:"error,omitempty" yaml:"error,omitempty"`
}
