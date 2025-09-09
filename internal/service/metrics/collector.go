package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"vexdb/internal/config"
	"vexdb/internal/logging"
)

var (
	ErrMetricsNotEnabled    = errors.New("metrics not enabled")
	ErrInvalidMetricsConfig = errors.New("invalid metrics configuration")
	ErrCollectorNotRunning  = errors.New("collector not running")
)

// MetricsConfig represents the metrics collection configuration
type MetricsConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	Port              int           `yaml:"port" json:"port"`
	Path              string        `yaml:"path" json:"path"`
	Namespace         string        `yaml:"namespace" json:"namespace"`
	Subsystem         string        `yaml:"subsystem" json:"subsystem"`
	EnableHistograms  bool          `yaml:"enable_histograms" json:"enable_histograms"`
	HistogramBuckets  []float64     `yaml:"histogram_buckets" json:"histogram_buckets"`
	EnableGauges      bool          `yaml:"enable_gauges" json:"enable_gauges"`
	EnableCounters    bool          `yaml:"enable_counters" json:"enable_counters"`
	EnableSummaries   bool          `yaml:"enable_summaries" json:"enable_summaries"`
	CollectInterval   time.Duration `yaml:"collect_interval" json:"collect_interval"`
	EnableRuntime     bool          `yaml:"enable_runtime" json:"enable_runtime"`
	EnableProcess     bool          `yaml:"enable_process" json:"enable_process"`
	EnableGoMetrics   bool          `yaml:"enable_go_metrics" json:"enable_go_metrics"`
	EnableCustom      bool          `yaml:"enable_custom" json:"enable_custom"`
}

// DefaultMetricsConfig returns the default metrics configuration
func DefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		Enabled:          true,
		Port:             9090,
		Path:             "/metrics",
		Namespace:        "vexdb",
		Subsystem:        "storage",
		EnableHistograms: true,
		HistogramBuckets: []float64{0.001, 0.01, 0.1, 1, 10, 100},
		EnableGauges:     true,
		EnableCounters:   true,
		EnableSummaries:  true,
		CollectInterval:  15 * time.Second,
		EnableRuntime:    true,
		EnableProcess:    true,
		EnableGoMetrics:  true,
		EnableCustom:     true,
	}
}

// MetricsCollector represents a metrics collector
type MetricsCollector struct {
	config     *MetricsConfig
	logger     logging.Logger
	
	// Prometheus registry
	registry   *prometheus.Registry
	
	// Custom metrics
	customMetrics map[string]prometheus.Collector
	mu           sync.RWMutex
	
	// Collection state
	started     bool
	stopped     bool
	collectDone chan struct{}
}

// StorageMetrics represents storage-specific metrics
type StorageMetrics struct {
	// Vector operations
	VectorInserts     prometheus.Counter
	VectorUpdates     prometheus.Counter
	VectorDeletes     prometheus.Counter
	VectorReads       prometheus.Counter
	VectorSearches    prometheus.Counter
	
	// Storage operations
	SegmentCreates    prometheus.Counter
	SegmentReads      prometheus.Counter
	SegmentWrites     prometheus.Counter
	SegmentDeletes    prometheus.Counter
	SegmentCompactions prometheus.Counter
	
	// Buffer operations
	BufferFlushes     prometheus.Counter
	BufferEvictions   prometheus.Counter
	BufferHits        prometheus.Counter
	BufferMisses      prometheus.Counter
	
	// Search operations
	SearchOperations  prometheus.Counter
	SearchErrors      prometheus.Counter
	SearchLatency     prometheus.Histogram
	ParallelSearchOperations prometheus.Counter
	ParallelSearchLatency prometheus.Histogram
	
	// Compression operations
	CompressionOps    prometheus.Counter
	CompressionErrors prometheus.Counter
	CompressionRatio  prometheus.Histogram
	
	// Storage size
	StorageSize       prometheus.Gauge
	VectorCount       prometheus.Gauge
	SegmentCount      prometheus.Gauge
	BufferSize        prometheus.Gauge
	
	// Performance
	InsertLatency     prometheus.Histogram
	ReadLatency       prometheus.Histogram
	DeleteLatency     prometheus.Histogram
	FlushLatency      prometheus.Histogram
	CompactionLatency prometheus.Histogram
	
	// Errors
	Errors            prometheus.CounterVec
}

// ServiceMetrics represents service-specific metrics
type ServiceMetrics struct {
	// gRPC metrics
	GRPCRequests      prometheus.Counter
	GRPCErrors        prometheus.Counter
	GRPCLatency       prometheus.Histogram
	GRPCConnections   prometheus.Gauge
	
	// HTTP metrics
	HTTPRequests      prometheus.CounterVec
	HTTPErrors        prometheus.CounterVec
	HTTPLatency       prometheus.HistogramVec
	HTTPConnections   prometheus.Gauge
	
	// Health check metrics
	HealthCheckErrors prometheus.Counter
	HealthCheckLatency prometheus.Histogram
	
	// Service metrics
	ServiceUptime     prometheus.Gauge
	ServiceVersion    prometheus.GaugeVec
	ServiceErrors     prometheus.CounterVec
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(cfg *config.Config, logger logging.Logger) (*MetricsCollector, error) {
	metricsConfig := DefaultMetricsConfig()
	
	if cfg != nil {
		if metricsCfg, ok := cfg.Get("metrics"); ok {
			if cfgMap, ok := metricsCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					metricsConfig.Enabled = enabled
				}
				if port, ok := cfgMap["port"].(int); ok {
					metricsConfig.Port = port
				}
				if path, ok := cfgMap["path"].(string); ok {
					metricsConfig.Path = path
				}
				if namespace, ok := cfgMap["namespace"].(string); ok {
					metricsConfig.Namespace = namespace
				}
				if subsystem, ok := cfgMap["subsystem"].(string); ok {
					metricsConfig.Subsystem = subsystem
				}
				if enableHistograms, ok := cfgMap["enable_histograms"].(bool); ok {
					metricsConfig.EnableHistograms = enableHistograms
				}
				if histogramBuckets, ok := cfgMap["histogram_buckets"].([]interface{}); ok {
					metricsConfig.HistogramBuckets = make([]float64, len(histogramBuckets))
					for i, bucket := range histogramBuckets {
						if val, ok := bucket.(float64); ok {
							metricsConfig.HistogramBuckets[i] = val
						}
					}
				}
				if enableGauges, ok := cfgMap["enable_gauges"].(bool); ok {
					metricsConfig.EnableGauges = enableGauges
				}
				if enableCounters, ok := cfgMap["enable_counters"].(bool); ok {
					metricsConfig.EnableCounters = enableCounters
				}
				if enableSummaries, ok := cfgMap["enable_summaries"].(bool); ok {
					metricsConfig.EnableSummaries = enableSummaries
				}
				if collectInterval, ok := cfgMap["collect_interval"].(string); ok {
					if duration, err := time.ParseDuration(collectInterval); err == nil {
						metricsConfig.CollectInterval = duration
					}
				}
				if enableRuntime, ok := cfgMap["enable_runtime"].(bool); ok {
					metricsConfig.EnableRuntime = enableRuntime
				}
				if enableProcess, ok := cfgMap["enable_process"].(bool); ok {
					metricsConfig.EnableProcess = enableProcess
				}
				if enableGoMetrics, ok := cfgMap["enable_go_metrics"].(bool); ok {
					metricsConfig.EnableGoMetrics = enableGoMetrics
				}
				if enableCustom, ok := cfgMap["enable_custom"].(bool); ok {
					metricsConfig.EnableCustom = enableCustom
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateMetricsConfig(metricsConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMetricsConfig, err)
	}
	
	collector := &MetricsCollector{
		config:        metricsConfig,
		logger:        logger,
		registry:      prometheus.NewRegistry(),
		customMetrics: make(map[string]prometheus.Collector),
		collectDone:   make(chan struct{}),
	}
	
	// Initialize default metrics
	if err := collector.initializeDefaultMetrics(); err != nil {
		return nil, fmt.Errorf("failed to initialize default metrics: %w", err)
	}
	
	collector.logger.Info("Created metrics collector",
		"enabled", metricsConfig.Enabled,
		"port", metricsConfig.Port,
		"path", metricsConfig.Path,
		"namespace", metricsConfig.Namespace,
		"subsystem", metricsConfig.Subsystem)
	
	return collector, nil
}

// validateMetricsConfig validates the metrics configuration
func validateMetricsConfig(cfg *MetricsConfig) error {
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	
	if cfg.Path == "" {
		return errors.New("path cannot be empty")
	}
	
	if cfg.Namespace == "" {
		return errors.New("namespace cannot be empty")
	}
	
	if cfg.CollectInterval <= 0 {
		return errors.New("collect interval must be positive")
	}
	
	for _, bucket := range cfg.HistogramBuckets {
		if bucket <= 0 {
			return errors.New("histogram buckets must be positive")
		}
	}
	
	return nil
}

// initializeDefaultMetrics initializes default metrics
func (m *MetricsCollector) initializeDefaultMetrics() error {
	if !m.config.Enabled {
		return nil
	}
	
	// Register default collectors
	if m.config.EnableRuntime {
		m.registry.MustRegister(prometheus.NewGoCollector())
	}
	
	if m.config.EnableProcess {
		m.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	}
	
	if m.config.EnableGoMetrics {
		m.registry.MustRegister(prometheus.NewGoCollector())
	}
	
	return nil
}

// Start starts the metrics collector
func (m *MetricsCollector) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.started {
		return nil
	}
	
	if !m.config.Enabled {
		m.logger.Info("Metrics collection is disabled")
		return nil
	}
	
	// Start collection goroutine
	go m.collectMetrics()
	
	m.started = true
	m.stopped = false
	
	m.logger.Info("Started metrics collector")
	
	return nil
}

// Stop stops the metrics collector
func (m *MetricsCollector) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.stopped {
		return nil
	}
	
	if !m.started {
		return ErrCollectorNotRunning
	}
	
	// Signal collection to stop
	close(m.collectDone)
	
	m.stopped = true
	m.started = false
	
	m.logger.Info("Stopped metrics collector")
	
	return nil
}

// IsRunning checks if the metrics collector is running
func (m *MetricsCollector) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.started && !m.stopped
}

// GetRegistry returns the Prometheus registry
func (m *MetricsCollector) GetRegistry() *prometheus.Registry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.registry
}

// GetConfig returns the metrics configuration
func (m *MetricsCollector) GetConfig() *MetricsConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy of config
	config := *m.config
	return &config
}

// UpdateConfig updates the metrics configuration
func (m *MetricsCollector) UpdateConfig(config *MetricsConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate new configuration
	if err := validateMetricsConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMetricsConfig, err)
	}
	
	m.config = config
	
	m.logger.Info("Updated metrics collector configuration", "config", config)
	
	return nil
}

// RegisterCustomMetric registers a custom metric
func (m *MetricsCollector) RegisterCustomMetric(name string, metric prometheus.Collector) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.config.Enabled {
		return ErrMetricsNotEnabled
	}
	
	if !m.config.EnableCustom {
		return errors.New("custom metrics are disabled")
	}
	
	if _, exists := m.customMetrics[name]; exists {
		return fmt.Errorf("metric %s is already registered", name)
	}
	
	// Register metric
	if err := m.registry.Register(metric); err != nil {
		return fmt.Errorf("failed to register metric %s: %w", name, err)
	}
	
	m.customMetrics[name] = metric
	
	m.logger.Info("Registered custom metric", "name", name)
	
	return nil
}

// UnregisterCustomMetric unregisters a custom metric
func (m *MetricsCollector) UnregisterCustomMetric(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.config.Enabled {
		return ErrMetricsNotEnabled
	}
	
	metric, exists := m.customMetrics[name]
	if !exists {
		return fmt.Errorf("metric %s is not registered", name)
	}
	
	// Unregister metric
	m.registry.Unregister(metric)
	delete(m.customMetrics, name)
	
	m.logger.Info("Unregistered custom metric", "name", name)
	
	return nil
}

// GetCustomMetrics returns registered custom metrics
func (m *MetricsCollector) GetCustomMetrics() map[string]prometheus.Collector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	metrics := make(map[string]prometheus.Collector)
	for name, metric := range m.customMetrics {
		metrics[name] = metric
	}
	
	return metrics
}

// NewStorageMetrics creates new storage-specific metrics
func (m *MetricsCollector) NewStorageMetrics() (*StorageMetrics, error) {
	if !m.config.Enabled {
		return nil, ErrMetricsNotEnabled
	}
	
	metrics := &StorageMetrics{
		// Vector operations
		VectorInserts: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_inserts_total",
			Help:      "Total number of vector insert operations",
		}, []string{"operation"}).WithLabelValues("insert"),
		
		VectorUpdates: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_updates_total",
			Help:      "Total number of vector update operations",
		}, []string{"operation"}).WithLabelValues("update"),
		
		VectorDeletes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_deletes_total",
			Help:      "Total number of vector delete operations",
		}, []string{"operation"}).WithLabelValues("delete"),
		
		VectorReads: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_reads_total",
			Help:      "Total number of vector read operations",
		}, []string{"operation"}).WithLabelValues("read"),
		
		VectorSearches: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_searches_total",
			Help:      "Total number of vector search operations",
		}, []string{"operation"}).WithLabelValues("search"),
		
		// Storage operations
		SegmentCreates: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_creates_total",
			Help:      "Total number of segment create operations",
		}, []string{"operation"}).WithLabelValues("create"),
		
		SegmentReads: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_reads_total",
			Help:      "Total number of segment read operations",
		}, []string{"operation"}).WithLabelValues("read"),
		
		SegmentWrites: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_writes_total",
			Help:      "Total number of segment write operations",
		}, []string{"operation"}).WithLabelValues("write"),
		
		SegmentDeletes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_deletes_total",
			Help:      "Total number of segment delete operations",
		}, []string{"operation"}).WithLabelValues("delete"),
		
		SegmentCompactions: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_compactions_total",
			Help:      "Total number of segment compaction operations",
		}, []string{"operation"}).WithLabelValues("compact"),
		
		// Buffer operations
		BufferFlushes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "buffer_flushes_total",
			Help:      "Total number of buffer flush operations",
		}, []string{"operation"}).WithLabelValues("flush"),
		
		BufferEvictions: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "buffer_evictions_total",
			Help:      "Total number of buffer eviction operations",
		}, []string{"operation"}).WithLabelValues("evict"),
		
		BufferHits: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "buffer_hits_total",
			Help:      "Total number of buffer hits",
		}, []string{"operation"}).WithLabelValues("hit"),
		
		BufferMisses: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "buffer_misses_total",
			Help:      "Total number of buffer misses",
		}, []string{"operation"}).WithLabelValues("miss"),
		
		// Search operations
		SearchOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "search_operations_total",
			Help:      "Total number of search operations",
		}, []string{"type"}).WithLabelValues("search"),
		
		SearchErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "search_errors_total",
			Help:      "Total number of search errors",
		}, []string{"type"}),
		
		ParallelSearchOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "parallel_search_operations_total",
			Help:      "Total number of parallel search operations",
		}, []string{"type"}).WithLabelValues("parallel_search"),
		
		// Storage size
		StorageSize: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "storage_size_bytes",
			Help:      "Current storage size in bytes",
		}),
		
		VectorCount: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "vector_count",
			Help:      "Current number of vectors",
		}),
		
		SegmentCount: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "segment_count",
			Help:      "Current number of segments",
		}),
		
		BufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "buffer_size_bytes",
			Help:      "Current buffer size in bytes",
		}),
		
		// Errors
		Errors: *promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors",
		}, []string{"component", "type"}),
	}
	
	// Initialize histograms if enabled
	if m.config.EnableHistograms {
		metrics.SearchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "search_latency_seconds",
			Help:      "Search operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"type"}).WithLabelValues("search")
		
		metrics.ParallelSearchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "parallel_search_latency_seconds",
			Help:      "Parallel search operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"type"}).WithLabelValues("parallel_search")
		
		metrics.CompressionRatio = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "compression_ratio",
			Help:      "Compression ratio (compressed_size / original_size)",
			Buckets:   []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
		}, []string{"algorithm"}).WithLabelValues("lz4")
		
		metrics.InsertLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "insert_latency_seconds",
			Help:      "Insert operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"operation"}).WithLabelValues("insert")
		
		metrics.ReadLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "read_latency_seconds",
			Help:      "Read operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"operation"}).WithLabelValues("read")
		
		metrics.DeleteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "delete_latency_seconds",
			Help:      "Delete operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"operation"}).WithLabelValues("delete")
		
		metrics.FlushLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "flush_latency_seconds",
			Help:      "Flush operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"operation"}).WithLabelValues("flush")
		
		metrics.CompactionLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "compaction_latency_seconds",
			Help:      "Compaction operation latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"operation"}).WithLabelValues("compact")
		
		metrics.CompressionOps = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "compression_operations_total",
			Help:      "Total number of compression operations",
		}, []string{"algorithm"}).WithLabelValues("lz4")
		
		metrics.CompressionErrors = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: m.config.Subsystem,
			Name:      "compression_errors_total",
			Help:      "Total number of compression errors",
		}, []string{"algorithm"}).WithLabelValues("lz4")
	}
	
	return metrics, nil
}

// NewServiceMetrics creates new service-specific metrics
func (m *MetricsCollector) NewServiceMetrics() (*ServiceMetrics, error) {
	if !m.config.Enabled {
		return nil, ErrMetricsNotEnabled
	}
	
	metrics := &ServiceMetrics{
		// gRPC metrics
		GRPCRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "grpc_requests_total",
			Help:      "Total number of gRPC requests",
		}, []string{"service"}).WithLabelValues("grpc"),
		
		GRPCErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "grpc_errors_total",
			Help:      "Total number of gRPC errors",
		}, []string{"service"}).WithLabelValues("grpc"),
		
		GRPCConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "grpc_connections",
			Help:      "Current number of gRPC connections",
		}),
		
		// HTTP metrics
		HTTPRequests: *promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests",
		}, []string{"method", "endpoint"}),
		
		HTTPErrors: *promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "http_errors_total",
			Help:      "Total number of HTTP errors",
		}, []string{"method", "endpoint", "status_code"}),
		
		HTTPConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "http_connections",
			Help:      "Current number of HTTP connections",
		}),
		
		// Health check metrics
		HealthCheckErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "health_check_errors_total",
			Help:      "Total number of health check errors",
		}, []string{"service"}).WithLabelValues("health"),
		
		// Service metrics
		ServiceUptime: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "uptime_seconds",
			Help:      "Service uptime in seconds",
		}),
		
		ServiceVersion: *promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "version_info",
			Help:      "Service version information",
		}, []string{"version", "build"}),
		
		ServiceErrors: *promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "service_errors_total",
			Help:      "Total number of service errors",
		}, []string{"component", "type"}),
	}
	
	// Initialize histograms if enabled
	if m.config.EnableHistograms {
		metrics.GRPCLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "grpc_latency_seconds",
			Help:      "gRPC request latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"method"}).WithLabelValues("grpc")
		
		metrics.HTTPLatency = *promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "http_latency_seconds",
			Help:      "HTTP request latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"method", "endpoint"})
		
		metrics.HealthCheckLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.config.Namespace,
			Subsystem: "service",
			Name:      "health_check_latency_seconds",
			Help:      "Health check latency in seconds",
			Buckets:   m.config.HistogramBuckets,
		}, []string{"service"}).WithLabelValues("health")
	}
	
	return metrics, nil
}

// collectMetrics collects metrics periodically
func (m *MetricsCollector) collectMetrics() {
	ticker := time.NewTicker(m.config.CollectInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			m.collectSystemMetrics()
		case <-m.collectDone:
			return
		}
	}
}

// collectSystemMetrics collects system metrics
func (m *MetricsCollector) collectSystemMetrics() {
	// This is where you would collect system-specific metrics
	// For now, we'll just log that collection happened
	m.logger.Debug("Collected system metrics")
}

// Validate validates the metrics collector state
func (m *MetricsCollector) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Validate configuration
	if err := validateMetricsConfig(m.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}