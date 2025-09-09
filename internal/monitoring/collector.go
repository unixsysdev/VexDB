package monitoring

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Collector manages all monitoring metrics and observability
type Collector struct {
	logger *zap.Logger
	config *Config

	// Service metrics
	serviceUp   *prometheus.GaugeVec
	serviceInfo *prometheus.GaugeVec

	// Request metrics
	requestCount    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	responseSize    *prometheus.HistogramVec

	// Search metrics
	searchCount    *prometheus.CounterVec
	searchDuration *prometheus.HistogramVec
	searchResults  *prometheus.HistogramVec
	searchErrors   *prometheus.CounterVec

	// Storage metrics
	storageVectors    *prometheus.GaugeVec
	storageSegments   *prometheus.GaugeVec
	storageSize       *prometheus.GaugeVec
	storageOperations *prometheus.CounterVec
	storageDuration   *prometheus.HistogramVec

	// Cluster metrics
	clusterNodes    *prometheus.GaugeVec
	clusterReplicas *prometheus.GaugeVec
	clusterHealth   *prometheus.GaugeVec
	clusterLatency  *prometheus.HistogramVec

	// System metrics
	systemMemory     *prometheus.GaugeVec
	systemCPU        *prometheus.GaugeVec
	systemGoroutines *prometheus.GaugeVec
	systemGC         *prometheus.GaugeVec

	// Protocol metrics
	protocolConnections *prometheus.GaugeVec
	protocolMessages    *prometheus.CounterVec
	protocolErrors      *prometheus.CounterVec

	// Replication metrics
	replicationLag        *prometheus.GaugeVec
	replicationThroughput *prometheus.CounterVec
	replicationErrors     *prometheus.CounterVec

	// Cache metrics
	cacheSize      *prometheus.GaugeVec
	cacheHits      *prometheus.CounterVec
	cacheMisses    *prometheus.CounterVec
	cacheEvictions *prometheus.CounterVec

	// Custom metrics registry
	customMetrics      map[string]prometheus.Metric
	customMetricsMutex sync.RWMutex

	// Health checks
	healthChecks      map[string]HealthCheck
	healthChecksMutex sync.RWMutex

	// Alerting
	alerts      map[string]*Alert
	alertsMutex sync.RWMutex
}

// Config represents monitoring configuration
type Config struct {
	Enabled             bool          `yaml:"enabled" json:"enabled"`
	Namespace           string        `yaml:"namespace" json:"namespace"`
	Subsystem           string        `yaml:"subsystem" json:"subsystem"`
	CollectInterval     time.Duration `yaml:"collect_interval" json:"collect_interval"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval" json:"health_check_interval"`
	EnableProfiling     bool          `yaml:"enable_profiling" json:"enable_profiling"`
	ProfilingPort       int           `yaml:"profiling_port" json:"profiling_port"`
	EnableTracing       bool          `yaml:"enable_tracing" json:"enable_tracing"`
	TracingEndpoint     string        `yaml:"tracing_endpoint" json:"tracing_endpoint"`
	ServiceName         string        `yaml:"service_name" json:"service_name"`
	ServiceVersion      string        `yaml:"service_version" json:"service_version"`
	ServiceInstance     string        `yaml:"service_instance" json:"service_instance"`
}

// HealthCheck represents a health check function
type HealthCheck struct {
	Name       string
	Check      func(ctx context.Context) error
	Interval   time.Duration
	Timeout    time.Duration
	LastCheck  time.Time
	LastStatus bool
	LastError  error
}

// Alert represents an alert condition
type Alert struct {
	ID          string
	Name        string
	Description string
	Severity    string
	Condition   func() bool
	LastTrigger time.Time
	Count       int
	Enabled     bool
}

// NewCollector creates a new monitoring collector
func NewCollector(logger *zap.Logger, config *Config) *Collector {
	if config == nil {
		config = DefaultConfig()
	}

	c := &Collector{
		logger:        logger,
		config:        config,
		customMetrics: make(map[string]prometheus.Metric),
		healthChecks:  make(map[string]HealthCheck),
		alerts:        make(map[string]*Alert),
	}

	c.initializeMetrics()

	if config.Enabled {
		go c.startCollection()
		go c.startHealthChecks()
	}

	return c
}

// DefaultConfig returns default monitoring configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:             true,
		Namespace:           "vexdb",
		Subsystem:           "",
		CollectInterval:     15 * time.Second,
		HealthCheckInterval: 30 * time.Second,
		EnableProfiling:     false,
		ProfilingPort:       6060,
		EnableTracing:       false,
		TracingEndpoint:     "",
		ServiceName:         "vexdb",
		ServiceVersion:      "1.0.0",
		ServiceInstance:     "default",
	}
}

// initializeMetrics initializes all Prometheus metrics
func (c *Collector) initializeMetrics() {
	namespace := c.config.Namespace
	subsystem := c.config.Subsystem

	// Service metrics
	c.serviceUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "service_up",
		Help:      "Indicates if the service is up (1) or down (0)",
	}, []string{"service", "instance", "version"})

	c.serviceInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "service_info",
		Help:      "Information about the service",
	}, []string{"service", "version", "instance", "build_time", "git_commit"})

	// Request metrics
	c.requestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_count_total",
		Help:      "Total number of requests",
	}, []string{"service", "method", "endpoint", "status", "protocol"})

	c.requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_duration_seconds",
		Help:      "Request duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"service", "method", "endpoint", "protocol"})

	c.requestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_size_bytes",
		Help:      "Request size in bytes",
		Buckets:   []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216},
	}, []string{"service", "method", "endpoint"})

	c.responseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "response_size_bytes",
		Help:      "Response size in bytes",
		Buckets:   []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216},
	}, []string{"service", "method", "endpoint"})

	// Search metrics
	c.searchCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_count_total",
		Help:      "Total number of search operations",
	}, []string{"service", "type", "cluster"})

	c.searchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_duration_seconds",
		Help:      "Search operation duration in seconds",
		Buckets:   []float64{0.001, 0.01, 0.1, 1.0, 10.0, 60.0},
	}, []string{"service", "type", "cluster"})

	c.searchResults = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_results_count",
		Help:      "Number of search results returned",
		Buckets:   []float64{1, 10, 100, 1000, 10000},
	}, []string{"service", "type", "cluster"})

	c.searchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_errors_total",
		Help:      "Total number of search errors",
	}, []string{"service", "type", "cluster", "error_type"})

	// Storage metrics
	c.storageVectors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "storage_vectors_total",
		Help:      "Total number of vectors in storage",
	}, []string{"service", "cluster", "node"})

	c.storageSegments = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "storage_segments_total",
		Help:      "Total number of storage segments",
	}, []string{"service", "cluster", "node"})

	c.storageSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "storage_size_bytes",
		Help:      "Total storage size in bytes",
	}, []string{"service", "cluster", "node"})

	c.storageOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "storage_operations_total",
		Help:      "Total number of storage operations",
	}, []string{"service", "operation", "cluster", "node"})

	c.storageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "storage_operation_duration_seconds",
		Help:      "Storage operation duration in seconds",
		Buckets:   []float64{0.0001, 0.001, 0.01, 0.1, 1.0, 10.0},
	}, []string{"service", "operation", "cluster", "node"})

	// Cluster metrics
	c.clusterNodes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cluster_nodes_total",
		Help:      "Total number of cluster nodes",
	}, []string{"service", "cluster", "status"})

	c.clusterReplicas = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cluster_replicas_total",
		Help:      "Total number of cluster replicas",
	}, []string{"service", "cluster", "status"})

	c.clusterHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cluster_health_score",
		Help:      "Cluster health score (0-1, where 1 is healthy)",
	}, []string{"service", "cluster"})

	c.clusterLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cluster_latency_seconds",
		Help:      "Cluster communication latency in seconds",
		Buckets:   []float64{0.0001, 0.001, 0.01, 0.1, 1.0},
	}, []string{"service", "cluster", "operation"})

	// System metrics
	c.systemMemory = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "system_memory_bytes",
		Help:      "System memory usage in bytes",
	}, []string{"service", "type", "instance"})

	c.systemCPU = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "system_cpu_percent",
		Help:      "System CPU usage percentage",
	}, []string{"service", "type", "instance"})

	c.systemGoroutines = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "system_goroutines_total",
		Help:      "Total number of goroutines",
	}, []string{"service", "instance"})

	c.systemGC = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "system_gc_total",
		Help:      "Total number of garbage collections",
	}, []string{"service", "instance", "type"})

	// Protocol metrics
	c.protocolConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "protocol_connections_total",
		Help:      "Total number of active connections",
	}, []string{"service", "protocol", "instance"})

	c.protocolMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "protocol_messages_total",
		Help:      "Total number of protocol messages",
	}, []string{"service", "protocol", "direction", "type"})

	c.protocolErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "protocol_errors_total",
		Help:      "Total number of protocol errors",
	}, []string{"service", "protocol", "error_type"})

	// Replication metrics
	c.replicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "replication_lag_seconds",
		Help:      "Replication lag in seconds",
	}, []string{"service", "cluster", "source", "target"})

	c.replicationThroughput = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "replication_throughput_bytes_total",
		Help:      "Total replication throughput in bytes",
	}, []string{"service", "cluster", "source", "target"})

	c.replicationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "replication_errors_total",
		Help:      "Total number of replication errors",
	}, []string{"service", "cluster", "source", "target", "error_type"})

	// Cache metrics
	c.cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cache_size_bytes",
		Help:      "Cache size in bytes",
	}, []string{"service", "cache_name", "instance"})

	c.cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cache_hits_total",
		Help:      "Total number of cache hits",
	}, []string{"service", "cache_name"})

	c.cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cache_misses_total",
		Help:      "Total number of cache misses",
	}, []string{"service", "cache_name"})

	c.cacheEvictions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cache_evictions_total",
		Help:      "Total number of cache evictions",
	}, []string{"service", "cache_name", "reason"})

	// Set initial service metrics
	c.serviceUp.WithLabelValues(c.config.ServiceName, c.config.ServiceInstance, c.config.ServiceVersion).Set(1)
	c.serviceInfo.WithLabelValues(c.config.ServiceName, c.config.ServiceVersion, c.config.ServiceInstance, "", "").Set(1)
}

// startCollection starts the metrics collection loop
func (c *Collector) startCollection() {
	ticker := time.NewTicker(c.config.CollectInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.collectSystemMetrics()
		c.checkAlerts()
	}
}

// startHealthChecks starts the health check loop
func (c *Collector) startHealthChecks() {
	ticker := time.NewTicker(c.config.HealthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.runHealthChecks()
	}
}

// collectSystemMetrics collects system-level metrics
func (c *Collector) collectSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	instance := c.config.ServiceInstance

	// Memory metrics
	c.systemMemory.WithLabelValues(c.config.ServiceName, "allocated", instance).Set(float64(m.Alloc))
	c.systemMemory.WithLabelValues(c.config.ServiceName, "total", instance).Set(float64(m.Sys))
	c.systemMemory.WithLabelValues(c.config.ServiceName, "heap", instance).Set(float64(m.HeapAlloc))
	c.systemMemory.WithLabelValues(c.config.ServiceName, "stack", instance).Set(float64(m.StackSys))

	// Goroutines
	c.systemGoroutines.WithLabelValues(c.config.ServiceName, instance).Set(float64(runtime.NumGoroutine()))

	// GC metrics
	c.systemGC.WithLabelValues(c.config.ServiceName, instance, "num_gc").Set(float64(m.NumGC))
	c.systemGC.WithLabelValues(c.config.ServiceName, instance, "pause_total").Set(float64(m.PauseTotalNs))
}

// RecordRequest records request metrics
func (c *Collector) RecordRequest(service, method, endpoint, status, protocol string, duration time.Duration, requestSize, responseSize int64) {
	if !c.config.Enabled {
		return
	}

	c.requestCount.WithLabelValues(service, method, endpoint, status, protocol).Inc()
	c.requestDuration.WithLabelValues(service, method, endpoint, protocol).Observe(duration.Seconds())

	if requestSize > 0 {
		c.requestSize.WithLabelValues(service, method, endpoint).Observe(float64(requestSize))
	}

	if responseSize > 0 {
		c.responseSize.WithLabelValues(service, method, endpoint).Observe(float64(responseSize))
	}
}

// RecordSearch records search operation metrics
func (c *Collector) RecordSearch(service, searchType, cluster string, duration time.Duration, resultCount int, err error) {
	if !c.config.Enabled {
		return
	}

	c.searchCount.WithLabelValues(service, searchType, cluster).Inc()
	c.searchDuration.WithLabelValues(service, searchType, cluster).Observe(duration.Seconds())
	c.searchResults.WithLabelValues(service, searchType, cluster).Observe(float64(resultCount))

	if err != nil {
		errorType := "unknown"
		if err != nil {
			errorType = fmt.Sprintf("%T", err)
		}
		c.searchErrors.WithLabelValues(service, searchType, cluster, errorType).Inc()
	}
}

// RecordStorage records storage operation metrics
func (c *Collector) RecordStorage(service, operation, cluster, node string, duration time.Duration, err error) {
	if !c.config.Enabled {
		return
	}

	c.storageOperations.WithLabelValues(service, operation, cluster, node).Inc()
	c.storageDuration.WithLabelValues(service, operation, cluster, node).Observe(duration.Seconds())

	if err != nil {
		// storage error tracking not implemented
	}
}

// UpdateStorageMetrics updates storage-related metrics
func (c *Collector) UpdateStorageMetrics(service, cluster, node string, vectorCount, segmentCount int64, sizeBytes int64) {
	if !c.config.Enabled {
		return
	}

	c.storageVectors.WithLabelValues(service, cluster, node).Set(float64(vectorCount))
	c.storageSegments.WithLabelValues(service, cluster, node).Set(float64(segmentCount))
	c.storageSize.WithLabelValues(service, cluster, node).Set(float64(sizeBytes))
}

// UpdateClusterMetrics updates cluster-related metrics
func (c *Collector) UpdateClusterMetrics(service, cluster string, nodeCount, replicaCount int, healthScore float64) {
	if !c.config.Enabled {
		return
	}

	c.clusterNodes.WithLabelValues(service, cluster, "total").Set(float64(nodeCount))
	c.clusterNodes.WithLabelValues(service, cluster, "healthy").Set(float64(nodeCount)) // This would need actual health status
	c.clusterReplicas.WithLabelValues(service, cluster, "total").Set(float64(replicaCount))
	c.clusterReplicas.WithLabelValues(service, cluster, "healthy").Set(float64(replicaCount)) // This would need actual health status
	c.clusterHealth.WithLabelValues(service, cluster).Set(healthScore)
}

// RecordClusterLatency records cluster communication latency
func (c *Collector) RecordClusterLatency(service, cluster, operation string, duration time.Duration) {
	if !c.config.Enabled {
		return
	}

	c.clusterLatency.WithLabelValues(service, cluster, operation).Observe(duration.Seconds())
}

// UpdateProtocolMetrics updates protocol-related metrics
func (c *Collector) UpdateProtocolMetrics(service, protocol string, connectionCount int) {
	if !c.config.Enabled {
		return
	}

	c.protocolConnections.WithLabelValues(service, protocol, c.config.ServiceInstance).Set(float64(connectionCount))
}

// RecordProtocolMessage records protocol message metrics
func (c *Collector) RecordProtocolMessage(service, protocol, direction, msgType string) {
	if !c.config.Enabled {
		return
	}

	c.protocolMessages.WithLabelValues(service, protocol, direction, msgType).Inc()
}

// RecordProtocolError records protocol error metrics
func (c *Collector) RecordProtocolError(service, protocol, errorType string) {
	if !c.config.Enabled {
		return
	}

	c.protocolErrors.WithLabelValues(service, protocol, errorType).Inc()
}

// UpdateReplicationMetrics updates replication-related metrics
func (c *Collector) UpdateReplicationMetrics(service, cluster, source, target string, lagSeconds float64) {
	if !c.config.Enabled {
		return
	}

	c.replicationLag.WithLabelValues(service, cluster, source, target).Set(lagSeconds)
}

// RecordReplicationThroughput records replication throughput metrics
func (c *Collector) RecordReplicationThroughput(service, cluster, source, target string, bytes int64) {
	if !c.config.Enabled {
		return
	}

	c.replicationThroughput.WithLabelValues(service, cluster, source, target).Add(float64(bytes))
}

// RecordReplicationError records replication error metrics
func (c *Collector) RecordReplicationError(service, cluster, source, target, errorType string) {
	if !c.config.Enabled {
		return
	}

	c.replicationErrors.WithLabelValues(service, cluster, source, target, errorType).Inc()
}

// UpdateCacheMetrics updates cache-related metrics
func (c *Collector) UpdateCacheMetrics(service, cacheName string, sizeBytes int64) {
	if !c.config.Enabled {
		return
	}

	c.cacheSize.WithLabelValues(service, cacheName, c.config.ServiceInstance).Set(float64(sizeBytes))
}

// RecordCacheHit records cache hit metrics
func (c *Collector) RecordCacheHit(service, cacheName string) {
	if !c.config.Enabled {
		return
	}

	c.cacheHits.WithLabelValues(service, cacheName).Inc()
}

// RecordCacheMiss records cache miss metrics
func (c *Collector) RecordCacheMiss(service, cacheName string) {
	if !c.config.Enabled {
		return
	}

	c.cacheMisses.WithLabelValues(service, cacheName).Inc()
}

// RecordCacheEviction records cache eviction metrics
func (c *Collector) RecordCacheEviction(service, cacheName, reason string) {
	if !c.config.Enabled {
		return
	}

	c.cacheEvictions.WithLabelValues(service, cacheName, reason).Inc()
}

// RegisterHealthCheck registers a health check
func (c *Collector) RegisterHealthCheck(name string, checkFunc func(ctx context.Context) error, interval, timeout time.Duration) {
	c.healthChecksMutex.Lock()
	defer c.healthChecksMutex.Unlock()

	c.healthChecks[name] = HealthCheck{
		Name:     name,
		Check:    checkFunc,
		Interval: interval,
		Timeout:  timeout,
	}
}

// runHealthChecks runs all registered health checks
func (c *Collector) runHealthChecks() {
	c.healthChecksMutex.RLock()
	healthChecks := make([]HealthCheck, 0, len(c.healthChecks))
	for _, hc := range c.healthChecks {
		healthChecks = append(healthChecks, hc)
	}
	c.healthChecksMutex.RUnlock()

	for _, hc := range healthChecks {
		go c.runHealthCheck(hc)
	}
}

// runHealthCheck runs a single health check
func (c *Collector) runHealthCheck(hc HealthCheck) {
	ctx, cancel := context.WithTimeout(context.Background(), hc.Timeout)
	defer cancel()

	err := hc.Check(ctx)

	c.healthChecksMutex.Lock()
	defer c.healthChecksMutex.Unlock()

	if existing, exists := c.healthChecks[hc.Name]; exists {
		existing.LastCheck = time.Now()
		existing.LastStatus = err == nil
		existing.LastError = err
		c.healthChecks[hc.Name] = existing
	}

	if err != nil {
		c.logger.Error("Health check failed",
			zap.String("check", hc.Name),
			zap.Error(err),
			zap.Duration("timeout", hc.Timeout))
	}
}

// GetHealthStatus returns the status of all health checks
func (c *Collector) GetHealthStatus() map[string]HealthCheck {
	c.healthChecksMutex.RLock()
	defer c.healthChecksMutex.RUnlock()

	result := make(map[string]HealthCheck)
	for name, hc := range c.healthChecks {
		result[name] = hc
	}

	return result
}

// RegisterAlert registers an alert
func (c *Collector) RegisterAlert(id, name, description, severity string, condition func() bool, enabled bool) {
	c.alertsMutex.Lock()
	defer c.alertsMutex.Unlock()

	c.alerts[id] = &Alert{
		ID:          id,
		Name:        name,
		Description: description,
		Severity:    severity,
		Condition:   condition,
		Enabled:     enabled,
	}
}

// checkAlerts checks all registered alerts
func (c *Collector) checkAlerts() {
	c.alertsMutex.RLock()
	alerts := make([]*Alert, 0, len(c.alerts))
	for _, alert := range c.alerts {
		if alert.Enabled {
			alerts = append(alerts, alert)
		}
	}
	c.alertsMutex.RUnlock()

	for _, alert := range alerts {
		go c.checkAlert(alert)
	}
}

// checkAlert checks a single alert
func (c *Collector) checkAlert(alert *Alert) {
	if alert.Condition() {
		c.alertsMutex.Lock()
		alert.LastTrigger = time.Now()
		alert.Count++
		c.alertsMutex.Unlock()

		c.logger.Warn("Alert triggered",
			zap.String("id", alert.ID),
			zap.String("name", alert.Name),
			zap.String("description", alert.Description),
			zap.String("severity", alert.Severity),
			zap.Int("count", alert.Count))
	}
}

// GetAlerts returns all alerts
func (c *Collector) GetAlerts() map[string]*Alert {
	c.alertsMutex.RLock()
	defer c.alertsMutex.RUnlock()

	result := make(map[string]*Alert)
	for id, alert := range c.alerts {
		result[id] = alert
	}

	return result
}

// RegisterCustomMetric registers a custom metric
func (c *Collector) RegisterCustomMetric(name string, metric prometheus.Metric) error {
	c.customMetricsMutex.Lock()
	defer c.customMetricsMutex.Unlock()

	if _, exists := c.customMetrics[name]; exists {
		return fmt.Errorf("metric %s already exists", name)
	}

	c.customMetrics[name] = metric
	return nil
}

// GetCustomMetrics returns all custom metrics
func (c *Collector) GetCustomMetrics() map[string]prometheus.Metric {
	c.customMetricsMutex.RLock()
	defer c.customMetricsMutex.RUnlock()

	result := make(map[string]prometheus.Metric)
	for name, metric := range c.customMetrics {
		result[name] = metric
	}

	return result
}

// SetServiceStatus sets the service status
func (c *Collector) SetServiceStatus(up bool) {
	if !c.config.Enabled {
		return
	}

	value := 0.0
	if up {
		value = 1.0
	}

	c.serviceUp.WithLabelValues(c.config.ServiceName, c.config.ServiceInstance, c.config.ServiceVersion).Set(value)
}

// Shutdown gracefully shuts down the collector
func (c *Collector) Shutdown() {
	c.logger.Info("Shutting down monitoring collector")
	c.SetServiceStatus(false)
}
