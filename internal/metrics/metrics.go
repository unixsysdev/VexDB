package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector is a type alias for Metrics to provide a consistent interface
type Collector = Metrics

// Metrics represents the metrics collection system
type Metrics struct {
	config     interface{}
	registry   *prometheus.Registry
	collectors map[string]prometheus.Collector
	mu         sync.RWMutex
}

// Config represents metrics configuration
type Config struct {
	Enabled      bool          `yaml:"enabled" json:"enabled"`
	Port         int           `yaml:"port" json:"port"`
	Path         string        `yaml:"path" json:"path"`
	Namespace    string        `yaml:"namespace" json:"namespace"`
	Subsystem    string        `yaml:"subsystem" json:"subsystem"`
	Interval     time.Duration `yaml:"interval" json:"interval"`
	Buckets      []float64     `yaml:"buckets" json:"buckets"`
	Labels       []string      `yaml:"labels" json:"labels"`
}

// Counter represents a counter metric
type Counter struct {
	metric *prometheus.CounterVec
	labels []string
}

// Gauge represents a gauge metric
type Gauge struct {
	metric *prometheus.GaugeVec
	labels []string
}

// Histogram represents a histogram metric
type Histogram struct {
	metric *prometheus.HistogramVec
	labels []string
}

// Summary represents a summary metric
type Summary struct {
	metric *prometheus.SummaryVec
	labels []string
}

// NewMetrics creates a new metrics collection system
func NewMetrics(cfg interface{}) (*Metrics, error) {
	// For now, use default configuration
	// TODO: Implement proper configuration extraction based on config type

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return &Metrics{
		config:     cfg,
		registry:   registry,
		collectors: make(map[string]prometheus.Collector),
	}, nil
}

// NewCounter creates a new counter metric
func (m *Metrics) NewCounter(name, help string, labels []string) *Counter {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := m.getFullName(name)
	if _, exists := m.collectors[fullName]; exists {
		return &Counter{
			metric: m.collectors[fullName].(*prometheus.CounterVec),
			labels: labels,
		}
	}

	counter := promauto.With(m.registry).NewCounterVec(
		prometheus.CounterOpts{
			Name:      fullName,
			Help:      help,
			Namespace: m.getNamespace(),
			Subsystem: m.getSubsystem(),
		},
		labels,
	)

	m.collectors[fullName] = counter
	return &Counter{
		metric: counter,
		labels: labels,
	}
}

// NewGauge creates a new gauge metric
func (m *Metrics) NewGauge(name, help string, labels []string) *Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := m.getFullName(name)
	if _, exists := m.collectors[fullName]; exists {
		return &Gauge{
			metric: m.collectors[fullName].(*prometheus.GaugeVec),
			labels: labels,
		}
	}

	gauge := promauto.With(m.registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Name:      fullName,
			Help:      help,
			Namespace: m.getNamespace(),
			Subsystem: m.getSubsystem(),
		},
		labels,
	)

	m.collectors[fullName] = gauge
	return &Gauge{
		metric: gauge,
		labels: labels,
	}
}

// NewHistogram creates a new histogram metric
func (m *Metrics) NewHistogram(name, help string, labels []string, buckets []float64) *Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := m.getFullName(name)
	if _, exists := m.collectors[fullName]; exists {
		return &Histogram{
			metric: m.collectors[fullName].(*prometheus.HistogramVec),
			labels: labels,
		}
	}

	if len(buckets) == 0 {
		buckets = prometheus.DefBuckets
	}

	histogram := promauto.With(m.registry).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      fullName,
			Help:      help,
			Namespace: m.getNamespace(),
			Subsystem: m.getSubsystem(),
			Buckets:   buckets,
		},
		labels,
	)

	m.collectors[fullName] = histogram
	return &Histogram{
		metric: histogram,
		labels: labels,
	}
}

// NewSummary creates a new summary metric
func (m *Metrics) NewSummary(name, help string, labels []string) *Summary {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := m.getFullName(name)
	if _, exists := m.collectors[fullName]; exists {
		return &Summary{
			metric: m.collectors[fullName].(*prometheus.SummaryVec),
			labels: labels,
		}
	}

	summary := promauto.With(m.registry).NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       fullName,
			Help:       help,
			Namespace:  m.getNamespace(),
			Subsystem:  m.getSubsystem(),
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		labels,
	)

	m.collectors[fullName] = summary
	return &Summary{
		metric: summary,
		labels: labels,
	}
}

// GetRegistry returns the Prometheus registry
func (m *Metrics) GetRegistry() *prometheus.Registry {
	return m.registry
}

// GetConfig returns the metrics configuration
func (m *Metrics) GetConfig() *Config {
	return &Config{
		Enabled:   true,
		Port:      9090,
		Path:      "/metrics",
		Namespace: "vexdb",
		Subsystem: "",
		Interval:  15 * time.Second,
		Buckets:   prometheus.DefBuckets,
		Labels:    []string{"service", "node", "cluster"},
	}
}

// Start starts the metrics collection
func (m *Metrics) Start(ctx context.Context) error {
	if !m.IsEnabled() {
		return nil
	}

	// Start background metrics collection if needed
	go func() {
		ticker := time.NewTicker(m.GetConfig().Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.collectSystemMetrics(ctx)
			}
		}
	}()

	return nil
}

// Stop stops the metrics collection
func (m *Metrics) Stop() error {
	// Clean up resources if needed
	return nil
}

// IsEnabled returns whether metrics collection is enabled
func (m *Metrics) IsEnabled() bool {
	return true
}

// collectSystemMetrics collects system metrics
func (m *Metrics) collectSystemMetrics(ctx context.Context) {
	// Collect system-level metrics here
	// This could include CPU, memory, disk, network metrics
}

// getFullName returns the full metric name with namespace and subsystem
func (m *Metrics) getFullName(name string) string {
	cfg := m.GetConfig()
	fullName := ""
	if cfg.Namespace != "" {
		fullName += cfg.Namespace + "_"
	}
	if cfg.Subsystem != "" {
		fullName += cfg.Subsystem + "_"
	}
	fullName += name
	return fullName
}

// getNamespace returns the namespace
func (m *Metrics) getNamespace() string {
	return m.GetConfig().Namespace
}

// getSubsystem returns the subsystem
func (m *Metrics) getSubsystem() string {
	return m.GetConfig().Subsystem
}

// Counter methods

// Inc increments the counter
func (c *Counter) Inc(labels ...string) {
	c.metric.WithLabelValues(labels...).Inc()
}

// Add adds a value to the counter
func (c *Counter) Add(value float64, labels ...string) {
	c.metric.WithLabelValues(labels...).Add(value)
}

// Gauge methods

// Set sets the gauge value
func (g *Gauge) Set(value float64, labels ...string) {
	g.metric.WithLabelValues(labels...).Set(value)
}

// Inc increments the gauge
func (g *Gauge) Inc(labels ...string) {
	g.metric.WithLabelValues(labels...).Inc()
}

// Dec decrements the gauge
func (g *Gauge) Dec(labels ...string) {
	g.metric.WithLabelValues(labels...).Dec()
}

// Add adds a value to the gauge
func (g *Gauge) Add(value float64, labels ...string) {
	g.metric.WithLabelValues(labels...).Add(value)
}

// Sub subtracts a value from the gauge
func (g *Gauge) Sub(value float64, labels ...string) {
	g.metric.WithLabelValues(labels...).Sub(value)
}

// Histogram methods

// Observe observes a value
func (h *Histogram) Observe(value float64, labels ...string) {
	h.metric.WithLabelValues(labels...).Observe(value)
}

// Summary methods

// Observe observes a value
func (s *Summary) Observe(value float64, labels ...string) {
	s.metric.WithLabelValues(labels...).Observe(value)
}

// ServiceMetrics represents service-specific metrics
type ServiceMetrics struct {
	RequestsTotal     *Counter
	RequestsDuration  *Histogram
	RequestsActive    *Gauge
	ErrorsTotal       *Counter
	QueueSize         *Gauge
	ProcessingTime    *Histogram
	Throughput        *Counter
	Latency           *Histogram
	CPUUsage          *Gauge
	MemoryUsage       *Gauge
	DiskUsage         *Gauge
	NetworkUsage      *Gauge
	ConnectionsActive *Gauge
	ConnectionsTotal  *Counter
}

// NewServiceMetrics creates service-specific metrics
func NewServiceMetrics(m *Metrics, serviceName string) *ServiceMetrics {
	return &ServiceMetrics{
		RequestsTotal: m.NewCounter(
			"requests_total",
			"Total number of requests",
			[]string{"service", "method", "status"},
		),
		RequestsDuration: m.NewHistogram(
			"requests_duration_seconds",
			"Request duration in seconds",
			[]string{"service", "method"},
			[]float64{0.001, 0.01, 0.1, 1, 10},
		),
		RequestsActive: m.NewGauge(
			"requests_active",
			"Number of active requests",
			[]string{"service"},
		),
		ErrorsTotal: m.NewCounter(
			"errors_total",
			"Total number of errors",
			[]string{"service", "method", "error_type"},
		),
		QueueSize: m.NewGauge(
			"queue_size",
			"Size of the request queue",
			[]string{"service"},
		),
		ProcessingTime: m.NewHistogram(
			"processing_time_seconds",
			"Processing time in seconds",
			[]string{"service", "operation"},
			[]float64{0.001, 0.01, 0.1, 1, 10},
		),
		Throughput: m.NewCounter(
			"throughput_total",
			"Total throughput",
			[]string{"service", "operation"},
		),
		Latency: m.NewHistogram(
			"latency_seconds",
			"Latency in seconds",
			[]string{"service", "operation"},
			[]float64{0.001, 0.01, 0.1, 1, 10},
		),
		CPUUsage: m.NewGauge(
			"cpu_usage_percent",
			"CPU usage percentage",
			[]string{"service"},
		),
		MemoryUsage: m.NewGauge(
			"memory_usage_bytes",
			"Memory usage in bytes",
			[]string{"service"},
		),
		DiskUsage: m.NewGauge(
			"disk_usage_bytes",
			"Disk usage in bytes",
			[]string{"service"},
		),
		NetworkUsage: m.NewGauge(
			"network_usage_bytes",
			"Network usage in bytes",
			[]string{"service"},
		),
		ConnectionsActive: m.NewGauge(
			"connections_active",
			"Number of active connections",
			[]string{"service"},
		),
		ConnectionsTotal: m.NewCounter(
			"connections_total",
			"Total number of connections",
			[]string{"service"},
		),
	}
}

// StorageMetrics represents storage-specific metrics
type StorageMetrics struct {
	VectorsTotal      *Counter
	VectorsSize       *Gauge
	SegmentsTotal     *Gauge
	SegmentsSize      *Gauge
	ReadOperations    *Counter
	WriteOperations   *Counter
	DeleteOperations  *Counter
	ReadLatency       *Histogram
	WriteLatency      *Histogram
	DeleteLatency     *Histogram
	CompactionTime    *Histogram
	IndexSize         *Gauge
	CacheHits         *Counter
	CacheMisses       *Counter
	CacheHitRatio     *Gauge
	DiskReadBytes     *Counter
	DiskWriteBytes    *Counter
	MemoryUsage       *Gauge
	DiskUsage         *Gauge
}

// NewStorageMetrics creates storage-specific metrics
func NewStorageMetrics(m *Metrics, storageName string) *StorageMetrics {
	return &StorageMetrics{
		VectorsTotal: m.NewCounter(
			"vectors_total",
			"Total number of vectors",
			[]string{"storage", "cluster"},
		),
		VectorsSize: m.NewGauge(
			"vectors_size_bytes",
			"Total size of vectors in bytes",
			[]string{"storage", "cluster"},
		),
		SegmentsTotal: m.NewGauge(
			"segments_total",
			"Total number of segments",
			[]string{"storage", "cluster"},
		),
		SegmentsSize: m.NewGauge(
			"segments_size_bytes",
			"Total size of segments in bytes",
			[]string{"storage", "cluster"},
		),
		ReadOperations: m.NewCounter(
			"read_operations_total",
			"Total number of read operations",
			[]string{"storage", "operation"},
		),
		WriteOperations: m.NewCounter(
			"write_operations_total",
			"Total number of write operations",
			[]string{"storage", "operation"},
		),
		DeleteOperations: m.NewCounter(
			"delete_operations_total",
			"Total number of delete operations",
			[]string{"storage", "operation"},
		),
		ReadLatency: m.NewHistogram(
			"read_latency_seconds",
			"Read latency in seconds",
			[]string{"storage", "operation"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		WriteLatency: m.NewHistogram(
			"write_latency_seconds",
			"Write latency in seconds",
			[]string{"storage", "operation"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		DeleteLatency: m.NewHistogram(
			"delete_latency_seconds",
			"Delete latency in seconds",
			[]string{"storage", "operation"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		CompactionTime: m.NewHistogram(
			"compaction_time_seconds",
			"Compaction time in seconds",
			[]string{"storage"},
			[]float64{0.1, 1, 10, 60, 300},
		),
		IndexSize: m.NewGauge(
			"index_size_bytes",
			"Size of the index in bytes",
			[]string{"storage", "cluster"},
		),
		CacheHits: m.NewCounter(
			"cache_hits_total",
			"Total number of cache hits",
			[]string{"storage", "cache_type"},
		),
		CacheMisses: m.NewCounter(
			"cache_misses_total",
			"Total number of cache misses",
			[]string{"storage", "cache_type"},
		),
		CacheHitRatio: m.NewGauge(
			"cache_hit_ratio",
			"Cache hit ratio",
			[]string{"storage", "cache_type"},
		),
		DiskReadBytes: m.NewCounter(
			"disk_read_bytes_total",
			"Total bytes read from disk",
			[]string{"storage"},
		),
		DiskWriteBytes: m.NewCounter(
			"disk_write_bytes_total",
			"Total bytes written to disk",
			[]string{"storage"},
		),
		MemoryUsage: m.NewGauge(
			"memory_usage_bytes",
			"Memory usage in bytes",
			[]string{"storage"},
		),
		DiskUsage: m.NewGauge(
			"disk_usage_bytes",
			"Disk usage in bytes",
			[]string{"storage"},
		),
	}
}

// SearchMetrics represents search-specific metrics
type SearchMetrics struct {
	QueriesTotal      *Counter
	QueriesDuration   *Histogram
	QueriesActive     *Gauge
	ResultsTotal      *Counter
	ResultsSize       *Gauge
	Accuracy          *Gauge
	Recall            *Gauge
	Precision         *Gauge
	IndexSearchTime   *Histogram
	VectorSearchTime  *Histogram
	FilteringTime     *Histogram
	RankingTime       *Histogram
	MergingTime       *Histogram
	CacheHits         *Counter
	CacheMisses       *Counter
	Timeouts          *Counter
	Errors            *Counter
}

// NewSearchMetrics creates search-specific metrics
func NewSearchMetrics(m *Metrics, searchName string) *SearchMetrics {
	return &SearchMetrics{
		QueriesTotal: m.NewCounter(
			"queries_total",
			"Total number of queries",
			[]string{"search", "type"},
		),
		QueriesDuration: m.NewHistogram(
			"queries_duration_seconds",
			"Query duration in seconds",
			[]string{"search", "type"},
			[]float64{0.001, 0.01, 0.1, 1, 10},
		),
		QueriesActive: m.NewGauge(
			"queries_active",
			"Number of active queries",
			[]string{"search"},
		),
		ResultsTotal: m.NewCounter(
			"results_total",
			"Total number of results",
			[]string{"search", "type"},
		),
		ResultsSize: m.NewGauge(
			"results_size_bytes",
			"Size of results in bytes",
			[]string{"search", "type"},
		),
		Accuracy: m.NewGauge(
			"accuracy_ratio",
			"Search accuracy ratio",
			[]string{"search", "type"},
		),
		Recall: m.NewGauge(
			"recall_ratio",
			"Search recall ratio",
			[]string{"search", "type"},
		),
		Precision: m.NewGauge(
			"precision_ratio",
			"Search precision ratio",
			[]string{"search", "type"},
		),
		IndexSearchTime: m.NewHistogram(
			"index_search_time_seconds",
			"Index search time in seconds",
			[]string{"search"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		VectorSearchTime: m.NewHistogram(
			"vector_search_time_seconds",
			"Vector search time in seconds",
			[]string{"search"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		FilteringTime: m.NewHistogram(
			"filtering_time_seconds",
			"Filtering time in seconds",
			[]string{"search"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		RankingTime: m.NewHistogram(
			"ranking_time_seconds",
			"Ranking time in seconds",
			[]string{"search"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		MergingTime: m.NewHistogram(
			"merging_time_seconds",
			"Merging time in seconds",
			[]string{"search"},
			[]float64{0.0001, 0.001, 0.01, 0.1, 1},
		),
		CacheHits: m.NewCounter(
			"cache_hits_total",
			"Total number of cache hits",
			[]string{"search", "cache_type"},
		),
		CacheMisses: m.NewCounter(
			"cache_misses_total",
			"Total number of cache misses",
			[]string{"search", "cache_type"},
		),
		Timeouts: m.NewCounter(
			"timeouts_total",
			"Total number of timeouts",
			[]string{"search", "type"},
		),
		Errors: m.NewCounter(
			"errors_total",
			"Total number of errors",
			[]string{"search", "type", "error_type"},
		),
	}
}

// Timer provides a simple way to measure operation duration
type Timer struct {
	start    time.Time
	histogram *Histogram
	labels   []string
}

// NewTimer creates a new timer
func NewTimer(histogram *Histogram, labels ...string) *Timer {
	return &Timer{
		start:    time.Now(),
		histogram: histogram,
		labels:   labels,
	}
}

// ObserveDuration observes the duration since the timer was created
func (t *Timer) ObserveDuration() {
	duration := time.Since(t.start).Seconds()
	t.histogram.Observe(duration, t.labels...)
}

// Stop stops the timer and observes the duration
func (t *Timer) Stop() {
	t.ObserveDuration()
}