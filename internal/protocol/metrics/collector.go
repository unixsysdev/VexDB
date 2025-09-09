package metrics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"strings"

	"go.uber.org/zap"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

var (
	ErrCollectorNotRunning     = errors.New("metrics collector not running")
	ErrCollectorAlreadyRunning = errors.New("metrics collector already running")
	ErrInvalidConfig           = errors.New("invalid metrics collector configuration")
	ErrMetricNotFound          = errors.New("metric not found")
	ErrInvalidMetricType       = errors.New("invalid metric type")
)

// MetricsConfig represents the metrics collector configuration
type MetricsConfig struct {
	Enabled           bool                 `yaml:"enabled" json:"enabled"`
	EnableMetrics     bool                 `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool                 `yaml:"enable_logging" json:"enable_logging"`
	EnableTracing     bool                 `yaml:"enable_tracing" json:"enable_tracing"`
	EnableAggregation bool                 `yaml:"enable_aggregation" json:"enable_aggregation"`
	EnableExport      bool                 `yaml:"enable_export" json:"enable_export"`
	AggregationWindow time.Duration        `yaml:"aggregation_window" json:"aggregation_window"`
	ExportInterval    time.Duration        `yaml:"export_interval" json:"export_interval"`
	MaxMetrics        int                  `yaml:"max_metrics" json:"max_metrics"`
	MetricTTL         time.Duration        `yaml:"metric_ttl" json:"metric_ttl"`
	CustomMetrics     []CustomMetricConfig `yaml:"custom_metrics" json:"custom_metrics"`
	Exporters         []ExporterConfig     `yaml:"exporters" json:"exporters"`
	Aggregators       []AggregatorConfig   `yaml:"aggregators" json:"aggregators"`
}

// CustomMetricConfig represents a custom metric configuration
type CustomMetricConfig struct {
	Name        string                 `yaml:"name" json:"name"`
	Type        string                 `yaml:"type" json:"type"`
	Description string                 `yaml:"description" json:"description"`
	Labels      []string               `yaml:"labels" json:"labels"`
	Buckets     []float64              `yaml:"buckets" json:"buckets"`
	Objectives  map[float64]float64    `yaml:"objectives" json:"objectives"`
	Enabled     bool                   `yaml:"enabled" json:"enabled"`
	Config      map[string]interface{} `yaml:"config" json:"config"`
}

// ExporterConfig represents an exporter configuration
type ExporterConfig struct {
	Name     string                 `yaml:"name" json:"name"`
	Type     string                 `yaml:"type" json:"type"`
	Enabled  bool                   `yaml:"enabled" json:"enabled"`
	Interval time.Duration          `yaml:"interval" json:"interval"`
	Config   map[string]interface{} `yaml:"config" json:"config"`
}

// AggregatorConfig represents an aggregator configuration
type AggregatorConfig struct {
	Name      string                 `yaml:"name" json:"name"`
	Type      string                 `yaml:"type" json:"type"`
	Enabled   bool                   `yaml:"enabled" json:"enabled"`
	Window    time.Duration          `yaml:"window" json:"window"`
	Functions []string               `yaml:"functions" json:"functions"`
	Config    map[string]interface{} `yaml:"config" json:"config"`
}

// DefaultMetricsConfig returns the default metrics collector configuration
func DefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		Enabled:           true,
		EnableMetrics:     true,
		EnableLogging:     true,
		EnableTracing:     false,
		EnableAggregation: true,
		EnableExport:      false,
		AggregationWindow: 1 * time.Minute,
		ExportInterval:    30 * time.Second,
		MaxMetrics:        10000,
		MetricTTL:         24 * time.Hour,
		CustomMetrics:     getDefaultCustomMetrics(),
		Exporters:         []ExporterConfig{},
		Aggregators:       getDefaultAggregators(),
	}
}

// getDefaultCustomMetrics returns the default custom metrics
func getDefaultCustomMetrics() []CustomMetricConfig {
	return []CustomMetricConfig{
		{
			Name:        "ingestion_requests_total",
			Type:        "counter",
			Description: "Total number of ingestion requests",
			Labels:      []string{"protocol", "method", "status"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_request_duration_seconds",
			Type:        "histogram",
			Description: "Duration of ingestion requests in seconds",
			Labels:      []string{"protocol", "method"},
			Buckets:     []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0},
			Enabled:     true,
		},
		{
			Name:        "ingestion_vectors_total",
			Type:        "counter",
			Description: "Total number of vectors ingested",
			Labels:      []string{"protocol", "cluster"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_bytes_total",
			Type:        "counter",
			Description: "Total number of bytes ingested",
			Labels:      []string{"protocol"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_errors_total",
			Type:        "counter",
			Description: "Total number of ingestion errors",
			Labels:      []string{"protocol", "error_type"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_active_connections",
			Type:        "gauge",
			Description: "Number of active ingestion connections",
			Labels:      []string{"protocol"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_queue_size",
			Type:        "gauge",
			Description: "Size of the ingestion queue",
			Labels:      []string{"protocol"},
			Enabled:     true,
		},
		{
			Name:        "ingestion_throughput_vectors_per_second",
			Type:        "gauge",
			Description: "Throughput of vectors per second",
			Labels:      []string{"protocol"},
			Enabled:     true,
		},
	}
}

// getDefaultAggregators returns the default aggregators
func getDefaultAggregators() []AggregatorConfig {
	return []AggregatorConfig{
		{
			Name:      "request_rate",
			Type:      "rate",
			Enabled:   true,
			Window:    1 * time.Minute,
			Functions: []string{"sum", "avg", "max"},
		},
		{
			Name:      "error_rate",
			Type:      "rate",
			Enabled:   true,
			Window:    5 * time.Minute,
			Functions: []string{"sum", "avg"},
		},
		{
			Name:      "latency_percentiles",
			Type:      "percentile",
			Enabled:   true,
			Window:    5 * time.Minute,
			Functions: []string{"p50", "p95", "p99"},
		},
	}
}

// MetricEntry represents a metric entry
type MetricEntry struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Type      string                 `json:"type"`
	Value     float64                `json:"value"`
	Labels    map[string]string      `json:"labels"`
	Timestamp time.Time              `json:"timestamp"`
	Source    string                 `json:"source"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// AggregatedMetric represents an aggregated metric
type AggregatedMetric struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Window    time.Duration     `json:"window"`
	Function  string            `json:"function"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// MetricsCollector represents a metrics collector
type MetricsCollector struct {
	config  *MetricsConfig
	logger  logging.Logger
	metrics *metrics.ServiceMetrics

	// Metrics storage
	entries     []MetricEntry
	metricIndex map[string]int
	mu          sync.RWMutex

	// Aggregation
	aggregators map[string]*Aggregator
	aggMu       sync.RWMutex

	// Export
	exporters map[string]*Exporter
	exportMu  sync.RWMutex

	// Lifecycle
	started   bool
	stopped   bool
	startTime time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// Statistics
	stats *MetricsCollectorStats
}

// MetricsCollectorStats represents metrics collector statistics
type MetricsCollectorStats struct {
	TotalMetrics      int64            `json:"total_metrics"`
	ActiveMetrics     int64            `json:"active_metrics"`
	ExpiredMetrics    int64            `json:"expired_metrics"`
	AggregatedMetrics int64            `json:"aggregated_metrics"`
	ExportedMetrics   int64            `json:"exported_metrics"`
	FailedExports     int64            `json:"failed_exports"`
	MetricsByType     map[string]int64 `json:"metrics_by_type"`
	MetricsBySource   map[string]int64 `json:"metrics_by_source"`
	StartTime         time.Time        `json:"start_time"`
	Uptime            time.Duration    `json:"uptime"`
}

// Aggregator represents a metrics aggregator
type Aggregator struct {
	config     AggregatorConfig
	metrics    []MetricEntry
	window     time.Duration
	functions  []string
	lastUpdate time.Time
}

// Exporter represents a metrics exporter
type Exporter struct {
	config     ExporterConfig
	lastExport time.Time
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*MetricsCollector, error) {
	collectorConfig := DefaultMetricsConfig()

	if cfg != nil {
		if metricsCfg, ok := cfg.Get("metrics_collector"); ok {
			if cfgMap, ok := metricsCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					collectorConfig.Enabled = enabled
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					collectorConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					collectorConfig.EnableLogging = enableLogging
				}
				if enableTracing, ok := cfgMap["enable_tracing"].(bool); ok {
					collectorConfig.EnableTracing = enableTracing
				}
				if enableAggregation, ok := cfgMap["enable_aggregation"].(bool); ok {
					collectorConfig.EnableAggregation = enableAggregation
				}
				if enableExport, ok := cfgMap["enable_export"].(bool); ok {
					collectorConfig.EnableExport = enableExport
				}
				if aggregationWindow, ok := cfgMap["aggregation_window"].(string); ok {
					if duration, err := time.ParseDuration(aggregationWindow); err == nil {
						collectorConfig.AggregationWindow = duration
					}
				}
				if exportInterval, ok := cfgMap["export_interval"].(string); ok {
					if duration, err := time.ParseDuration(exportInterval); err == nil {
						collectorConfig.ExportInterval = duration
					}
				}
				if maxMetrics, ok := cfgMap["max_metrics"].(int); ok {
					collectorConfig.MaxMetrics = maxMetrics
				}
				if metricTTL, ok := cfgMap["metric_ttl"].(string); ok {
					if duration, err := time.ParseDuration(metricTTL); err == nil {
						collectorConfig.MetricTTL = duration
					}
				}
			}
		}
	}

	// Validate configuration
	if err := validateMetricsConfig(collectorConfig); err != nil {
		return nil, fmt.Errorf("invalid metrics collector configuration: %w", err)
	}

	collector := &MetricsCollector{
		config:      collectorConfig,
		logger:      logger,
		metrics:     metrics,
		entries:     make([]MetricEntry, 0),
		metricIndex: make(map[string]int),
		aggregators: make(map[string]*Aggregator),
		exporters:   make(map[string]*Exporter),
		startTime:   time.Now(),
		stats: &MetricsCollectorStats{
			MetricsByType:   make(map[string]int64),
			MetricsBySource: make(map[string]int64),
			StartTime:       time.Now(),
		},
	}

	// Initialize aggregators
	if collectorConfig.EnableAggregation {
		collector.initializeAggregators()
	}

	// Initialize exporters
	if collectorConfig.EnableExport {
		collector.initializeExporters()
	}

	collector.logger.Info("Created metrics collector")

	return collector, nil
}

// validateMetricsConfig validates the metrics collector configuration
func validateMetricsConfig(config *MetricsConfig) error {
	if config == nil {
		return errors.New("metrics collector configuration cannot be nil")
	}

	if config.MaxMetrics <= 0 {
		return errors.New("max metrics must be positive")
	}

	if config.MetricTTL <= 0 {
		return errors.New("metric TTL must be positive")
	}

	if config.AggregationWindow <= 0 {
		return errors.New("aggregation window must be positive")
	}

	if config.ExportInterval <= 0 {
		return errors.New("export interval must be positive")
	}

	for _, metric := range config.CustomMetrics {
		if metric.Name == "" {
			return errors.New("metric name cannot be empty")
		}
		if metric.Type == "" {
			return errors.New("metric type cannot be empty")
		}
		if metric.Type != "counter" && metric.Type != "gauge" && metric.Type != "histogram" && metric.Type != "summary" {
			return fmt.Errorf("invalid metric type: %s", metric.Type)
		}
	}

	for _, aggregator := range config.Aggregators {
		if aggregator.Name == "" {
			return errors.New("aggregator name cannot be empty")
		}
		if aggregator.Type == "" {
			return errors.New("aggregator type cannot be empty")
		}
		if aggregator.Window <= 0 {
			return errors.New("aggregator window must be positive")
		}
	}

	for _, exporter := range config.Exporters {
		if exporter.Name == "" {
			return errors.New("exporter name cannot be empty")
		}
		if exporter.Type == "" {
			return errors.New("exporter type cannot be empty")
		}
		if exporter.Interval <= 0 {
			return errors.New("exporter interval must be positive")
		}
	}

	return nil
}

// initializeAggregators initializes the aggregators
func (c *MetricsCollector) initializeAggregators() {
	c.aggMu.Lock()
	defer c.aggMu.Unlock()

	for _, aggConfig := range c.config.Aggregators {
		if aggConfig.Enabled {
			c.aggregators[aggConfig.Name] = &Aggregator{
				config:     aggConfig,
				metrics:    make([]MetricEntry, 0),
				window:     aggConfig.Window,
				functions:  aggConfig.Functions,
				lastUpdate: time.Now(),
			}
		}
	}
}

// initializeExporters initializes the exporters
func (c *MetricsCollector) initializeExporters() {
	c.exportMu.Lock()
	defer c.exportMu.Unlock()

	for _, expConfig := range c.config.Exporters {
		if expConfig.Enabled {
			c.exporters[expConfig.Name] = &Exporter{
				config:     expConfig,
				lastExport: time.Now(),
			}
		}
	}
}

// Start starts the metrics collector
func (c *MetricsCollector) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return ErrCollectorAlreadyRunning
	}

	if !c.config.Enabled {
		c.logger.Info("Metrics collector is disabled")
		return nil
	}

	c.started = true
	c.stopped = false
	c.ctx, c.cancel = context.WithCancel(context.Background())

	// Start aggregation goroutine
	if c.config.EnableAggregation {
		go c.runAggregation()
	}

	// Start export goroutine
	if c.config.EnableExport {
		go c.runExport()
	}

	c.logger.Info("Started metrics collector")

	return nil
}

// Stop stops the metrics collector
func (c *MetricsCollector) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return nil
	}

	if !c.started {
		return ErrCollectorNotRunning
	}

	c.stopped = true
	c.started = false
	if c.cancel != nil {
		c.cancel()
	}

	c.logger.Info("Stopped metrics collector")

	return nil
}

// IsRunning checks if the metrics collector is running
func (c *MetricsCollector) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.started && !c.stopped
}

// RecordMetric records a metric
func (c *MetricsCollector) RecordMetric(ctx context.Context, name, metricType string, value float64, labels map[string]string, source string) error {
	if !c.config.Enabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return ErrCollectorNotRunning
	}

	// Create metric entry
	entry := MetricEntry{
		ID:        generateMetricID(),
		Name:      name,
		Type:      metricType,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
		Source:    source,
		Metadata:  make(map[string]interface{}),
	}

	// Add to metrics storage
	c.addMetricEntry(entry)

	// Update statistics
	c.updateStats(entry)

	// Log metric if enabled
	if c.config.EnableLogging {
		c.logMetric(entry)
	}

	// Update service metrics if enabled
	if c.config.EnableMetrics && c.metrics != nil {
		c.updateServiceMetrics(entry)
	}

	return nil
}

// RecordRequestMetrics records request metrics
func (c *MetricsCollector) RecordRequestMetrics(ctx context.Context, req *adapter.Request, resp *adapter.Response, duration time.Duration) error {
	if !c.config.Enabled {
		return nil
	}

	// Record request count
	labels := map[string]string{
		"protocol": string(req.Protocol),
		"method":   req.Method,
		"status":   fmt.Sprintf("%d", resp.Status),
	}

	if err := c.RecordMetric(ctx, "ingestion_requests_total", "counter", 1, labels, "request"); err != nil {
		return err
	}

	// Record request duration
	durationLabels := map[string]string{
		"protocol": string(req.Protocol),
		"method":   req.Method,
	}

	if err := c.RecordMetric(ctx, "ingestion_request_duration_seconds", "histogram", duration.Seconds(), durationLabels, "request"); err != nil {
		return err
	}

	// Record bytes processed
	bytesLabels := map[string]string{
		"protocol": string(req.Protocol),
	}

	totalBytes := len(req.Body) + len(resp.Body)
	if err := c.RecordMetric(ctx, "ingestion_bytes_total", "counter", float64(totalBytes), bytesLabels, "request"); err != nil {
		return err
	}

	// Record error if any
	if resp.Status >= 400 {
		errorLabels := map[string]string{
			"protocol":   string(req.Protocol),
			"error_type": getErrorTypeFromStatus(resp.Status),
		}

		if err := c.RecordMetric(ctx, "ingestion_errors_total", "counter", 1, errorLabels, "request"); err != nil {
			return err
		}
	}

	return nil
}

// RecordVectorMetrics records vector metrics
func (c *MetricsCollector) RecordVectorMetrics(ctx context.Context, protocol adapter.Protocol, vectorCount int, clusterID string) error {
	if !c.config.Enabled {
		return nil
	}

	// Record vector count
	labels := map[string]string{
		"protocol": string(protocol),
		"cluster":  clusterID,
	}

	if err := c.RecordMetric(ctx, "ingestion_vectors_total", "counter", float64(vectorCount), labels, "vector"); err != nil {
		return err
	}

	return nil
}

// RecordConnectionMetrics records connection metrics
func (c *MetricsCollector) RecordConnectionMetrics(ctx context.Context, protocol adapter.Protocol, activeConnections, queueSize int) error {
	if !c.config.Enabled {
		return nil
	}

	// Record active connections
	connLabels := map[string]string{
		"protocol": string(protocol),
	}

	if err := c.RecordMetric(ctx, "ingestion_active_connections", "gauge", float64(activeConnections), connLabels, "connection"); err != nil {
		return err
	}

	// Record queue size
	if err := c.RecordMetric(ctx, "ingestion_queue_size", "gauge", float64(queueSize), connLabels, "connection"); err != nil {
		return err
	}

	return nil
}

// RecordThroughputMetrics records throughput metrics
func (c *MetricsCollector) RecordThroughputMetrics(ctx context.Context, protocol adapter.Protocol, vectorsPerSecond float64) error {
	if !c.config.Enabled {
		return nil
	}

	// Record throughput
	labels := map[string]string{
		"protocol": string(protocol),
	}

	if err := c.RecordMetric(ctx, "ingestion_throughput_vectors_per_second", "gauge", vectorsPerSecond, labels, "throughput"); err != nil {
		return err
	}

	return nil
}

// addMetricEntry adds a metric entry to the storage
func (c *MetricsCollector) addMetricEntry(entry MetricEntry) {
	c.entries = append(c.entries, entry)
	c.metricIndex[entry.ID] = len(c.entries) - 1

	// Clean up old metrics
	if len(c.entries) > c.config.MaxMetrics {
		// Remove oldest metrics
		removed := len(c.entries) - c.config.MaxMetrics
		c.entries = c.entries[removed:]

		// Rebuild index
		c.metricIndex = make(map[string]int)
		for i, metric := range c.entries {
			c.metricIndex[metric.ID] = i
		}
	}

	// Clean up expired metrics
	now := time.Now()
	validMetrics := make([]MetricEntry, 0)
	for _, metric := range c.entries {
		if now.Sub(metric.Timestamp) <= c.config.MetricTTL {
			validMetrics = append(validMetrics, metric)
		}
	}

	if len(validMetrics) != len(c.entries) {
		c.entries = validMetrics
		c.metricIndex = make(map[string]int)
		for i, metric := range c.entries {
			c.metricIndex[metric.ID] = i
		}
	}
}

// updateStats updates metrics collector statistics
func (c *MetricsCollector) updateStats(entry MetricEntry) {
	c.stats.TotalMetrics++
	c.stats.ActiveMetrics = int64(len(c.entries))
	c.stats.MetricsByType[entry.Type]++
	c.stats.MetricsBySource[entry.Source]++
}

// logMetric logs a metric
func (c *MetricsCollector) logMetric(entry MetricEntry) {
	c.logger.Debug("Recorded metric",
		zap.String("metric_id", entry.ID),
		zap.String("name", entry.Name),
		zap.String("type", entry.Type),
		zap.Float64("value", entry.Value),
		zap.String("source", entry.Source))
}

// updateServiceMetrics updates service metrics
func (c *MetricsCollector) updateServiceMetrics(entry MetricEntry) {
	if c.metrics == nil {
		return
	}

	switch entry.Type {
	case "counter":
		c.metrics.RequestsTotal.Add(entry.Value, "ingestion", entry.Name, "success")
	case "gauge":
		c.metrics.QueueSize.Set(entry.Value, "ingestion")
	case "histogram":
		c.metrics.RequestsDuration.Observe(entry.Value, "ingestion", entry.Name)
	}
}

// runAggregation runs the aggregation process
func (c *MetricsCollector) runAggregation() {
	ticker := time.NewTicker(c.config.AggregationWindow)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.config.EnableAggregation {
				c.aggregateMetrics()
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// aggregateMetrics aggregates metrics
func (c *MetricsCollector) aggregateMetrics() {
	c.aggMu.Lock()
	defer c.aggMu.Unlock()

	now := time.Now()

	for name, aggregator := range c.aggregators {
		// Get metrics within the aggregation window
		windowStart := now.Add(-aggregator.window)
		windowMetrics := make([]MetricEntry, 0)

		c.mu.RLock()
		for _, metric := range c.entries {
			if metric.Timestamp.After(windowStart) {
				windowMetrics = append(windowMetrics, metric)
			}
		}
		c.mu.RUnlock()

		// Apply aggregation functions
		for _, function := range aggregator.functions {
			aggregated := c.applyAggregation(windowMetrics, function)
			if aggregated != nil {
				// Record aggregated metric
				aggEntry := MetricEntry{
					ID:        generateMetricID(),
					Name:      fmt.Sprintf("%s_%s", name, function),
					Type:      "gauge",
					Value:     aggregated.Value,
					Labels:    aggregated.Labels,
					Timestamp: now,
					Source:    "aggregation",
					Metadata: map[string]interface{}{
						"aggregator": name,
						"function":   function,
						"window":     aggregator.window,
					},
				}

				c.mu.Lock()
				c.addMetricEntry(aggEntry)
				c.stats.AggregatedMetrics++
				c.mu.Unlock()
			}
		}

		aggregator.lastUpdate = now
	}
}

// applyAggregation applies an aggregation function to metrics
func (c *MetricsCollector) applyAggregation(metrics []MetricEntry, function string) *AggregatedMetric {
	if len(metrics) == 0 {
		return nil
	}

	// Group metrics by name and labels
	groups := make(map[string][]MetricEntry)
	for _, metric := range metrics {
		key := metric.Name + "|" + mapToString(metric.Labels)
		groups[key] = append(groups[key], metric)
	}

	// Apply aggregation function to each group
	var results []AggregatedMetric
	for _, group := range groups {
		var value float64
		switch function {
		case "sum":
			for _, metric := range group {
				value += metric.Value
			}
		case "avg":
			for _, metric := range group {
				value += metric.Value
			}
			value /= float64(len(group))
		case "max":
			value = group[0].Value
			for _, metric := range group {
				if metric.Value > value {
					value = metric.Value
				}
			}
		case "min":
			value = group[0].Value
			for _, metric := range group {
				if metric.Value < value {
					value = metric.Value
				}
			}
		case "p50":
			value = calculatePercentile(group, 0.5)
		case "p95":
			value = calculatePercentile(group, 0.95)
		case "p99":
			value = calculatePercentile(group, 0.99)
		default:
			continue
		}

		results = append(results, AggregatedMetric{
			Name:     group[0].Name,
			Type:     group[0].Type,
			Function: function,
			Value:    value,
			Labels:   group[0].Labels,
		})
	}

	if len(results) == 0 {
		return nil
	}

	// Return the first result (in a real implementation, you might want to handle multiple results)
	return &results[0]
}

// runExport runs the export process
func (c *MetricsCollector) runExport() {
	ticker := time.NewTicker(c.config.ExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.config.EnableExport {
				c.exportMetrics()
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// exportMetrics exports metrics
func (c *MetricsCollector) exportMetrics() {
	c.exportMu.Lock()
	defer c.exportMu.Unlock()

	now := time.Now()

	for name, exporter := range c.exporters {
		// Check if it's time to export
		if now.Sub(exporter.lastExport) < exporter.config.Interval {
			continue
		}

		// Get metrics to export
		c.mu.RLock()
		metricsToExport := make([]MetricEntry, len(c.entries))
		copy(metricsToExport, c.entries)
		c.mu.RUnlock()

		// Export metrics
		if err := c.doExport(exporter, metricsToExport); err != nil {
			c.logger.Error("Failed to export metrics",
				zap.String("exporter", name),
				zap.Error(err))
			c.stats.FailedExports++
		} else {
			c.stats.ExportedMetrics += int64(len(metricsToExport))
		}

		exporter.lastExport = now
	}
}

// doExport exports metrics using a specific exporter
func (c *MetricsCollector) doExport(exporter *Exporter, metrics []MetricEntry) error {
	// This is a placeholder for export logic
	// In a real implementation, you would export to various systems
	// like Prometheus, InfluxDB, Graphite, etc.

	switch exporter.config.Type {
	case "log":
		c.logger.Info("Exporting metrics")
		return nil
	case "prometheus":
		// Placeholder for Prometheus export
		c.logger.Debug("Exporting to Prometheus")
		return nil
	default:
		return fmt.Errorf("unknown exporter type: %s", exporter.config.Type)
	}
}

// GetMetrics returns all metrics
func (c *MetricsCollector) GetMetrics() []MetricEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metricsCopy := make([]MetricEntry, len(c.entries))
	copy(metricsCopy, c.entries)
	return metricsCopy
}

// GetMetric returns a metric by ID
func (c *MetricsCollector) GetMetric(id string) (*MetricEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	index, exists := c.metricIndex[id]
	if !exists {
		return nil, ErrMetricNotFound
	}

	entry := c.entries[index]
	return &entry, nil
}

// GetStats returns metrics collector statistics
func (c *MetricsCollector) GetStats() *MetricsCollectorStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Update uptime
	c.stats.Uptime = time.Since(c.stats.StartTime)

	// Return a copy of stats
	stats := *c.stats
	stats.MetricsByType = make(map[string]int64)
	stats.MetricsBySource = make(map[string]int64)

	for k, v := range c.stats.MetricsByType {
		stats.MetricsByType[k] = v
	}

	for k, v := range c.stats.MetricsBySource {
		stats.MetricsBySource[k] = v
	}

	return &stats
}

// ResetStats resets metrics collector statistics
func (c *MetricsCollector) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats = &MetricsCollectorStats{
		MetricsByType:   make(map[string]int64),
		MetricsBySource: make(map[string]int64),
		StartTime:       time.Now(),
	}

	c.logger.Info("Metrics collector statistics reset")
}

// GetConfig returns the metrics collector configuration
func (c *MetricsCollector) GetConfig() *MetricsConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy of config
	config := *c.config
	return &config
}

// UpdateConfig updates the metrics collector configuration
func (c *MetricsCollector) UpdateConfig(config *MetricsConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate new configuration
	if err := validateMetricsConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	c.config = config

	// Re-initialize aggregators and exporters
	if config.EnableAggregation {
		c.initializeAggregators()
	}

	if config.EnableExport {
		c.initializeExporters()
	}

	c.logger.Info("Updated metrics collector configuration")

	return nil
}

// Validate validates the metrics collector state
func (c *MetricsCollector) Validate() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Validate configuration
	if err := validateMetricsConfig(c.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	return nil
}

// generateMetricID generates a unique metric ID
func generateMetricID() string {
	return fmt.Sprintf("metric_%d", time.Now().UnixNano())
}

// mapToString converts a map to a string
func mapToString(m map[string]string) string {
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, ",")
}

// calculatePercentile calculates a percentile from metrics
func calculatePercentile(metrics []MetricEntry, percentile float64) float64 {
	if len(metrics) == 0 {
		return 0
	}

	// Extract values
	values := make([]float64, len(metrics))
	for i, metric := range metrics {
		values[i] = metric.Value
	}

	// Sort values
	sort.Float64s(values)

	// Calculate percentile index
	index := int(float64(len(values)-1) * percentile)

	return values[index]
}

// getErrorTypeFromStatus returns error type from HTTP status code
func getErrorTypeFromStatus(status int) string {
	switch {
	case status >= 400 && status < 500:
		return "client_error"
	case status >= 500:
		return "server_error"
	default:
		return "success"
	}
}
