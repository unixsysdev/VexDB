package testutil

import (
	"context"
	"errors"
	"sync"
	"time"

	"vexdb/internal/types"
)

// MockSearchService implements a mock search service for testing
type MockSearchService struct {
	mu sync.RWMutex
	
	// Configuration
	ShouldFail      bool
	FailureError    error
	SearchDelay     time.Duration
	
	// Search results to return
	SearchResults   []*types.SearchResult
	
	// Call tracking
	SearchCalls     []SearchCall
	MultiClusterCalls []MultiClusterCall
	HealthCheckCalls []HealthCheckCall
	GetMetricsCalls []GetMetricsCall
	GetConfigCalls  []GetConfigCall
	UpdateConfigCalls []UpdateConfigCall
}

// SearchCall represents a search call
type SearchCall struct {
	Ctx            context.Context
	Query          *types.Vector
	K              int
	MetadataFilter map[string]string
}

// MultiClusterCall represents a multi-cluster search call
type MultiClusterCall struct {
	Ctx            context.Context
	Query          *types.Vector
	K              int
	ClusterIDs     []string
	MetadataFilter map[string]string
}

// HealthCheckCall represents a health check call
type HealthCheckCall struct {
	Ctx      context.Context
	Detailed bool
}

// GetMetricsCall represents a get metrics call
type GetMetricsCall struct {
	Ctx        context.Context
	MetricType string
	ClusterID  string
	NodeID     string
}

// GetConfigCall represents a get config call
type GetConfigCall struct {
	Ctx context.Context
}

// UpdateConfigCall represents an update config call
type UpdateConfigCall struct {
	Ctx     context.Context
	Updates map[string]string
}

// NewMockSearchService creates a new mock search service
func NewMockSearchService() *MockSearchService {
	return &MockSearchService{
		SearchResults: make([]*types.SearchResult, 0),
		SearchCalls: make([]SearchCall, 0),
		MultiClusterCalls: make([]MultiClusterCall, 0),
		HealthCheckCalls: make([]HealthCheckCall, 0),
		GetMetricsCalls: make([]GetMetricsCall, 0),
		GetConfigCalls: make([]GetConfigCall, 0),
		UpdateConfigCalls: make([]UpdateConfigCall, 0),
	}
}

// Search performs a similarity search
func (m *MockSearchService) Search(ctx context.Context, query *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := SearchCall{
		Ctx:            ctx,
		Query:          query,
		K:              k,
		MetadataFilter: filter,
	}
	m.SearchCalls = append(m.SearchCalls, call)
	
	// Simulate delay if configured
	if m.SearchDelay > 0 {
		time.Sleep(m.SearchDelay)
	}
	
	// Check if we should fail
	if m.ShouldFail {
		if m.FailureError != nil {
			return nil, m.FailureError
		}
		return nil, errors.New("mock search service failure")
	}
	
	// Return configured results
	results := make([]*types.SearchResult, len(m.SearchResults))
	copy(results, m.SearchResults)
	return results, nil
}

// MultiClusterSearch performs a search across multiple clusters
func (m *MockSearchService) MultiClusterSearch(ctx context.Context, query *types.Vector, k int, clusterIDs []string, filter map[string]string) ([]*types.SearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := MultiClusterCall{
		Ctx:            ctx,
		Query:          query,
		K:              k,
		ClusterIDs:     clusterIDs,
		MetadataFilter: filter,
	}
	m.MultiClusterCalls = append(m.MultiClusterCalls, call)
	
	// Simulate delay if configured
	if m.SearchDelay > 0 {
		time.Sleep(m.SearchDelay)
	}
	
	// Check if we should fail
	if m.ShouldFail {
		if m.FailureError != nil {
			return nil, m.FailureError
		}
		return nil, errors.New("mock multi-cluster search service failure")
	}
	
	// Return configured results
	results := make([]*types.SearchResult, len(m.SearchResults))
	copy(results, m.SearchResults)
	return results, nil
}

// GetClusterInfo returns cluster information
func (m *MockSearchService) GetClusterInfo(ctx context.Context, clusterID string) (*types.ClusterInfo, error) {
	if m.ShouldFail {
		return nil, errors.New("mock get cluster info failure")
	}
	
	return &types.ClusterInfo{
		ID:        clusterID,
		Name:      "test-cluster",
		Config:    types.ClusterConfig{},
		Metadata:  map[string]string{"region": "us-east-1"},
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}, nil
}

// GetClusterStatus returns cluster status
func (m *MockSearchService) GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error) {
	if m.ShouldFail {
		return nil, errors.New("mock get cluster status failure")
	}
	
	return &types.ClusterStatus{
		ID:        "test-cluster-status",
		Status:    "active",
		Health:    "healthy",
		Message:   "Cluster is healthy",
		Timestamp: time.Now().Unix(),
		Nodes:     []string{"node1", "node2"},
		Metrics:   map[string]interface{}{"vector_count": 1000},
	}, nil
}

// HealthCheck performs a health check
func (m *MockSearchService) HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := HealthCheckCall{
		Ctx:      ctx,
		Detailed: detailed,
	}
	m.HealthCheckCalls = append(m.HealthCheckCalls, call)
	
	if m.ShouldFail {
		return nil, errors.New("mock health check failure")
	}
	
	return &types.HealthStatus{
		Healthy: true,
		Status:  "healthy",
		Message: "Service is healthy",
		Details: map[string]string{
			"uptime": "1h30m",
			"version": "1.0.0",
		},
	}, nil
}

// GetMetrics returns metrics
func (m *MockSearchService) GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := GetMetricsCall{
		Ctx:        ctx,
		MetricType: metricType,
		ClusterID:  clusterID,
		NodeID:     nodeID,
	}
	m.GetMetricsCalls = append(m.GetMetricsCalls, call)
	
	if m.ShouldFail {
		return nil, errors.New("mock get metrics failure")
	}
	
	return map[string]float64{
		"requests_total": 1000,
		"avg_latency":    0.05,
		"error_rate":     0.01,
	}, nil
}

// GetConfig returns configuration
func (m *MockSearchService) GetConfig(ctx context.Context) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := GetConfigCall{
		Ctx: ctx,
	}
	m.GetConfigCalls = append(m.GetConfigCalls, call)
	
	if m.ShouldFail {
		return nil, errors.New("mock get config failure")
	}
	
	return map[string]string{
		"search.engine.max_results": "1000",
		"search.engine.default_k":   "10",
		"api.http.enabled":          "true",
	}, nil
}

// UpdateConfig updates configuration
func (m *MockSearchService) UpdateConfig(ctx context.Context, updates map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := UpdateConfigCall{
		Ctx:     ctx,
		Updates: updates,
	}
	m.UpdateConfigCalls = append(m.UpdateConfigCalls, call)
	
	if m.ShouldFail {
		return errors.New("mock update config failure")
	}
	
	return nil
}

// SetSearchResults sets the search results to return
func (m *MockSearchService) SetSearchResults(results []*types.SearchResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SearchResults = results
}

// SetShouldFail configures whether the service should fail
func (m *MockSearchService) SetShouldFail(shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ShouldFail = shouldFail
}

// SetFailureError sets a specific error to return when failing
func (m *MockSearchService) SetFailureError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailureError = err
}

// SetSearchDelay sets a delay for search operations
func (m *MockSearchService) SetSearchDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SearchDelay = delay
}

// GetSearchCallCount returns the number of search calls made
func (m *MockSearchService) GetSearchCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.SearchCalls)
}

// GetLastSearchCall returns the last search call made
func (m *MockSearchService) GetLastSearchCall() *SearchCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.SearchCalls) == 0 {
		return nil
	}
	return &m.SearchCalls[len(m.SearchCalls)-1]
}

// GetMultiClusterCallCount returns the number of multi-cluster search calls made
func (m *MockSearchService) GetMultiClusterCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.MultiClusterCalls)
}

// GetLastMultiClusterCall returns the last multi-cluster search call made
func (m *MockSearchService) GetLastMultiClusterCall() *MultiClusterCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.MultiClusterCalls) == 0 {
		return nil
	}
	return &m.MultiClusterCalls[len(m.MultiClusterCalls)-1]
}

// GetHealthCheckCallCount returns the number of health check calls made
func (m *MockSearchService) GetHealthCheckCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.HealthCheckCalls)
}

// GetLastHealthCheckCall returns the last health check call made
func (m *MockSearchService) GetLastHealthCheckCall() *HealthCheckCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.HealthCheckCalls) == 0 {
		return nil
	}
	return &m.HealthCheckCalls[len(m.HealthCheckCalls)-1]
}

// Reset clears all call tracking
func (m *MockSearchService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.SearchCalls = make([]SearchCall, 0)
	m.MultiClusterCalls = make([]MultiClusterCall, 0)
	m.HealthCheckCalls = make([]HealthCheckCall, 0)
	m.GetMetricsCalls = make([]GetMetricsCall, 0)
	m.GetConfigCalls = make([]GetConfigCall, 0)
	m.UpdateConfigCalls = make([]UpdateConfigCall, 0)
}

// MockVectorGenerator generates test vectors
type MockVectorGenerator struct {
	dimension int
	seed      int64
}

// NewMockVectorGenerator creates a new mock vector generator
func NewMockVectorGenerator(dimension int) *MockVectorGenerator {
	return &MockVectorGenerator{
		dimension: dimension,
		seed:      12345, // Fixed seed for reproducible results
	}
}

// GenerateVector generates a test vector
func (g *MockVectorGenerator) GenerateVector(id string) *types.Vector {
	data := make([]float32, g.dimension)
	for i := 0; i < g.dimension; i++ {
		// Simple deterministic generation
		data[i] = float32((i + int(g.seed)) % 100) / 100.0
	}
	
	return &types.Vector{
		ID:       id,
		Data:     data,
		Metadata: map[string]interface{}{
			"source": "test",
			"dimension": g.dimension,
		},
		ClusterID: uint32(g.seed % 10),
	}
}

// GenerateSearchResults generates mock search results
func (g *MockVectorGenerator) GenerateSearchResults(query *types.Vector, k int) []*types.SearchResult {
	results := make([]*types.SearchResult, k)
	
	for i := 0; i < k; i++ {
		vectorID := fmt.Sprintf("result_%d", i+1)
		vector := g.GenerateVector(vectorID)
		
		// Calculate a simple distance (not mathematically correct, just for testing)
		distance := float64(i) * 0.1
		
		results[i] = &types.SearchResult{
			Vector:   vector,
			Distance: distance,
		}
	}
	
	return results
}

// MockMetricsCollector implements a mock metrics collector
type MockMetricsCollector struct {
	mu sync.RWMutex
	
	// Metrics storage
	metrics map[string]float64
	labels  map[string]map[string]string
	
	// Call tracking
	RecordHistogramCalls []RecordHistogramCall
	RecordCounterCalls   []RecordCounterCall
	RecordGaugeCalls     []RecordGaugeCall
}

// RecordHistogramCall represents a record histogram call
type RecordHistogramCall struct {
	Name   string
	Value  float64
	Labels map[string]string
}

// RecordCounterCall represents a record counter call
type RecordCounterCall struct {
	Name   string
	Labels map[string]string
}

// RecordGaugeCall represents a record gauge call
type RecordGaugeCall struct {
	Name   string
	Value  float64
	Labels map[string]string
}

// NewMockMetricsCollector creates a new mock metrics collector
func NewMockMetricsCollector() *MockMetricsCollector {
	return &MockMetricsCollector{
		metrics: make(map[string]float64),
		labels:  make(map[string]map[string]string),
		RecordHistogramCalls: make([]RecordHistogramCall, 0),
		RecordCounterCalls:   make([]RecordCounterCall, 0),
		RecordGaugeCalls:     make([]RecordGaugeCall, 0),
	}
}

// RecordHistogram records a histogram metric
func (m *MockMetricsCollector) RecordHistogram(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := RecordHistogramCall{
		Name:   name,
		Value:  value,
		Labels: labels,
	}
	m.RecordHistogramCalls = append(m.RecordHistogramCalls, call)
	
	// Store the metric
	key := m.buildMetricKey(name, labels)
	m.metrics[key] = value
	m.labels[key] = labels
}

// RecordCounter records a counter metric
func (m *MockMetricsCollector) RecordCounter(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := RecordCounterCall{
		Name:   name,
		Labels: labels,
	}
	m.RecordCounterCalls = append(m.RecordCounterCalls, call)
	
	// Increment the counter
	key := m.buildMetricKey(name, labels)
	m.metrics[key]++
	if m.labels[key] == nil {
		m.labels[key] = labels
	}
}

// RecordGauge records a gauge metric
func (m *MockMetricsCollector) RecordGauge(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	call := RecordGaugeCall{
		Name:   name,
		Value:  value,
		Labels: labels,
	}
	m.RecordGaugeCalls = append(m.RecordGaugeCalls, call)
	
	// Set the gauge value
	key := m.buildMetricKey(name, labels)
	m.metrics[key] = value
	m.labels[key] = labels
}

// GetMetric returns the value of a metric
func (m *MockMetricsCollector) GetMetric(name string, labels map[string]string) (float64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	key := m.buildMetricKey(name, labels)
	value, exists := m.metrics[key]
	return value, exists
}

// GetAllMetrics returns all recorded metrics
func (m *MockMetricsCollector) GetAllMetrics() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make(map[string]float64)
	for k, v := range m.metrics {
		result[k] = v
	}
	return result
}

// GetHistogramCallCount returns the number of histogram calls
func (m *MockMetricsCollector) GetHistogramCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.RecordHistogramCalls)
}

// GetCounterCallCount returns the number of counter calls
func (m *MockMetricsCollector) GetCounterCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.RecordCounterCalls)
}

// GetGaugeCallCount returns the number of gauge calls
func (m *MockMetricsCollector) GetGaugeCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.RecordGaugeCalls)
}

// Reset clears all metrics and call tracking
func (m *MockMetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.metrics = make(map[string]float64)
	m.labels = make(map[string]map[string]string)
	m.RecordHistogramCalls = make([]RecordHistogramCall, 0)
	m.RecordCounterCalls = make([]RecordCounterCall, 0)
	m.RecordGaugeCalls = make([]RecordGaugeCall, 0)
}

// buildMetricKey builds a unique key for a metric with labels
func (m *MockMetricsCollector) buildMetricKey(name string, labels map[string]string) string {
	key := name
	for k, v := range labels {
		key += "|" + k + "=" + v
	}
	return key
}

// MockLogger implements a mock logger for testing
type MockLogger struct {
	mu sync.RWMutex
	
	// Log storage
	DebugLogs []LogEntry
	InfoLogs  []LogEntry
	WarnLogs  []LogEntry
	ErrorLogs []LogEntry
	FatalLogs []LogEntry
}

// LogEntry represents a log entry
type LogEntry struct {
	Message string
	Fields  []interface{}
}

// NewMockLogger creates a new mock logger
func NewMockLogger() *MockLogger {
	return &MockLogger{
		DebugLogs: make([]LogEntry, 0),
		InfoLogs:  make([]LogEntry, 0),
		WarnLogs:  make([]LogEntry, 0),
		ErrorLogs: make([]LogEntry, 0),
		FatalLogs: make([]LogEntry, 0),
	}
}

// Debug logs a debug message
func (l *MockLogger) Debug(msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.DebugLogs = append(l.DebugLogs, LogEntry{Message: msg, Fields: fields})
}

// Info logs an info message
func (l *MockLogger) Info(msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.InfoLogs = append(l.InfoLogs, LogEntry{Message: msg, Fields: fields})
}

// Warn logs a warning message
func (l *MockLogger) Warn(msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.WarnLogs = append(l.WarnLogs, LogEntry{Message: msg, Fields: fields})
}

// Error logs an error message
func (l *MockLogger) Error(msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ErrorLogs = append(l.ErrorLogs, LogEntry{Message: msg, Fields: fields})
}

// Fatal logs a fatal message
func (l *MockLogger) Fatal(msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.FatalLogs = append(l.FatalLogs, LogEntry{Message: msg, Fields: fields})
}

// With returns a new logger with additional fields
func (l *MockLogger) With(fields ...interface{}) interface{} {
	return l // For simplicity, return the same logger
}

// WithContext returns a new logger with context
func (l *MockLogger) WithContext(ctx context.Context) interface{} {
	return l // For simplicity, return the same logger
}

// Measure logs a performance measurement
func (l *MockLogger) Measure(operation string, duration time.Duration, fields ...interface{}) {
	l.Info("performance measurement", append([]interface{}{"operation", operation, "duration", duration}, fields...)...)
}

// Structured logs a structured message
func (l *MockLogger) Structured(level string, msg string, fields map[string]interface{}) {
	l.Info(msg, fields)
}

// GetLogCount returns the total number of log entries
func (l *MockLogger) GetLogCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.DebugLogs) + len(l.InfoLogs) + len(l.WarnLogs) + len(l.ErrorLogs) + len(l.FatalLogs)
}

// GetErrorCount returns the number of error log entries
func (l *MockLogger) GetErrorCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.ErrorLogs)
}

// GetInfoCount returns the number of info log entries
func (l *MockLogger) GetInfoCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.InfoLogs)
}

// Reset clears all log entries
func (l *MockLogger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.DebugLogs = make([]LogEntry, 0)
	l.InfoLogs = make([]LogEntry, 0)
	l.WarnLogs = make([]LogEntry, 0)
	l.ErrorLogs = make([]LogEntry, 0)
	l.FatalLogs = make([]LogEntry, 0)
}