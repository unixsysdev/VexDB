package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	
	"vexdb/internal/config"
	"vexdb/internal/errors"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
)

var (
	ErrHealthCheckFailed    = errors.New(errors.ErrorCodeHealthCheckFailed, "health check failed")
	ErrServiceNotRegistered = errors.New(errors.ErrorCodeInvalidArgument, "service not registered")
	ErrInvalidCheckConfig   = errors.New(errors.ErrorCodeConfigInvalid, "invalid health check configuration")
	ErrCheckTimeout         = errors.New(errors.ErrorCodeHealthCheckTimeout, "health check timeout")
)

// HealthStatus represents the health status
type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusUnhealthy HealthStatus = "unhealthy"
	StatusDegraded  HealthStatus = "degraded"
	StatusUnknown   HealthStatus = "unknown"
)

// HealthCheck represents a health check function
type HealthCheck func(ctx context.Context) error

// HealthCheckConfig represents health check configuration
type HealthCheckConfig struct {
	Name        string        `yaml:"name" json:"name"`
	Enabled     bool          `yaml:"enabled" json:"enabled"`
	Interval    time.Duration `yaml:"interval" json:"interval"`
	Timeout     time.Duration `yaml:"timeout" json:"timeout"`
	InitialDelay time.Duration `yaml:"initial_delay" json:"initial_delay"`
	Critical    bool          `yaml:"critical" json:"critical"`
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Name      string        `json:"name"`
	Status    HealthStatus  `json:"status"`
	Message   string        `json:"message,omitempty"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
	Error     string        `json:"error,omitempty"`
	LastCheck time.Time     `json:"last_check"`
	Checks    int           `json:"checks"`
	Failures  int           `json:"failures"`
}

// HealthReport represents a comprehensive health report
type HealthReport struct {
	Status       HealthStatus         `json:"status"`
	Timestamp    time.Time            `json:"timestamp"`
	Version      string               `json:"version,omitempty"`
	Uptime       time.Duration        `json:"uptime"`
	Checks       map[string]HealthCheckResult `json:"checks"`
	Summary      HealthSummary        `json:"summary"`
}

// HealthSummary represents a summary of health checks
type HealthSummary struct {
	Total      int `json:"total"`
	Healthy    int `json:"healthy"`
	Unhealthy  int `json:"unhealthy"`
	Degraded   int `json:"degraded"`
	Unknown    int `json:"unknown"`
	Critical   int `json:"critical"`
}

// HealthConfig represents the health check service configuration
type HealthConfig struct {
	Enabled        bool                `yaml:"enabled" json:"enabled"`
	Port           int                 `yaml:"port" json:"port"`
	Path           string              `yaml:"path" json:"path"`
	ReadTimeout    time.Duration       `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration       `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout    time.Duration       `yaml:"idle_timeout" json:"idle_timeout"`
	Checks         []HealthCheckConfig `yaml:"checks" json:"checks"`
	EnableDetailed bool                `yaml:"enable_detailed" json:"enable_detailed"`
	EnableMetrics  bool                `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultHealthConfig returns the default health check configuration
func DefaultHealthConfig() *HealthConfig {
	return &HealthConfig{
		Enabled:        true,
		Port:           8080,
		Path:           "/health",
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    60 * time.Second,
		Checks: []HealthCheckConfig{
			{
				Name:        "storage",
				Enabled:     true,
				Interval:    30 * time.Second,
				Timeout:     10 * time.Second,
				InitialDelay: 5 * time.Second,
				Critical:    true,
			},
			{
				Name:        "search",
				Enabled:     true,
				Interval:    30 * time.Second,
				Timeout:     10 * time.Second,
				InitialDelay: 5 * time.Second,
				Critical:    true,
			},
			{
				Name:        "memory",
				Enabled:     true,
				Interval:    60 * time.Second,
				Timeout:     5 * time.Second,
				InitialDelay: 10 * time.Second,
				Critical:    false,
			},
			{
				Name:        "disk",
				Enabled:     true,
				Interval:    60 * time.Second,
				Timeout:     5 * time.Second,
				InitialDelay: 10 * time.Second,
				Critical:    false,
			},
		},
		EnableDetailed: true,
		EnableMetrics:  true,
	}
}

// HealthEndpoints represents health check endpoints
type HealthEndpoints struct {
	config     *HealthConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Health checks
	checks     map[string]HealthCheck
	results    map[string]HealthCheckResult
	mu         sync.RWMutex
	
	// HTTP server
	server     *http.Server
	startTime  time.Time
	
	// Lifecycle
	started    bool
	stopped    bool
	shutdown   chan struct{}
}

// NewHealthEndpoints creates new health check endpoints
func NewHealthEndpoints(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*HealthEndpoints, error) {
	healthConfig := DefaultHealthConfig()
	
	// For now, use default config since we don't have a generic Get method
	// TODO: Implement proper configuration extraction based on config type
	
	// Validate configuration
	if err := validateHealthConfig(healthConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCheckConfig, err)
	}
	
	endpoints := &HealthEndpoints{
		config:    healthConfig,
		logger:    logger,
		metrics:   metrics,
		checks:    make(map[string]HealthCheck),
		results:   make(map[string]HealthCheckResult),
		startTime: time.Now(),
		shutdown:  make(chan struct{}),
	}
	
	// Initialize HTTP server
	endpoints.initializeHTTPServer()
	
	endpoints.logger.Info("Created health endpoints",
		zap.Bool("enabled", healthConfig.Enabled),
		zap.Int("port", healthConfig.Port),
		zap.String("path", healthConfig.Path),
		zap.Int("checks_count", len(healthConfig.Checks)))
	
	return endpoints, nil
}

// validateHealthConfig validates the health configuration
func validateHealthConfig(cfg *HealthConfig) error {
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return errors.New(errors.ErrorCodeInvalidArgument, "port must be between 1 and 65535")
	}
	
	if cfg.Path == "" {
		return errors.New(errors.ErrorCodeInvalidArgument, "path cannot be empty")
	}
	
	if cfg.ReadTimeout <= 0 {
		return errors.New(errors.ErrorCodeInvalidArgument, "read timeout must be positive")
	}
	
	if cfg.WriteTimeout <= 0 {
		return errors.New(errors.ErrorCodeInvalidArgument, "write timeout must be positive")
	}
	
	if cfg.IdleTimeout <= 0 {
		return errors.New(errors.ErrorCodeInvalidArgument, "idle timeout must be positive")
	}
	
	for _, check := range cfg.Checks {
		if check.Name == "" {
			return errors.New(errors.ErrorCodeInvalidArgument, "check name cannot be empty")
		}
		if check.Interval <= 0 {
			return errors.New(errors.ErrorCodeInvalidArgument, "check interval must be positive")
		}
		if check.Timeout <= 0 {
			return errors.New(errors.ErrorCodeInvalidArgument, "check timeout must be positive")
		}
		if check.InitialDelay < 0 {
			return errors.New(errors.ErrorCodeInvalidArgument, "check initial delay must be non-negative")
		}
	}
	
	return nil
}

// initializeHTTPServer initializes the HTTP server
func (h *HealthEndpoints) initializeHTTPServer() {
	mux := http.NewServeMux()
	
	// Register health endpoints
	mux.HandleFunc(h.config.Path, h.handleHealth)
	mux.HandleFunc(h.config.Path+"/live", h.handleLiveness)
	mux.HandleFunc(h.config.Path+"/ready", h.handleReadiness)
	mux.HandleFunc(h.config.Path+"/detailed", h.handleDetailedHealth)
	
	h.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", h.config.Port),
		Handler:      mux,
		ReadTimeout:  h.config.ReadTimeout,
		WriteTimeout: h.config.WriteTimeout,
		IdleTimeout:  h.config.IdleTimeout,
	}
}

// Start starts the health endpoints
func (h *HealthEndpoints) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.started {
		return nil
	}
	
	// Start HTTP server
	go func() {
		h.logger.Info("Starting health endpoints server", zap.Int("port", h.config.Port))
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.logger.Error("Health endpoints server failed", zap.Error(err))
		}
	}()
	
	// Start health check runners
	for _, checkConfig := range h.config.Checks {
		if checkConfig.Enabled {
			go h.runHealthCheck(checkConfig)
		}
	}
	
	h.started = true
	h.stopped = false
	
	h.logger.Info("Started health endpoints")
	
	return nil
}

// Stop stops the health endpoints
func (h *HealthEndpoints) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.stopped {
		return nil
	}
	
	if !h.started {
		return nil
	}
	
	// Signal shutdown
	close(h.shutdown)
	
	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := h.server.Shutdown(ctx); err != nil {
		h.logger.Error("Failed to shutdown health endpoints server", zap.Error(err))
	}
	
	h.stopped = true
	h.started = false
	
	h.logger.Info("Stopped health endpoints")
	
	return nil
}

// IsRunning checks if the health endpoints are running
func (h *HealthEndpoints) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return h.started && !h.stopped
}

// RegisterCheck registers a health check
func (h *HealthEndpoints) RegisterCheck(name string, check HealthCheck) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if _, exists := h.checks[name]; exists {
		return fmt.Errorf("%w: %s", ErrServiceNotRegistered, name)
	}
	
	h.checks[name] = check
	
	// Initialize result
	h.results[name] = HealthCheckResult{
		Name:      name,
		Status:    StatusUnknown,
		Timestamp: time.Now(),
		LastCheck: time.Now(),
		Checks:    0,
		Failures:  0,
	}
	
	h.logger.Info("Registered health check", zap.String("name", name))
	
	return nil
}

// UnregisterCheck unregisters a health check
func (h *HealthEndpoints) UnregisterCheck(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if _, exists := h.checks[name]; !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotRegistered, name)
	}
	
	delete(h.checks, name)
	delete(h.results, name)
	
	h.logger.Info("Unregistered health check", zap.String("name", name))
	
	return nil
}

// GetCheckResult returns the result of a specific health check
func (h *HealthEndpoints) GetCheckResult(name string) (HealthCheckResult, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	result, exists := h.results[name]
	return result, exists
}

// GetHealthReport returns a comprehensive health report
func (h *HealthEndpoints) GetHealthReport() *HealthReport {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	report := &HealthReport{
		Timestamp: time.Now(),
		Version:   "1.0.0",
		Uptime:    time.Since(h.startTime),
		Checks:    make(map[string]HealthCheckResult),
	}
	
	// Copy results
	for name, result := range h.results {
		report.Checks[name] = result
	}
	
	// Calculate overall status and summary
	report.Status, report.Summary = h.calculateHealthStatus(report.Checks)
	
	return report
}

// IsHealthy checks if a specific service is healthy
func (h *HealthEndpoints) IsHealthy(service string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	result, exists := h.results[service]
	if !exists {
		return false
	}
	
	return result.Status == StatusHealthy
}

// GetConfig returns the health configuration
func (h *HealthEndpoints) GetConfig() *HealthConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Return a copy of config
	config := *h.config
	return &config
}

// UpdateConfig updates the health configuration
func (h *HealthEndpoints) UpdateConfig(config *HealthConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Validate new configuration
	if err := validateHealthConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCheckConfig, err)
	}
	
	h.config = config
	
	// Reinitialize HTTP server
	h.initializeHTTPServer()
	
	h.logger.Info("Updated health endpoints configuration", zap.Any("config", config))
	
	return nil
}

// Validate validates the health endpoints state
func (h *HealthEndpoints) Validate() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Validate configuration
	if err := validateHealthConfig(h.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// runHealthCheck runs a health check periodically
func (h *HealthEndpoints) runHealthCheck(config HealthCheckConfig) {
	// Wait for initial delay
	time.Sleep(config.InitialDelay)
	
	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			h.executeHealthCheck(config)
		case <-h.shutdown:
			return
		}
	}
}

// executeHealthCheck executes a single health check
func (h *HealthEndpoints) executeHealthCheck(config HealthCheckConfig) {
	h.mu.RLock()
	check, exists := h.checks[config.Name]
	h.mu.RUnlock()
	
	if !exists {
		return
	}
	
	start := time.Now()
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	
	// Execute health check
	err := check(ctx)
	duration := time.Since(start)
	
	// Update result
	h.mu.Lock()
	defer h.mu.Unlock()
	
	result, exists := h.results[config.Name]
	if !exists {
		result = HealthCheckResult{
			Name: config.Name,
		}
	}
	
	result.Duration = duration
	result.Timestamp = time.Now()
	result.LastCheck = time.Now()
	result.Checks++
	
	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err.Error()
		result.Failures++
		
		h.logger.Error("Health check failed",
			zap.String("name", config.Name),
			zap.Duration("duration", duration),
			zap.Error(err))
	} else {
		result.Status = StatusHealthy
		result.Error = ""
		
		h.logger.Debug("Health check passed",
			zap.String("name", config.Name),
			zap.Duration("duration", duration))
	}
	
	h.results[config.Name] = result
	
	// Update metrics if enabled
	if h.config.EnableMetrics && h.metrics != nil {
		if err != nil {
			h.metrics.ErrorsTotal.Inc("health", "check", "health_check_failed")
		} else {
			h.metrics.Latency.Observe(duration.Seconds(), "health", "check")
		}
	}
}

// calculateHealthStatus calculates overall health status
func (h *HealthEndpoints) calculateHealthStatus(checks map[string]HealthCheckResult) (HealthStatus, HealthSummary) {
	summary := HealthSummary{}
	
	for _, check := range checks {
		summary.Total++
		
		switch check.Status {
		case StatusHealthy:
			summary.Healthy++
		case StatusUnhealthy:
			summary.Unhealthy++
		case StatusDegraded:
			summary.Degraded++
		case StatusUnknown:
			summary.Unknown++
		}
		
		// Count critical failures
		if check.Status == StatusUnhealthy {
			// Check if this is a critical service
			for _, config := range h.config.Checks {
				if config.Name == check.Name && config.Critical {
					summary.Critical++
					break
				}
			}
		}
	}
	
	// Determine overall status
	var status HealthStatus
	if summary.Critical > 0 {
		status = StatusUnhealthy
	} else if summary.Unhealthy > 0 {
		status = StatusDegraded
	} else if summary.Healthy == summary.Total {
		status = StatusHealthy
	} else {
		status = StatusUnknown
	}
	
	return status, summary
}

// HTTP handlers
func (h *HealthEndpoints) handleHealth(w http.ResponseWriter, r *http.Request) {
	report := h.GetHealthReport()
	
	status := http.StatusOK
	if report.Status == StatusUnhealthy {
		status = http.StatusServiceUnavailable
	} else if report.Status == StatusDegraded {
		status = http.StatusPartialContent
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": report.Status,
		"timestamp": report.Timestamp,
		"uptime": report.Uptime.String(),
	})
}

func (h *HealthEndpoints) handleLiveness(w http.ResponseWriter, r *http.Request) {
	// Simple liveness check - just return 200 if running
	if h.IsRunning() {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "alive",
		})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "dead",
		})
	}
}

func (h *HealthEndpoints) handleReadiness(w http.ResponseWriter, r *http.Request) {
	report := h.GetHealthReport()
	
	status := http.StatusOK
	if report.Status == StatusUnhealthy {
		status = http.StatusServiceUnavailable
	} else if report.Status == StatusDegraded {
		status = http.StatusPartialContent
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": report.Status,
		"ready": report.Status == StatusHealthy,
		"timestamp": report.Timestamp,
	})
}

func (h *HealthEndpoints) handleDetailedHealth(w http.ResponseWriter, r *http.Request) {
	if !h.config.EnableDetailed {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	
	report := h.GetHealthReport()
	
	status := http.StatusOK
	if report.Status == StatusUnhealthy {
		status = http.StatusServiceUnavailable
	} else if report.Status == StatusDegraded {
		status = http.StatusPartialContent
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	
	json.NewEncoder(w).Encode(report)
}

// Helper functions
func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := m[key].(bool); ok {
		return val
	}
	return defaultValue
}

func getDuration(m map[string]interface{}, key string, defaultValue time.Duration) time.Duration {
	if val, ok := m[key].(string); ok {
		if duration, err := time.ParseDuration(val); err == nil {
			return duration
		}
	}
	return defaultValue
}