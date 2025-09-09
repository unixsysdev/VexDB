package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vexdb/vexdb/internal/config"
	"github.com/vexdb/vexdb/internal/logging"
	"github.com/vexdb/vexdb/internal/metrics"
)

var (
	ErrShutdownTimeout    = errors.New("shutdown timeout")
	ErrShutdownFailed     = errors.New("shutdown failed")
	ErrInvalidShutdownConfig = errors.New("invalid shutdown configuration")
	ErrHandlerNotRunning  = errors.New("shutdown handler not running")
)

// ShutdownConfig represents the shutdown handler configuration
type ShutdownConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	Timeout           time.Duration `yaml:"timeout" json:"timeout"`
	GracePeriod       time.Duration `yaml:"grace_period" json:"grace_period"`
	ForceKillDelay    time.Duration `yaml:"force_kill_delay" json:"force_kill_delay"`
	EnableSignals     bool          `yaml:"enable_signals" json:"enable_signals"`
	Signals           []string      `yaml:"signals" json:"signals"`
	EnableHealthCheck bool          `yaml:"enable_health_check" json:"enable_health_check"`
	HealthCheckDelay  time.Duration `yaml:"health_check_delay" json:"health_check_delay"`
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultShutdownConfig returns the default shutdown configuration
func DefaultShutdownConfig() *ShutdownConfig {
	return &ShutdownConfig{
		Enabled:           true,
		Timeout:           30 * time.Second,
		GracePeriod:       10 * time.Second,
		ForceKillDelay:    5 * time.Second,
		EnableSignals:     true,
		Signals:           []string{"SIGINT", "SIGTERM", "SIGQUIT"},
		EnableHealthCheck: true,
		HealthCheckDelay:  2 * time.Second,
		EnableMetrics:     true,
	}
}

// ShutdownHook represents a shutdown hook function
type ShutdownHook func(ctx context.Context) error

// ShutdownHookConfig represents a shutdown hook configuration
type ShutdownHookConfig struct {
	Name        string        `yaml:"name" json:"name"`
	Order       int           `yaml:"order" json:"order"`
	Timeout     time.Duration `yaml:"timeout" json:"timeout"`
	Critical    bool          `yaml:"critical" json:"critical"`
	RetryCount  int           `yaml:"retry_count" json:"retry_count"`
	RetryDelay  time.Duration `yaml:"retry_delay" json:"retry_delay"`
}

// ShutdownResult represents the result of a shutdown operation
type ShutdownResult struct {
	HookName    string        `json:"hook_name"`
	Success     bool          `json:"success"`
	Duration    time.Duration `json:"duration"`
	Error       string        `json:"error,omitempty"`
	Retries     int           `json:"retries"`
	TimedOut    bool          `json:"timed_out"`
}

// ShutdownReport represents a comprehensive shutdown report
type ShutdownReport struct {
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Duration    time.Duration     `json:"duration"`
	Timeout     bool              `json:"timeout"`
	Forced      bool              `json:"forced"`
	Results     []ShutdownResult  `json:"results"`
	Summary     ShutdownSummary   `json:"summary"`
	Signal      string            `json:"signal,omitempty"`
}

// ShutdownSummary represents a summary of shutdown operations
type ShutdownSummary struct {
	TotalHooks   int `json:"total_hooks"`
	Successful   int `json:"successful"`
	Failed       int `json:"failed"`
	TimedOut     int `json:"timed_out"`
	Critical     int `json:"critical"`
}

// ShutdownHandler represents a shutdown handler
type ShutdownHandler struct {
	config     *ShutdownConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Shutdown hooks
	hooks      map[string]ShutdownHook
	hookConfigs map[string]ShutdownHookConfig
	mu         sync.RWMutex
	
	// Shutdown state
	started    bool
	stopped    bool
	shutdown   chan struct{}
	shutdownCtx context.Context
	cancelCtx  context.CancelFunc
	
	// Signal handling
	signalChan chan os.Signal
	
	// Shutdown tracking
	startTime  time.Time
	results    []ShutdownResult
}

// NewShutdownHandler creates a new shutdown handler
func NewShutdownHandler(cfg *config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*ShutdownHandler, error) {
	shutdownConfig := DefaultShutdownConfig()
	
	if cfg != nil {
		if shutdownCfg, ok := cfg.Get("shutdown"); ok {
			if cfgMap, ok := shutdownCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					shutdownConfig.Enabled = enabled
				}
				if timeout, ok := cfgMap["timeout"].(string); ok {
					if duration, err := time.ParseDuration(timeout); err == nil {
						shutdownConfig.Timeout = duration
					}
				}
				if gracePeriod, ok := cfgMap["grace_period"].(string); ok {
					if duration, err := time.ParseDuration(gracePeriod); err == nil {
						shutdownConfig.GracePeriod = duration
					}
				}
				if forceKillDelay, ok := cfgMap["force_kill_delay"].(string); ok {
					if duration, err := time.ParseDuration(forceKillDelay); err == nil {
						shutdownConfig.ForceKillDelay = duration
					}
				}
				if enableSignals, ok := cfgMap["enable_signals"].(bool); ok {
					shutdownConfig.EnableSignals = enableSignals
				}
				if signals, ok := cfgMap["signals"].([]interface{}); ok {
					shutdownConfig.Signals = make([]string, len(signals))
					for i, signal := range signals {
						if str, ok := signal.(string); ok {
							shutdownConfig.Signals[i] = str
						}
					}
				}
				if enableHealthCheck, ok := cfgMap["enable_health_check"].(bool); ok {
					shutdownConfig.EnableHealthCheck = enableHealthCheck
				}
				if healthCheckDelay, ok := cfgMap["health_check_delay"].(string); ok {
					if duration, err := time.ParseDuration(healthCheckDelay); err == nil {
						shutdownConfig.HealthCheckDelay = duration
					}
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					shutdownConfig.EnableMetrics = enableMetrics
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateShutdownConfig(shutdownConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidShutdownConfig, err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	handler := &ShutdownHandler{
		config:       shutdownConfig,
		logger:       logger,
		metrics:      metrics,
		hooks:        make(map[string]ShutdownHook),
		hookConfigs:  make(map[string]ShutdownHookConfig),
		shutdown:     make(chan struct{}),
		shutdownCtx:  ctx,
		cancelCtx:    cancel,
		signalChan:   make(chan os.Signal, 1),
		results:      make([]ShutdownResult, 0),
	}
	
	handler.logger.Info("Created shutdown handler",
		"enabled", shutdownConfig.Enabled,
		"timeout", shutdownConfig.Timeout,
		"grace_period", shutdownConfig.GracePeriod,
		"enable_signals", shutdownConfig.EnableSignals,
		"signals", shutdownConfig.Signals)
	
	return handler, nil
}

// validateShutdownConfig validates the shutdown configuration
func validateShutdownConfig(cfg *ShutdownConfig) error {
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	
	if cfg.GracePeriod <= 0 {
		return errors.New("grace period must be positive")
	}
	
	if cfg.ForceKillDelay <= 0 {
		return errors.New("force kill delay must be positive")
	}
	
	if cfg.GracePeriod >= cfg.Timeout {
		return errors.New("grace period must be less than timeout")
	}
	
	if cfg.HealthCheckDelay <= 0 {
		return errors.New("health check delay must be positive")
	}
	
	for _, signal := range cfg.Signals {
		if signal == "" {
			return errors.New("signal cannot be empty")
		}
	}
	
	return nil
}

// Start starts the shutdown handler
func (h *ShutdownHandler) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.started {
		return nil
	}
	
	if !h.config.Enabled {
		h.logger.Info("Shutdown handler is disabled")
		return nil
	}
	
	// Start signal handling
	if h.config.EnableSignals {
		h.startSignalHandling()
	}
	
	h.started = true
	h.stopped = false
	
	h.logger.Info("Started shutdown handler")
	
	return nil
}

// Stop stops the shutdown handler
func (h *ShutdownHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.stopped {
		return nil
	}
	
	if !h.started {
		return ErrHandlerNotRunning
	}
	
	// Stop signal handling
	if h.config.EnableSignals {
		h.stopSignalHandling()
	}
	
	h.stopped = true
	h.started = false
	
	h.logger.Info("Stopped shutdown handler")
	
	return nil
}

// IsRunning checks if the shutdown handler is running
func (h *ShutdownHandler) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return h.started && !h.stopped
}

// RegisterHook registers a shutdown hook
func (h *ShutdownHandler) RegisterHook(name string, hook ShutdownHook, config ShutdownHookConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if _, exists := h.hooks[name]; exists {
		return fmt.Errorf("hook %s is already registered", name)
	}
	
	// Validate hook config
	if config.Name == "" {
		config.Name = name
	}
	if config.Timeout <= 0 {
		config.Timeout = h.config.GracePeriod
	}
	if config.RetryCount < 0 {
		config.RetryCount = 0
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 1 * time.Second
	}
	
	h.hooks[name] = hook
	h.hookConfigs[name] = config
	
	h.logger.Info("Registered shutdown hook",
		"name", name,
		"order", config.Order,
		"timeout", config.Timeout,
		"critical", config.Critical,
		"retry_count", config.RetryCount)
	
	return nil
}

// UnregisterHook unregisters a shutdown hook
func (h *ShutdownHandler) UnregisterHook(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if _, exists := h.hooks[name]; !exists {
		return fmt.Errorf("hook %s is not registered", name)
	}
	
	delete(h.hooks, name)
	delete(h.hookConfigs, name)
	
	h.logger.Info("Unregistered shutdown hook", "name", name)
	
	return nil
}

// GetRegisteredHooks returns registered shutdown hooks
func (h *ShutdownHandler) GetRegisteredHooks() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	hooks := make([]string, 0, len(h.hooks))
	for name := range h.hooks {
		hooks = append(hooks, name)
	}
	
	return hooks
}

// GetConfig returns the shutdown configuration
func (h *ShutdownHandler) GetConfig() *ShutdownConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Return a copy of config
	config := *h.config
	return &config
}

// UpdateConfig updates the shutdown configuration
func (h *ShutdownHandler) UpdateConfig(config *ShutdownConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Validate new configuration
	if err := validateShutdownConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidShutdownConfig, err)
	}
	
	h.config = config
	
	h.logger.Info("Updated shutdown handler configuration", "config", config)
	
	return nil
}

// Shutdown initiates a graceful shutdown
func (h *ShutdownHandler) Shutdown() error {
	return h.ShutdownWithSignal("")
}

// ShutdownWithSignal initiates a graceful shutdown with a specific signal
func (h *ShutdownHandler) ShutdownWithSignal(signal string) error {
	h.mu.Lock()
	
	if h.stopped {
		h.mu.Unlock()
		return ErrHandlerNotRunning
	}
	
	if !h.started {
		h.mu.Unlock()
		return ErrHandlerNotRunning
	}
	
	// Check if shutdown is already in progress
	select {
	case <-h.shutdown:
		h.mu.Unlock()
		return errors.New("shutdown already in progress")
	default:
	}
	
	h.startTime = time.Now()
	h.results = make([]ShutdownResult, 0)
	
	// Cancel context to signal shutdown
	h.cancelCtx()
	
	h.mu.Unlock()
	
	h.logger.Info("Initiating graceful shutdown", "signal", signal)
	
	// Execute shutdown hooks
	if err := h.executeShutdownHooks(); err != nil {
		h.logger.Error("Shutdown failed", "error", err)
		return fmt.Errorf("%w: %v", ErrShutdownFailed, err)
	}
	
	h.logger.Info("Graceful shutdown completed")
	
	return nil
}

// ForceShutdown forces an immediate shutdown
func (h *ShutdownHandler) ForceShutdown() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.stopped {
		return nil
	}
	
	if !h.started {
		return ErrHandlerNotRunning
	}
	
	h.logger.Info("Forcing immediate shutdown")
	
	// Cancel context
	h.cancelCtx()
	
	// Close shutdown channel
	close(h.shutdown)
	
	h.stopped = true
	h.started = false
	
	return nil
}

// GetShutdownReport returns the shutdown report
func (h *ShutdownHandler) GetShutdownReport() *ShutdownReport {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	if len(h.results) == 0 {
		return nil
	}
	
	report := &ShutdownReport{
		StartTime: h.startTime,
		EndTime:   time.Now(),
		Duration:  time.Since(h.startTime),
		Results:   make([]ShutdownResult, len(h.results)),
	}
	
	// Copy results
	copy(report.Results, h.results)
	
	// Calculate summary
	report.Summary = h.calculateShutdownSummary(report.Results)
	
	return report
}

// GetContext returns the shutdown context
func (h *ShutdownHandler) GetContext() context.Context {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return h.shutdownCtx
}

// IsShuttingDown checks if shutdown is in progress
func (h *ShutdownHandler) IsShuttingDown() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	select {
	case <-h.shutdown:
		return true
	default:
		return false
	}
}

// Validate validates the shutdown handler state
func (h *ShutdownHandler) Validate() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Validate configuration
	if err := validateShutdownConfig(h.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// startSignalHandling starts signal handling
func (h *ShutdownHandler) startSignalHandling() {
	// Convert signal strings to syscall signals
	signals := make([]os.Signal, len(h.config.Signals))
	for i, sigStr := range h.config.Signals {
		switch sigStr {
		case "SIGINT":
			signals[i] = syscall.SIGINT
		case "SIGTERM":
			signals[i] = syscall.SIGTERM
		case "SIGQUIT":
			signals[i] = syscall.SIGQUIT
		case "SIGHUP":
			signals[i] = syscall.SIGHUP
		default:
			h.logger.Warn("Unknown signal", "signal", sigStr)
			continue
		}
	}
	
	// Notify on signals
	signal.Notify(h.signalChan, signals...)
	
	// Start signal handler goroutine
	go h.handleSignals()
}

// stopSignalHandling stops signal handling
func (h *ShutdownHandler) stopSignalHandling() {
	signal.Stop(h.signalChan)
	close(h.signalChan)
}

// handleSignals handles incoming signals
func (h *ShutdownHandler) handleSignals() {
	for sig := range h.signalChan {
		h.logger.Info("Received shutdown signal", "signal", sig)
		
		// Convert signal to string
		sigStr := ""
		switch sig {
		case syscall.SIGINT:
			sigStr = "SIGINT"
		case syscall.SIGTERM:
			sigStr = "SIGTERM"
		case syscall.SIGQUIT:
			sigStr = "SIGQUIT"
		case syscall.SIGHUP:
			sigStr = "SIGHUP"
		}
		
		// Initiate shutdown
		if err := h.ShutdownWithSignal(sigStr); err != nil {
			h.logger.Error("Shutdown failed", "error", err)
		}
		
		break
	}
}

// executeShutdownHooks executes all shutdown hooks
func (h *ShutdownHandler) executeShutdownHooks() error {
	// Sort hooks by order
	sortedHooks := h.getSortedHooks()
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
	defer cancel()
	
	// Execute hooks in order
	for _, hookConfig := range sortedHooks {
		select {
		case <-ctx.Done():
			h.logger.Error("Shutdown timeout reached", "hook", hookConfig.Name)
			return ErrShutdownTimeout
		default:
		}
		
		result := h.executeShutdownHook(ctx, hookConfig)
		h.results = append(h.results, result)
		
		// Update metrics if enabled
		if h.config.EnableMetrics && h.metrics != nil {
			if result.Success {
				h.metrics.ServiceErrors.WithLabelValues("shutdown", "success").Inc()
			} else {
				h.metrics.ServiceErrors.WithLabelValues("shutdown", "failure").Inc()
			}
		}
		
		// If critical hook failed, stop shutdown
		if hookConfig.Critical && !result.Success {
			h.logger.Error("Critical shutdown hook failed", "hook", hookConfig.Name, "error", result.Error)
			return fmt.Errorf("critical hook %s failed: %s", hookConfig.Name, result.Error)
		}
	}
	
	// Close shutdown channel
	close(h.shutdown)
	
	return nil
}

// executeShutdownHook executes a single shutdown hook
func (h *ShutdownHandler) executeShutdownHook(ctx context.Context, config ShutdownHookConfig) ShutdownResult {
	result := ShutdownResult{
		HookName: config.Name,
		Success:  false,
		Duration: 0,
		Retries:  0,
		TimedOut: false,
	}
	
	hook, exists := h.hooks[config.Name]
	if !exists {
		result.Error = "hook not found"
		return result
	}
	
	start := time.Now()
	
	// Execute with retries
	for i := 0; i <= config.RetryCount; i++ {
		if i > 0 {
			result.Retries++
			
			// Wait before retry
			select {
			case <-time.After(config.RetryDelay):
			case <-ctx.Done():
				result.TimedOut = true
				result.Error = "timeout during retry delay"
				result.Duration = time.Since(start)
				return result
			}
		}
		
		// Create hook context with timeout
		hookCtx, cancel := context.WithTimeout(ctx, config.Timeout)
		
		// Execute hook
		err := hook(hookCtx)
		cancel()
		
		if err == nil {
			result.Success = true
			result.Duration = time.Since(start)
			return result
		}
		
		result.Error = err.Error()
		
		// Check if context was cancelled
		if ctx.Err() != nil {
			result.TimedOut = true
			result.Duration = time.Since(start)
			return result
		}
		
		h.logger.Warn("Shutdown hook failed, retrying",
			"hook", config.Name,
			"attempt", i+1,
			"max_attempts", config.RetryCount+1,
			"error", err)
	}
	
	result.Duration = time.Since(start)
	return result
}

// getSortedHooks returns hooks sorted by order
func (h *ShutdownHandler) getSortedHooks() []ShutdownHookConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Copy hook configs
	hooks := make([]ShutdownHookConfig, 0, len(h.hookConfigs))
	for _, config := range h.hookConfigs {
		hooks = append(hooks, config)
	}
	
	// Simple bubble sort by order
	for i := 0; i < len(hooks)-1; i++ {
		for j := i + 1; j < len(hooks); j++ {
			if hooks[i].Order > hooks[j].Order {
				hooks[i], hooks[j] = hooks[j], hooks[i]
			}
		}
	}
	
	return hooks
}

// calculateShutdownSummary calculates shutdown summary
func (h *ShutdownHandler) calculateShutdownSummary(results []ShutdownResult) ShutdownSummary {
	summary := ShutdownSummary{
		TotalHooks: len(results),
	}
	
	for _, result := range results {
		if result.Success {
			summary.Successful++
		} else {
			summary.Failed++
		}
		
		if result.TimedOut {
			summary.TimedOut++
		}
		
		// Count critical failures
		if config, exists := h.hookConfigs[result.HookName]; exists && config.Critical && !result.Success {
			summary.Critical++
		}
	}
	
	return summary
}