package backpressure

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

var (
	ErrBackpressureThresholdExceeded = errors.New("backpressure threshold exceeded")
	ErrInvalidBackpressureConfig     = errors.New("invalid backpressure configuration")
	ErrHandlerNotRunning             = errors.New("backpressure handler not running")
	ErrHandlerAlreadyRunning         = errors.New("backpressure handler already running")
	ErrInvalidThreshold              = errors.New("invalid backpressure threshold")
)

// BackpressureConfig represents the backpressure configuration
type BackpressureConfig struct {
	Enabled           bool                         `yaml:"enabled" json:"enabled"`
	Thresholds        adapter.BackpressureThresholds `yaml:"thresholds" json:"thresholds"`
	EnableMetrics     bool                         `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool                         `yaml:"enable_logging" json:"enable_logging"`
	EnableAdaptive    bool                         `yaml:"enable_adaptive" json:"enable_adaptive"`
	AdaptiveWindow    time.Duration                `yaml:"adaptive_window" json:"adaptive_window"`
	AdaptiveFactor    float64                      `yaml:"adaptive_factor" json:"adaptive_factor"`
	EnablePredictive  bool                         `yaml:"enable_predictive" json:"enable_predictive"`
	PredictionWindow  time.Duration                `yaml:"prediction_window" json:"prediction_window"`
	PredictionFactor  float64                      `yaml:"prediction_factor" json:"prediction_factor"`
	EnableGraceful    bool                         `yaml:"enable_graceful" json:"enable_graceful"`
	GracefulPeriod    time.Duration                `yaml:"graceful_period" json:"graceful_period"`
	EnableCircuit     bool                         `yaml:"enable_circuit" json:"enable_circuit"`
	CircuitThreshold  float64                      `yaml:"circuit_threshold" json:"circuit_threshold"`
	CircuitRecovery   float64                      `yaml:"circuit_recovery" json:"circuit_recovery"`
	SamplingInterval  time.Duration                `yaml:"sampling_interval" json:"sampling_interval"`
	MaxSamples        int                          `yaml:"max_samples" json:"max_samples"`
}

// DefaultBackpressureConfig returns the default backpressure configuration
func DefaultBackpressureConfig() *BackpressureConfig {
	return &BackpressureConfig{
		Enabled:          true,
		Thresholds: adapter.BackpressureThresholds{
			Warning:   0.7,  // 70%
			Critical:  0.85, // 85%
			Emergency: 0.95, // 95%
			Window:    time.Minute,
		},
		EnableMetrics:    true,
		EnableLogging:    true,
		EnableAdaptive:   true,
		AdaptiveWindow:   5 * time.Minute,
		AdaptiveFactor:   0.1,
		EnablePredictive: true,
		PredictionWindow: 10 * time.Minute,
		PredictionFactor: 0.2,
		EnableGraceful:   true,
		GracefulPeriod:   30 * time.Second,
		EnableCircuit:    true,
		CircuitThreshold: 0.9,
		CircuitRecovery:  0.5,
		SamplingInterval: time.Second,
		MaxSamples:       1000,
	}
}

// LoadSample represents a load sample
type LoadSample struct {
	Timestamp time.Time `json:"timestamp"`
	Load      float64   `json:"load"`
	Source    string    `json:"source"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// BackpressureHandler represents a backpressure handler
type BackpressureHandler struct {
	config     *BackpressureConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Load samples
	samples    []LoadSample
	mu         sync.RWMutex
	
	// Adaptive thresholds
	adaptiveThresholds adapter.BackpressureThresholds
	adaptiveMu        sync.RWMutex
	
	// Predictive load
	predictedLoad     float64
	predictionMu      sync.RWMutex
	
	// Circuit breaker
	circuitOpen       bool
	circuitMu         sync.RWMutex
	circuitLastTrip   time.Time
	
	// Graceful mode
	gracefulMode      bool
	gracefulMu        sync.RWMutex
	gracefulStartTime time.Time
	
	// Sampling
	samplingTicker    *time.Ticker
	samplingDone      chan struct{}
	
	// Lifecycle
	started    bool
	stopped    bool
	startTime  time.Time
	
	// Statistics
	stats      *BackpressureStats
}

// BackpressureStats represents backpressure statistics
type BackpressureStats struct {
	CurrentLoad      float64                     `json:"current_load"`
	AverageLoad      float64                     `json:"average_load"`
	MaxLoad          float64                     `json:"max_load"`
	MinLoad          float64                     `json:"min_load"`
	PressureEvents   int64                       `json:"pressure_events"`
	ActionsTaken     map[string]int64            `json:"actions_taken"`
	LastPressureTime time.Time                   `json:"last_pressure_time"`
	Thresholds       adapter.BackpressureThresholds `json:"thresholds"`
	AdaptiveThresholds adapter.BackpressureThresholds `json:"adaptive_thresholds"`
	PredictedLoad    float64                     `json:"predicted_load"`
	CircuitOpen      bool                        `json:"circuit_open"`
	CircuitTrips     int64                       `json:"circuit_trips"`
	GracefulMode     bool                        `json:"graceful_mode"`
	GracefulEvents   int64                       `json:"graceful_events"`
	StartTime        time.Time                   `json:"start_time"`
	Uptime           time.Duration               `json:"uptime"`
}

// NewBackpressureHandler creates a new backpressure handler
func NewBackpressureHandler(cfg *config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*BackpressureHandler, error) {
	handlerConfig := DefaultBackpressureConfig()
	
	if cfg != nil {
		if backpressureCfg, ok := cfg.Get("backpressure"); ok {
			if cfgMap, ok := backpressureCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					handlerConfig.Enabled = enabled
				}
				if thresholds, ok := cfgMap["thresholds"].(map[string]interface{}); ok {
					if warning, ok := thresholds["warning"].(float64); ok {
						handlerConfig.Thresholds.Warning = warning
					}
					if critical, ok := thresholds["critical"].(float64); ok {
						handlerConfig.Thresholds.Critical = critical
					}
					if emergency, ok := thresholds["emergency"].(float64); ok {
						handlerConfig.Thresholds.Emergency = emergency
					}
					if window, ok := thresholds["window"].(string); ok {
						if duration, err := time.ParseDuration(window); err == nil {
							handlerConfig.Thresholds.Window = duration
						}
					}
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					handlerConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					handlerConfig.EnableLogging = enableLogging
				}
				if enableAdaptive, ok := cfgMap["enable_adaptive"].(bool); ok {
					handlerConfig.EnableAdaptive = enableAdaptive
				}
				if adaptiveWindow, ok := cfgMap["adaptive_window"].(string); ok {
					if duration, err := time.ParseDuration(adaptiveWindow); err == nil {
						handlerConfig.AdaptiveWindow = duration
					}
				}
				if adaptiveFactor, ok := cfgMap["adaptive_factor"].(float64); ok {
					handlerConfig.AdaptiveFactor = adaptiveFactor
				}
				if enablePredictive, ok := cfgMap["enable_predictive"].(bool); ok {
					handlerConfig.EnablePredictive = enablePredictive
				}
				if predictionWindow, ok := cfgMap["prediction_window"].(string); ok {
					if duration, err := time.ParseDuration(predictionWindow); err == nil {
						handlerConfig.PredictionWindow = predictionWindow
					}
				}
				if predictionFactor, ok := cfgMap["prediction_factor"].(float64); ok {
					handlerConfig.PredictionFactor = predictionFactor
				}
				if enableGraceful, ok := cfgMap["enable_graceful"].(bool); ok {
					handlerConfig.EnableGraceful = enableGraceful
				}
				if gracefulPeriod, ok := cfgMap["graceful_period"].(string); ok {
					if duration, err := time.ParseDuration(gracefulPeriod); err == nil {
						handlerConfig.GracefulPeriod = duration
					}
				}
				if enableCircuit, ok := cfgMap["enable_circuit"].(bool); ok {
					handlerConfig.EnableCircuit = enableCircuit
				}
				if circuitThreshold, ok := cfgMap["circuit_threshold"].(float64); ok {
					handlerConfig.CircuitThreshold = circuitThreshold
				}
				if circuitRecovery, ok := cfgMap["circuit_recovery"].(float64); ok {
					handlerConfig.CircuitRecovery = circuitRecovery
				}
				if samplingInterval, ok := cfgMap["sampling_interval"].(string); ok {
					if duration, err := time.ParseDuration(samplingInterval); err == nil {
						handlerConfig.SamplingInterval = duration
					}
				}
				if maxSamples, ok := cfgMap["max_samples"].(int); ok {
					handlerConfig.MaxSamples = maxSamples
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateBackpressureConfig(handlerConfig); err != nil {
		return nil, fmt.Errorf("invalid backpressure configuration: %w", err)
	}
	
	handler := &BackpressureHandler{
		config:            handlerConfig,
		logger:            logger,
		metrics:           metrics,
		samples:           make([]LoadSample, 0),
		adaptiveThresholds: handlerConfig.Thresholds,
		predictedLoad:     0.0,
		circuitOpen:        false,
		circuitLastTrip:    time.Time{},
		gracefulMode:       false,
		gracefulStartTime:  time.Time{},
		samplingDone:       make(chan struct{}),
		startTime:          time.Now(),
		stats: &BackpressureStats{
			ActionsTaken: make(map[string]int64),
			Thresholds:   handlerConfig.Thresholds,
			AdaptiveThresholds: handlerConfig.Thresholds,
			StartTime:    time.Now(),
		},
	}
	
	handler.logger.Info("Created backpressure handler",
		"enabled", handlerConfig.Enabled,
		"warning_threshold", handlerConfig.Thresholds.Warning,
		"critical_threshold", handlerConfig.Thresholds.Critical,
		"emergency_threshold", handlerConfig.Thresholds.Emergency)
	
	return handler, nil
}

// validateBackpressureConfig validates the backpressure configuration
func validateBackpressureConfig(config *BackpressureConfig) error {
	if config == nil {
		return errors.New("backpressure configuration cannot be nil")
	}
	
	if config.Thresholds.Warning <= 0 || config.Thresholds.Warning > 1.0 {
		return errors.New("warning threshold must be between 0 and 1")
	}
	
	if config.Thresholds.Critical <= 0 || config.Thresholds.Critical > 1.0 {
		return errors.New("critical threshold must be between 0 and 1")
	}
	
	if config.Thresholds.Emergency <= 0 || config.Thresholds.Emergency > 1.0 {
		return errors.New("emergency threshold must be between 0 and 1")
	}
	
	if config.Thresholds.Warning >= config.Thresholds.Critical {
		return errors.New("warning threshold must be less than critical threshold")
	}
	
	if config.Thresholds.Critical >= config.Thresholds.Emergency {
		return errors.New("critical threshold must be less than emergency threshold")
	}
	
	if config.Thresholds.Window <= 0 {
		return errors.New("threshold window must be positive")
	}
	
	if config.AdaptiveWindow <= 0 {
		return errors.New("adaptive window must be positive")
	}
	
	if config.AdaptiveFactor < 0 || config.AdaptiveFactor > 1.0 {
		return errors.New("adaptive factor must be between 0 and 1")
	}
	
	if config.PredictionWindow <= 0 {
		return errors.New("prediction window must be positive")
	}
	
	if config.PredictionFactor < 0 || config.PredictionFactor > 1.0 {
		return errors.New("prediction factor must be between 0 and 1")
	}
	
	if config.GracefulPeriod <= 0 {
		return errors.New("graceful period must be positive")
	}
	
	if config.CircuitThreshold <= 0 || config.CircuitThreshold > 1.0 {
		return errors.New("circuit threshold must be between 0 and 1")
	}
	
	if config.CircuitRecovery <= 0 || config.CircuitRecovery > 1.0 {
		return errors.New("circuit recovery must be between 0 and 1")
	}
	
	if config.CircuitRecovery >= config.CircuitThreshold {
		return errors.New("circuit recovery must be less than circuit threshold")
	}
	
	if config.SamplingInterval <= 0 {
		return errors.New("sampling interval must be positive")
	}
	
	if config.MaxSamples <= 0 {
		return errors.New("max samples must be positive")
	}
	
	return nil
}

// Start starts the backpressure handler
func (h *BackpressureHandler) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.started {
		return ErrHandlerAlreadyRunning
	}
	
	if !h.config.Enabled {
		h.logger.Info("Backpressure handler is disabled")
		return nil
	}
	
	// Start sampling ticker
	h.samplingTicker = time.NewTicker(h.config.SamplingInterval)
	go h.sampleLoad()
	
	h.started = true
	h.stopped = false
	
	h.logger.Info("Started backpressure handler")
	
	return nil
}

// Stop stops the backpressure handler
func (h *BackpressureHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.stopped {
		return nil
	}
	
	if !h.started {
		return ErrHandlerNotRunning
	}
	
	// Stop sampling ticker
	if h.samplingTicker != nil {
		h.samplingTicker.Stop()
	}
	
	// Stop sampling routine
	close(h.samplingDone)
	
	h.stopped = true
	h.started = false
	
	h.logger.Info("Stopped backpressure handler")
	
	return nil
}

// IsRunning checks if the backpressure handler is running
func (h *BackpressureHandler) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return h.started && !h.stopped
}

// HandlePressure handles backpressure based on current load
func (h *BackpressureHandler) HandlePressure(ctx context.Context, currentLoad float64) (adapter.BackpressureAction, error) {
	if !h.config.Enabled {
		return adapter.BackpressureAction{
			Action:   "allow",
			Severity: "none",
			Delay:    0,
			Reject:   false,
			Limit:    0,
			Message:  "Backpressure handling disabled",
		}, nil
	}
	
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if !h.started {
		return adapter.BackpressureAction{}, ErrHandlerNotRunning
	}
	
	// Add load sample
	h.addLoadSample(currentLoad)
	
	// Update statistics
	h.updateStats(currentLoad)
	
	// Check circuit breaker
	if h.config.EnableCircuit {
		if action, err := h.checkCircuitBreaker(currentLoad); err != nil {
			return action, err
		}
	}
	
	// Check adaptive thresholds
	if h.config.EnableAdaptive {
		h.updateAdaptiveThresholds()
	}
	
	// Check predictive load
	if h.config.EnablePredictive {
		h.updatePredictiveLoad()
	}
	
	// Determine backpressure action
	action := h.determineAction(currentLoad)
	
	// Log backpressure events
	if h.config.EnableLogging && action.Severity != "none" {
		h.logger.Warn("Backpressure action triggered",
			"current_load", currentLoad,
			"action", action.Action,
			"severity", action.Severity,
			"delay", action.Delay,
			"reject", action.Reject,
			"limit", action.Limit)
	}
	
	// Update metrics if enabled
	if h.config.EnableMetrics && h.metrics != nil {
		h.updateBackpressureMetrics(action)
	}
	
	return action, nil
}

// GetBackpressureStats returns backpressure statistics
func (h *BackpressureHandler) GetBackpressureStats() adapter.BackpressureStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Update uptime
	h.stats.Uptime = time.Since(h.stats.StartTime)
	
	// Return a copy of stats
	stats := *h.stats
	return adapter.BackpressureStats{
		CurrentLoad:      stats.CurrentLoad,
		AverageLoad:      stats.AverageLoad,
		MaxLoad:          stats.MaxLoad,
		MinLoad:          stats.MinLoad,
		PressureEvents:   stats.PressureEvents,
		ActionsTaken:     stats.ActionsTaken,
		LastPressureTime: stats.LastPressureTime,
		Thresholds:       stats.Thresholds,
		AdaptiveThresholds: stats.AdaptiveThresholds,
		PredictedLoad:    stats.PredictedLoad,
		CircuitOpen:      stats.CircuitOpen,
		CircuitTrips:     stats.CircuitTrips,
		GracefulMode:     stats.GracefulMode,
		GracefulEvents:   stats.GracefulEvents,
		StartTime:        stats.StartTime,
		Uptime:           stats.Uptime,
	}
}

// SetThresholds sets the backpressure thresholds
func (h *BackpressureHandler) SetThresholds(thresholds adapter.BackpressureThresholds) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Validate thresholds
	if thresholds.Warning <= 0 || thresholds.Warning > 1.0 {
		return ErrInvalidThreshold
	}
	if thresholds.Critical <= 0 || thresholds.Critical > 1.0 {
		return ErrInvalidThreshold
	}
	if thresholds.Emergency <= 0 || thresholds.Emergency > 1.0 {
		return ErrInvalidThreshold
	}
	if thresholds.Warning >= thresholds.Critical {
		return ErrInvalidThreshold
	}
	if thresholds.Critical >= thresholds.Emergency {
		return ErrInvalidThreshold
	}
	if thresholds.Window <= 0 {
		return ErrInvalidThreshold
	}
	
	h.config.Thresholds = thresholds
	h.stats.Thresholds = thresholds
	
	if h.config.EnableLogging {
		h.logger.Info("Backpressure thresholds updated",
			"warning", thresholds.Warning,
			"critical", thresholds.Critical,
			"emergency", thresholds.Emergency)
	}
	
	return nil
}

// GetConfig returns the backpressure configuration
func (h *BackpressureHandler) GetConfig() *BackpressureConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Return a copy of config
	config := *h.config
	return &config
}

// UpdateConfig updates the backpressure configuration
func (h *BackpressureHandler) UpdateConfig(config *BackpressureConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Validate new configuration
	if err := validateBackpressureConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	h.config = config
	
	h.logger.Info("Updated backpressure configuration", "config", config)
	
	return nil
}

// Validate validates the backpressure handler state
func (h *BackpressureHandler) Validate() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Validate configuration
	if err := validateBackpressureConfig(h.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// addLoadSample adds a load sample
func (h *BackpressureHandler) addLoadSample(load float64) {
	sample := LoadSample{
		Timestamp: time.Now(),
		Load:      load,
		Source:    "system",
		Metadata:  make(map[string]interface{}),
	}
	
	h.samples = append(h.samples, sample)
	
	// Keep only the most recent samples
	if len(h.samples) > h.config.MaxSamples {
		h.samples = h.samples[len(h.samples)-h.config.MaxSamples:]
	}
}

// updateStats updates backpressure statistics
func (h *BackpressureHandler) updateStats(currentLoad float64) {
	h.stats.CurrentLoad = currentLoad
	
	// Update min/max load
	if h.stats.MaxLoad == 0 || currentLoad > h.stats.MaxLoad {
		h.stats.MaxLoad = currentLoad
	}
	if h.stats.MinLoad == 0 || currentLoad < h.stats.MinLoad {
		h.stats.MinLoad = currentLoad
	}
	
	// Update average load
	if len(h.samples) > 0 {
		totalLoad := 0.0
		for _, sample := range h.samples {
			totalLoad += sample.Load
		}
		h.stats.AverageLoad = totalLoad / float64(len(h.samples))
	}
	
	// Update predicted load
	h.stats.PredictedLoad = h.predictedLoad
	
	// Update circuit status
	h.stats.CircuitOpen = h.circuitOpen
	
	// Update graceful mode status
	h.stats.GracefulMode = h.gracefulMode
}

// checkCircuitBreaker checks the circuit breaker state
func (h *BackpressureHandler) checkCircuitBreaker(currentLoad float64) (adapter.BackpressureAction, error) {
	h.circuitMu.Lock()
	defer h.circuitMu.Unlock()
	
	if h.circuitOpen {
		// Check if we should recover
		if currentLoad <= h.config.CircuitRecovery {
			h.circuitOpen = false
			h.logger.Info("Circuit breaker recovered", "load", currentLoad)
		} else {
			return adapter.BackpressureAction{
				Action:   "reject",
				Severity: "emergency",
				Delay:    0,
				Reject:   true,
				Limit:    0,
				Message:  "Circuit breaker open - rejecting all requests",
			}, ErrBackpressureThresholdExceeded
		}
	} else {
		// Check if we should trip
		if currentLoad >= h.config.CircuitThreshold {
			h.circuitOpen = true
			h.circuitLastTrip = time.Now()
			h.stats.CircuitTrips++
			h.logger.Warn("Circuit breaker tripped", "load", currentLoad)
		}
	}
	
	return adapter.BackpressureAction{}, nil
}

// updateAdaptiveThresholds updates adaptive thresholds
func (h *BackpressureHandler) updateAdaptiveThresholds() {
	h.adaptiveMu.Lock()
	defer h.adaptiveMu.Unlock()
	
	if len(h.samples) < 10 {
		return
	}
	
	// Calculate load trend
	recentSamples := h.samples[len(h.samples)-10:]
	trend := 0.0
	for i := 1; i < len(recentSamples); i++ {
		trend += recentSamples[i].Load - recentSamples[i-1].Load
	}
	trend /= float64(len(recentSamples) - 1)
	
	// Adjust thresholds based on trend
	adjustment := trend * h.config.AdaptiveFactor
	
	h.adaptiveThresholds.Warning = math.Max(0.1, math.Min(0.9, h.config.Thresholds.Warning+adjustment))
	h.adaptiveThresholds.Critical = math.Max(0.1, math.Min(0.9, h.config.Thresholds.Critical+adjustment))
	h.adaptiveThresholds.Emergency = math.Max(0.1, math.Min(0.9, h.config.Thresholds.Emergency+adjustment))
	
	// Ensure threshold ordering
	h.adaptiveThresholds.Warning = math.Min(h.adaptiveThresholds.Warning, h.adaptiveThresholds.Critical-0.01)
	h.adaptiveThresholds.Critical = math.Min(h.adaptiveThresholds.Critical, h.adaptiveThresholds.Emergency-0.01)
	
	h.stats.AdaptiveThresholds = h.adaptiveThresholds
}

// updatePredictiveLoad updates the predicted load
func (h *BackpressureHandler) updatePredictiveLoad() {
	h.predictionMu.Lock()
	defer h.predictionMu.Unlock()
	
	if len(h.samples) < 2 {
		return
	}
	
	// Simple linear prediction based on recent trend
	recentSamples := h.samples[len(h.samples)-min(10, len(h.samples)):]
	
	// Calculate trend
	trend := 0.0
	for i := 1; i < len(recentSamples); i++ {
		trend += recentSamples[i].Load - recentSamples[i-1].Load
	}
	trend /= float64(len(recentSamples) - 1)
	
	// Predict future load
	h.predictedLoad = recentSamples[len(recentSamples)-1].Load + trend*h.config.PredictionFactor
}

// determineAction determines the backpressure action based on current load
func (h *BackpressureHandler) determineAction(currentLoad float64) adapter.BackpressureAction {
	thresholds := h.config.Thresholds
	if h.config.EnableAdaptive {
		h.adaptiveMu.RLock()
		thresholds = h.adaptiveThresholds
		h.adaptiveMu.RUnlock()
	}
	
	action := adapter.BackpressureAction{
		Action:   "allow",
		Severity: "none",
		Delay:    0,
		Reject:   false,
		Limit:    0,
		Message:  "Request allowed",
	}
	
	// Check thresholds
	if currentLoad >= thresholds.Emergency {
		action.Action = "reject"
		action.Severity = "emergency"
		action.Reject = true
		action.Message = "Emergency backpressure - rejecting requests"
		h.stats.PressureEvents++
		h.stats.LastPressureTime = time.Now()
		h.stats.ActionsTaken["emergency_reject"]++
	} else if currentLoad >= thresholds.Critical {
		action.Action = "delay"
		action.Severity = "critical"
		action.Delay = 100 * time.Millisecond
		action.Limit = 50
		action.Message = "Critical backpressure - delaying requests"
		h.stats.PressureEvents++
		h.stats.LastPressureTime = time.Now()
		h.stats.ActionsTaken["critical_delay"]++
	} else if currentLoad >= thresholds.Warning {
		action.Action = "limit"
		action.Severity = "warning"
		action.Limit = 100
		action.Message = "Warning backpressure - limiting requests"
		h.stats.PressureEvents++
		h.stats.LastPressureTime = time.Now()
		h.stats.ActionsTaken["warning_limit"]++
	}
	
	// Check graceful mode
	if h.config.EnableGraceful && action.Severity != "none" {
		h.gracefulMu.Lock()
		if !h.gracefulMode {
			h.gracefulMode = true
			h.gracefulStartTime = time.Now()
			h.stats.GracefulEvents++
		}
		h.gracefulMu.Unlock()
	} else {
		h.gracefulMu.Lock()
		if h.gracefulMode && time.Since(h.gracefulStartTime) > h.config.GracefulPeriod {
			h.gracefulMode = false
		}
		h.gracefulMu.Unlock()
	}
	
	return action
}

// sampleLoad samples the current load
func (h *BackpressureHandler) sampleLoad() {
	for {
		select {
		case <-h.samplingTicker.C:
			h.mu.Lock()
			// Calculate current load based on samples
			if len(h.samples) > 0 {
				currentLoad := h.samples[len(h.samples)-1].Load
				h.updateStats(currentLoad)
			}
			h.mu.Unlock()
			
		case <-h.samplingDone:
			return
		}
	}
}

// updateBackpressureMetrics updates backpressure metrics
func (h *BackpressureHandler) updateBackpressureMetrics(action adapter.BackpressureAction) {
	if h.metrics == nil {
		return
	}
	
	// Update load metrics
	h.metrics.ServiceGauge.WithLabelValues("backpressure", "current_load").Set(h.stats.CurrentLoad)
	h.metrics.ServiceGauge.WithLabelValues("backpressure", "average_load").Set(h.stats.AverageLoad)
	h.metrics.ServiceGauge.WithLabelValues("backpressure", "predicted_load").Set(h.stats.PredictedLoad)
	
	// Update action metrics
	h.metrics.ServiceCounter.WithLabelValues("backpressure", "actions", action.Action).Inc()
	h.metrics.ServiceCounter.WithLabelValues("backpressure", "severity", action.Severity).Inc()
	
	// Update circuit breaker metrics
	if h.stats.CircuitOpen {
		h.metrics.ServiceGauge.WithLabelValues("backpressure", "circuit_open").Set(1)
	} else {
		h.metrics.ServiceGauge.WithLabelValues("backpressure", "circuit_open").Set(0)
	}
	
	// Update graceful mode metrics
	if h.stats.GracefulMode {
		h.metrics.ServiceGauge.WithLabelValues("backpressure", "graceful_mode").Set(1)
	} else {
		h.metrics.ServiceGauge.WithLabelValues("backpressure", "graceful_mode").Set(0)
	}
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}