package error

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"strings"

	"go.uber.org/zap"

	"vxdb/internal/config"
	"vxdb/internal/logging"
	"vxdb/internal/metrics"
	"vxdb/internal/protocol/adapter"
)

var (
	ErrHandlerNotRunning     = errors.New("error handler not running")
	ErrHandlerAlreadyRunning = errors.New("error handler already running")
	ErrInvalidConfig         = errors.New("invalid error handler configuration")
	ErrRecoveryFailed        = errors.New("recovery failed")
	ErrNotificationFailed    = errors.New("notification failed")
)

// ErrorConfig represents the error handler configuration
type ErrorConfig struct {
	Enabled              bool                    `yaml:"enabled" json:"enabled"`
	EnableMetrics        bool                    `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging        bool                    `yaml:"enable_logging" json:"enable_logging"`
	EnableTracing        bool                    `yaml:"enable_tracing" json:"enable_tracing"`
	EnableRecovery       bool                    `yaml:"enable_recovery" json:"enable_recovery"`
	EnableNotifications  bool                    `yaml:"enable_notifications" json:"enable_notifications"`
	EnableDebug          bool                    `yaml:"enable_debug" json:"enable_debug"`
	MaxErrorSize         int                     `yaml:"max_error_size" json:"max_error_size"`
	ErrorHistorySize     int                     `yaml:"error_history_size" json:"error_history_size"`
	ErrorTTL             time.Duration           `yaml:"error_ttl" json:"error_ttl"`
	RecoveryStrategies   []RecoveryStrategy      `yaml:"recovery_strategies" json:"recovery_strategies"`
	NotificationChannels []NotificationChannel   `yaml:"notification_channels" json:"notification_channels"`
	ErrorMappings        map[string]ErrorMapping `yaml:"error_mappings" json:"error_mappings"`
	HTTPErrorCodes       map[int]string          `yaml:"http_error_codes" json:"http_error_codes"`
	GRPCErrorCodes       map[string]string       `yaml:"grpc_error_codes" json:"grpc_error_codes"`
}

// RecoveryStrategy represents a recovery strategy
type RecoveryStrategy struct {
	Name        string                 `yaml:"name" json:"name"`
	Type        string                 `yaml:"type" json:"type"`
	MaxAttempts int                    `yaml:"max_attempts" json:"max_attempts"`
	Backoff     time.Duration          `yaml:"backoff" json:"backoff"`
	Conditions  map[string]interface{} `yaml:"conditions" json:"conditions"`
	Actions     []string               `yaml:"actions" json:"actions"`
	Enabled     bool                   `yaml:"enabled" json:"enabled"`
}

// NotificationChannel represents a notification channel
type NotificationChannel struct {
	Name     string                 `yaml:"name" json:"name"`
	Type     string                 `yaml:"type" json:"type"`
	Enabled  bool                   `yaml:"enabled" json:"enabled"`
	Config   map[string]interface{} `yaml:"config" json:"config"`
	Triggers []string               `yaml:"triggers" json:"triggers"`
}

// ErrorMapping represents an error mapping
type ErrorMapping struct {
	OriginalError string                 `yaml:"original_error" json:"original_error"`
	MappedError   string                 `yaml:"mapped_error" json:"mapped_error"`
	HTTPCode      int                    `yaml:"http_code" json:"http_code"`
	GRPCCode      string                 `yaml:"grpc_code" json:"grpc_code"`
	Severity      string                 `yaml:"severity" json:"severity"`
	Actions       []string               `yaml:"actions" json:"actions"`
	Metadata      map[string]interface{} `yaml:"metadata" json:"metadata"`
}

// DefaultErrorConfig returns the default error handler configuration
func DefaultErrorConfig() *ErrorConfig {
	return &ErrorConfig{
		Enabled:              true,
		EnableMetrics:        true,
		EnableLogging:        true,
		EnableTracing:        false,
		EnableRecovery:       true,
		EnableNotifications:  false,
		EnableDebug:          false,
		MaxErrorSize:         1024 * 1024, // 1MB
		ErrorHistorySize:     1000,
		ErrorTTL:             24 * time.Hour,
		RecoveryStrategies:   getDefaultRecoveryStrategies(),
		NotificationChannels: []NotificationChannel{},
		ErrorMappings:        getDefaultErrorMappings(),
		HTTPErrorCodes:       getDefaultHTTPErrorCodes(),
		GRPCErrorCodes:       getDefaultGRPCErrorCodes(),
	}
}

// getDefaultRecoveryStrategies returns the default recovery strategies
func getDefaultRecoveryStrategies() []RecoveryStrategy {
	return []RecoveryStrategy{
		{
			Name:        "connection_retry",
			Type:        "retry",
			MaxAttempts: 3,
			Backoff:     1 * time.Second,
			Conditions:  map[string]interface{}{"error_type": "connection"},
			Actions:     []string{"reconnect", "backoff"},
			Enabled:     true,
		},
		{
			Name:        "timeout_retry",
			Type:        "retry",
			MaxAttempts: 2,
			Backoff:     500 * time.Millisecond,
			Conditions:  map[string]interface{}{"error_type": "timeout"},
			Actions:     []string{"retry", "reduce_timeout"},
			Enabled:     true,
		},
		{
			Name:        "circuit_breaker",
			Type:        "circuit_breaker",
			MaxAttempts: 5,
			Backoff:     5 * time.Second,
			Conditions:  map[string]interface{}{"error_type": "service_unavailable"},
			Actions:     []string{"open_circuit", "fallback"},
			Enabled:     true,
		},
	}
}

// getDefaultErrorMappings returns the default error mappings
func getDefaultErrorMappings() map[string]ErrorMapping {
	return map[string]ErrorMapping{
		"connection_refused": {
			OriginalError: "connection refused",
			MappedError:   "service_unavailable",
			HTTPCode:      503,
			GRPCCode:      "unavailable",
			Severity:      "error",
			Actions:       []string{"retry", "circuit_breaker"},
			Metadata:      map[string]interface{}{"retryable": true},
		},
		"timeout": {
			OriginalError: "timeout",
			MappedError:   "timeout",
			HTTPCode:      504,
			GRPCCode:      "deadline_exceeded",
			Severity:      "warning",
			Actions:       []string{"retry", "reduce_timeout"},
			Metadata:      map[string]interface{}{"retryable": true},
		},
		"not_found": {
			OriginalError: "not found",
			MappedError:   "not_found",
			HTTPCode:      404,
			GRPCCode:      "not_found",
			Severity:      "info",
			Actions:       []string{"log"},
			Metadata:      map[string]interface{}{"retryable": false},
		},
		"permission_denied": {
			OriginalError: "permission denied",
			MappedError:   "permission_denied",
			HTTPCode:      403,
			GRPCCode:      "permission_denied",
			Severity:      "warning",
			Actions:       []string{"log", "alert"},
			Metadata:      map[string]interface{}{"retryable": false},
		},
		"invalid_argument": {
			OriginalError: "invalid argument",
			MappedError:   "invalid_argument",
			HTTPCode:      400,
			GRPCCode:      "invalid_argument",
			Severity:      "warning",
			Actions:       []string{"log", "client_notify"},
			Metadata:      map[string]interface{}{"retryable": false},
		},
	}
}

// getDefaultHTTPErrorCodes returns the default HTTP error codes
func getDefaultHTTPErrorCodes() map[int]string {
	return map[int]string{
		400: "Bad Request",
		401: "Unauthorized",
		403: "Forbidden",
		404: "Not Found",
		405: "Method Not Allowed",
		408: "Request Timeout",
		409: "Conflict",
		413: "Payload Too Large",
		429: "Too Many Requests",
		500: "Internal Server Error",
		501: "Not Implemented",
		502: "Bad Gateway",
		503: "Service Unavailable",
		504: "Gateway Timeout",
	}
}

// getDefaultGRPCErrorCodes returns the default gRPC error codes
func getDefaultGRPCErrorCodes() map[string]string {
	return map[string]string{
		"cancelled":           "Cancelled",
		"unknown":             "Unknown",
		"invalid_argument":    "Invalid Argument",
		"deadline_exceeded":   "Deadline Exceeded",
		"not_found":           "Not Found",
		"already_exists":      "Already Exists",
		"permission_denied":   "Permission Denied",
		"resource_exhausted":  "Resource Exhausted",
		"failed_precondition": "Failed Precondition",
		"aborted":             "Aborted",
		"out_of_range":        "Out of Range",
		"unimplemented":       "Unimplemented",
		"internal":            "Internal",
		"unavailable":         "Unavailable",
		"data_loss":           "Data Loss",
		"unauthenticated":     "Unauthenticated",
	}
}

// ErrorEntry represents an error entry
type ErrorEntry struct {
	ID            string                 `json:"id"`
	Timestamp     time.Time              `json:"timestamp"`
	Error         error                  `json:"error"`
	Stack         string                 `json:"stack"`
	Context       map[string]interface{} `json:"context"`
	Request       *adapter.Request       `json:"request,omitempty"`
	Response      *adapter.Response      `json:"response,omitempty"`
	Severity      string                 `json:"severity"`
	Category      string                 `json:"category"`
	Recovered     bool                   `json:"recovered"`
	Retryable     bool                   `json:"retryable"`
	Actions       []string               `json:"actions"`
	Notifications []NotificationResult   `json:"notifications,omitempty"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// NotificationResult represents a notification result
type NotificationResult struct {
	Channel   string                 `json:"channel"`
	Success   bool                   `json:"success"`
	Error     error                  `json:"error,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// ErrorHandler represents an error handler
type ErrorHandler struct {
	config  *ErrorConfig
	logger  logging.Logger
	metrics *metrics.ServiceMetrics

	// Error history
	errors     []ErrorEntry
	errorIndex map[string]int
	mu         sync.RWMutex

	// Recovery
	recovery *RecoveryManager

	// Notifications
	notifications *NotificationManager

	// Lifecycle
	started   bool
	stopped   bool
	startTime time.Time

	// Statistics
	stats *ErrorHandlerStats
}

// ErrorHandlerStats represents error handler statistics
type ErrorHandlerStats struct {
	TotalErrors         int64            `json:"total_errors"`
	RecoveredErrors     int64            `json:"recovered_errors"`
	FailedRecoveries    int64            `json:"failed_recoveries"`
	Notifications       int64            `json:"notifications"`
	FailedNotifications int64            `json:"failed_notifications"`
	ErrorByCategory     map[string]int64 `json:"error_by_category"`
	ErrorBySeverity     map[string]int64 `json:"error_by_severity"`
	AvgRecoveryTime     time.Duration    `json:"avg_recovery_time"`
	MaxRecoveryTime     time.Duration    `json:"max_recovery_time"`
	StartTime           time.Time        `json:"start_time"`
	Uptime              time.Duration    `json:"uptime"`
}

// RecoveryManager manages recovery strategies
type RecoveryManager struct {
	strategies []RecoveryStrategy
	mu         sync.RWMutex
}

// NotificationManager manages notification channels
type NotificationManager struct {
	channels []NotificationChannel
	mu       sync.RWMutex
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*ErrorHandler, error) {
	handlerConfig := DefaultErrorConfig()

	if cfg != nil {
		if errorCfg, ok := cfg.Get("error_handler"); ok {
			if cfgMap, ok := errorCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					handlerConfig.Enabled = enabled
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					handlerConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					handlerConfig.EnableLogging = enableLogging
				}
				if enableTracing, ok := cfgMap["enable_tracing"].(bool); ok {
					handlerConfig.EnableTracing = enableTracing
				}
				if enableRecovery, ok := cfgMap["enable_recovery"].(bool); ok {
					handlerConfig.EnableRecovery = enableRecovery
				}
				if enableNotifications, ok := cfgMap["enable_notifications"].(bool); ok {
					handlerConfig.EnableNotifications = enableNotifications
				}
				if enableDebug, ok := cfgMap["enable_debug"].(bool); ok {
					handlerConfig.EnableDebug = enableDebug
				}
				if maxErrorSize, ok := cfgMap["max_error_size"].(int); ok {
					handlerConfig.MaxErrorSize = maxErrorSize
				}
				if errorHistorySize, ok := cfgMap["error_history_size"].(int); ok {
					handlerConfig.ErrorHistorySize = errorHistorySize
				}
				if errorTTL, ok := cfgMap["error_ttl"].(string); ok {
					if duration, err := time.ParseDuration(errorTTL); err == nil {
						handlerConfig.ErrorTTL = duration
					}
				}
			}
		}
	}

	// Validate configuration
	if err := validateErrorConfig(handlerConfig); err != nil {
		return nil, fmt.Errorf("invalid error handler configuration: %w", err)
	}

	handler := &ErrorHandler{
		config:     handlerConfig,
		logger:     logger,
		metrics:    metrics,
		errors:     make([]ErrorEntry, 0),
		errorIndex: make(map[string]int),
		startTime:  time.Now(),
		stats: &ErrorHandlerStats{
			ErrorByCategory: make(map[string]int64),
			ErrorBySeverity: make(map[string]int64),
			StartTime:       time.Now(),
		},
	}

	// Initialize recovery manager
	handler.recovery = &RecoveryManager{
		strategies: handlerConfig.RecoveryStrategies,
	}

	// Initialize notification manager
	handler.notifications = &NotificationManager{
		channels: handlerConfig.NotificationChannels,
	}

	handler.logger.Info("Created error handler")

	return handler, nil
}

// validateErrorConfig validates the error handler configuration
func validateErrorConfig(config *ErrorConfig) error {
	if config == nil {
		return errors.New("error handler configuration cannot be nil")
	}

	if config.MaxErrorSize <= 0 {
		return errors.New("max error size must be positive")
	}

	if config.ErrorHistorySize <= 0 {
		return errors.New("error history size must be positive")
	}

	if config.ErrorTTL <= 0 {
		return errors.New("error TTL must be positive")
	}

	for _, strategy := range config.RecoveryStrategies {
		if strategy.Name == "" {
			return errors.New("recovery strategy name cannot be empty")
		}
		if strategy.Type == "" {
			return errors.New("recovery strategy type cannot be empty")
		}
		if strategy.MaxAttempts <= 0 {
			return errors.New("recovery strategy max attempts must be positive")
		}
		if strategy.Backoff < 0 {
			return errors.New("recovery strategy backoff cannot be negative")
		}
	}

	for _, channel := range config.NotificationChannels {
		if channel.Name == "" {
			return errors.New("notification channel name cannot be empty")
		}
		if channel.Type == "" {
			return errors.New("notification channel type cannot be empty")
		}
	}

	return nil
}

// Start starts the error handler
func (h *ErrorHandler) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.started {
		return ErrHandlerAlreadyRunning
	}

	if !h.config.Enabled {
		h.logger.Info("Error handler is disabled")
		return nil
	}

	h.started = true
	h.stopped = false

	h.logger.Info("Started error handler")

	return nil
}

// Stop stops the error handler
func (h *ErrorHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.stopped {
		return nil
	}

	if !h.started {
		return ErrHandlerNotRunning
	}

	h.stopped = true
	h.started = false

	h.logger.Info("Stopped error handler")

	return nil
}

// IsRunning checks if the error handler is running
func (h *ErrorHandler) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.started && !h.stopped
}

// HandleError handles an error
func (h *ErrorHandler) HandleError(ctx context.Context, err error, req *adapter.Request, resp *adapter.Response) (*ErrorEntry, error) {
	if !h.config.Enabled {
		return nil, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.started {
		return nil, ErrHandlerNotRunning
	}

	startTime := time.Now()

	// Create error entry
	entry := h.createErrorEntry(err, req, resp)

	// Add to error history
	h.addErrorEntry(entry)

	// Update statistics
	h.updateStats(entry)

	// Log error
	if h.config.EnableLogging {
		h.logError(entry)
	}

	// Update metrics if enabled
	if h.config.EnableMetrics && h.metrics != nil {
		h.updateErrorMetrics(entry)
	}

	// Attempt recovery if enabled
	if h.config.EnableRecovery {
		if recovered, recoveryErr := h.attemptRecovery(ctx, &entry); recovered {
			entry.Recovered = true
			entry.Actions = append(entry.Actions, "recovered")
			if recoveryErr != nil {
				entry.Metadata["recovery_error"] = recoveryErr.Error()
			}
		}
	}

	// Send notifications if enabled
	if h.config.EnableNotifications {
		if notificationResults, err := h.sendNotifications(&entry); err == nil {
			entry.Notifications = notificationResults
		}
	}

	// Update duration
	entry.Metadata["handling_duration"] = time.Since(startTime)

	return &entry, nil
}

// createErrorEntry creates an error entry
func (h *ErrorHandler) createErrorEntry(err error, req *adapter.Request, resp *adapter.Response) ErrorEntry {
	entry := ErrorEntry{
		ID:        generateErrorID(),
		Timestamp: time.Now(),
		Error:     err,
		Context:   make(map[string]interface{}),
		Request:   req,
		Response:  resp,
		Actions:   make([]string, 0),
		Metadata:  make(map[string]interface{}),
	}

	// Get stack trace if debug is enabled
	if h.config.EnableDebug {
		entry.Stack = string(debug.Stack())
	}

	// Map error
	if mapping, exists := h.config.ErrorMappings[getErrorType(err)]; exists {
		entry.Severity = mapping.Severity
		entry.Category = mapping.MappedError
		entry.Retryable = getRetryableFromMetadata(mapping.Metadata)
		entry.Actions = append(entry.Actions, mapping.Actions...)
		entry.Metadata["http_code"] = mapping.HTTPCode
		entry.Metadata["grpc_code"] = mapping.GRPCCode
	} else {
		entry.Severity = "error"
		entry.Category = "unknown"
		entry.Retryable = false
		entry.Actions = append(entry.Actions, "log")
	}

	// Add context information
	if req != nil {
		entry.Context["request_id"] = req.ID
		entry.Context["method"] = req.Method
		entry.Context["path"] = req.Path
		entry.Context["protocol"] = req.Protocol
		entry.Context["client_ip"] = req.ClientIP
	}

	if resp != nil {
		entry.Context["response_code"] = resp.Status
		entry.Context["response_size"] = len(resp.Body)
	}

	return entry
}

// addErrorEntry adds an error entry to the history
func (h *ErrorHandler) addErrorEntry(entry ErrorEntry) {
	h.errors = append(h.errors, entry)
	h.errorIndex[entry.ID] = len(h.errors) - 1

	// Clean up old errors
	if len(h.errors) > h.config.ErrorHistorySize {
		// Remove oldest errors
		removed := len(h.errors) - h.config.ErrorHistorySize
		h.errors = h.errors[removed:]

		// Rebuild index
		h.errorIndex = make(map[string]int)
		for i, err := range h.errors {
			h.errorIndex[err.ID] = i
		}
	}

	// Clean up expired errors
	now := time.Now()
	validErrors := make([]ErrorEntry, 0)
	for _, err := range h.errors {
		if now.Sub(err.Timestamp) <= h.config.ErrorTTL {
			validErrors = append(validErrors, err)
		}
	}

	if len(validErrors) != len(h.errors) {
		h.errors = validErrors
		h.errorIndex = make(map[string]int)
		for i, err := range h.errors {
			h.errorIndex[err.ID] = i
		}
	}
}

// updateStats updates error handler statistics
func (h *ErrorHandler) updateStats(entry ErrorEntry) {
	h.stats.TotalErrors++

	if entry.Recovered {
		h.stats.RecoveredErrors++
	}

	h.stats.ErrorByCategory[entry.Category]++
	h.stats.ErrorBySeverity[entry.Severity]++

	// Update recovery time stats
	if entry.Recovered {
		if handlingDuration, ok := entry.Metadata["handling_duration"].(time.Duration); ok {
			if h.stats.MaxRecoveryTime == 0 || handlingDuration > h.stats.MaxRecoveryTime {
				h.stats.MaxRecoveryTime = handlingDuration
			}

			h.stats.AvgRecoveryTime = time.Duration(
				(int64(h.stats.AvgRecoveryTime)*(h.stats.RecoveredErrors-1) + int64(handlingDuration)) /
					h.stats.RecoveredErrors,
			)
		}
	}
}

// logError logs an error
func (h *ErrorHandler) logError(entry ErrorEntry) {
	fields := []zap.Field{
		zap.String("error_id", entry.ID),
		zap.String("error", entry.Error.Error()),
		zap.String("severity", entry.Severity),
		zap.String("category", entry.Category),
		zap.Bool("recovered", entry.Recovered),
		zap.Bool("retryable", entry.Retryable),
	}

	for key, value := range entry.Context {
		fields = append(fields, zap.Any(key, value))
	}

	switch entry.Severity {
	case "critical":
		h.logger.Error("Critical error occurred", fields...)
	case "error":
		h.logger.Error("Error occurred", fields...)
	case "warning":
		h.logger.Warn("Warning occurred", fields...)
	case "info":
		h.logger.Info("Info occurred", fields...)
	default:
		h.logger.Debug("Debug occurred", fields...)
	}
}

// updateErrorMetrics updates error metrics
func (h *ErrorHandler) updateErrorMetrics(entry ErrorEntry) {
	if h.metrics == nil {
		return
	}

	h.metrics.ErrorsTotal.Add(1, "error_handler", "handle", entry.Category)
	h.metrics.ServiceErrors.Add(1, "error_handler", entry.Severity)

	if entry.Recovered {
		h.metrics.ServiceErrors.Add(1, "error_handler", "recovered")
	}

	if entry.Retryable {
		h.metrics.ServiceErrors.Add(1, "error_handler", "retryable")
	}

	for _, action := range entry.Actions {
		h.metrics.ServiceErrors.Add(1, "error_handler", action)
	}
}

// attemptRecovery attempts to recover from an error
func (h *ErrorHandler) attemptRecovery(ctx context.Context, entry *ErrorEntry) (bool, error) {
	h.recovery.mu.RLock()
	defer h.recovery.mu.RUnlock()

	for _, strategy := range h.recovery.strategies {
		if !strategy.Enabled {
			continue
		}

		if h.matchesRecoveryConditions(strategy, entry) {
			return h.executeRecoveryStrategy(ctx, strategy, entry)
		}
	}

	return false, nil
}

// matchesRecoveryConditions checks if an error matches recovery conditions
func (h *ErrorHandler) matchesRecoveryConditions(strategy RecoveryStrategy, entry *ErrorEntry) bool {
	if errorType, ok := strategy.Conditions["error_type"].(string); ok {
		if errorType != entry.Category {
			return false
		}
	}

	if severity, ok := strategy.Conditions["severity"].(string); ok {
		if severity != entry.Severity {
			return false
		}
	}

	if retryable, ok := strategy.Conditions["retryable"].(bool); ok {
		if retryable != entry.Retryable {
			return false
		}
	}

	return true
}

// executeRecoveryStrategy executes a recovery strategy
func (h *ErrorHandler) executeRecoveryStrategy(ctx context.Context, strategy RecoveryStrategy, entry *ErrorEntry) (bool, error) {
	startTime := time.Now()

	for attempt := 1; attempt <= strategy.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		// Execute recovery actions
		for _, action := range strategy.Actions {
			switch action {
			case "reconnect":
				// Placeholder for reconnection logic
				h.logger.Debug("Attempting reconnection",
					zap.String("error_id", entry.ID),
					zap.Int("attempt", attempt))
			case "backoff":
				// Apply backoff
				if attempt > 1 {
					backoffDuration := strategy.Backoff * time.Duration(attempt-1)
					select {
					case <-time.After(backoffDuration):
					case <-ctx.Done():
						return false, ctx.Err()
					}
				}
			case "retry":
				// Placeholder for retry logic
				h.logger.Debug("Attempting retry",
					zap.String("error_id", entry.ID),
					zap.Int("attempt", attempt))
			case "reduce_timeout":
				// Placeholder for timeout reduction logic
				h.logger.Debug("Reducing timeout",
					zap.String("error_id", entry.ID),
					zap.Int("attempt", attempt))
			case "open_circuit":
				// Placeholder for circuit breaker logic
				h.logger.Debug("Opening circuit breaker",
					zap.String("error_id", entry.ID))
			case "fallback":
				// Placeholder for fallback logic
				h.logger.Debug("Executing fallback",
					zap.String("error_id", entry.ID))
			}
		}

		// Check if recovery was successful
		if h.checkRecoverySuccess(strategy, entry) {
			h.logger.Info("Recovery successful")
			return true, nil
		}
	}

	h.logger.Warn("Recovery failed",
		zap.String("error_id", entry.ID),
		zap.String("strategy", strategy.Name),
		zap.Int("max_attempts", strategy.MaxAttempts),
		zap.Duration("duration", time.Since(startTime)))

	return false, ErrRecoveryFailed
}

// checkRecoverySuccess checks if recovery was successful
func (h *ErrorHandler) checkRecoverySuccess(strategy RecoveryStrategy, entry *ErrorEntry) bool {
	// This is a placeholder for recovery success checking logic
	// In a real implementation, you would check if the error condition has been resolved
	return false
}

// sendNotifications sends notifications for an error
func (h *ErrorHandler) sendNotifications(entry *ErrorEntry) ([]NotificationResult, error) {
	h.notifications.mu.RLock()
	defer h.notifications.mu.RUnlock()

	results := make([]NotificationResult, 0)

	for _, channel := range h.notifications.channels {
		if !channel.Enabled {
			continue
		}

		if h.shouldNotify(channel, entry) {
			result := h.sendNotification(channel, entry)
			results = append(results, result)

			if result.Success {
				h.stats.Notifications++
			} else {
				h.stats.FailedNotifications++
			}
		}
	}

	return results, nil
}

// shouldNotify checks if a notification should be sent
func (h *ErrorHandler) shouldNotify(channel NotificationChannel, entry *ErrorEntry) bool {
	for _, trigger := range channel.Triggers {
		if trigger == entry.Severity || trigger == entry.Category {
			return true
		}
	}
	return false
}

// sendNotification sends a notification
func (h *ErrorHandler) sendNotification(channel NotificationChannel, entry *ErrorEntry) NotificationResult {
	result := NotificationResult{
		Channel:   channel.Name,
		Success:   false,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	defer func() {
		if !result.Success {
			result.Error = fmt.Errorf("notification failed")
		}
	}()

	// This is a placeholder for notification sending logic
	// In a real implementation, you would send notifications to various channels
	// like email, Slack, PagerDuty, etc.

	switch channel.Type {
	case "log":
		h.logger.Info("Sending notification")
		result.Success = true
	case "webhook":
		// Placeholder for webhook notification
		h.logger.Debug("Sending webhook notification",
			zap.String("channel", channel.Name),
			zap.String("error_id", entry.ID))
		result.Success = true
	default:
		h.logger.Warn("Unknown notification channel type",
			zap.String("channel", channel.Name),
			zap.String("type", channel.Type))
		result.Error = fmt.Errorf("unknown channel type: %s", channel.Type)
	}

	return result
}

// GetErrorHistory returns the error history
func (h *ErrorHandler) GetErrorHistory() []ErrorEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	history := make([]ErrorEntry, len(h.errors))
	copy(history, h.errors)
	return history
}

// GetError returns an error by ID
func (h *ErrorHandler) GetError(id string) (*ErrorEntry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	index, exists := h.errorIndex[id]
	if !exists {
		return nil, fmt.Errorf("error not found: %s", id)
	}

	entry := h.errors[index]
	return &entry, nil
}

// GetStats returns error handler statistics
func (h *ErrorHandler) GetStats() *ErrorHandlerStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Update uptime
	h.stats.Uptime = time.Since(h.stats.StartTime)

	// Return a copy of stats
	stats := *h.stats
	stats.ErrorByCategory = make(map[string]int64)
	stats.ErrorBySeverity = make(map[string]int64)

	for k, v := range h.stats.ErrorByCategory {
		stats.ErrorByCategory[k] = v
	}

	for k, v := range h.stats.ErrorBySeverity {
		stats.ErrorBySeverity[k] = v
	}

	return &stats
}

// ResetStats resets error handler statistics
func (h *ErrorHandler) ResetStats() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.stats = &ErrorHandlerStats{
		ErrorByCategory: make(map[string]int64),
		ErrorBySeverity: make(map[string]int64),
		StartTime:       time.Now(),
	}

	h.logger.Info("Error handler statistics reset")
}

// GetConfig returns the error handler configuration
func (h *ErrorHandler) GetConfig() *ErrorConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Return a copy of config
	config := *h.config
	return &config
}

// UpdateConfig updates the error handler configuration
func (h *ErrorHandler) UpdateConfig(config *ErrorConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Validate new configuration
	if err := validateErrorConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	h.config = config

	// Update recovery manager
	h.recovery.strategies = config.RecoveryStrategies

	// Update notification manager
	h.notifications.channels = config.NotificationChannels

	h.logger.Info("Updated error handler configuration")

	return nil
}

// Validate validates the error handler state
func (h *ErrorHandler) Validate() error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Validate configuration
	if err := validateErrorConfig(h.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	return nil
}

// generateErrorID generates a unique error ID
func generateErrorID() string {
	return fmt.Sprintf("err_%d", time.Now().UnixNano())
}

// getErrorType returns the type of an error
func getErrorType(err error) string {
	if err == nil {
		return "unknown"
	}

	errStr := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errStr, "connection refused"):
		return "connection_refused"
	case strings.Contains(errStr, "timeout"):
		return "timeout"
	case strings.Contains(errStr, "not found"):
		return "not_found"
	case strings.Contains(errStr, "permission denied"):
		return "permission_denied"
	case strings.Contains(errStr, "invalid argument"):
		return "invalid_argument"
	default:
		return "unknown"
	}
}

// getRetryableFromMetadata returns retryable flag from metadata
func getRetryableFromMetadata(metadata map[string]interface{}) bool {
	if retryable, ok := metadata["retryable"].(bool); ok {
		return retryable
	}
	return false
}
