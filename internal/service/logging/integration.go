package logging

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
)

var (
	ErrLoggerNotInitialized = errors.New("logger not initialized")
	ErrInvalidLogConfig     = errors.New("invalid logging configuration")
	ErrLogRotationFailed    = errors.New("log rotation failed")
	ErrLogWriteFailed       = errors.New("log write failed")
)

// LoggingConfig represents the logging configuration
type LoggingConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	Level          string        `yaml:"level" json:"level"`
	Format         string        `yaml:"format" json:"format"`
	Output         string        `yaml:"output" json:"output"`
	OutputPath     string        `yaml:"output_path" json:"output_path"`
	MaxSize        int64         `yaml:"max_size" json:"max_size"`
	MaxBackups     int           `yaml:"max_backups" json:"max_backups"`
	MaxAge         int           `yaml:"max_age" json:"max_age"`
	Compress       bool          `yaml:"compress" json:"compress"`
	RotateInterval time.Duration `yaml:"rotate_interval" json:"rotate_interval"`
	EnableCaller   bool          `yaml:"enable_caller" json:"enable_caller"`
	EnableStack    bool          `yaml:"enable_stack" json:"enable_stack"`
	EnableContext  bool          `yaml:"enable_context" json:"enable_context"`
	EnableMetrics  bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultLoggingConfig returns the default logging configuration
func DefaultLoggingConfig() *LoggingConfig {
	return &LoggingConfig{
		Enabled:        true,
		Level:          "info",
		Format:         "json",
		Output:         "stdout",
		OutputPath:     "logs/vexstorage.log",
		MaxSize:        100 * 1024 * 1024, // 100MB
		MaxBackups:     5,
		MaxAge:         30, // days
		Compress:       true,
		RotateInterval: 24 * time.Hour,
		EnableCaller:   true,
		EnableStack:    false,
		EnableContext:  true,
		EnableMetrics:  true,
	}
}

// LoggingIntegration represents a logging integration
type LoggingIntegration struct {
	config     *LoggingConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Log rotation
	rotateChan chan struct{}
	rotateDone chan struct{}
	
	// Lifecycle
	started    bool
	stopped    bool
	mu         sync.RWMutex
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Caller    *CallerInfo            `json:"caller,omitempty"`
	Stack     string                 `json:"stack,omitempty"`
	Context   map[string]interface{} `json:"context,omitempty"`
}

// CallerInfo represents caller information
type CallerInfo struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

// NewLoggingIntegration creates a new logging integration
func NewLoggingIntegration(cfg *config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*LoggingIntegration, error) {
	loggingConfig := DefaultLoggingConfig()
	
	if cfg != nil {
		if loggingCfg, ok := cfg.Get("logging"); ok {
			if cfgMap, ok := loggingCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					loggingConfig.Enabled = enabled
				}
				if level, ok := cfgMap["level"].(string); ok {
					loggingConfig.Level = level
				}
				if format, ok := cfgMap["format"].(string); ok {
					loggingConfig.Format = format
				}
				if output, ok := cfgMap["output"].(string); ok {
					loggingConfig.Output = output
				}
				if outputPath, ok := cfgMap["output_path"].(string); ok {
					loggingConfig.OutputPath = outputPath
				}
				if maxSize, ok := cfgMap["max_size"].(int64); ok {
					loggingConfig.MaxSize = maxSize
				}
				if maxBackups, ok := cfgMap["max_backups"].(int); ok {
					loggingConfig.MaxBackups = maxBackups
				}
				if maxAge, ok := cfgMap["max_age"].(int); ok {
					loggingConfig.MaxAge = maxAge
				}
				if compress, ok := cfgMap["compress"].(bool); ok {
					loggingConfig.Compress = compress
				}
				if rotateInterval, ok := cfgMap["rotate_interval"].(string); ok {
					if duration, err := time.ParseDuration(rotateInterval); err == nil {
						loggingConfig.RotateInterval = duration
					}
				}
				if enableCaller, ok := cfgMap["enable_caller"].(bool); ok {
					loggingConfig.EnableCaller = enableCaller
				}
				if enableStack, ok := cfgMap["enable_stack"].(bool); ok {
					loggingConfig.EnableStack = enableStack
				}
				if enableContext, ok := cfgMap["enable_context"].(bool); ok {
					loggingConfig.EnableContext = enableContext
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					loggingConfig.EnableMetrics = enableMetrics
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateLoggingConfig(loggingConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLogConfig, err)
	}
	
	integration := &LoggingIntegration{
		config:     loggingConfig,
		logger:     logger,
		metrics:    metrics,
		rotateChan: make(chan struct{}),
		rotateDone: make(chan struct{}),
	}
	
	integration.logger.Info("Created logging integration",
		"enabled", loggingConfig.Enabled,
		"level", loggingConfig.Level,
		"format", loggingConfig.Format,
		"output", loggingConfig.Output,
		"output_path", loggingConfig.OutputPath)
	
	return integration, nil
}

// validateLoggingConfig validates the logging configuration
func validateLoggingConfig(cfg *LoggingConfig) error {
	if cfg.Level == "" {
		return errors.New("log level cannot be empty")
	}
	
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
		"fatal": true,
		"panic": true,
	}
	
	if !validLevels[cfg.Level] {
		return fmt.Errorf("invalid log level: %s", cfg.Level)
	}
	
	if cfg.Format == "" {
		return errors.New("log format cannot be empty")
	}
	
	validFormats := map[string]bool{
		"json": true,
		"text": true,
		"console": true,
	}
	
	if !validFormats[cfg.Format] {
		return fmt.Errorf("invalid log format: %s", cfg.Format)
	}
	
	if cfg.Output == "" {
		return errors.New("log output cannot be empty")
	}
	
	validOutputs := map[string]bool{
		"stdout": true,
		"stderr": true,
		"file":   true,
	}
	
	if !validOutputs[cfg.Output] {
		return fmt.Errorf("invalid log output: %s", cfg.Output)
	}
	
	if cfg.Output == "file" && cfg.OutputPath == "" {
		return errors.New("output path cannot be empty when output is file")
	}
	
	if cfg.MaxSize <= 0 {
		return errors.New("max size must be positive")
	}
	
	if cfg.MaxBackups < 0 {
		return errors.New("max backups cannot be negative")
	}
	
	if cfg.MaxAge <= 0 {
		return errors.New("max age must be positive")
	}
	
	if cfg.RotateInterval <= 0 {
		return errors.New("rotate interval must be positive")
	}
	
	return nil
}

// Start starts the logging integration
func (l *LoggingIntegration) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if l.started {
		return nil
	}
	
	if !l.config.Enabled {
		l.logger.Info("Logging integration is disabled")
		return nil
	}
	
	// Create log directory if needed
	if l.config.Output == "file" {
		if err := l.createLogDirectory(); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}
	}
	
	// Start log rotation
	go l.startLogRotation()
	
	l.started = true
	l.stopped = false
	
	l.logger.Info("Started logging integration")
	
	return nil
}

// Stop stops the logging integration
func (l *LoggingIntegration) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if l.stopped {
		return nil
	}
	
	if !l.started {
		return ErrLoggerNotInitialized
	}
	
	// Stop log rotation
	close(l.rotateDone)
	
	l.stopped = true
	l.started = false
	
	l.logger.Info("Stopped logging integration")
	
	return nil
}

// IsRunning checks if the logging integration is running
func (l *LoggingIntegration) IsRunning() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	return l.started && !l.stopped
}

// GetLogger returns the underlying logger
func (l *LoggingIntegration) GetLogger() logging.Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	return l.logger
}

// GetConfig returns the logging configuration
func (l *LoggingIntegration) GetConfig() *LoggingConfig {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	// Return a copy of config
	config := *l.config
	return &config
}

// UpdateConfig updates the logging configuration
func (l *LoggingIntegration) UpdateConfig(config *LoggingConfig) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// Validate new configuration
	if err := validateLoggingConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidLogConfig, err)
	}
	
	l.config = config
	
	l.logger.Info("Updated logging integration configuration", "config", config)
	
	return nil
}

// LogWithContext logs a message with context
func (l *LoggingIntegration) LogWithContext(ctx context.Context, level, message string, fields map[string]interface{}) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if !l.config.Enabled {
		return
	}
	
	// Create log entry
	entry := &LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Fields:    fields,
	}
	
	// Add caller information if enabled
	if l.config.EnableCaller {
		entry.Caller = l.getCallerInfo()
	}
	
	// Add stack trace if enabled and level is error or higher
	if l.config.EnableStack && (level == "error" || level == "fatal" || level == "panic") {
		entry.Stack = l.getStackTrace()
	}
	
	// Add context if enabled
	if l.config.EnableContext && ctx != nil {
		entry.Context = l.extractContext(ctx)
	}
	
	// Log the entry
	l.logEntry(entry)
	
	// Update metrics if enabled
	if l.config.EnableMetrics && l.metrics != nil {
		l.updateLogMetrics(level)
	}
}

// Debug logs a debug message
func (l *LoggingIntegration) Debug(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "debug", message, fields)
}

// DebugWithContext logs a debug message with context
func (l *LoggingIntegration) DebugWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "debug", message, fields)
}

// Info logs an info message
func (l *LoggingIntegration) Info(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "info", message, fields)
}

// InfoWithContext logs an info message with context
func (l *LoggingIntegration) InfoWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "info", message, fields)
}

// Warn logs a warning message
func (l *LoggingIntegration) Warn(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "warn", message, fields)
}

// WarnWithContext logs a warning message with context
func (l *LoggingIntegration) WarnWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "warn", message, fields)
}

// Error logs an error message
func (l *LoggingIntegration) Error(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "error", message, fields)
}

// ErrorWithContext logs an error message with context
func (l *LoggingIntegration) ErrorWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "error", message, fields)
}

// Fatal logs a fatal message
func (l *LoggingIntegration) Fatal(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "fatal", message, fields)
	os.Exit(1)
}

// FatalWithContext logs a fatal message with context
func (l *LoggingIntegration) FatalWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "fatal", message, fields)
	os.Exit(1)
}

// Panic logs a panic message
func (l *LoggingIntegration) Panic(message string, fields map[string]interface{}) {
	l.LogWithContext(context.Background(), "panic", message, fields)
	panic(message)
}

// PanicWithContext logs a panic message with context
func (l *LoggingIntegration) PanicWithContext(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, "panic", message, fields)
	panic(message)
}

// RotateLogs forces log rotation
func (l *LoggingIntegration) RotateLogs() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if !l.config.Enabled {
		return nil
	}
	
	if l.config.Output != "file" {
		return nil
	}
	
	// Trigger rotation
	l.rotateChan <- struct{}{}
	
	return nil
}

// Validate validates the logging integration state
func (l *LoggingIntegration) Validate() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	// Validate configuration
	if err := validateLoggingConfig(l.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// createLogDirectory creates the log directory
func (l *LoggingIntegration) createLogDirectory() error {
	dir := filepath.Dir(l.config.OutputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	return nil
}

// startLogRotation starts the log rotation process
func (l *LoggingIntegration) startLogRotation() {
	if l.config.Output != "file" {
		return
	}
	
	ticker := time.NewTicker(l.config.RotateInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := l.rotateLogs(); err != nil {
				l.logger.Error("Failed to rotate logs", "error", err)
			}
		case <-l.rotateChan:
			if err := l.rotateLogs(); err != nil {
				l.logger.Error("Failed to rotate logs", "error", err)
			}
		case <-l.rotateDone:
			return
		}
	}
}

// rotateLogs rotates the log files
func (l *LoggingIntegration) rotateLogs() error {
	// This is a simplified log rotation implementation
	// In a real implementation, you would use a proper log rotation library
	
	l.logger.Info("Rotating logs", "path", l.config.OutputPath)
	
	// Check if file exists and needs rotation
	if info, err := os.Stat(l.config.OutputPath); err == nil {
		if info.Size() >= l.config.MaxSize {
			// Create backup filename
			timestamp := time.Now().Format("20060102_150405")
			backupPath := fmt.Sprintf("%s.%s", l.config.OutputPath, timestamp)
			
			// Rename current log file
			if err := os.Rename(l.config.OutputPath, backupPath); err != nil {
				return fmt.Errorf("%w: %v", ErrLogRotationFailed, err)
			}
			
			// Compress backup if enabled
			if l.config.Compress {
				go l.compressLogFile(backupPath)
			}
			
			// Clean up old backups
			go l.cleanupOldBackups()
		}
	}
	
	return nil
}

// compressLogFile compresses a log file
func (l *LoggingIntegration) compressLogFile(path string) {
	// This is a placeholder for log file compression
	// In a real implementation, you would use gzip or similar
	l.logger.Debug("Compressing log file", "path", path)
}

// cleanupOldBackups cleans up old log backups
func (l *LoggingIntegration) cleanupOldBackups() {
	// This is a placeholder for cleaning up old backups
	// In a real implementation, you would remove old backup files
	l.logger.Debug("Cleaning up old log backups")
}

// getCallerInfo gets caller information
func (l *LoggingIntegration) getCallerInfo() *CallerInfo {
	_, file, line, ok := runtime.Caller(3) // Skip 3 frames to get the actual caller
	if !ok {
		return nil
	}
	
	return &CallerInfo{
		File:     filepath.Base(file),
		Line:     line,
		Function: l.getFunctionName(file, line),
	}
}

// getFunctionName gets the function name from file and line
func (l *LoggingIntegration) getFunctionName(file string, line int) string {
	// This is a simplified implementation
	// In a real implementation, you would use runtime.Callers to get the function name
	return "unknown"
}

// getStackTrace gets the stack trace
func (l *LoggingIntegration) getStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// extractContext extracts context information
func (l *LoggingIntegration) extractContext(ctx context.Context) map[string]interface{} {
	contextMap := make(map[string]interface{})
	
	// Extract common context values
	if deadline, ok := ctx.Deadline(); ok {
		contextMap["deadline"] = deadline.Format(time.RFC3339)
	}
	
	if err := ctx.Err(); err != nil {
		contextMap["error"] = err.Error()
	}
	
	// Extract request ID if present
	if requestID := ctx.Value("request_id"); requestID != nil {
		contextMap["request_id"] = requestID
	}
	
	// Extract user ID if present
	if userID := ctx.Value("user_id"); userID != nil {
		contextMap["user_id"] = userID
	}
	
	// Extract trace ID if present
	if traceID := ctx.Value("trace_id"); traceID != nil {
		contextMap["trace_id"] = traceID
	}
	
	return contextMap
}

// logEntry logs a log entry
func (l *LoggingIntegration) logEntry(entry *LogEntry) {
	switch entry.Level {
	case "debug":
		l.logger.Debug(entry.Message, entry.Fields)
	case "info":
		l.logger.Info(entry.Message, entry.Fields)
	case "warn":
		l.logger.Warn(entry.Message, entry.Fields)
	case "error":
		l.logger.Error(entry.Message, entry.Fields)
	case "fatal":
		l.logger.Fatal(entry.Message, entry.Fields)
	case "panic":
		l.logger.Panic(entry.Message, entry.Fields)
	default:
		l.logger.Info(entry.Message, entry.Fields)
	}
}

// updateLogMetrics updates log metrics
func (l *LoggingIntegration) updateLogMetrics(level string) {
	switch level {
	case "debug":
		l.metrics.ServiceErrors.WithLabelValues("logging", "debug").Inc()
	case "info":
		l.metrics.ServiceErrors.WithLabelValues("logging", "info").Inc()
	case "warn":
		l.metrics.ServiceErrors.WithLabelValues("logging", "warn").Inc()
	case "error":
		l.metrics.ServiceErrors.WithLabelValues("logging", "error").Inc()
	case "fatal":
		l.metrics.ServiceErrors.WithLabelValues("logging", "fatal").Inc()
	case "panic":
		l.metrics.ServiceErrors.WithLabelValues("logging", "panic").Inc()
	}
}