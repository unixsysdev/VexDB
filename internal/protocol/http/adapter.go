
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
	"vexdb/internal/types"
	"go.uber.org/zap"
)

// HTTPAdapter implements the ProtocolAdapter interface for HTTP REST API
type HTTPAdapter struct {
	config     *HTTPConfig
	server     *http.Server
	listener   net.Listener
	logger     logging.Logger
	metrics    *metrics.IngestionMetrics
	validator  adapter.Validator
	handler    adapter.Handler
	protocol   string
	startTime  time.Time
	mu         sync.RWMutex
	shutdown   bool
}

// HTTPConfig represents the HTTP adapter configuration
type HTTPConfig struct {
	Host               string        `yaml:"host" json:"host"`
	Port               int           `yaml:"port" json:"port"`
	ReadTimeout        time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout       time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	MaxHeaderBytes     int           `yaml:"max_header_bytes" json:"max_header_bytes"`
	EnableMetrics      bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableHealth       bool          `yaml:"enable_health" json:"enable_health"`
	EnableCORS         bool          `yaml:"enable_cors" json:"enable_cors"`
	CORSOrigins        []string      `yaml:"cors_origins" json:"cors_origins"`
	CORSMethods        []string      `yaml:"cors_methods" json:"cors_methods"`
	CORSHeaders        []string      `yaml:"cors_headers" json:"cors_headers"`
	MaxRequestBodySize int64         `yaml:"max_request_body_size" json:"max_request_body_size"`
}

// DefaultHTTPConfig returns the default HTTP configuration
func DefaultHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		Host:               "0.0.0.0",
		Port:               8080,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		IdleTimeout:        60 * time.Second,
		MaxHeaderBytes:     1 << 20, // 1MB
		EnableMetrics:      true,
		EnableHealth:       true,
		EnableCORS:         true,
		CORSOrigins:        []string{"*"},
		CORSMethods:        []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSHeaders:        []string{"Content-Type", "Authorization"},
		MaxRequestBodySize: 10 << 20, // 10MB
	}
}

// NewHTTPAdapter creates a new HTTP adapter
func NewHTTPAdapter(cfg config.Config, logger logging.Logger, metrics *metrics.IngestionMetrics, validator adapter.Validator, handler adapter.Handler) (*HTTPAdapter, error) {
	httpConfig := DefaultHTTPConfig()
	
	if cfg != nil {
		if httpCfg, ok := cfg.Get("http"); ok {
			if cfgMap, ok := httpCfg.(map[string]interface{}); ok {
				if host, ok := cfgMap["host"].(string); ok {
					httpConfig.Host = host
				}
				if port, ok := cfgMap["port"].(int); ok {
					httpConfig.Port = port
				}
				if readTimeout, ok := cfgMap["read_timeout"].(int); ok {
					httpConfig.ReadTimeout = time.Duration(readTimeout) * time.Second
				}
				if writeTimeout, ok := cfgMap["write_timeout"].(int); ok {
					httpConfig.WriteTimeout = time.Duration(writeTimeout) * time.Second
				}
				if idleTimeout, ok := cfgMap["idle_timeout"].(int); ok {
					httpConfig.IdleTimeout = time.Duration(idleTimeout) * time.Second
				}
				if maxHeaderBytes, ok := cfgMap["max_header_bytes"].(int); ok {
					httpConfig.MaxHeaderBytes = maxHeaderBytes
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					httpConfig.EnableMetrics = enableMetrics
				}
				if enableHealth, ok := cfgMap["enable_health"].(bool); ok {
					httpConfig.EnableHealth = enableHealth
				}
				if enableCORS, ok := cfgMap["enable_cors"].(bool); ok {
					httpConfig.EnableCORS = enableCORS
				}
				if corsOrigins, ok := cfgMap["cors_origins"].([]interface{}); ok {
					for _, origin := range corsOrigins {
						if originStr, ok := origin.(string); ok {
							httpConfig.CORSOrigins = append(httpConfig.CORSOrigins, originStr)
						}
					}
				}
				if corsMethods, ok := cfgMap["cors_methods"].([]interface{}); ok {
					for _, method := range corsMethods {
						if methodStr, ok := method.(string); ok {
							httpConfig.CORSMethods = append(httpConfig.CORSMethods, methodStr)
						}
					}
				}
				if corsHeaders, ok := cfgMap["cors_headers"].([]interface{}); ok {
					for _, header := range corsHeaders {
						if headerStr, ok := header.(string); ok {
							httpConfig.CORSHeaders = append(httpConfig.CORSHeaders, headerStr)
						}
					}
				}
				if maxRequestBodySize, ok := cfgMap["max_request_body_size"].(int); ok {
					httpConfig.MaxRequestBodySize = int64(maxRequestBodySize)
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateHTTPConfig(httpConfig); err != nil {
		return nil, fmt.Errorf("invalid HTTP configuration: %w", err)
	}
	
	adapter := &HTTPAdapter{
		config:    httpConfig,
		logger:    logger,
		metrics:   metrics,
		validator: validator,
		handler:   handler,
		protocol:  "http",
		startTime: time.Now(),
	}
	
	// Create HTTP server
	adapter.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", httpConfig.Host, httpConfig.Port),
		ReadTimeout:  httpConfig.ReadTimeout,
		WriteTimeout: httpConfig.WriteTimeout,
		IdleTimeout:  httpConfig.IdleTimeout,
		MaxHeaderBytes: httpConfig.MaxHeaderBytes,
	}
	
	// Setup routes
	mux := http.NewServeMux()
	
	// Vector ingestion endpoints
	mux.HandleFunc("/api/v1/vectors", adapter.handleVectors)
	mux.HandleFunc("/api/v1/vectors/", adapter.handleVectorByID)
	
	// Batch operations
	mux.HandleFunc("/api/v1/batch", adapter.handleBatch)
	
	// Health check
	if httpConfig.EnableHealth {
		mux.HandleFunc("/health", adapter.handleHealth)
		mux.HandleFunc("/ready", adapter.handleReady)
	}
	
	// Metrics
	if httpConfig.EnableMetrics {
		mux.HandleFunc("/metrics", adapter.handleMetrics)
	}
	
	// Apply CORS middleware if enabled
	handler := http.Handler(mux)
	if httpConfig.EnableCORS {
		handler = adapter.corsMiddleware(handler)
	}
	
	// Apply metrics middleware if enabled
	if httpConfig.EnableMetrics {
		handler = adapter.metricsMiddleware(handler)
	}
	
	// Apply logging middleware
	handler = adapter.loggingMiddleware(handler)
	
	adapter.server.Handler = handler
	
	adapter.logger.Info("Created HTTP adapter",
		zap.String("host", httpConfig.Host),
		zap.Int("port", httpConfig.Port),
		zap.Duration("read_timeout", httpConfig.ReadTimeout),
		zap.Duration("write_timeout", httpConfig.WriteTimeout),
		zap.Bool("enable_cors", httpConfig.EnableCORS),
		zap.Bool("enable_health", httpConfig.EnableHealth),
		zap.Bool("enable_metrics", httpConfig.EnableMetrics))
	
	return adapter, nil
}

// validateHTTPConfig validates the HTTP configuration
func validateHTTPConfig(cfg *HTTPConfig) error {
	if cfg.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("read timeout must be positive")
	}
	if cfg.WriteTimeout <= 0 {
		return fmt.Errorf("write timeout must be positive")
	}
	if cfg.IdleTimeout <= 0 {
		return fmt.Errorf("idle timeout must be positive")
	}
	if cfg.MaxHeaderBytes <= 0 {
		return fmt.Errorf("max header bytes must be positive")
	}
	if cfg.MaxRequestBodySize <= 0 {
		return fmt.Errorf("max request body size must be positive")
	}
	
	return nil
}

// Start starts the HTTP adapter
func (a *HTTPAdapter) Start(ctx context.Context) error {
	a.logger.Info("Starting HTTP adapter", zap.String("address", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)))
	
	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port))
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	a.listener = listener
	
	// Start server in goroutine
	go func() {
		if err := a.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			a.logger.Error("HTTP server failed", zap.Error(err))
		}
	}()
	
	// Update metrics
	a.metrics.AdaptersStarted.Inc("http", "start")
	
	return nil
}

// Stop stops the HTTP adapter
func (a *HTTPAdapter) Stop(ctx context.Context) error {
	a.logger.Info("Stopping HTTP adapter")
	
	a.mu.Lock()
	a.shutdown = true
	a.mu.Unlock()
	
	// Graceful shutdown
	if a.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		
		if err := a.server.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("HTTP server shutdown failed", zap.Error(err))
			return err
		}
	}
	
	// Close listener
	if a.listener != nil {
		a.listener.Close()
	}
	
	// Update metrics
	a.metrics.AdaptersStopped.Inc("http", "stop")
	
	return nil
}

// GetProtocol returns the protocol name
func (a *HTTPAdapter) GetProtocol() string {
	return a.protocol
}

// GetAddress returns the adapter address
func (a *HTTPAdapter) GetAddress() string {
	return fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
}

// GetMetrics returns adapter metrics
func (a *HTTPAdapter) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"protocol":            a.protocol,
		"address":             a.GetAddress(),
		"uptime":              time.Since(a.startTime).String(),
		"read_timeout":        a.config.ReadTimeout.String(),
		"write_timeout":       a.config.WriteTimeout.String(),
		"idle_timeout":        a.config.IdleTimeout.String(),
		"max_header_bytes":    a.config.MaxHeaderBytes,
		"max_request_body_size": a.config.MaxRequestBodySize,
		"enable_cors":         a.config.EnableCORS,
		"enable_health":       a.config.EnableHealth,
		"enable_metrics":      a.config.EnableMetrics,
	}
}

// handleVectors handles vector operations (GET, POST)
func (a *HTTPAdapter) handleVectors(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		a.handleInsertVector(w, r)
	case http.MethodGet:
		a.handleListVectors(w, r)
	default:
		a.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleVectorByID handles individual vector operations
func (a *HTTPAdapter) handleVectorByID(w http.ResponseWriter, r *http.Request) {
	// Extract vector ID from URL path
	// Implementation depends on specific routing requirements
	a.writeErrorResponse(w, http.StatusNotImplemented, "Not implemented")
}

// handleBatch handles batch operations
func (a *HTTPAdapter) handleBatch(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		a.handleBatchInsert(w, r)
	default:
		a.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleHealth handles health check requests
func (a *HTTPAdapter) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	shutdown := a.shutdown
	a.mu.RUnlock()
	
	if shutdown {
		a.writeErrorResponse(w, http.StatusServiceUnavailable, "Service is shutting down")
		return
	}
	
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"protocol":  a.protocol,
		"address":   a.GetAddress(),
		"uptime":    time.Since(a.startTime).String(),
	}
	
	a.writeJSONResponse(w, http.StatusOK, response)
}

// handleReady handles readiness check requests
func (a *HTTPAdapter) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if the service is ready to accept requests
	// This could include checking database connections, etc.
	
	response := map[string]interface{}{
		"status":    "ready",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"protocol":  a.protocol,
		"address":   a.GetAddress(),
	}
	
	a.writeJSONResponse(w, http.StatusOK, response)
}

// handleMetrics handles metrics requests
func (a *HTTPAdapter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Return metrics in Prometheus format
	// Implementation depends on metrics collection system
	a.writeErrorResponse(w, http.StatusNotImplemented, "Metrics endpoint not implemented")
}

// handleInsertVector handles single vector insertion
func (a *HTTPAdapter) handleInsertVector(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var vector types.Vector
	if err := json.NewDecoder(r.Body).Decode(&vector); err != nil {
		a.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}
	
	// Validate vector
	if err := a.validator.ValidateVector(&vector); err != nil {
		a.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid vector: %v", err))
		return
	}
	
	// Handle vector insertion
	// Implementation depends on handler interface
	a.writeErrorResponse(w, http.StatusNotImplemented, "Vector insertion not implemented")
}

// handleListVectors handles vector listing
func (a *HTTPAdapter) handleListVectors(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query()
	limitStr := query.Get("limit")
	offsetStr := query.Get("offset")
	
	limit := 100 // default limit
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	
	offset := 0 // default offset
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}
	
	// Handle vector listing
	// Implementation depends on handler interface
	a.writeErrorResponse(w, http.StatusNotImplemented, "Vector listing not implemented")
}

// handleBatchInsert handles batch vector insertion
func (a *HTTPAdapter) handleBatchInsert(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var vectors []types.Vector
	if err := json.NewDecoder(r.Body).Decode(&vectors); err != nil {
		a.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}
	
	// Validate vectors
	for i, vector := range vectors {
		if err := a.validator.ValidateVector(&vector); err != nil {
			a.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid vector at index %d: %v", i, err))
			return
		}
	}
	
	// Handle batch insertion
	// Implementation depends on handler interface
	a.writeErrorResponse(w, http.StatusNotImplemented, "Batch insertion not implemented")
}

// writeJSONResponse writes a JSON response
func (a *HTTPAdapter) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(data); err != nil {
		a.logger.Error("Failed to encode JSON response", zap.Error(err))
	}
}

// writeErrorResponse writes an error response
func (a *HTTPAdapter) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	response := map[string]interface{}{
		"error":     message,
		"status":    statusCode,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	a.writeJSONResponse(w, statusCode, response)
}

// corsMiddleware applies CORS headers
func (a *HTTPAdapter) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", a.getCORSOrigin(r))
		w.Header().Set("Access-Control-Allow-Methods", a.getCORSMethods())
		w.Header().Set("Access-Control-Allow-Headers", a.getCORSHeaders())
		w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours
		
		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		
		next.ServeHTTP(w, r)
	})
}

// getCORSOrigin returns the appropriate CORS origin header
func (a *HTTPAdapter) getCORSOrigin(r *http.Request) string {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return "*"
	}
	
	// Check if origin is allowed
	for _, allowedOrigin := range a.config.CORSOrigins {
		if allowedOrigin == "*" || allowedOrigin == origin {
			return origin
		}
	}
	
	return ""
}

// getCORSMethods returns the CORS methods header
func (a *HTTPAdapter) getCORSMethods() string {
	if len(a.config.CORSMethods) == 0 {
		return "GET,POST,PUT,DELETE,OPTIONS"
	}
	
	result := ""
	for i, method := range a.config.CORSMethods {
		if i > 0 {
			result += ","
		}
		result += method
	}
	return result
}

// getCORSHeaders returns the CORS headers header
func (a *HTTPAdapter) getCORSHeaders() string {
	if len(a.config.CORSHeaders) == 0 {
		return "Content-Type,Authorization"
	}
	
	result := ""
	for i, header := range a.config.CORSHeaders {
		if i > 0 {
			result += ","
		}
		result += header
	}
	return result
}

// metricsMiddleware collects request metrics
func (a *HTTPAdapter) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w}
		
		next.ServeHTTP(wrapped, r)
		
		duration := time.Since(start)
		
		// Record metrics
		a.metrics.RequestDuration.Observe(duration.Seconds(), "http", r.Method+" "+r.URL.Path)
		a.metrics.RequestsTotal.Inc("http", r.Method+" "+r.URL.Path, strconv.Itoa(wrapped.status))
	})
}

// loggingMiddleware logs HTTP requests
func (a *HTTPAdapter) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w}
		
		a.logger.Info("HTTP request started",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()))
		
		next.ServeHTTP(wrapped, r)
		
		duration := time.Since(start)
		
		a.logger.Info("HTTP request completed",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapped.status),
			zap.Duration("duration", duration),
			zap.String("remote_addr", r.RemoteAddr))
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Write captures the status code if not already set
func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}