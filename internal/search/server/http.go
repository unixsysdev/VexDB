package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"vxdb/internal/metrics"
	"vxdb/internal/search/api"
	"vxdb/internal/search/auth"
	"vxdb/internal/search/config"
	"vxdb/internal/service"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// HTTPServer represents the HTTP server for the search service
type HTTPServer struct {
	config        *config.SearchServiceConfig
	logger        *zap.Logger
	metrics       *metrics.Collector
	searchService service.SearchService
	server        *http.Server
	router        *mux.Router
}

// NewHTTPServer creates a new HTTP server for the search service
func NewHTTPServer(cfg *config.SearchServiceConfig, logger *zap.Logger, metrics *metrics.Collector, searchService service.SearchService) *HTTPServer {
	return &HTTPServer{
		config:        cfg,
		logger:        logger,
		metrics:       metrics,
		searchService: searchService,
	}
}

// Start starts the HTTP server
func (s *HTTPServer) Start(ctx context.Context) error {
	if !s.config.API.HTTP.Enabled {
		s.logger.Info("HTTP server is disabled")
		return nil
	}

	s.logger.Info("Starting HTTP server", zap.String("address", s.getAddress()))

	// Create router
	s.router = mux.NewRouter()

	// Setup middleware
	s.setupMiddleware()

	// Setup routes
	s.setupRoutes()

	// Create HTTP server
	s.server = &http.Server{
		Addr:           s.getAddress(),
		Handler:        s.router,
		ReadTimeout:    s.config.Server.ReadTimeout,
		WriteTimeout:   s.config.Server.WriteTimeout,
		IdleTimeout:    s.config.Server.IdleTimeout,
		MaxHeaderBytes: s.config.Server.MaxHeaderBytes,
	}

	// Setup TLS if configured
	if s.config.API.HTTP.TLS != nil && s.config.API.HTTP.TLS.Enabled {
		tlsConfig, err := s.setupTLS()
		if err != nil {
			return fmt.Errorf("failed to setup TLS: %w", err)
		}
		s.server.TLSConfig = tlsConfig
	}

	// Start server in background
	go func() {
		var err error
		if s.server.TLSConfig != nil {
			err = s.server.ListenAndServeTLS("", "")
		} else {
			err = s.server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	s.logger.Info("HTTP server started successfully", zap.String("address", s.getAddress()))
	return nil
}

// Stop stops the HTTP server
func (s *HTTPServer) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.logger.Info("Stopping HTTP server")

	// Shutdown server with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		s.logger.Error("Failed to shutdown HTTP server gracefully", zap.Error(err))
		return err
	}

	s.logger.Info("HTTP server stopped successfully")
	return nil
}

// setupMiddleware sets up the HTTP middleware
func (s *HTTPServer) setupMiddleware() {
	// Logging middleware
	loggingMiddleware := auth.NewLoggingMiddleware(s.logger)
	s.router.Use(loggingMiddleware.LoggingMiddleware)

	// CORS middleware
	if s.config.API.HTTP.CORS.Enabled {
		corsMiddleware := auth.NewCORSMiddleware(&s.config.API.HTTP.CORS)
		s.router.Use(corsMiddleware.CORSMiddleware)
	}

	// Authentication middleware
	if s.config.Auth.Enabled {
		authMiddleware := auth.NewMiddleware(&s.config.Auth, s.logger)
		s.router.Use(authMiddleware.AuthMiddleware)
	}

	// Rate limiting middleware
	if s.config.RateLimit.Enabled {
		rateLimitMiddleware := auth.NewRateLimitMiddleware(&s.config.RateLimit, s.logger)
		s.router.Use(rateLimitMiddleware.RateLimitMiddleware)
	}

	// Metrics middleware
	metricsMiddleware := s.createMetricsMiddleware()
	s.router.Use(metricsMiddleware)

	// Recovery middleware
	recoveryMiddleware := s.createRecoveryMiddleware()
	s.router.Use(recoveryMiddleware)
}

// setupRoutes sets up the HTTP routes
func (s *HTTPServer) setupRoutes() {
	// Create search handler
	searchHandler := api.NewSearchHandler(s.config, s.logger, s.metrics, s.searchService)

	// Register API routes
	apiRouter := s.router.PathPrefix(s.config.API.HTTP.BasePath).Subrouter()
	searchHandler.RegisterRoutes(apiRouter)

	// Health check endpoint
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
	s.router.HandleFunc("/health/detailed", s.handleDetailedHealth).Methods("GET")

	// Metrics endpoint
	if s.config.Metrics.Enabled {
		s.router.HandleFunc(s.config.Metrics.Path, s.handleMetrics).Methods("GET")
	}

	// Root endpoint
	s.router.HandleFunc("/", s.handleRoot).Methods("GET")
}

// setupTLS sets up TLS configuration
func (s *HTTPServer) setupTLS() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Configure TLS settings
	if s.config.API.HTTP.TLS.CertFile != "" && s.config.API.HTTP.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(s.config.API.HTTP.TLS.CertFile, s.config.API.HTTP.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if s.config.API.HTTP.TLS.ClientAuth {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// createMetricsMiddleware creates the metrics middleware
func (s *HTTPServer) createMetricsMiddleware() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Create response writer wrapper to capture status code
			wrapper := &responseWriterWrapper{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Process request
			next.ServeHTTP(wrapper, r)

			// Record metrics
			duration := time.Since(start)
			s.metrics.RecordHistogram("http_request_duration_seconds", duration.Seconds(), map[string]string{
				"method": r.Method,
				"path":   r.URL.Path,
				"status": fmt.Sprintf("%d", wrapper.statusCode),
			})

			s.metrics.RecordCounter("http_requests_total", 1, map[string]string{
				"method": r.Method,
				"path":   r.URL.Path,
				"status": fmt.Sprintf("%d", wrapper.statusCode),
			})
		})
	}
}

// createRecoveryMiddleware creates the recovery middleware
func (s *HTTPServer) createRecoveryMiddleware() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					s.logger.Error("HTTP server panic recovered",
						zap.Any("error", err),
						zap.String("path", r.URL.Path),
						zap.String("method", r.Method),
					)

					// Send error response
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)

					// Simple JSON encoding
					jsonResponse := fmt.Sprintf(`{"success":false,"error":"internal_server_error","message":"An internal server error occurred","timestamp":"%s"}`,
						time.Now().Format(time.RFC3339))

					w.Write([]byte(jsonResponse))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// Handler functions

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	health, err := s.searchService.HealthCheck(ctx, false)
	if err != nil {
		s.logger.Error("Health check failed", zap.Error(err))
		http.Error(w, "Health check failed", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"healthy":   health.Healthy,
		"status":    health.Status,
		"message":   health.Message,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	s.sendJSONResponse(w, http.StatusOK, response)
}

func (s *HTTPServer) handleDetailedHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	health, err := s.searchService.HealthCheck(ctx, true)
	if err != nil {
		s.logger.Error("Detailed health check failed", zap.Error(err))
		http.Error(w, "Health check failed", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"healthy":   health.Healthy,
		"status":    health.Status,
		"message":   health.Message,
		"details":   health.Details,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	s.sendJSONResponse(w, http.StatusOK, response)
}

func (s *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Get metrics from the metrics collector
	// For now, just return a simple response
	// In a real implementation, this would get metrics from the metrics collector

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("# VxDB Search Service Metrics\n"))
}

func (s *HTTPServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"service":   "VxDB Search Service",
		"version":   "1.0.0",
		"status":    "running",
		"timestamp": time.Now().Format(time.RFC3339),
		"endpoints": map[string]interface{}{
			"health":          "/health",
			"health_detailed": "/health/detailed",
			"metrics":         s.config.Metrics.Path,
			"api":             s.config.API.HTTP.BasePath,
		},
	}

	s.sendJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (s *HTTPServer) getAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)
}

func (s *HTTPServer) sendJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	encoder := json.NewEncoder(w)
	if s.config.API.Response.PrettyPrint {
		encoder.SetIndent("", "  ")
	}

	if err := encoder.Encode(data); err != nil {
		s.logger.Error("Failed to encode JSON response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// responseWriterWrapper wraps http.ResponseWriter to capture status code
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWrapper) Write(data []byte) (int, error) {
	return w.ResponseWriter.Write(data)
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	CertFile   string `yaml:"cert_file" json:"cert_file"`
	KeyFile    string `yaml:"key_file" json:"key_file"`
	ClientAuth bool   `yaml:"client_auth" json:"client_auth"`
}
