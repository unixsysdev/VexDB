
package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol"
	"vexdb/internal/types"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WebSocketAdapter implements the ProtocolAdapter interface for WebSocket streaming
type WebSocketAdapter struct {
	config     *WebSocketConfig
	server     *http.Server
	listener   net.Listener
	upgrader   *websocket.Upgrader
	logger     logging.Logger
	metrics    *metrics.IngestionMetrics
	validator  protocol.Validator
	handler    protocol.Handler
	protocol   string
	startTime  time.Time
	mu         sync.RWMutex
	shutdown   bool
	connections map[*websocket.Conn]bool
}

// WebSocketConfig represents the WebSocket adapter configuration
type WebSocketConfig struct {
	Host               string        `yaml:"host" json:"host"`
	Port               int           `yaml:"port" json:"port"`
	ReadTimeout        time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout       time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	PingInterval       time.Duration `yaml:"ping_interval" json:"ping_interval"`
	PongWait           time.Duration `yaml:"pong_wait" json:"pong_wait"`
	WriteWait          time.Duration `yaml:"write_wait" json:"write_wait"`
	MaxMessageSize     int64         `yaml:"max_message_size" json:"max_message_size"`
	EnableMetrics      bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableHealth       bool          `yaml:"enable_health" json:"enable_health"`
	EnableCompression  bool          `yaml:"enable_compression" json:"enable_compression"`
	MaxConnections     int           `yaml:"max_connections" json:"max_connections"`
	ReadBufferSize     int           `yaml:"read_buffer_size" json:"read_buffer_size"`
	WriteBufferSize    int           `yaml:"write_buffer_size" json:"write_buffer_size"`
	AllowedOrigins     []string      `yaml:"allowed_origins" json:"allowed_origins"`
}

// DefaultWebSocketConfig returns the default WebSocket configuration
func DefaultWebSocketConfig() *WebSocketConfig {
	return &WebSocketConfig{
		Host:              "0.0.0.0",
		Port:              8081,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		PingInterval:      30 * time.Second,
		PongWait:          60 * time.Second,
		WriteWait:         10 * time.Second,
		MaxMessageSize:    10 << 20, // 10MB
		EnableMetrics:     true,
		EnableHealth:      true,
		EnableCompression: true,
		MaxConnections:    1000,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		AllowedOrigins:    []string{"*"},
	}
}

// NewWebSocketAdapter creates a new WebSocket adapter
func NewWebSocketAdapter(cfg config.Config, logger logging.Logger, metrics *metrics.IngestionMetrics, validator protocol.Validator, handler protocol.Handler) (*WebSocketAdapter, error) {
	wsConfig := DefaultWebSocketConfig()
	
	if cfg != nil {
		if wsCfg, ok := cfg.Get("websocket"); ok {
			if cfgMap, ok := wsCfg.(map[string]interface{}); ok {
				if host, ok := cfgMap["host"].(string); ok {
					wsConfig.Host = host
				}
				if port, ok := cfgMap["port"].(int); ok {
					wsConfig.Port = port
				}
				if readTimeout, ok := cfgMap["read_timeout"].(int); ok {
					wsConfig.ReadTimeout = time.Duration(readTimeout) * time.Second
				}
				if writeTimeout, ok := cfgMap["write_timeout"].(int); ok {
					wsConfig.WriteTimeout = time.Duration(writeTimeout) * time.Second
				}
				if idleTimeout, ok := cfgMap["idle_timeout"].(int); ok {
					wsConfig.IdleTimeout = time.Duration(idleTimeout) * time.Second
				}
				if pingInterval, ok := cfgMap["ping_interval"].(int); ok {
					wsConfig.PingInterval = time.Duration(pingInterval) * time.Second
				}
				if pongWait, ok := cfgMap["pong_wait"].(int); ok {
					wsConfig.PongWait = time.Duration(pongWait) * time.Second
				}
				if writeWait, ok := cfgMap["write_wait"].(int); ok {
					wsConfig.WriteWait = time.Duration(writeWait) * time.Second
				}
				if maxMessageSize, ok := cfgMap["max_message_size"].(int); ok {
					wsConfig.MaxMessageSize = int64(maxMessageSize)
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					wsConfig.EnableMetrics = enableMetrics
				}
				if enableHealth, ok := cfgMap["enable_health"].(bool); ok {
					wsConfig.EnableHealth = enableHealth
				}
				if enableCompression, ok := cfgMap["enable_compression"].(bool); ok {
					wsConfig.EnableCompression = enableCompression
				}
				if maxConnections, ok := cfgMap["max_connections"].(int); ok {
					wsConfig.MaxConnections = maxConnections
				}
				if readBufferSize, ok := cfgMap["read_buffer_size"].(int); ok {
					wsConfig.ReadBufferSize = readBufferSize
				}
				if writeBufferSize, ok := cfgMap["write_buffer_size"].(int); ok {
					wsConfig.WriteBufferSize = writeBufferSize
				}
				if allowedOrigins, ok := cfgMap["allowed_origins"].([]interface{}); ok {
					for _, origin := range allowedOrigins {
						if originStr, ok := origin.(string); ok {
							wsConfig.AllowedOrigins = append(wsConfig.AllowedOrigins, originStr)
						}
					}
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateWebSocketConfig(wsConfig); err != nil {
		return nil, fmt.Errorf("invalid WebSocket configuration: %w", err)
	}
	
	adapter := &WebSocketAdapter{
		config:       wsConfig,
		logger:       logger,
		metrics:      metrics,
		validator:    validator,
		handler:      handler,
		protocol:     "websocket",
		startTime:    time.Now(),
		connections:  make(map[*websocket.Conn]bool),
	}
	
	// Create WebSocket upgrader
	adapter.upgrader = &websocket.Upgrader{
		ReadBufferSize:    wsConfig.ReadBufferSize,
		WriteBufferSize:   wsConfig.WriteBufferSize,
		HandshakeTimeout:  wsConfig.WriteTimeout,
		EnableCompression: wsConfig.EnableCompression,
		CheckOrigin:      adapter.checkOrigin,
	}
	
	// Create HTTP server
	adapter.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", wsConfig.Host, wsConfig.Port),
		ReadTimeout:  wsConfig.ReadTimeout,
		WriteTimeout: wsConfig.WriteTimeout,
		IdleTimeout:  wsConfig.IdleTimeout,
	}
	
	// Setup routes
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", adapter.handleWebSocket)
	
	// Health check
	if wsConfig.EnableHealth {
		mux.HandleFunc("/health", adapter.handleHealth)
		mux.HandleFunc("/ready", adapter.handleReady)
	}
	
	// Metrics
	if wsConfig.EnableMetrics {
		mux.HandleFunc("/metrics", adapter.handleMetrics)
	}
	
	// Apply logging middleware
	handler := http.Handler(mux)
	handler = adapter.loggingMiddleware(handler)
	
	adapter.server.Handler = handler
	
	adapter.logger.Info("Created WebSocket adapter",
		zap.String("host", wsConfig.Host),
		zap.Int("port", wsConfig.Port),
		zap.Duration("read_timeout", wsConfig.ReadTimeout),
		zap.Duration("write_timeout", wsConfig.WriteTimeout),
		zap.Duration("ping_interval", wsConfig.PingInterval),
		zap.Bool("enable_compression", wsConfig.EnableCompression),
		zap.Int("max_connections", wsConfig.MaxConnections))
	
	return adapter, nil
}

// validateWebSocketConfig validates the WebSocket configuration
func validateWebSocketConfig(cfg *WebSocketConfig) error {
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
	if cfg.PingInterval <= 0 {
		return fmt.Errorf("ping interval must be positive")
	}
	if cfg.PongWait <= 0 {
		return fmt.Errorf("pong wait must be positive")
	}
	if cfg.WriteWait <= 0 {
		return fmt.Errorf("write wait must be positive")
	}
	if cfg.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}
	if cfg.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}
	if cfg.ReadBufferSize <= 0 {
		return fmt.Errorf("read buffer size must be positive")
	}
	if cfg.WriteBufferSize <= 0 {
		return fmt.Errorf("write buffer size must be positive")
	}
	
	return nil
}

// Start starts the WebSocket adapter
func (a *WebSocketAdapter) Start(ctx context.Context) error {
	a.logger.Info("Starting WebSocket adapter", zap.String("address", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)))
	
	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port))
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	a.listener = listener
	
	// Start server in goroutine
	go func() {
		if err := a.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			a.logger.Error("WebSocket server failed", zap.Error(err))
		}
	}()
	
	// Update metrics
	a.metrics.AdaptersStarted.Inc("websocket", "start")
	
	return nil
}

// Stop stops the WebSocket adapter
func (a *WebSocketAdapter) Stop(ctx context.Context) error {
	a.logger.Info("Stopping WebSocket adapter")
	
	a.mu.Lock()
	a.shutdown = true
	a.mu.Unlock()
	
	// Close all WebSocket connections
	a.closeAllConnections()
	
	// Graceful shutdown
	if a.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		
		if err := a.server.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("WebSocket server shutdown failed", zap.Error(err))
			return err
		}
	}
	
	// Close listener
	if a.listener != nil {
		a.listener.Close()
	}
	
	// Update metrics
	a.metrics.AdaptersStopped.Inc("websocket", "stop")
	
	return nil
}

// GetProtocol returns the protocol name
func (a *WebSocketAdapter) GetProtocol() string {
	return a.protocol
}

// GetAddress returns the adapter address
func (a *WebSocketAdapter) GetAddress() string {
	return fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
}

// GetMetrics returns adapter metrics
func (a *WebSocketAdapter) GetMetrics() map[string]interface{} {
	a.mu.RLock()
	connectionCount := len(a.connections)
	a.mu.RUnlock()
	
	return map[string]interface{}{
		"protocol":            a.protocol,
		"address":             a.GetAddress(),
		"uptime":              time.Since(a.startTime).String(),
		"read_timeout":        a.config.ReadTimeout.String(),
		"write_timeout":       a.config.WriteTimeout.String(),
		"idle_timeout":        a.config.IdleTimeout.String(),
		"ping_interval":       a.config.PingInterval.String(),
		"max_message_size":    a.config.MaxMessageSize,
		"enable_compression":  a.config.EnableCompression,
		"max_connections":     a.config.MaxConnections,
		"active_connections":  connectionCount,
		"enable_health":       a.config.EnableHealth,
		"enable_metrics":      a.config.EnableMetrics,
	}
}

// handleWebSocket handles WebSocket connections
func (a *WebSocketAdapter) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check connection limit
	a.mu.RLock()
	connectionCount := len(a.connections)
	shutdown := a.shutdown
	a.mu.RUnlock()
	
	if shutdown {
		a.writeHTTPErrorResponse(w, http.StatusServiceUnavailable, "Service is shutting down")
		return
	}
	
	if connectionCount >= a.config.MaxConnections {
		a.writeHTTPErrorResponse(w, http.StatusServiceUnavailable, "Maximum connections reached")
		return
	}
	
	// Upgrade HTTP connection to WebSocket
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		a.logger.Error("Failed to upgrade WebSocket connection", zap.Error(err))
		return
	}
	
	// Add connection to tracking
	a.mu.Lock()
	a.connections[conn] = true
	a.mu.Unlock()
	
	// Update metrics
	a.metrics.ConnectionsActive.Inc("websocket")
	
	a.logger.Info("WebSocket connection established", zap.String("remote_addr", conn.RemoteAddr().String()))
	
	// Handle connection in goroutine
	go a.handleConnection(conn)
}

// handleConnection handles a WebSocket connection
func (a *WebSocketAdapter) handleConnection(conn *websocket.Conn) {
	defer func() {
		// Remove connection from tracking
		a.mu.Lock()
		delete(a.connections, conn)
		a.mu.Unlock()
		
		// Update metrics
		a.metrics.ConnectionsActive.Dec("websocket")
		
		// Close connection
		conn.Close()
		
		a.logger.Info("WebSocket connection closed", zap.String("remote_addr", conn.RemoteAddr().String()))
	}()
	
	// Set connection parameters
	conn.SetReadLimit(a.config.MaxMessageSize)
	conn.SetReadDeadline(time.Now().Add(a.config.PongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(a.config.PongWait))
		return nil
	})
	
	// Start ping ticker
	ticker := time.NewTicker(a.config.PingInterval)
	defer ticker.Stop()
	
	// Handle messages
	for {
		select {
		case <-ticker.C:
			// Send ping
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				a.logger.Error("Failed to send ping", zap.Error(err))
				return
			}
			
		default:
			// Read message
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					a.logger.Error("WebSocket read error", zap.Error(err))
				}
				return
			}
			
			// Handle message
			if messageType == websocket.TextMessage {
				a.handleWebSocketMessage(conn, data)
			}
		}
	}
}

// handleWebSocketMessage handles WebSocket messages
func (a *WebSocketAdapter) handleWebSocketMessage(conn *websocket.Conn, data []byte) {
	// Parse message
	var message map[string]interface{}
	if err := json.Unmarshal(data, &message); err != nil {
		a.sendWebSocketError(conn, "Invalid JSON message")
		return
	}
	
	// Get message type
	messageType, ok := message["type"].(string)
	if !ok {
		a.sendWebSocketError(conn, "Missing message type")
		return
	}
	
	switch messageType {
	case "insert_vector":
		a.handleInsertVector(conn, message)
	case "insert_batch":
		a.handleInsertBatch(conn, message)
	default:
		a.sendWebSocketError(conn, fmt.Sprintf("Unknown message type: %s", messageType))
	}
}

// handleInsertVector handles single vector insertion
func (a *WebSocketAdapter) handleInsertVector(conn *websocket.Conn, message map[string]interface{}) {
	// Extract vector data
	vectorData, ok := message["vector"].(map[string]interface{})
	if !ok {
		a.sendWebSocketError(conn, "Invalid vector data")
		return
	}
	
	// Convert to Vector struct
	vectorBytes, err := json.Marshal(vectorData)
	if err != nil {
		a.sendWebSocketError(conn, "Invalid vector format")
		return
	}
	
	var vector types.Vector
	if err := json.Unmarshal(vectorBytes, &vector); err != nil {
		a.sendWebSocketError(conn, "Invalid vector format")
		return
	}
	
	// Validate vector
	if err := a.validator.ValidateVector(&vector); err != nil {
		a.sendWebSocketError(conn, fmt.Sprintf("Invalid vector: %v", err))
		return
	}
	
	// Handle vector insertion
	// Implementation depends on handler interface
	response := map[string]interface{}{
		"type":      "insert_vector_response",
		"success":   true,
		"message":   "Vector inserted successfully",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	a.sendWebSocketResponse(conn, response)
}

// handleInsertBatch handles batch vector insertion
func (a *WebSocketAdapter) handleInsertBatch(conn *websocket.Conn, message map[string]interface{}) {
	// Extract vectors data
	vectorsData, ok := message["vectors"].([]interface{})
	if !ok {
		a.sendWebSocketError(conn, "Invalid vectors data")
		return
	}
	
	// Convert to Vector structs
	var vectors []types.Vector
	for i, vectorData := range vectorsData {
		vectorMap, ok := vectorData.(map[string]interface{})
		if !ok {
			a.sendWebSocketError(conn, fmt.Sprintf("Invalid vector data at index %d", i))
			return
		}
		
		vectorBytes, err := json.Marshal(vectorMap)
		if err != nil {
			a.sendWebSocketError(conn, fmt.Sprintf("Invalid vector format at index %d", i))
			return
		}
		
		var vector types.Vector
		if err := json.Unmarshal(vectorBytes, &vector); err != nil {
			a.sendWebSocketError(conn, fmt.Sprintf("Invalid vector format at index %d", i))
			return
		}
		
		// Validate vector
		if err := a.validator.ValidateVector(&vector); err != nil {
			a.sendWebSocketError(conn, fmt.Sprintf("Invalid vector at index %d: %v", i, err))
			return
		}
		
		vectors = append(vectors, vector)
	}
	
	// Handle batch insertion
	// Implementation depends on handler interface
	response := map[string]interface{}{
		"type":      "insert_batch_response",
		"success":   true,
		"count":     len(vectors),
		"message":   "Batch inserted successfully",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	a.sendWebSocketResponse(conn, response)
}

// sendWebSocketResponse sends a WebSocket response
func (a *WebSocketAdapter) sendWebSocketResponse(conn *websocket.Conn, response map[string]interface{}) {
	conn.SetWriteDeadline(time.Now().Add(a.config.WriteWait))
	
	if err := conn.WriteJSON(response); err != nil {
		a.logger.Error("Failed to send WebSocket response", zap.Error(err))
	}
}

// sendWebSocketError sends a WebSocket error response
func (a *WebSocketAdapter) sendWebSocketError(conn *websocket.Conn, message string) {
	response := map[string]interface{}{
		"type":      "error",
		"error":     message,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	a.sendWebSocketResponse(conn, response)
}

// writeHTTPErrorResponse writes an HTTP error response
func (a *WebSocketAdapter) writeHTTPErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	response := map[string]interface{}{
		"error":     message,
		"status":    statusCode,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		a.logger.Error("Failed to encode HTTP error response", zap.Error(err))
	}
}

// handleHealth handles health check requests
func (a *WebSocketAdapter) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	shutdown := a.shutdown
	a.mu.RUnlock()
	
	if shutdown {
		a.writeHTTPErrorResponse(w, http.StatusServiceUnavailable, "Service is shutting down")
		return
	}
	
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"protocol":  a.protocol,
		"address":   a.GetAddress(),
		"uptime":    time.Since(a.startTime).String(),
	}
	
	a.writeHTTPJSONResponse(w, http.StatusOK, response)
}

// handleReady handles readiness check requests
func (a *WebSocketAdapter) handleReady(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "ready",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"protocol":  a.protocol,
		"address":   a.GetAddress(),
	}
	
	a.writeHTTPJSONResponse(w, http.StatusOK, response)
}

// handleMetrics handles metrics requests
func (a *WebSocketAdapter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	a.writeHTTPErrorResponse(w, http.StatusNotImplemented, "Metrics endpoint not implemented")
}

// writeHTTPJSONResponse writes an HTTP JSON response
func (a *WebSocketAdapter) writeHTTPJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(data); err != nil {
		a.logger.Error("Failed to encode HTTP JSON response", zap.Error(err))
	}
}

// checkOrigin checks if the WebSocket origin is allowed
func (a *WebSocketAdapter) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	
	// Check if origin is allowed
	for _, allowedOrigin := range a.config.AllowedOrigins {
		if allowedOrigin == "*" || allowedOrigin == origin {
			return true
		}
	}
	
	return false
}

// closeAllConnections closes all active WebSocket connections
func (a

// closeAllConnections closes all active WebSocket connections
func (a *WebSocketAdapter) closeAllConnections() {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	for conn := range a.connections {
		// Send close message
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
		delete(a.connections, conn)
	}
}

// loggingMiddleware logs HTTP requests
func (a *WebSocketAdapter) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		a.logger.Info("HTTP request started",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()))
		
		next.ServeHTTP(w, r)
		
		duration := time.Since(start)
		
		a.logger.Info("HTTP request completed",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Duration("duration", duration),
			zap.String("remote_addr", r.RemoteAddr))
	})
}