package connection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

var (
	ErrConnectionNotFound      = errors.New("connection not found")
	ErrConnectionAlreadyExists = errors.New("connection already exists")
	ErrConnectionLimitExceeded = errors.New("connection limit exceeded")
	ErrConnectionClosed        = errors.New("connection closed")
	ErrInvalidConnectionID     = errors.New("invalid connection ID")
	ErrManagerNotRunning       = errors.New("connection manager not running")
	ErrManagerAlreadyRunning   = errors.New("connection manager already running")
	ErrManagerShutdownFailed   = errors.New("connection manager shutdown failed")
)

// ConnectionManagerConfig represents the connection manager configuration
type ConnectionManagerConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	MaxConnections    int           `yaml:"max_connections" json:"max_connections"`
	IdleTimeout       time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout" json:"write_timeout"`
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool          `yaml:"enable_logging" json:"enable_logging"`
	EnableTracing     bool          `yaml:"enable_tracing" json:"enable_tracing"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	EnableCompression bool          `yaml:"enable_compression" json:"enable_compression"`
	BufferSize        int           `yaml:"buffer_size" json:"buffer_size"`
}

// DefaultConnectionManagerConfig returns the default connection manager configuration
func DefaultConnectionManagerConfig() *ConnectionManagerConfig {
	return &ConnectionManagerConfig{
		Enabled:           true,
		MaxConnections:    1000,
		IdleTimeout:       5 * time.Minute,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		EnableMetrics:     true,
		EnableLogging:     true,
		EnableTracing:     false,
		CleanupInterval:   1 * time.Minute,
		EnableCompression: false,
		BufferSize:        4096,
	}
}

// ConnectionManager represents a connection manager
type ConnectionManager struct {
	config  *ConnectionManagerConfig
	logger  logging.Logger
	metrics *metrics.ServiceMetrics

	// Connection storage
	connections map[string]*adapter.Connection
	mu          sync.RWMutex

	// Connection channels
	connectChan    chan *adapter.Connection
	disconnectChan chan *adapter.Connection
	cleanupChan    chan struct{}
	cleanupDone    chan struct{}

	// Connection handlers
	connectionHandler adapter.ConnectionHandler
	errorHandler      func(ctx context.Context, conn *adapter.Connection, err error)

	// Lifecycle
	started   bool
	stopped   bool
	startTime time.Time

	// Statistics
	stats *ConnectionManagerStats
}

// ConnectionManagerStats represents connection manager statistics
type ConnectionManagerStats struct {
	TotalConnections    int64         `json:"total_connections"`
	ActiveConnections   int64         `json:"active_connections"`
	ClosedConnections   int64         `json:"closed_connections"`
	RejectedConnections int64         `json:"rejected_connections"`
	TotalBytesRead      int64         `json:"total_bytes_read"`
	TotalBytesWritten   int64         `json:"total_bytes_written"`
	TotalRequests       int64         `json:"total_requests"`
	AvgConnectionTime   time.Duration `json:"avg_connection_time"`
	MaxConnectionTime   time.Duration `json:"max_connection_time"`
	MinConnectionTime   time.Duration `json:"min_connection_time"`
	StartTime           time.Time     `json:"start_time"`
	Uptime              time.Duration `json:"uptime"`
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*ConnectionManager, error) {
	managerConfig := DefaultConnectionManagerConfig()

	if cfg != nil {
		if managerCfg, ok := cfg.Get("connection_manager"); ok {
			if cfgMap, ok := managerCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					managerConfig.Enabled = enabled
				}
				if maxConnections, ok := cfgMap["max_connections"].(int); ok {
					managerConfig.MaxConnections = maxConnections
				}
				if idleTimeout, ok := cfgMap["idle_timeout"].(string); ok {
					if duration, err := time.ParseDuration(idleTimeout); err == nil {
						managerConfig.IdleTimeout = duration
					}
				}
				if readTimeout, ok := cfgMap["read_timeout"].(string); ok {
					if duration, err := time.ParseDuration(readTimeout); err == nil {
						managerConfig.ReadTimeout = duration
					}
				}
				if writeTimeout, ok := cfgMap["write_timeout"].(string); ok {
					if duration, err := time.ParseDuration(writeTimeout); err == nil {
						managerConfig.WriteTimeout = duration
					}
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					managerConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					managerConfig.EnableLogging = enableLogging
				}
				if enableTracing, ok := cfgMap["enable_tracing"].(bool); ok {
					managerConfig.EnableTracing = enableTracing
				}
				if cleanupInterval, ok := cfgMap["cleanup_interval"].(string); ok {
					if duration, err := time.ParseDuration(cleanupInterval); err == nil {
						managerConfig.CleanupInterval = duration
					}
				}
				if enableCompression, ok := cfgMap["enable_compression"].(bool); ok {
					managerConfig.EnableCompression = enableCompression
				}
				if bufferSize, ok := cfgMap["buffer_size"].(int); ok {
					managerConfig.BufferSize = bufferSize
				}
			}
		}
	}

	// Validate configuration
	if err := validateConnectionManagerConfig(managerConfig); err != nil {
		return nil, fmt.Errorf("invalid connection manager configuration: %w", err)
	}

	manager := &ConnectionManager{
		config:         managerConfig,
		logger:         logger,
		metrics:        metrics,
		connections:    make(map[string]*adapter.Connection),
		connectChan:    make(chan *adapter.Connection, 100),
		disconnectChan: make(chan *adapter.Connection, 100),
		cleanupChan:    make(chan struct{}),
		cleanupDone:    make(chan struct{}),
		stats: &ConnectionManagerStats{
			StartTime: time.Now(),
		},
	}

	manager.logger.Info("Created connection manager")

	return manager, nil
}

// validateConnectionManagerConfig validates the connection manager configuration
func validateConnectionManagerConfig(config *ConnectionManagerConfig) error {
	if config == nil {
		return errors.New("connection manager configuration cannot be nil")
	}

	if config.MaxConnections <= 0 {
		return errors.New("max connections must be positive")
	}

	if config.IdleTimeout <= 0 {
		return errors.New("idle timeout must be positive")
	}

	if config.ReadTimeout <= 0 {
		return errors.New("read timeout must be positive")
	}

	if config.WriteTimeout <= 0 {
		return errors.New("write timeout must be positive")
	}

	if config.CleanupInterval <= 0 {
		return errors.New("cleanup interval must be positive")
	}

	if config.BufferSize <= 0 {
		return errors.New("buffer size must be positive")
	}

	return nil
}

// Start starts the connection manager
func (m *ConnectionManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return ErrManagerAlreadyRunning
	}

	if !m.config.Enabled {
		m.logger.Info("Connection manager is disabled")
		return nil
	}

	// Start connection processor
	go m.processConnections()

	// Start cleanup routine
	go m.cleanupIdleConnections()

	m.started = true
	m.stopped = false
	m.startTime = time.Now()

	m.logger.Info("Started connection manager")

	return nil
}

// Stop stops the connection manager
func (m *ConnectionManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return nil
	}

	if !m.started {
		return ErrManagerNotRunning
	}

	// Close all connections
	if err := m.CloseAllConnections(); err != nil {
		m.logger.Error("Failed to close all connections", zap.Error(err))
	}

	// Stop cleanup routine
	close(m.cleanupChan)

	// Wait for cleanup to finish
	<-m.cleanupDone

	m.stopped = true
	m.started = false

	m.logger.Info("Stopped connection manager")

	return nil
}

// IsRunning checks if the connection manager is running
func (m *ConnectionManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.started && !m.stopped
}

// GetConfig returns the connection manager configuration
func (m *ConnectionManager) GetConfig() *ConnectionManagerConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy of config
	config := *m.config
	return &config
}

// UpdateConfig updates the connection manager configuration
func (m *ConnectionManager) UpdateConfig(config *ConnectionManagerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate new configuration
	if err := validateConnectionManagerConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	m.config = config

	m.logger.Info("Updated connection manager configuration", zap.Any("config", config))

	return nil
}

// AddConnection adds a new connection
func (m *ConnectionManager) AddConnection(conn *adapter.Connection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return ErrManagerNotRunning
	}

	// Check connection limit
	if len(m.connections) >= m.config.MaxConnections {
		m.stats.RejectedConnections++
		return ErrConnectionLimitExceeded
	}

	// Check if connection already exists
	if _, exists := m.connections[conn.ID]; exists {
		return ErrConnectionAlreadyExists
	}

	// Add connection
	m.connections[conn.ID] = conn
	m.stats.TotalConnections++
	m.stats.ActiveConnections++

	// Send to connection processor
	select {
	case m.connectChan <- conn:
	default:
		m.logger.Warn("Connection channel full, dropping connection", zap.String("connection_id", conn.ID))
	}

	if m.config.EnableLogging {
		m.logger.Info("Connection added",
			zap.String("connection_id", conn.ID),
			zap.String("remote_addr", conn.RemoteAddr),
			zap.String("protocol", string(conn.Protocol)),
			zap.Int("total_connections", len(m.connections)))
	}

	return nil
}

// RemoveConnection removes a connection
func (m *ConnectionManager) RemoveConnection(connID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, exists := m.connections[connID]
	if !exists {
		return ErrConnectionNotFound
	}

	// Remove connection
	delete(m.connections, connID)
	m.stats.ActiveConnections--
	m.stats.ClosedConnections++

	// Update connection time stats
	connectionTime := time.Since(conn.Established)
	m.stats.AvgConnectionTime = time.Duration(
		(int64(m.stats.AvgConnectionTime)*m.stats.ClosedConnections + int64(connectionTime)) /
			(m.stats.ClosedConnections + 1),
	)

	if connectionTime > m.stats.MaxConnectionTime {
		m.stats.MaxConnectionTime = connectionTime
	}

	if m.stats.MinConnectionTime == 0 || connectionTime < m.stats.MinConnectionTime {
		m.stats.MinConnectionTime = connectionTime
	}

	// Send to disconnection processor
	select {
	case m.disconnectChan <- conn:
	default:
		m.logger.Warn("Disconnection channel full, dropping connection", zap.String("connection_id", conn.ID))
	}

	if m.config.EnableLogging {
		m.logger.Info("Connection removed",
			zap.String("connection_id", conn.ID),
			zap.String("remote_addr", conn.RemoteAddr),
			zap.String("protocol", string(conn.Protocol)),
			zap.Duration("connection_time", connectionTime),
			zap.Int("total_connections", len(m.connections)))
	}

	return nil
}

// GetConnection returns a connection by ID
func (m *ConnectionManager) GetConnection(connID string) (*adapter.Connection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, exists := m.connections[connID]
	if !exists {
		return nil, false
	}

	return conn, true
}

// GetAllConnections returns all active connections
func (m *ConnectionManager) GetAllConnections() []*adapter.Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connections := make([]*adapter.Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		connections = append(connections, conn)
	}

	return connections
}

// CloseConnection closes a connection by ID
func (m *ConnectionManager) CloseConnection(connID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, exists := m.connections[connID]
	if !exists {
		return ErrConnectionNotFound
	}

	// Mark connection as inactive
	conn.Active = false

	if m.config.EnableLogging {
		m.logger.Info("Closing connection",
			zap.String("connection_id", conn.ID),
			zap.String("remote_addr", conn.RemoteAddr),
			zap.String("protocol", string(conn.Protocol)))
	}

	return nil
}

// CloseAllConnections closes all connections
func (m *ConnectionManager) CloseAllConnections() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.config.EnableLogging {
		m.logger.Info("Closing all connections", zap.Int("count", len(m.connections)))
	}

	// Mark all connections as inactive
	for _, conn := range m.connections {
		conn.Active = false
	}

	return nil
}

// GetStats returns connection manager statistics
func (m *ConnectionManager) GetStats() *ConnectionManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Update uptime
	m.stats.Uptime = time.Since(m.startTime)

	// Return a copy of stats
	stats := *m.stats
	return &stats
}

// ResetStats resets connection manager statistics
func (m *ConnectionManager) ResetStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats = &ConnectionManagerStats{
		StartTime: time.Now(),
	}

	m.logger.Info("Connection manager statistics reset")
}

// SetConnectionHandler sets the connection handler
func (m *ConnectionManager) SetConnectionHandler(handler adapter.ConnectionHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectionHandler = handler

	m.logger.Info("Connection handler set")
}

// SetErrorHandler sets the error handler
func (m *ConnectionManager) SetErrorHandler(handler func(ctx context.Context, conn *adapter.Connection, err error)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errorHandler = handler

	m.logger.Info("Error handler set")
}

// UpdateConnectionStats updates connection statistics
func (m *ConnectionManager) UpdateConnectionStats(connID string, bytesRead, bytesWritten int64, requests int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, exists := m.connections[connID]
	if !exists {
		return
	}

	// Update connection stats
	conn.BytesRead += bytesRead
	conn.BytesWritten += bytesWritten
	conn.Requests += requests
	conn.LastActivity = time.Now()

	// Update manager stats
	m.stats.TotalBytesRead += bytesRead
	m.stats.TotalBytesWritten += bytesWritten
	m.stats.TotalRequests += requests
}

// Validate validates the connection manager state
func (m *ConnectionManager) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Validate configuration
	if err := validateConnectionManagerConfig(m.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	return nil
}

// processConnections processes connection events
func (m *ConnectionManager) processConnections() {
	for {
		select {
		case conn := <-m.connectChan:
			if m.connectionHandler != nil {
				go func(c *adapter.Connection) {
					ctx := context.Background()
					if err := m.connectionHandler(ctx, c); err != nil {
						if m.errorHandler != nil {
							m.errorHandler(ctx, c, err)
						} else {
							m.logger.Error("Connection handler error",
								zap.String("connection_id", c.ID),
								zap.Error(err))
						}
					}
				}(conn)
			}

		case conn := <-m.disconnectChan:
			// Handle disconnection events if needed
			if m.config.EnableLogging {
				m.logger.Debug("Connection disconnected",
					zap.String("connection_id", conn.ID),
					zap.String("remote_addr", conn.RemoteAddr))
			}
		}
	}
}

// cleanupIdleConnections cleans up idle connections
func (m *ConnectionManager) cleanupIdleConnections() {
	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupIdle()

		case <-m.cleanupChan:
			close(m.cleanupDone)
			return
		}
	}
}

// cleanupIdle cleans up idle connections
func (m *ConnectionManager) cleanupIdle() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	idleConnections := 0

	for id, conn := range m.connections {
		if now.Sub(conn.LastActivity) > m.config.IdleTimeout {
			// Close idle connection
			conn.Active = false
			delete(m.connections, id)
			m.stats.ActiveConnections--
			m.stats.ClosedConnections++
			idleConnections++

			if m.config.EnableLogging {
				m.logger.Info("Closed idle connection",
					zap.String("connection_id", conn.ID),
					zap.String("remote_addr", conn.RemoteAddr),
					zap.Duration("idle_time", now.Sub(conn.LastActivity)))
			}
		}
	}

	if idleConnections > 0 && m.config.EnableLogging {
		m.logger.Info("Cleaned up idle connections",
			zap.Int("count", idleConnections),
			zap.Int("remaining_connections", len(m.connections)))
	}
}
