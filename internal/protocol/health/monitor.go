
package health

import (
	"context"
	"sync"
	"time"

	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"go.uber.org/zap"
)

// ConnectionHealthMonitor monitors the health of protocol connections
type ConnectionHealthMonitor struct {
	logger          logging.Logger
	metrics         *metrics.IngestionMetrics
	checkInterval   time.Duration
	timeout         time.Duration
	healthyThreshold int
	unhealthyThreshold int
	mu              sync.RWMutex
	connections     map[string]*ConnectionHealth
	shutdown        chan struct{}
	wg              sync.WaitGroup
}

// ConnectionHealth represents the health status of a connection
type ConnectionHealth struct {
	ID               string
	Protocol         string
	RemoteAddr       string
	LastActivity     time.Time
	Status           ConnectionStatus
	HealthScore      float64
	FailedChecks     int
	SuccessfulChecks int
	CreatedAt        time.Time
	mu               sync.RWMutex
}

// ConnectionStatus represents the health status of a connection
type ConnectionStatus int

const (
	StatusUnknown ConnectionStatus = iota
	StatusHealthy
	StatusDegraded
	StatusUnhealthy
	StatusClosed
)

// String returns the string representation of ConnectionStatus
func (s ConnectionStatus) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusDegraded:
		return "degraded"
	case StatusUnhealthy:
		return "unhealthy"
	case StatusClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// MonitorConfig represents the configuration for the connection health monitor
type MonitorConfig struct {
	CheckInterval      time.Duration `yaml:"check_interval" json:"check_interval"`
	Timeout           time.Duration `yaml:"timeout" json:"timeout"`
	HealthyThreshold  int           `yaml:"healthy_threshold" json:"healthy_threshold"`
	UnhealthyThreshold int          `yaml:"unhealthy_threshold" json:"unhealthy_threshold"`
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultMonitorConfig returns the default monitor configuration
func DefaultMonitorConfig() *MonitorConfig {
	return &MonitorConfig{
		CheckInterval:      30 * time.Second,
		Timeout:           5 * time.Second,
		HealthyThreshold:  3,
		UnhealthyThreshold: 5,
		EnableMetrics:     true,
	}
}

// NewConnectionHealthMonitor creates a new connection health monitor
func NewConnectionHealthMonitor(config *MonitorConfig, logger logging.Logger, metrics *metrics.IngestionMetrics) *ConnectionHealthMonitor {
	if config == nil {
		config = DefaultMonitorConfig()
	}

	monitor := &ConnectionHealthMonitor{
		logger:            logger,
		metrics:           metrics,
		checkInterval:     config.CheckInterval,
		timeout:           config.Timeout,
		healthyThreshold:  config.HealthyThreshold,
		unhealthyThreshold: config.UnhealthyThreshold,
		connections:       make(map[string]*ConnectionHealth),
		shutdown:          make(chan struct{}),
	}

	monitor.logger.Info("Created connection health monitor",
		zap.Duration("check_interval", monitor.checkInterval),
		zap.Duration("timeout", monitor.timeout),
		zap.Int("healthy_threshold", monitor.healthyThreshold),
		zap.Int("unhealthy_threshold", monitor.unhealthyThreshold))

	return monitor
}

// Start starts the connection health monitor
func (m *ConnectionHealthMonitor) Start(ctx context.Context) error {
	m.logger.Info("Starting connection health monitor")

	m.wg.Add(1)
	go m.runHealthChecks(ctx)

	return nil
}

// Stop stops the connection health monitor
func (m *ConnectionHealthMonitor) Stop() {
	m.logger.Info("Stopping connection health monitor")

	close(m.shutdown)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all connections
	for _, conn := range m.connections {
		conn.mu.Lock()
		conn.Status = StatusClosed
		conn.mu.Unlock()
	}

	m.logger.Info("Connection health monitor stopped")
}

// RegisterConnection registers a new connection for health monitoring
func (m *ConnectionHealthMonitor) RegisterConnection(id, protocol, remoteAddr string) *ConnectionHealth {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn := &ConnectionHealth{
		ID:           id,
		Protocol:     protocol,
		RemoteAddr:   remoteAddr,
		LastActivity: time.Now(),
		Status:       StatusHealthy,
		HealthScore:  1.0,
		CreatedAt:    time.Now(),
	}

	m.connections[id] = conn

	m.logger.Info("Registered connection for health monitoring",
		zap.String("connection_id", id),
		zap.String("protocol", protocol),
		zap.String("remote_addr", remoteAddr))

	if m.metrics != nil {
		m.metrics.IncrementConnectionCount(protocol)
	}

	return conn
}

// UnregisterConnection unregisters a connection from health monitoring
func (m *ConnectionHealthMonitor) UnregisterConnection(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conn, exists := m.connections[id]; exists {
		conn.mu.Lock()
		conn.Status = StatusClosed
		conn.mu.Unlock()

		delete(m.connections, id)

		m.logger.Info("Unregistered connection from health monitoring",
			zap.String("connection_id", id),
			zap.String("protocol", conn.Protocol))

		if m.metrics != nil {
			m.metrics.DecrementConnectionCount(conn.Protocol)
		}
	}
}

// UpdateActivity updates the last activity time for a connection
func (m *ConnectionHealthMonitor) UpdateActivity(id string) {
	m.mu.RLock()
	conn, exists := m.connections[id]
	m.mu.RUnlock()

	if exists {
		conn.mu.Lock()
		conn.LastActivity = time.Now()
		conn.mu.Unlock()
	}
}

// GetConnectionHealth returns the health status of a connection
func (m *ConnectionHealthMonitor) GetConnectionHealth(id string) (*ConnectionHealth, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, exists := m.connections[id]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	copy := &ConnectionHealth{
		ID:               conn.ID,
		Protocol:         conn.Protocol,
		RemoteAddr:       conn.RemoteAddr,
		LastActivity:     conn.LastActivity,
		Status:           conn.Status,
		HealthScore:      conn.HealthScore,
		FailedChecks:     conn.FailedChecks,
		SuccessfulChecks: conn.SuccessfulChecks,
		CreatedAt:        conn.CreatedAt,
	}

	return copy, true
}

// GetAllConnectionHealth returns the health status of all connections
func (m *ConnectionHealthMonitor) GetAllConnectionHealth() []*ConnectionHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connections := make([]*ConnectionHealth, 0, len(m.connections))
	for _, conn := range m.connections {
		conn.mu.RLock()
		copy := &ConnectionHealth{
			ID:               conn.ID,
			Protocol:         conn.Protocol,
			RemoteAddr:       conn.RemoteAddr,
			LastActivity:     conn.LastActivity,
			Status:           conn.Status,
			HealthScore:      conn.HealthScore,
			FailedChecks:     conn.FailedChecks,
			SuccessfulChecks: conn.SuccessfulChecks,
			CreatedAt:        conn.CreatedAt,
		}
		conn.mu.RUnlock()
		connections = append(connections, copy)
	}

	return connections
}

// GetConnectionStats returns statistics about connections
func (m *ConnectionHealthMonitor) GetConnectionStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_connections"] = len(m.connections)

	protocolStats := make(map[string]int)
	statusStats := make(map[string]int)

	for _, conn := range m.connections {
		conn.mu.RLock()
		protocolStats[conn.Protocol]++
		statusStats[conn.Status.String()]++
		conn.mu.RUnlock()
	}

	stats["by_protocol"] = protocolStats
	stats["by_status"] = statusStats

	return stats
}

// runHealthChecks runs periodic health checks on all connections
func (m *ConnectionHealthMonitor) runHealthChecks(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()


	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Health check loop stopped due to context cancellation")
			return
		case <-m.shutdown:
			m.logger.Info("Health check loop stopped due to shutdown")
			return
		case <-ticker.C:
			m.performHealthChecks()
		}
	}
}

// performHealthChecks performs health checks on all connections
func (m *ConnectionHealthMonitor) performHealthChecks() {
	m.mu.RLock()
	connections := make([]*ConnectionHealth, 0, len(m.connections))
	for _, conn := range m.connections {
		connections = append(connections, conn)
	}
	m.mu.RUnlock()

	for _, conn := range connections {
		m.checkConnectionHealth(conn)
	}
}

// checkConnectionHealth checks the health of a single connection
func (m *ConnectionHealthMonitor) checkConnectionHealth(conn *ConnectionHealth) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Skip closed connections
	if conn.Status == StatusClosed {
		return
	}

	// Calculate time since last activity
	inactiveDuration := time.Since(conn.LastActivity)

	// Determine health status based on activity and thresholds
	var newStatus ConnectionStatus
	var healthScore float64

	if inactiveDuration < m.timeout {
		// Connection is active
		newStatus = StatusHealthy
		healthScore = 1.0
		conn.SuccessfulChecks++
		conn.FailedChecks = 0
	} else if inactiveDuration < m.timeout*2 {
		// Connection is inactive but not too long
		newStatus = StatusDegraded
		healthScore = 0.5
		conn.FailedChecks++
	} else {
		// Connection has been inactive for too long
		newStatus = StatusUnhealthy
		healthScore = 0.0
		conn.FailedChecks++
	}

	// Update status based on thresholds
	if conn.FailedChecks >= m.unhealthyThreshold {
		newStatus = StatusUnhealthy
		healthScore = 0.0
	} else if conn.SuccessfulChecks >= m.healthyThreshold {
		if newStatus != StatusUnhealthy {
			newStatus = StatusHealthy
			healthScore = 1.0
		}
	}

	// Log status changes
	if conn.Status != newStatus {
		m.logger.Info("Connection health status changed",
			zap.String("connection_id", conn.ID),
			zap.String("protocol", conn.Protocol),
			zap.String("remote_addr", conn.RemoteAddr),
			zap.String("old_status", conn.Status.String()),
			zap.String("new_status", newStatus.String()),
			zap.Duration("inactive_duration", inactiveDuration),
			zap.Int("failed_checks", conn.FailedChecks),
			zap.Int("successful_checks", conn.SuccessfulChecks))

		// Update metrics
		if m.metrics != nil {
			if newStatus == StatusUnhealthy {
				m.metrics.IncrementUnhealthyConnections(conn.Protocol)
			} else if newStatus == StatusHealthy && conn.Status == StatusUnhealthy {
				m.metrics.DecrementUnhealthyConnections(conn.Protocol)
			}
		}
	}

	// Update connection health
	conn.Status = newStatus
	conn.HealthScore = healthScore
}

// CloseUnhealthyConnections closes all unhealthy connections
func (m *ConnectionHealthMonitor) CloseUnhealthyConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	closedCount := 0

	for _, conn := range m.connections {
		conn.mu.RLock()
		if conn.Status == StatusUnhealthy {
			conn.mu.RUnlock()
			// Close the connection (this would be implemented by the specific protocol adapter)
			m.logger.Info("Closing unhealthy connection",
				zap.String("connection_id", conn.ID),
				zap.String("protocol", conn.Protocol),
				zap.String("remote_addr", conn.RemoteAddr))
			closedCount++
		} else {
			conn.mu.RUnlock()
		}
	}

	return closedCount
}

// GetUnhealthyConnections returns all unhealthy connections
func (m *ConnectionHealthMonitor) GetUnhealthyConnections() []*ConnectionHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var unhealthy []*ConnectionHealth
	for _, conn := range m.connections {
		conn.mu.RLock()
		if conn.Status == StatusUnhealthy {
			copy := &ConnectionHealth{
				ID:               conn.ID,
				Protocol:         conn.Protocol,
				RemoteAddr:       conn.RemoteAddr,
				LastActivity:     conn.LastActivity,
				Status:           conn.Status,
				HealthScore:      conn.HealthScore,
				FailedChecks:     conn.FailedChecks,
				SuccessfulChecks: conn.SuccessfulChecks,
				CreatedAt:        conn.CreatedAt,
			}
			unhealthy = append(unhealthy, copy)
		}
		conn.mu.RUnlock()
	}

	return unhealthy
}
