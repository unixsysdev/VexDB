package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

// RedisAdapter implements the ProtocolAdapter interface for Redis protocol (RESP)
type RedisAdapter struct {
	config      *RedisConfig
	listener    net.Listener
	logger      logging.Logger
	metrics     *metrics.IngestionMetrics
	validator   adapter.Validator
	handler     adapter.Handler
	protocol    string
	startTime   time.Time
	mu          sync.RWMutex
	shutdown    bool
	connections map[net.Conn]bool
}

// RedisConfig represents the Redis adapter configuration
type RedisConfig struct {
	Host           string        `yaml:"host" json:"host"`
	Port           int           `yaml:"port" json:"port"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	MaxMessageSize int64         `yaml:"max_message_size" json:"max_message_size"`
	EnableMetrics  bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableHealth   bool          `yaml:"enable_health" json:"enable_health"`
	MaxConnections int           `yaml:"max_connections" json:"max_connections"`
	BufferSize     int           `yaml:"buffer_size" json:"buffer_size"`
}

// DefaultRedisConfig returns the default Redis configuration
func DefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Host:           "0.0.0.0",
		Port:           6379,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxMessageSize: 10 << 20, // 10MB
		EnableMetrics:  true,
		EnableHealth:   true,
		MaxConnections: 1000,
		BufferSize:     4096,
	}
}

// NewRedisAdapter creates a new Redis adapter
func NewRedisAdapter(cfg config.Config, logger logging.Logger, metrics *metrics.IngestionMetrics, validator adapter.Validator, handler adapter.Handler) (*RedisAdapter, error) {
	redisConfig := DefaultRedisConfig()

	if cfg != nil {
		if redisCfg, ok := cfg.Get("redis"); ok {
			if cfgMap, ok := redisCfg.(map[string]interface{}); ok {
				if host, ok := cfgMap["host"].(string); ok {
					redisConfig.Host = host
				}
				if port, ok := cfgMap["port"].(int); ok {
					redisConfig.Port = port
				}
				if readTimeout, ok := cfgMap["read_timeout"].(int); ok {
					redisConfig.ReadTimeout = time.Duration(readTimeout) * time.Second
				}
				if writeTimeout, ok := cfgMap["write_timeout"].(int); ok {
					redisConfig.WriteTimeout = time.Duration(writeTimeout) * time.Second
				}
				if idleTimeout, ok := cfgMap["idle_timeout"].(int); ok {
					redisConfig.IdleTimeout = time.Duration(idleTimeout) * time.Second
				}
				if maxMessageSize, ok := cfgMap["max_message_size"].(int); ok {
					redisConfig.MaxMessageSize = int64(maxMessageSize)
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					redisConfig.EnableMetrics = enableMetrics
				}
				if enableHealth, ok := cfgMap["enable_health"].(bool); ok {
					redisConfig.EnableHealth = enableHealth
				}
				if maxConnections, ok := cfgMap["max_connections"].(int); ok {
					redisConfig.MaxConnections = maxConnections
				}
				if bufferSize, ok := cfgMap["buffer_size"].(int); ok {
					redisConfig.BufferSize = bufferSize
				}
			}
		}
	}

	// Validate configuration
	if err := validateRedisConfig(redisConfig); err != nil {
		return nil, fmt.Errorf("invalid Redis configuration: %w", err)
	}

	adapter := &RedisAdapter{
		config:      redisConfig,
		logger:      logger,
		metrics:     metrics,
		validator:   validator,
		handler:     handler,
		protocol:    "redis",
		startTime:   time.Now(),
		connections: make(map[net.Conn]bool),
	}

	adapter.logger.Info("Created Redis adapter",
		zap.String("host", redisConfig.Host),
		zap.Int("port", redisConfig.Port),
		zap.Duration("read_timeout", redisConfig.ReadTimeout),
		zap.Duration("write_timeout", redisConfig.WriteTimeout),
		zap.Int("max_connections", redisConfig.MaxConnections))

	return adapter, nil
}

// validateRedisConfig validates the Redis configuration
func validateRedisConfig(cfg *RedisConfig) error {
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
	if cfg.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}
	if cfg.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}
	if cfg.BufferSize <= 0 {
		return fmt.Errorf("buffer size must be positive")
	}

	return nil
}

// Start starts the Redis adapter
func (a *RedisAdapter) Start(ctx context.Context) error {
	a.logger.Info("Starting Redis adapter", zap.String("address", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)))

	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port))
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	a.listener = listener

	// Start accepting connections
	go a.acceptConnections()

	// Update metrics
	a.metrics.AdaptersStarted.Inc("redis", "start")

	return nil
}

// Stop stops the Redis adapter
func (a *RedisAdapter) Stop(ctx context.Context) error {
	a.logger.Info("Stopping Redis adapter")

	a.mu.Lock()
	a.shutdown = true
	a.mu.Unlock()

	// Close all connections
	a.closeAllConnections()

	// Close listener
	if a.listener != nil {
		a.listener.Close()
	}

	// Update metrics
	a.metrics.AdaptersStopped.Inc("redis", "stop")

	return nil
}

// GetProtocol returns the protocol name
func (a *RedisAdapter) GetProtocol() string {
	return a.protocol
}

// GetAddress returns the adapter address
func (a *RedisAdapter) GetAddress() string {
	return fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
}

// GetMetrics returns adapter metrics
func (a *RedisAdapter) GetMetrics() map[string]interface{} {
	a.mu.RLock()
	connectionCount := len(a.connections)
	a.mu.RUnlock()

	return map[string]interface{}{
		"protocol":           a.protocol,
		"address":            a.GetAddress(),
		"uptime":             time.Since(a.startTime).String(),
		"read_timeout":       a.config.ReadTimeout.String(),
		"write_timeout":      a.config.WriteTimeout.String(),
		"idle_timeout":       a.config.IdleTimeout.String(),
		"max_message_size":   a.config.MaxMessageSize,
		"max_connections":    a.config.MaxConnections,
		"active_connections": connectionCount,
		"enable_health":      a.config.EnableHealth,
		"enable_metrics":     a.config.EnableMetrics,
	}
}

// acceptConnections accepts incoming connections
func (a *RedisAdapter) acceptConnections() {
	for {
		conn, err := a.listener.Accept()
		if err != nil {
			a.mu.RLock()
			shutdown := a.shutdown
			a.mu.RUnlock()

			if shutdown {
				return
			}

			a.logger.Error("Failed to accept connection", zap.Error(err))
			continue
		}

		// Check connection limit
		a.mu.RLock()
		connectionCount := len(a.connections)
		shutdown := a.shutdown
		a.mu.RUnlock()

		if shutdown {
			conn.Close()
			continue
		}

		if connectionCount >= a.config.MaxConnections {
			conn.Close()
			continue
		}

		// Add connection to tracking
		a.mu.Lock()
		a.connections[conn] = true
		a.mu.Unlock()

		// Update metrics
		a.metrics.ConnectionsActive.Inc("redis")

		a.logger.Info("Redis connection established", zap.String("remote_addr", conn.RemoteAddr().String()))

		// Handle connection in goroutine
		go a.handleConnection(conn)
	}
}

// handleConnection handles a Redis connection
func (a *RedisAdapter) handleConnection(conn net.Conn) {
	defer func() {
		// Remove connection from tracking
		a.mu.Lock()
		delete(a.connections, conn)
		a.mu.Unlock()

		// Update metrics
		a.metrics.ConnectionsActive.Dec("redis")

		// Close connection
		conn.Close()

		a.logger.Info("Redis connection closed", zap.String("remote_addr", conn.RemoteAddr().String()))
	}()

	// Set connection timeouts
	conn.SetReadDeadline(time.Now().Add(a.config.ReadTimeout))
	conn.SetWriteDeadline(time.Now().Add(a.config.WriteTimeout))

	// Create reader
	reader := bufio.NewReaderSize(conn, a.config.BufferSize)

	// Handle commands
	for {
		// Read command
		command, err := a.readRESPCommand(reader)
		if err != nil {
			if err.Error() != "EOF" {
				a.logger.Error("Failed to read Redis command", zap.Error(err))
			}
			return
		}

		// Reset deadlines
		conn.SetReadDeadline(time.Now().Add(a.config.ReadTimeout))
		conn.SetWriteDeadline(time.Now().Add(a.config.WriteTimeout))

		// Handle command
		response := a.handleRedisCommand(command)

		// Send response
		if _, err := conn.Write([]byte(response)); err != nil {
			a.logger.Error("Failed to send Redis response", zap.Error(err))
			return
		}
	}
}

// readRESPCommand reads a RESP command from the reader
func (a *RedisAdapter) readRESPCommand(reader *bufio.Reader) ([]string, error) {
	// Read first byte to determine type
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 {
		return nil, fmt.Errorf("invalid RESP command")
	}

	prefix := line[0]
	content := strings.TrimSpace(line[1 : len(line)-2]) // Remove \r\n

	switch prefix {
	case '*': // Array
		arrayLength, err := strconv.Atoi(content)
		if err != nil {
			return nil, fmt.Errorf("invalid array length: %w", err)
		}

		command := make([]string, arrayLength)
		for i := 0; i < arrayLength; i++ {
			// Read bulk string prefix
			bulkLine, err := reader.ReadString('\n')
			if err != nil {
				return nil, err
			}

			if len(bulkLine) < 2 || bulkLine[0] != '$' {
				return nil, fmt.Errorf("expected bulk string prefix")
			}

			// Read bulk string content
			bulkContent, err := reader.ReadString('\n')
			if err != nil {
				return nil, err
			}

			command[i] = strings.TrimSpace(bulkContent[:len(bulkContent)-2]) // Remove \r\n
		}

		return command, nil

	default:
		return nil, fmt.Errorf("unsupported RESP prefix: %c", prefix)
	}
}

// handleRedisCommand handles a Redis command
func (a *RedisAdapter) handleRedisCommand(command []string) string {
	if len(command) == 0 {
		return "-ERR empty command\r\n"
	}

	cmd := strings.ToUpper(command[0])

	switch cmd {
	case "PING":
		return "+PONG\r\n"

	case "VECTOR.ADD":
		return a.handleVectorAdd(command[1:])

	case "VECTOR.MADD":
		return a.handleVectorMAdd(command[1:])

	case "HELLO":
		return a.handleHello()

	case "COMMAND":
		return a.handleCommand()

	case "INFO":
		return a.handleInfo()

	default:
		return "-ERR unknown command '" + cmd + "'\r\n"
	}
}

// handleVectorAdd handles VECTOR.ADD command
func (a *RedisAdapter) handleVectorAdd(args []string) string {
	if len(args) < 1 {
		return "-ERR wrong number of arguments for 'vector.add' command\r\n"
	}

	// Parse vector data (simplified - in real implementation would parse JSON)
	// For now, we'll just acknowledge the command
	return "+OK\r\n"
}

// handleVectorMAdd handles VECTOR.MADD command
func (a *RedisAdapter) handleVectorMAdd(args []string) string {
	if len(args) < 1 {
		return "-ERR wrong number of arguments for 'vector.madd' command\r\n"
	}

	// Parse multiple vector data (simplified)
	// For now, we'll just acknowledge the command
	return fmt.Sprintf(":%d\r\n", len(args))
}

// handleHello handles HELLO command
func (a *RedisAdapter) handleHello() string {
	// Simple HELLO response
	response := "*6\r\n" +
		"$6\r\nserver\r\n" +
		"$5\r\nredis\r\n" +
		"$7\r\nversion\r\n" +
		"$5\r\n7.0.0\r\n" +
		"$5\r\nproto\r\n" +
		":3\r\n" +
		"$4\r\nmode\r\n" +
		"$10\r\nstandalone\r\n" +
		"$4\r\nrole\r\n" +
		"$6\r\nmaster\r\n"
	return response
}

// handleCommand handles COMMAND command
func (a *RedisAdapter) handleCommand() string {
	// Simple COMMAND response
	response := "*1\r\n" +
		"*6\r\n" +
		"$10\r\nvector.add\r\n" +
		":-1\r\n" +
		":1\r\n" +
		":1\r\n" +
		":0\r\n" +
		"$0\r\n\r\n"
	return response
}

// handleInfo handles INFO command
func (a *RedisAdapter) handleInfo() string {
	// Simple INFO response
	response := "# Server\r\n" +
		"redis_version:7.0.0\r\n" +
		"redis_mode:standalone\r\n" +
		"os:Linux\r\n" +
		"arch_bits:64\r\n" +
		"multiplexing_api:epoll\r\n" +
		"gcc_version:11.3.0\r\n" +
		"process_id:1\r\n" +
		"run_id:abc123\r\n" +
		"tcp_port:" + strconv.Itoa(a.config.Port) + "\r\n" +
		"uptime_in_seconds:3600\r\n" +
		"uptime_in_days:0\r\n" +
		"# Clients\r\n" +
		"connected_clients:1\r\n" +
		"client_longest_output_list:0\r\n" +
		"client_biggest_input_buf:0\r\n" +
		"blocked_clients:0\r\n" +
		"# Memory\r\n" +
		"used_memory:1048576\r\n" +
		"used_memory_human:1M\r\n" +
		"used_memory_rss:2097152\r\n" +
		"used_memory_rss_human:2M\r\n" +
		"used_memory_peak:1048576\r\n" +
		"used_memory_peak_human:1M\r\n" +
		"# Persistence\r\n" +
		"loading:0\r\n" +
		"# Stats\r\n" +
		"total_connections_received:1\r\n" +
		"total_commands_processed:1\r\n" +
		"instantaneous_ops_per_sec:0\r\n" +
		"total_net_input_bytes:1024\r\n" +
		"total_net_output_bytes:1024\r\n" +
		"instantaneous_input_kbps:0.00\r\n" +
		"instantaneous_output_kbps:0.00\r\n" +
		"# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"master_replid:abc123\r\n" +
		"master_replid2:0000000000000000000000000000000000000000\r\n" +
		"master_repl_offset:0\r\n" +
		"second_repl_offset:-1\r\n" +
		"repl_backlog_active:0\r\n" +
		"repl_backlog_size:1048576\r\n" +
		"repl_backlog_first_byte_offset:0\r\n" +
		"repl_backlog_histlen:0\r\n" +
		"# CPU\r\n" +
		"used_cpu_sys:0.10\r\n" +
		"used_cpu_user:0.05\r\n" +
		"used_cpu_sys_children:0.00\r\n" +
		"used_cpu_user_children:0.00\r\n" +
		"# Cluster\r\n" +
		"cluster_enabled:0\r\n" +
		"# Keyspace\r\n" +
		"\r\n"
	return response
}

// closeAllConnections closes all active Redis connections
func (a *RedisAdapter) closeAllConnections() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for conn := range a.connections {
		conn.Close()
		delete(a.connections, conn)
	}
}
