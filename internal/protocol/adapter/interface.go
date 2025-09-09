package adapter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrAdapterNotRunning      = errors.New("adapter not running")
	ErrAdapterAlreadyRunning  = errors.New("adapter already running")
	ErrInvalidProtocolConfig  = errors.New("invalid protocol configuration")
	ErrConnectionLimitExceeded = errors.New("connection limit exceeded")
	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrRequestValidationFailed = errors.New("request validation failed")
	ErrUnsupportedProtocol    = errors.New("unsupported protocol")
	ErrAdapterShutdownFailed  = errors.New("adapter shutdown failed")
)

// Protocol represents the supported protocol types
type Protocol string

const (
	ProtocolHTTP     Protocol = "http"
	ProtocolHTTPS    Protocol = "https"
	ProtocolWebSocket Protocol = "websocket"
	ProtocolWSS      Protocol = "wss"
	ProtocolGRPC     Protocol = "grpc"
	ProtocolGRPCS    Protocol = "grpcs"
	ProtocolRedis    Protocol = "redis"
	ProtocolRediss   Protocol = "rediss"
)

// ProtocolConfig represents the base configuration for all protocol adapters
type ProtocolConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	Protocol          Protocol      `yaml:"protocol" json:"protocol"`
	Host              string        `yaml:"host" json:"host"`
	Port              int           `yaml:"port" json:"port"`
	ReadTimeout       time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	MaxConnections    int           `yaml:"max_connections" json:"max_connections"`
	EnableTLS         bool          `yaml:"enable_tls" json:"enable_tls"`
	TLSCertFile       string        `yaml:"tls_cert_file" json:"tls_cert_file"`
	TLSKeyFile        string        `yaml:"tls_key_file" json:"tls_key_file"`
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool          `yaml:"enable_logging" json:"enable_logging"`
	EnableTracing     bool          `yaml:"enable_tracing" json:"enable_tracing"`
	BufferSize        int           `yaml:"buffer_size" json:"buffer_size"`
	Compression       bool          `yaml:"compression" json:"compression"`
}

// Request represents a protocol-agnostic request
type Request struct {
	ID        string                 `json:"id"`
	Method    string                 `json:"method"`
	Path      string                 `json:"path"`
	Headers   map[string]string      `json:"headers"`
	Body      []byte                 `json:"body"`
	Query     map[string]string      `json:"query"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp time.Time              `json:"timestamp"`
	ClientIP  string                 `json:"client_ip"`
	UserAgent string                 `json:"user_agent"`
	Protocol  Protocol               `json:"protocol"`
}

// Response represents a protocol-agnostic response
type Response struct {
	ID        string                 `json:"id"`
	Status    int                    `json:"status"`
	Headers   map[string]string      `json:"headers"`
	Body      []byte                 `json:"body"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp time.Time              `json:"timestamp"`
	Duration  time.Duration          `json:"duration"`
	Error     string                 `json:"error,omitempty"`
}

// Connection represents a client connection
type Connection struct {
	ID           string        `json:"id"`
	RemoteAddr   string        `json:"remote_addr"`
	LocalAddr    string        `json:"local_addr"`
	Protocol     Protocol      `json:"protocol"`
	Established  time.Time     `json:"established"`
	LastActivity time.Time     `json:"last_activity"`
	BytesRead    int64         `json:"bytes_read"`
	BytesWritten int64         `json:"bytes_written"`
	Requests     int64         `json:"requests"`
	Active       bool          `json:"active"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// ConnectionStats represents connection statistics
type ConnectionStats struct {
	TotalConnections   int64         `json:"total_connections"`
	ActiveConnections  int64         `json:"active_connections"`
	ClosedConnections  int64         `json:"closed_connections"`
	RejectedConnections int64        `json:"rejected_connections"`
	TotalBytesRead     int64         `json:"total_bytes_read"`
	TotalBytesWritten  int64         `json:"total_bytes_written"`
	TotalRequests      int64         `json:"total_requests"`
	AvgConnectionTime  time.Duration `json:"avg_connection_time"`
	MaxConnectionTime  time.Duration `json:"max_connection_time"`
	MinConnectionTime  time.Duration `json:"min_connection_time"`
}

// RequestStats represents request statistics
type RequestStats struct {
	TotalRequests    int64         `json:"total_requests"`
	SuccessfulRequests int64       `json:"successful_requests"`
	FailedRequests   int64         `json:"failed_requests"`
	AvgRequestTime   time.Duration `json:"avg_request_time"`
	MaxRequestTime   time.Duration `json:"max_request_time"`
	MinRequestTime   time.Duration `json:"min_request_time"`
	TotalBytesRead   int64         `json:"total_bytes_read"`
	TotalBytesWritten int64        `json:"total_bytes_written"`
}

// AdapterStats represents adapter statistics
type AdapterStats struct {
	ConnectionStats ConnectionStats `json:"connection_stats"`
	RequestStats    RequestStats    `json:"request_stats"`
	StartTime       time.Time       `json:"start_time"`
	Uptime          time.Duration   `json:"uptime"`
	Protocol        Protocol        `json:"protocol"`
	Host            string          `json:"host"`
	Port            int             `json:"port"`
}

// RequestHandler represents a request handler function
type RequestHandler func(ctx context.Context, req *Request) (*Response, error)

// ConnectionHandler represents a connection handler function
type ConnectionHandler func(ctx context.Context, conn *Connection) error

// ErrorHandler represents an error handler function
type ErrorHandler func(ctx context.Context, err error, req *Request) *Response

// Middleware represents a middleware function
type Middleware func(next RequestHandler) RequestHandler

// ProtocolAdapter represents the interface for all protocol adapters
type ProtocolAdapter interface {
	// Lifecycle methods
	Start() error
	Stop() error
	IsRunning() bool
	
	// Configuration methods
	GetConfig() *ProtocolConfig
	UpdateConfig(config *ProtocolConfig) error
	Validate() error
	
	// Connection management
	GetConnection(id string) (*Connection, bool)
	GetAllConnections() []*Connection
	GetConnectionStats() *ConnectionStats
	CloseConnection(id string) error
	CloseAllConnections() error
	
	// Request handling
	SetRequestHandler(handler RequestHandler)
	SetConnectionHandler(handler ConnectionHandler)
	SetErrorHandler(handler ErrorHandler)
	AddMiddleware(middleware Middleware)
	
	// Statistics
	GetRequestStats() *RequestStats
	GetAdapterStats() *AdapterStats
	ResetStats()
	
	// Health checks
	HealthCheck(ctx context.Context) error
	ReadinessCheck(ctx context.Context) error
	LivenessCheck(ctx context.Context) error
	
	// Protocol-specific methods
	GetProtocol() Protocol
	GetAddress() string
	GetURL() string
}

// AdapterFactory represents a factory for creating protocol adapters
type AdapterFactory interface {
	CreateAdapter(config *ProtocolConfig, logger logging.Logger, metrics *metrics.ServiceMetrics) (ProtocolAdapter, error)
	GetSupportedProtocols() []Protocol
	ValidateConfig(config *ProtocolConfig) error
}

// AdapterRegistry represents a registry for protocol adapters
type AdapterRegistry interface {
	RegisterFactory(factory AdapterFactory) error
	GetFactory(protocol Protocol) (AdapterFactory, bool)
	GetSupportedProtocols() []Protocol
	CreateAdapter(protocol Protocol, config *ProtocolConfig, logger logging.Logger, metrics *metrics.ServiceMetrics) (ProtocolAdapter, error)
}

// RequestValidator represents a request validator interface
type RequestValidator interface {
	Validate(ctx context.Context, req *Request) error
	GetValidationRules() []ValidationRule
	AddRule(rule ValidationRule) error
	RemoveRule(name string) error
}

// ValidationRule represents a validation rule
type ValidationRule struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Conditions  map[string]interface{} `json:"conditions"`
	Message     string                 `json:"message"`
	Severity    string                 `json:"severity"`
	Enabled     bool                   `json:"enabled"`
	Priority    int                    `json:"priority"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// RateLimiter represents a rate limiter interface
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error)
	GetRateLimitStats(key string) (RateLimitStats, error)
	Reset(key string) error
	UpdateLimit(key string, limit int, window time.Duration) error
}

// RateLimitStats represents rate limit statistics
type RateLimitStats struct {
	Key           string        `json:"key"`
	Limit         int           `json:"limit"`
	Window        time.Duration `json:"window"`
	CurrentCount  int           `json:"current_count"`
	ResetTime     time.Time     `json:"reset_time"`
	TotalRequests int64         `json:"total_requests"`
	RejectedRequests int64      `json:"rejected_requests"`
	AllowedRequests int64       `json:"allowed_requests"`
}

// BackpressureHandler represents a backpressure handler interface
type BackpressureHandler interface {
	HandlePressure(ctx context.Context, currentLoad float64) (BackpressureAction, error)
	GetBackpressureStats() BackpressureStats
	SetThresholds(thresholds BackpressureThresholds) error
}

// BackpressureAction represents the action to take under backpressure
type BackpressureAction struct {
	Action    string                 `json:"action"`
	Severity  string                 `json:"severity"`
	Delay     time.Duration          `json:"delay"`
	Reject    bool                   `json:"reject"`
	Limit     int                    `json:"limit"`
	Message   string                 `json:"message"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// BackpressureStats represents backpressure statistics
type BackpressureStats struct {
	CurrentLoad      float64            `json:"current_load"`
	AverageLoad      float64            `json:"average_load"`
	MaxLoad          float64            `json:"max_load"`
	PressureEvents   int64              `json:"pressure_events"`
	ActionsTaken     map[string]int64   `json:"actions_taken"`
	LastPressureTime time.Time          `json:"last_pressure_time"`
	Thresholds       BackpressureThresholds `json:"thresholds"`
}

// BackpressureThresholds represents backpressure thresholds
type BackpressureThresholds struct {
	Warning    float64 `json:"warning"`
	Critical   float64 `json:"critical"`
	Emergency  float64 `json:"emergency"`
	Window     time.Duration `json:"window"`
}

// AdapterMetrics represents adapter-specific metrics
type AdapterMetrics interface {
	RecordConnection(conn *Connection)
	RecordDisconnection(conn *Connection)
	RecordRequest(req *Request, res *Response, duration time.Duration)
	RecordError(err error, req *Request)
	RecordBytesRead(bytes int64)
	RecordBytesWritten(bytes int64)
	GetMetrics() map[string]interface{}
	Reset()
}

// AdapterTracer represents adapter-specific tracing
type AdapterTracer interface {
	StartSpan(ctx context.Context, name string) context.Context
	EndSpan(ctx context.Context, err error)
	AddEvent(ctx context.Context, name string, attributes map[string]interface{})
	SetAttributes(ctx context.Context, attributes map[string]interface{})
	GetTraceID(ctx context.Context) string
	GetSpanID(ctx context.Context) string
}

// AdapterLogger represents adapter-specific logging
type AdapterLogger interface {
	Debug(ctx context.Context, message string, fields map[string]interface{})
	Info(ctx context.Context, message string, fields map[string]interface{})
	Warn(ctx context.Context, message string, fields map[string]interface{})
	Error(ctx context.Context, message string, fields map[string]interface{})
	Fatal(ctx context.Context, message string, fields map[string]interface{})
	Panic(ctx context.Context, message string, fields map[string]interface{})
	WithFields(fields map[string]interface{}) AdapterLogger
	WithContext(ctx context.Context) AdapterLogger
}

// AdapterContext represents adapter-specific context
type AdapterContext interface {
	GetRequestID() string
	GetTraceID() string
	GetSpanID() string
	GetClientIP() string
	GetUserAgent() string
	GetStartTime() time.Time
	GetMetadata(key string) (interface{}, bool)
	SetMetadata(key string, value interface{})
	GetConnection() *Connection
	GetRequest() *Request
	GetResponse() *Response
	WithValue(key, value interface{}) context.Context
	Deadline() (time.Time, bool)
	Done() <-chan struct{}
	Err() error
}

// DefaultProtocolConfig returns the default protocol configuration
func DefaultProtocolConfig() *ProtocolConfig {
	return &ProtocolConfig{
		Enabled:        true,
		Protocol:       ProtocolHTTP,
		Host:           "0.0.0.0",
		Port:           8080,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxConnections: 1000,
		EnableTLS:      false,
		EnableMetrics:  true,
		EnableLogging:  true,
		EnableTracing:  false,
		BufferSize:     4096,
		Compression:    false,
	}
}

// ValidateProtocolConfig validates the protocol configuration
func ValidateProtocolConfig(config *ProtocolConfig) error {
	if config == nil {
		return ErrInvalidProtocolConfig
	}
	
	if config.Protocol == "" {
		return errors.New("protocol cannot be empty")
	}
	
	if config.Host == "" {
		return errors.New("host cannot be empty")
	}
	
	if config.Port <= 0 || config.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	
	if config.ReadTimeout <= 0 {
		return errors.New("read timeout must be positive")
	}
	
	if config.WriteTimeout <= 0 {
		return errors.New("write timeout must be positive")
	}
	
	if config.IdleTimeout <= 0 {
		return errors.New("idle timeout must be positive")
	}
	
	if config.MaxConnections <= 0 {
		return errors.New("max connections must be positive")
	}
	
	if config.EnableTLS {
		if config.TLSCertFile == "" {
			return errors.New("TLS cert file cannot be empty when TLS is enabled")
		}
		if config.TLSKeyFile == "" {
			return errors.New("TLS key file cannot be empty when TLS is enabled")
		}
	}
	
	if config.BufferSize <= 0 {
		return errors.New("buffer size must be positive")
	}
	
	return nil
}

// NewRequest creates a new request
func NewRequest(method, path string, body []byte) *Request {
	return &Request{
		ID:        generateRequestID(),
		Method:    method,
		Path:      path,
		Headers:   make(map[string]string),
		Body:      body,
		Query:     make(map[string]string),
		Metadata:  make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

// NewResponse creates a new response
func NewResponse(status int, body []byte) *Response {
	return &Response{
		ID:        "",
		Status:    status,
		Headers:   make(map[string]string),
		Body:      body,
		Metadata:  make(map[string]interface{}),
		Timestamp: time.Now(),
		Duration:  0,
	}
}

// NewConnection creates a new connection
func NewConnection(remoteAddr, localAddr string, protocol Protocol) *Connection {
	return &Connection{
		ID:           generateConnectionID(),
		RemoteAddr:   remoteAddr,
		LocalAddr:    localAddr,
		Protocol:     protocol,
		Established:  time.Now(),
		LastActivity: time.Now(),
		BytesRead:    0,
		BytesWritten: 0,
		Requests:     0,
		Active:       true,
		Metadata:     make(map[string]interface{}),
	}
}

// generateRequestID generates a unique request ID
func generateRequestID() string {
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// generateConnectionID generates a unique connection ID
func generateConnectionID() string {
	return fmt.Sprintf("conn_%d_%d", time.Now().UnixNano(), rand.Int63())
}