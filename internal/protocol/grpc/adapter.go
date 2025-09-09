package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

// GRPCAdapter implements the ProtocolAdapter interface for gRPC
type GRPCAdapter struct {
	config    *GRPCConfig
	server    *grpc.Server
	listener  net.Listener
	logger    logging.Logger
	metrics   *metrics.IngestionMetrics
	validator adapter.Validator
	handler   adapter.Handler
	protocol  string
	startTime time.Time
	health    *health.Server
}

// GRPCConfig represents the gRPC adapter configuration
type GRPCConfig struct {
	Host                 string          `yaml:"host" json:"host"`
	Port                 int             `yaml:"port" json:"port"`
	MaxConnIdle          time.Duration   `yaml:"max_conn_idle" json:"max_conn_idle"`
	MaxConnAge           time.Duration   `yaml:"max_conn_age" json:"max_conn_age"`
	MaxConnAgeGrace      time.Duration   `yaml:"max_conn_age_grace" json:"max_conn_age_grace"`
	KeepaliveParams      KeepaliveParams `yaml:"keepalive_params" json:"keepalive_params"`
	KeepalivePolicy      KeepalivePolicy `yaml:"keepalive_policy" json:"keepalive_policy"`
	EnableReflection     bool            `yaml:"enable_reflection" json:"enable_reflection"`
	EnableHealth         bool            `yaml:"enable_health" json:"enable_health"`
	EnableMetrics        bool            `yaml:"enable_metrics" json:"enable_metrics"`
	MaxMessageSize       int             `yaml:"max_message_size" json:"max_message_size"`
	MaxConcurrentStreams uint32          `yaml:"max_concurrent_streams" json:"max_concurrent_streams"`
}

// KeepaliveParams represents gRPC keepalive parameters
type KeepaliveParams struct {
	MaxConnectionIdle     time.Duration `yaml:"max_connection_idle" json:"max_connection_idle"`
	MaxConnectionAge      time.Duration `yaml:"max_connection_age" json:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace" json:"max_connection_age_grace"`
	Time                  time.Duration `yaml:"time" json:"time"`
	Timeout               time.Duration `yaml:"timeout" json:"timeout"`
}

// KeepalivePolicy represents gRPC keepalive enforcement policy
type KeepalivePolicy struct {
	MinTime             time.Duration `yaml:"min_time" json:"min_time"`
	PermitWithoutStream bool          `yaml:"permit_without_stream" json:"permit_without_stream"`
}

// DefaultGRPCConfig returns the default gRPC configuration
func DefaultGRPCConfig() *GRPCConfig {
	return &GRPCConfig{
		Host:                 "0.0.0.0",
		Port:                 9090,
		MaxConnIdle:          5 * time.Minute,
		MaxConnAge:           30 * time.Minute,
		MaxConnAgeGrace:      5 * time.Minute,
		EnableReflection:     true,
		EnableHealth:         true,
		EnableMetrics:        true,
		MaxMessageSize:       4 << 20, // 4MB
		MaxConcurrentStreams: 1000,
		KeepaliveParams: KeepaliveParams{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Minute,
			Time:                  2 * time.Minute,
			Timeout:               20 * time.Second,
		},
		KeepalivePolicy: KeepalivePolicy{
			MinTime:             1 * time.Minute,
			PermitWithoutStream: false,
		},
	}
}

// NewGRPCAdapter creates a new gRPC adapter
func NewGRPCAdapter(cfg config.Config, logger logging.Logger, metrics *metrics.IngestionMetrics, validator adapter.Validator, handler adapter.Handler) (*GRPCAdapter, error) {
	grpcConfig := DefaultGRPCConfig()

	if cfg != nil {
		if grpcCfg, ok := cfg.Get("grpc"); ok {
			if cfgMap, ok := grpcCfg.(map[string]interface{}); ok {
				if host, ok := cfgMap["host"].(string); ok {
					grpcConfig.Host = host
				}
				if port, ok := cfgMap["port"].(int); ok {
					grpcConfig.Port = port
				}
				if maxConnIdle, ok := cfgMap["max_conn_idle"].(int); ok {
					grpcConfig.MaxConnIdle = time.Duration(maxConnIdle) * time.Second
				}
				if maxConnAge, ok := cfgMap["max_conn_age"].(int); ok {
					grpcConfig.MaxConnAge = time.Duration(maxConnAge) * time.Second
				}
				if maxConnAgeGrace, ok := cfgMap["max_conn_age_grace"].(int); ok {
					grpcConfig.MaxConnAgeGrace = time.Duration(maxConnAgeGrace) * time.Second
				}
				if enableReflection, ok := cfgMap["enable_reflection"].(bool); ok {
					grpcConfig.EnableReflection = enableReflection
				}
				if enableHealth, ok := cfgMap["enable_health"].(bool); ok {
					grpcConfig.EnableHealth = enableHealth
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					grpcConfig.EnableMetrics = enableMetrics
				}
				if maxMessageSize, ok := cfgMap["max_message_size"].(int); ok {
					grpcConfig.MaxMessageSize = maxMessageSize
				}
				if maxConcurrentStreams, ok := cfgMap["max_concurrent_streams"].(int); ok {
					grpcConfig.MaxConcurrentStreams = uint32(maxConcurrentStreams)
				}

				// Parse keepalive params
				if keepaliveParams, ok := cfgMap["keepalive_params"].(map[string]interface{}); ok {
					if maxConnectionIdle, ok := keepaliveParams["max_connection_idle"].(int); ok {
						grpcConfig.KeepaliveParams.MaxConnectionIdle = time.Duration(maxConnectionIdle) * time.Second
					}
					if maxConnectionAge, ok := keepaliveParams["max_connection_age"].(int); ok {
						grpcConfig.KeepaliveParams.MaxConnectionAge = time.Duration(maxConnectionAge) * time.Second
					}
					if maxConnectionAgeGrace, ok := keepaliveParams["max_connection_age_grace"].(int); ok {
						grpcConfig.KeepaliveParams.MaxConnectionAgeGrace = time.Duration(maxConnectionAgeGrace) * time.Second
					}
					if t, ok := keepaliveParams["time"].(int); ok {
						grpcConfig.KeepaliveParams.Time = time.Duration(t) * time.Second
					}
					if timeout, ok := keepaliveParams["timeout"].(int); ok {
						grpcConfig.KeepaliveParams.Timeout = time.Duration(timeout) * time.Second
					}
				}

				// Parse keepalive policy
				if keepalivePolicy, ok := cfgMap["keepalive_policy"].(map[string]interface{}); ok {
					if minTime, ok := keepalivePolicy["min_time"].(int); ok {
						grpcConfig.KeepalivePolicy.MinTime = time.Duration(minTime) * time.Second
					}
					if permitWithoutStream, ok := keepalivePolicy["permit_without_stream"].(bool); ok {
						grpcConfig.KeepalivePolicy.PermitWithoutStream = permitWithoutStream
					}
				}
			}
		}
	}

	// Validate configuration
	if err := validateGRPCConfig(grpcConfig); err != nil {
		return nil, fmt.Errorf("invalid gRPC configuration: %w", err)
	}

	adapter := &GRPCAdapter{
		config:    grpcConfig,
		logger:    logger,
		metrics:   metrics,
		validator: validator,
		handler:   handler,
		protocol:  "grpc",
		startTime: time.Now(),
		health:    health.NewServer(),
	}

	// Create gRPC server options
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(grpcConfig.MaxMessageSize),
		grpc.MaxSendMsgSize(grpcConfig.MaxMessageSize),
		grpc.MaxConcurrentStreams(grpcConfig.MaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     grpcConfig.KeepaliveParams.MaxConnectionIdle,
			MaxConnectionAge:      grpcConfig.KeepaliveParams.MaxConnectionAge,
			MaxConnectionAgeGrace: grpcConfig.KeepaliveParams.MaxConnectionAgeGrace,
			Time:                  grpcConfig.KeepaliveParams.Time,
			Timeout:               grpcConfig.KeepaliveParams.Timeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             grpcConfig.KeepalivePolicy.MinTime,
			PermitWithoutStream: grpcConfig.KeepalivePolicy.PermitWithoutStream,
		}),
	}

	// Add metrics interceptor if enabled
	if grpcConfig.EnableMetrics {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(
			adapter.metricsUnaryInterceptor,
			adapter.loggingUnaryInterceptor,
		))
	}

	// Create gRPC server
	adapter.server = grpc.NewServer(serverOpts...)

	// Register health service if enabled
	if grpcConfig.EnableHealth {
		grpc_health_v1.RegisterHealthServer(adapter.server, adapter.health)
		adapter.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	}

	// Enable reflection if enabled
	if grpcConfig.EnableReflection {
		reflection.Register(adapter.server)
	}

	adapter.logger.Info("Created gRPC adapter",
		zap.String("host", grpcConfig.Host),
		zap.Int("port", grpcConfig.Port),
		zap.Int("max_message_size", grpcConfig.MaxMessageSize),
		zap.Uint32("max_concurrent_streams", grpcConfig.MaxConcurrentStreams),
		zap.Bool("enable_reflection", grpcConfig.EnableReflection),
		zap.Bool("enable_health", grpcConfig.EnableHealth))

	return adapter, nil
}

// validateGRPCConfig validates the gRPC configuration
func validateGRPCConfig(cfg *GRPCConfig) error {
	if cfg.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if cfg.MaxConnIdle <= 0 {
		return fmt.Errorf("max conn idle must be positive")
	}
	if cfg.MaxConnAge <= 0 {
		return fmt.Errorf("max conn age must be positive")
	}
	if cfg.MaxConnAgeGrace <= 0 {
		return fmt.Errorf("max conn age grace must be positive")
	}
	if cfg.KeepaliveParams.MaxConnectionIdle <= 0 {
		return fmt.Errorf("keepalive max connection idle must be positive")
	}
	if cfg.KeepaliveParams.MaxConnectionAge <= 0 {
		return fmt.Errorf("keepalive max connection age must be positive")
	}
	if cfg.KeepaliveParams.MaxConnectionAgeGrace <= 0 {
		return fmt.Errorf("keepalive max connection age grace must be positive")
	}
	if cfg.KeepaliveParams.Time <= 0 {
		return fmt.Errorf("keepalive time must be positive")
	}
	if cfg.KeepaliveParams.Timeout <= 0 {
		return fmt.Errorf("keepalive timeout must be positive")
	}
	if cfg.KeepalivePolicy.MinTime <= 0 {
		return fmt.Errorf("keepalive policy min time must be positive")
	}
	if cfg.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}
	if cfg.MaxConcurrentStreams <= 0 {
		return fmt.Errorf("max concurrent streams must be positive")
	}

	return nil
}

// Start starts the gRPC adapter
func (a *GRPCAdapter) Start(ctx context.Context) error {
	a.logger.Info("Starting gRPC adapter", zap.String("address", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)))

	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.config.Host, a.config.Port))
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	a.listener = listener

	// Start server in goroutine
	go func() {
		if err := a.server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			a.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	// Update metrics
	a.metrics.AdaptersStarted.Inc("grpc", "start")

	return nil
}

// Stop stops the gRPC adapter
func (a *GRPCAdapter) Stop(ctx context.Context) error {
	a.logger.Info("Stopping gRPC adapter")

	// Update health status
	if a.config.EnableHealth {
		a.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}

	// Graceful shutdown
	if a.server != nil {
		a.server.GracefulStop()
	}

	// Close listener
	if a.listener != nil {
		a.listener.Close()
	}

	// Update metrics
	a.metrics.AdaptersStopped.Inc("grpc", "stop")

	return nil
}

// GetProtocol returns the protocol name
func (a *GRPCAdapter) GetProtocol() string {
	return a.protocol
}

// GetAddress returns the adapter address
func (a *GRPCAdapter) GetAddress() string {
	return fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
}

// GetMetrics returns adapter metrics
func (a *GRPCAdapter) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"protocol":               a.protocol,
		"address":                a.GetAddress(),
		"uptime":                 time.Since(a.startTime).String(),
		"max_message_size":       a.config.MaxMessageSize,
		"max_concurrent_streams": a.config.MaxConcurrentStreams,
		"enable_reflection":      a.config.EnableReflection,
		"enable_health":          a.config.EnableHealth,
		"enable_metrics":         a.config.EnableMetrics,
	}
}

// metricsUnaryInterceptor is a gRPC unary interceptor for metrics collection
func (a *GRPCAdapter) metricsUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	a.metrics.RequestDuration.Observe(duration.Seconds(), "grpc", info.FullMethod)

	if err != nil {
		a.metrics.RequestsTotal.Inc("grpc", info.FullMethod, "error")
	} else {
		a.metrics.RequestsTotal.Inc("grpc", info.FullMethod, "success")
	}

	return resp, err
}

// loggingUnaryInterceptor is a gRPC unary interceptor for logging
func (a *GRPCAdapter) loggingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	a.logger.Info("gRPC request started",
		zap.String("method", info.FullMethod),
		zap.Any("request", req))

	resp, err := handler(ctx, req)

	duration := time.Since(start)

	if err != nil {
		a.logger.Error("gRPC request failed",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.Error(err))
	} else {
		a.logger.Info("gRPC request completed",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.Any("response", resp))
	}

	return resp, err
}
