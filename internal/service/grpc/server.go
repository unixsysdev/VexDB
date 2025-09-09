package grpc

import (
    "context"
    "errors"
    "fmt"
    "net"
    "sync"
    "time"

	"go.uber.org/zap"
	"vexdb/internal/config"
	"vexdb/internal/health"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/keepalive"
)

var (
	ErrServerNotRunning    = errors.New("server is not running")
	ErrServerAlreadyRunning = errors.New("server is already running")
	ErrInvalidConfig       = errors.New("invalid server configuration")
	ErrServiceRegistration = errors.New("service registration failed")
)

// ServerConfig represents the gRPC server configuration
type ServerConfig struct {
	Host               string        `yaml:"host" json:"host"`
	Port               int           `yaml:"port" json:"port"`
	EnableReflection   bool          `yaml:"enable_reflection" json:"enable_reflection"`
	EnableHealthCheck  bool          `yaml:"enable_health_check" json:"enable_health_check"`
	MaxConnectionAge   time.Duration `yaml:"max_connection_age" json:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace" json:"max_connection_age_grace"`
	KeepaliveParams    *KeepaliveParams `yaml:"keepalive_params" json:"keepalive_params"`
	Interceptors       []InterceptorConfig `yaml:"interceptors" json:"interceptors"`
	EnableTLS          bool          `yaml:"enable_tls" json:"enable_tls"`
	TLSCertFile        string        `yaml:"tls_cert_file" json:"tls_cert_file"`
	TLSKeyFile         string        `yaml:"tls_key_file" json:"tls_key_file"`
}

// KeepaliveParams represents gRPC keepalive parameters
type KeepaliveParams struct {
	MaxConnectionIdle     time.Duration `yaml:"max_connection_idle" json:"max_connection_idle"`
	MaxConnectionAge      time.Duration `yaml:"max_connection_age" json:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace" json:"max_connection_age_grace"`
	Time                  time.Duration `yaml:"time" json:"time"`
	Timeout               time.Duration `yaml:"timeout" json:"timeout"`
}

// InterceptorConfig represents interceptor configuration
type InterceptorConfig struct {
	Name    string `yaml:"name" json:"name"`
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Order   int    `yaml:"order" json:"order"`
}

// DefaultServerConfig returns the default gRPC server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Host:              "0.0.0.0",
		Port:              50051,
		EnableReflection:  true,
		EnableHealthCheck: true,
		MaxConnectionAge:  5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
		KeepaliveParams: &KeepaliveParams{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      5 * time.Minute,
			MaxConnectionAgeGrace: 30 * time.Second,
			Time:                  2 * time.Hour,
			Timeout:               20 * time.Second,
		},
		Interceptors: []InterceptorConfig{
			{Name: "logging", Enabled: true, Order: 1},
			{Name: "metrics", Enabled: true, Order: 2},
			{Name: "recovery", Enabled: true, Order: 3},
			{Name: "validation", Enabled: true, Order: 4},
		},
		EnableTLS:   false,
	}
}

// Server represents a gRPC server
type Server struct {
	config     *ServerConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	health     health.HealthChecker
	
	// gRPC components
	server     *grpc.Server
	listener   net.Listener
	
	// Service registry
	services   map[string]interface{}
	mu         sync.RWMutex
	
	// Lifecycle
	started    bool
	stopped    bool
	shutdown   chan struct{}
}

// NewServer creates a new gRPC server
func NewServer(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics, health health.HealthChecker) (*Server, error) {
	serverConfig := DefaultServerConfig()
	
	if cfg != nil {
		if serverCfg, ok := cfg.Get("grpc"); ok {
			if cfgMap, ok := serverCfg.(map[string]interface{}); ok {
				if host, ok := cfgMap["host"].(string); ok {
					serverConfig.Host = host
				}
				if port, ok := cfgMap["port"].(int); ok {
					serverConfig.Port = port
				}
				if enableReflection, ok := cfgMap["enable_reflection"].(bool); ok {
					serverConfig.EnableReflection = enableReflection
				}
				if enableHealthCheck, ok := cfgMap["enable_health_check"].(bool); ok {
					serverConfig.EnableHealthCheck = enableHealthCheck
				}
				if maxConnectionAge, ok := cfgMap["max_connection_age"].(string); ok {
					if duration, err := time.ParseDuration(maxConnectionAge); err == nil {
						serverConfig.MaxConnectionAge = duration
					}
				}
				if maxConnectionAgeGrace, ok := cfgMap["max_connection_age_grace"].(string); ok {
					if duration, err := time.ParseDuration(maxConnectionAgeGrace); err == nil {
						serverConfig.MaxConnectionAgeGrace = duration
					}
				}
				if enableTLS, ok := cfgMap["enable_tls"].(bool); ok {
					serverConfig.EnableTLS = enableTLS
				}
				if tlsCertFile, ok := cfgMap["tls_cert_file"].(string); ok {
					serverConfig.TLSCertFile = tlsCertFile
				}
				if tlsKeyFile, ok := cfgMap["tls_key_file"].(string); ok {
					serverConfig.TLSKeyFile = tlsKeyFile
				}
				
				// Parse keepalive parameters
				if keepalive, ok := cfgMap["keepalive_params"].(map[string]interface{}); ok {
					if serverConfig.KeepaliveParams == nil {
						serverConfig.KeepaliveParams = &KeepaliveParams{}
					}
					if maxConnectionIdle, ok := keepalive["max_connection_idle"].(string); ok {
						if duration, err := time.ParseDuration(maxConnectionIdle); err == nil {
							serverConfig.KeepaliveParams.MaxConnectionIdle = duration
						}
					}
					if maxConnectionAge, ok := keepalive["max_connection_age"].(string); ok {
						if duration, err := time.ParseDuration(maxConnectionAge); err == nil {
							serverConfig.KeepaliveParams.MaxConnectionAge = duration
						}
					}
					if maxConnectionAgeGrace, ok := keepalive["max_connection_age_grace"].(string); ok {
						if duration, err := time.ParseDuration(maxConnectionAgeGrace); err == nil {
							serverConfig.KeepaliveParams.MaxConnectionAgeGrace = duration
						}
					}
					if timeParam, ok := keepalive["time"].(string); ok {
						if duration, err := time.ParseDuration(timeParam); err == nil {
							serverConfig.KeepaliveParams.Time = duration
						}
					}
					if timeout, ok := keepalive["timeout"].(string); ok {
						if duration, err := time.ParseDuration(timeout); err == nil {
							serverConfig.KeepaliveParams.Timeout = duration
						}
					}
				}
				
				// Parse interceptors
				if interceptors, ok := cfgMap["interceptors"].([]interface{}); ok {
					serverConfig.Interceptors = make([]InterceptorConfig, len(interceptors))
					for i, interceptor := range interceptors {
						if interceptorMap, ok := interceptor.(map[string]interface{}); ok {
							serverConfig.Interceptors[i] = InterceptorConfig{
								Name:    getString(interceptorMap, "name"),
								Enabled: getBool(interceptorMap, "enabled", true),
								Order:   getInt(interceptorMap, "order", i+1),
							}
						}
					}
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateServerConfig(serverConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	
	server := &Server{
		config:   serverConfig,
		logger:   logger,
		metrics:  metrics,
		health:   health,
		services: make(map[string]interface{}),
		shutdown: make(chan struct{}),
	}
	
	// Initialize gRPC server
	if err := server.initializeGRPCServer(); err != nil {
		return nil, fmt.Errorf("failed to initialize gRPC server: %w", err)
	}
	
	server.logger.Info("Created gRPC server",
		zap.String("host", serverConfig.Host),
		zap.Int("port", serverConfig.Port),
		zap.Bool("enable_reflection", serverConfig.EnableReflection),
		zap.Bool("enable_health_check", serverConfig.EnableHealthCheck),
		zap.Bool("enable_tls", serverConfig.EnableTLS))
	
	return server, nil
}

// validateServerConfig validates the server configuration
func validateServerConfig(cfg *ServerConfig) error {
	if cfg.Host == "" {
		return errors.New("host cannot be empty")
	}
	
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	
	if cfg.MaxConnectionAge <= 0 {
		return errors.New("max connection age must be positive")
	}
	
	if cfg.MaxConnectionAgeGrace <= 0 {
		return errors.New("max connection age grace must be positive")
	}
	
	if cfg.EnableTLS {
		if cfg.TLSCertFile == "" {
			return errors.New("TLS cert file is required when TLS is enabled")
		}
		if cfg.TLSKeyFile == "" {
			return errors.New("TLS key file is required when TLS is enabled")
		}
	}
	
	if cfg.KeepaliveParams != nil {
		if cfg.KeepaliveParams.MaxConnectionIdle <= 0 {
			return errors.New("max connection idle must be positive")
		}
		if cfg.KeepaliveParams.MaxConnectionAge <= 0 {
			return errors.New("max connection age must be positive")
		}
		if cfg.KeepaliveParams.MaxConnectionAgeGrace <= 0 {
			return errors.New("max connection age grace must be positive")
		}
		if cfg.KeepaliveParams.Time <= 0 {
			return errors.New("keepalive time must be positive")
		}
		if cfg.KeepaliveParams.Timeout <= 0 {
			return errors.New("keepalive timeout must be positive")
		}
	}
	
	return nil
}

// initializeGRPCServer initializes the gRPC server with interceptors
func (s *Server) initializeGRPCServer() error {
	// Create server options
	opts := s.getServerOptions()
	
	// Create gRPC server
	s.server = grpc.NewServer(opts...)
	
	// Register reflection service if enabled
	if s.config.EnableReflection {
		reflection.Register(s.server)
	}
	
	// Register health check service if enabled
	if s.config.EnableHealthCheck {
		grpc_health_v1.RegisterHealthServer(s.server, s)
	}
	
	return nil
}

// getServerOptions returns gRPC server options
func (s *Server) getServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption
	
	// Add keepalive parameters
	if s.config.KeepaliveParams != nil {
		keepalive := keepalive.ServerParameters{
			MaxConnectionIdle:     s.config.KeepaliveParams.MaxConnectionIdle,
			MaxConnectionAge:      s.config.KeepaliveParams.MaxConnectionAge,
			MaxConnectionAgeGrace: s.config.KeepaliveParams.MaxConnectionAgeGrace,
			Time:                  s.config.KeepaliveParams.Time,
			Timeout:               s.config.KeepaliveParams.Timeout,
		}
		opts = append(opts, grpc.KeepaliveParams(keepalive))
	}
	
	// Add interceptors
	interceptors := s.getInterceptors()
	if len(interceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(interceptors...))
		opts = append(opts, grpc.ChainStreamInterceptor(s.getStreamInterceptors()...))
	}
	
	return opts
}

// getInterceptors returns unary interceptors
func (s *Server) getInterceptors() []grpc.UnaryServerInterceptor {
	var interceptors []grpc.UnaryServerInterceptor
	
	// Sort interceptors by order
	sortedInterceptors := make([]InterceptorConfig, len(s.config.Interceptors))
	copy(sortedInterceptors, s.config.Interceptors)
	
	// Simple bubble sort by order
	for i := 0; i < len(sortedInterceptors)-1; i++ {
		for j := i + 1; j < len(sortedInterceptors); j++ {
			if sortedInterceptors[i].Order > sortedInterceptors[j].Order {
				sortedInterceptors[i], sortedInterceptors[j] = sortedInterceptors[j], sortedInterceptors[i]
			}
		}
	}
	
	for _, interceptor := range sortedInterceptors {
		if !interceptor.Enabled {
			continue
		}
		
		switch interceptor.Name {
		case "logging":
			interceptors = append(interceptors, s.loggingInterceptor)
		case "metrics":
			interceptors = append(interceptors, s.metricsInterceptor)
		case "recovery":
			interceptors = append(interceptors, s.recoveryInterceptor)
		case "validation":
			interceptors = append(interceptors, s.validationInterceptor)
		}
	}
	
	return interceptors
}

// getStreamInterceptors returns stream interceptors
func (s *Server) getStreamInterceptors() []grpc.StreamServerInterceptor {
	var interceptors []grpc.StreamServerInterceptor
	
	// Add stream interceptors as needed
	// For now, we'll use the same logic as unary interceptors
	
	return interceptors
}

// Start starts the gRPC server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.started {
		return ErrServerAlreadyRunning
	}
	
	// Create listener
	address := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	
	s.listener = listener
	
	// Start server in goroutine
	go func() {
		s.logger.Info("Starting gRPC server", zap.String("address", address))
		if err := s.server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			s.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()
	
	s.started = true
	s.stopped = false
	
	s.logger.Info("Started gRPC server", zap.String("address", address))
	
	return nil
}

// Stop stops the gRPC server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.stopped {
		return nil
	}
	
	if !s.started {
		return ErrServerNotRunning
	}
	
	// Graceful shutdown
	s.logger.Info("Stopping gRPC server")
	
	// Signal shutdown
	close(s.shutdown)
	
	// Graceful stop with timeout
	stopped := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()
	
	select {
	case <-stopped:
		s.logger.Info("gRPC server stopped gracefully")
	case <-time.After(30 * time.Second):
		s.logger.Warn("gRPC server shutdown timeout, forcing stop")
		s.server.Stop()
	}
	
	s.stopped = true
	s.started = false
	
	return nil
}

// IsRunning checks if the server is running
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return s.started && !s.stopped
}

// GetAddress returns the server address
func (s *Server) GetAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.listener == nil {
		return ""
	}
	
	return s.listener.Addr().String()
}

// RegisterService registers a gRPC service
func (s *Server) RegisterService(name string, service interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.started {
		return ErrServerNotRunning
	}
	
	// Check if service is already registered
	if _, exists := s.services[name]; exists {
		return fmt.Errorf("service %s is already registered", name)
	}
	
	// Register service (this is a simplified approach)
	// In a real implementation, you would use the specific registration methods
	s.services[name] = service
	
	s.logger.Info("Registered gRPC service", zap.String("name", name))
	
	return nil
}

// UnregisterService unregisters a gRPC service
func (s *Server) UnregisterService(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.started {
		return ErrServerNotRunning
	}
	
	// Check if service exists
	if _, exists := s.services[name]; !exists {
		return fmt.Errorf("service %s is not registered", name)
	}
	
	// Unregister service
	delete(s.services, name)
	
	s.logger.Info("Unregistered gRPC service", zap.String("name", name))
	
	return nil
}

// GetRegisteredServices returns registered services
func (s *Server) GetRegisteredServices() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	services := make([]string, 0, len(s.services))
	for name := range s.services {
		services = append(services, name)
	}
	
	return services
}

// GetConfig returns the server configuration
func (s *Server) GetConfig() *ServerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy of config
	config := *s.config
	return &config
}

// UpdateConfig updates the server configuration
func (s *Server) UpdateConfig(config *ServerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Validate new configuration
	if err := validateServerConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	
	s.config = config
	
	s.logger.Info("Updated gRPC server configuration", zap.Any("config", config))
	
	return nil
}

// Validate validates the server state
func (s *Server) Validate() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Validate configuration
	if err := validateServerConfig(s.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// Health check implementation
func (s *Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	service := req.Service
	
	// If no service specified, check server health
	if service == "" {
		if s.IsRunning() {
			return &grpc_health_v1.HealthCheckResponse{
				Status: grpc_health_v1.HealthCheckResponse_SERVING,
			}, nil
		}
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		}, nil
	}
	
	// Check specific service health
	s.mu.RLock()
	_, exists := s.services[service]
	s.mu.RUnlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("service %s not found", service))
	}
	
	// Check service health through health checker
    if s.health != nil {
        if status, err := s.health.CheckHealth(ctx, false); err == nil && status != nil && status.Status != "" {
            return &grpc_health_v1.HealthCheckResponse{
                Status: grpc_health_v1.HealthCheckResponse_SERVING,
            }, nil
        }
        return &grpc_health_v1.HealthCheckResponse{
            Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
        }, nil
    }
	
	// Default to serving if no health checker
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// Watch implements the health check streaming method
func (s *Server) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	// Simple implementation - just send current status
	resp, err := s.Check(stream.Context(), req)
	if err != nil {
		return err
	}
	
	return stream.Send(resp)
}

// List implements the HealthServer List method introduced in newer grpc-health versions.
func (s *Server) List(ctx context.Context, req *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    resp := &grpc_health_v1.HealthListResponse{Statuses: make(map[string]*grpc_health_v1.HealthCheckResponse)}
    for name := range s.services {
        resp.Statuses[name] = &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}
    }
    return resp, nil
}

// Interceptor implementations
func (s *Server) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	
	s.logger.Info("gRPC request started",
		zap.String("method", info.FullMethod),
		zap.String("request_type", fmt.Sprintf("%T", req)))
	
	resp, err := handler(ctx, req)
	
	duration := time.Since(start)
	
	if err != nil {
		s.logger.Error("gRPC request failed",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.Error(err))
	} else {
		s.logger.Info("gRPC request completed",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.String("response_type", fmt.Sprintf("%T", resp)))
	}
	
	return resp, err
}

func (s *Server) metricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	
	resp, err := handler(ctx, req)
	
	duration := time.Since(start)
	
	// Update metrics
    if s.metrics != nil {
        s.metrics.GRPCRequests.Inc("grpc")
        s.metrics.GRPCLatency.Observe(duration.Seconds())
        if err != nil {
            s.metrics.GRPCErrors.Inc("grpc")
        }
    }
	
	return resp, err
}

func (s *Server) recoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("gRPC panic recovered",
				zap.String("method", info.FullMethod),
				zap.Any("panic", r))
			
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	
	return handler(ctx, req)
}

func (s *Server) validationInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Validate request if it implements Validator interface
	if validator, ok := req.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	
	return handler(ctx, req)
}

// Helper functions
func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := m[key].(bool); ok {
		return val
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	if val, ok := m[key].(int); ok {
		return val
	}
	return defaultValue
}
