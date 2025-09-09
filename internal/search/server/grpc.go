package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/search/config"
	"vexdb/internal/search/grpc"
	"vexdb/internal/service"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// GRPCServer represents the gRPC server for the search service
type GRPCServer struct {
	config        *config.SearchServiceConfig
	logger        *zap.Logger
	metrics       *metrics.Collector
	searchService service.SearchService
	server        *grpc.Server
	listener      net.Listener
}

// NewGRPCServer creates a new gRPC server for the search service
func NewGRPCServer(cfg *config.SearchServiceConfig, logger *zap.Logger, metrics *metrics.Collector, searchService service.SearchService) *GRPCServer {
	return &GRPCServer{
		config:        cfg,
		logger:        logger,
		metrics:       metrics,
		searchService: searchService,
	}
}

// Start starts the gRPC server
func (s *GRPCServer) Start(ctx context.Context) error {
	if !s.config.API.GRPC.Enabled {
		s.logger.Info("gRPC server is disabled")
		return nil
	}

	s.logger.Info("Starting gRPC server", zap.String("address", s.getAddress()))

	// Create listener
	listener, err := net.Listen("tcp", s.getAddress())
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	s.listener = listener

	// Create gRPC server options
	opts := s.createServerOptions()

	// Create gRPC server
	s.server = grpc.NewServer(opts...)

	// Create and register search server
	searchServer := grpc.NewSearchServer(s.config, s.logger, s.metrics, s.searchService)
	searchServer.RegisterServer(s.server)

	// Register reflection service for development
	// Always register reflection for development
	reflection.Register(s.server)

	// Start server in background
	go func() {
		s.logger.Info("gRPC server listening", zap.String("address", s.getAddress()))
		if err := s.server.Serve(listener); err != nil {
			s.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	s.logger.Info("gRPC server started successfully", zap.String("address", s.getAddress()))
	return nil
}

// Stop stops the gRPC server
func (s *GRPCServer) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.logger.Info("Stopping gRPC server")

	// Graceful shutdown
	s.server.GracefulStop()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Failed to close gRPC listener", zap.Error(err))
		}
	}

	s.logger.Info("gRPC server stopped successfully")
	return nil
}

// createServerOptions creates gRPC server options
func (s *GRPCServer) createServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption

	// Set max message size
	if s.config.API.GRPC.MaxMessageSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(s.config.API.GRPC.MaxMessageSize))
		opts = append(opts, grpc.MaxSendMsgSize(s.config.API.GRPC.MaxMessageSize))
	}

	// Set max concurrent streams
	if s.config.API.GRPC.MaxConcurrentStreams > 0 {
		opts = append(opts, grpc.MaxConcurrentStreams(uint32(s.config.API.GRPC.MaxConcurrentStreams)))
	}

	// Set keepalive parameters
	if s.config.API.GRPC.Keepalive.Time > 0 {
		keepaliveParams := keepalive.ServerParameters{
			Time:    s.config.API.GRPC.Keepalive.Time,
			Timeout: s.config.API.GRPC.Keepalive.Timeout,
		}
		opts = append(opts, grpc.KeepaliveParams(keepaliveParams))

		keepalivePolicy := keepalive.EnforcementPolicy{
			MinTime:             s.config.API.GRPC.Keepalive.Time,
			PermitWithoutStream: s.config.API.GRPC.Keepalive.PermitWithoutStream,
		}
		opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepalivePolicy))
	}

	// Add interceptors
	opts = append(opts, grpc.UnaryInterceptor(s.unaryInterceptor))
	opts = append(opts, grpc.StreamInterceptor(s.streamInterceptor))

	return opts
}

// unaryInterceptor handles unary RPC calls
func (s *GRPCServer) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	method := info.FullMethod

	// Log request
	s.logger.Info("gRPC unary request started",
		zap.String("method", method),
		zap.Any("request", req),
	)

	// Process request
	resp, err := handler(ctx, req)

	// Record metrics
	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
	}

	s.metrics.RecordHistogram("grpc_request_duration_seconds", duration.Seconds(), map[string]string{
		"method": method,
		"type":   "unary",
		"status": status,
	})

	s.metrics.RecordCounter("grpc_requests_total", 1, map[string]string{
		"method": method,
		"type":   "unary",
		"status": status,
	})

	// Log response
	if err != nil {
		s.logger.Error("gRPC unary request failed",
			zap.String("method", method),
			zap.Error(err),
			zap.Duration("duration", duration),
		)
	} else {
		s.logger.Info("gRPC unary request completed",
			zap.String("method", method),
			zap.Duration("duration", duration),
		)
	}

	return resp, err
}

// streamInterceptor handles streaming RPC calls
func (s *GRPCServer) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	method := info.FullMethod

	// Log stream start
	s.logger.Info("gRPC stream started",
		zap.String("method", method),
	)

	// Process stream
	err := handler(srv, ss)

	// Record metrics
	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
	}

	s.metrics.RecordHistogram("grpc_request_duration_seconds", duration.Seconds(), map[string]string{
		"method": method,
		"type":   "stream",
		"status": status,
	})

	s.metrics.RecordCounter("grpc_requests_total", 1, map[string]string{
		"method": method,
		"type":   "stream",
		"status": status,
	})

	// Log stream end
	if err != nil {
		s.logger.Error("gRPC stream failed",
			zap.String("method", method),
			zap.Error(err),
			zap.Duration("duration", duration),
		)
	} else {
		s.logger.Info("gRPC stream completed",
			zap.String("method", method),
			zap.Duration("duration", duration),
		)
	}

	return err
}

// getAddress returns the server address
func (s *GRPCServer) getAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port+1) // Use port+1 for gRPC
}

// GetServiceInfo returns information about the gRPC service
func (s *GRPCServer) GetServiceInfo() map[string]interface{} {
	if s.server == nil {
		return map[string]interface{}{
			"status": "not_started",
		}
	}

	return map[string]interface{}{
		"status":    "running",
		"address":   s.getAddress(),
		"services":  s.server.GetServiceInfo(),
	}
}