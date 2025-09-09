package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	searchconfig "vexdb/internal/search/config"
	"vexdb/internal/search/server"
	"vexdb/internal/search/service"
	"vexdb/internal/service/shutdown"
	"vexdb/internal/storage/search"

	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config/vexsearch.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize logger
	logger, err := logging.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("Starting VexDB Search Service", zap.String("config", configPath))

	// Load configuration
	cfg, err := searchconfig.DefaultSearchServiceConfig(), nil
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.Fatal("Invalid configuration", zap.Error(err))
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize metrics collector
	metricsCollector := metrics.NewCollector()
	if err := metricsCollector.Start(ctx); err != nil {
		logger.Fatal("Failed to start metrics collector", zap.Error(err))
	}

	// Initialize search engine
	// For now, create a mock search engine
	// In a real implementation, this would be properly initialized with buffer and segment managers
	searchEngine, err := search.NewEngine(nil, logger, metricsCollector, nil, nil)
	if err != nil {
		logger.Fatal("Failed to create search engine", zap.Error(err))
	}

	// Create search service
	searchService, err := service.NewSearchService(cfg, logger, metricsCollector, searchEngine)
	if err != nil {
		logger.Fatal("Failed to create search service", zap.Error(err))
	}

	// Start search service
	if err := searchService.Start(ctx); err != nil {
		logger.Fatal("Failed to start search service", zap.Error(err))
	}

	// Create HTTP server
	httpServer := server.NewHTTPServer(cfg, logger, metricsCollector, searchService)

	// Create gRPC server
	grpcServer := server.NewGRPCServer(cfg, logger, metricsCollector, searchService)

	// Start HTTP server
	if err := httpServer.Start(ctx); err != nil {
		logger.Fatal("Failed to start HTTP server", zap.Error(err))
	}

	// Start gRPC server
	if err := grpcServer.Start(ctx); err != nil {
		logger.Fatal("Failed to start gRPC server", zap.Error(err))
	}

	// Create shutdown handler
	shutdownHandler := shutdown.NewHandler(logger)
	shutdownHandler.RegisterService("search_service", searchService)
	shutdownHandler.RegisterService("http_server", httpServer)
	shutdownHandler.RegisterService("grpc_server", grpcServer)
	shutdownHandler.RegisterService("metrics_collector", metricsCollector)

	logger.Info("VexDB Search Service started successfully")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutdown signal received, shutting down VexDB Search Service...")

	// Perform graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := shutdownHandler.Shutdown(shutdownCtx); err != nil {
		logger.Error("Graceful shutdown failed", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("VexDB Search Service stopped")
}

// Service wrapper types for shutdown handler
type httpServerWrapper struct {
	server *server.HTTPServer
}

func (w *httpServerWrapper) Start(ctx context.Context) error {
	return w.server.Start(ctx)
}

func (w *httpServerWrapper) Stop(ctx context.Context) error {
	return w.server.Stop(ctx)
}

func (w *httpServerWrapper) Reload(ctx context.Context) error {
	return nil // HTTP server doesn't support reload
}

func (w *httpServerWrapper) HealthCheck(ctx context.Context) bool {
	return true // HTTP server health is managed by the search service
}

func (w *httpServerWrapper) GetStatus(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"type": "http_server",
	}
}

func (w *httpServerWrapper) GetConfig() interface{} {
	return nil // HTTP server doesn't have a config
}

func (w *httpServerWrapper) UpdateConfig(config interface{}) error {
	return nil // HTTP server doesn't support config updates
}

func (w *httpServerWrapper) GetMetrics() map[string]interface{} {
	return nil // HTTP server metrics are collected by the metrics collector
}

type grpcServerWrapper struct {
	server *server.GRPCServer
}

func (w *grpcServerWrapper) Start(ctx context.Context) error {
	return w.server.Start(ctx)
}

func (w *grpcServerWrapper) Stop(ctx context.Context) error {
	return w.server.Stop(ctx)
}

func (w *grpcServerWrapper) Reload(ctx context.Context) error {
	return nil // gRPC server doesn't support reload
}

func (w *grpcServerWrapper) HealthCheck(ctx context.Context) bool {
	return true // gRPC server health is managed by the search service
}

func (w *grpcServerWrapper) GetStatus(ctx context.Context) map[string]interface{} {
	return w.server.GetServiceInfo()
}

func (w *grpcServerWrapper) GetConfig() interface{} {
	return nil // gRPC server doesn't have a config
}

func (w *grpcServerWrapper) UpdateConfig(config interface{}) error {
	return nil // gRPC server doesn't support config updates
}

func (w *grpcServerWrapper) GetMetrics() map[string]interface{} {
	return nil // gRPC server metrics are collected by the metrics collector
}