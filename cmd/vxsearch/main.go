package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	logging2 "vxdb/internal/logging"
	"vxdb/internal/metrics"
	searchconfig "vxdb/internal/search/config"
	"vxdb/internal/search/server"
	"vxdb/internal/search/service"
	"vxdb/internal/storage/search"

	"gopkg.in/yaml.v2"

	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "configs/vxsearch-production.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize logger
	logger, err := logging2.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("Starting VxDB Search Service", zap.String("config", configPath))

	// Load configuration
	searchConfig, err := loadConfig(configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Validate configuration
	if err := searchConfig.Validate(); err != nil {
		logger.Fatal("Invalid configuration", zap.Error(err))
	}

	// Initialize metrics
	metricsCollector, err := metrics.NewMetrics(searchConfig)
	if err != nil {
		logger.Fatal("Failed to initialize metrics", zap.Error(err))
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	searchEngine, err := createSearchEngine(logger, metricsCollector)
	if err != nil {
		logger.Fatal("Failed to create search engine", zap.Error(err))
	}

	// Create search service
	searchService, err := service.NewSearchService(searchConfig, logger, metricsCollector, searchEngine)
	if err != nil {
		logger.Fatal("Failed to create search service", zap.Error(err))
	}

	// Create HTTP server
	httpServer := server.NewHTTPServer(searchConfig, logger, metricsCollector, searchService)

	// Create gRPC server
	grpcServer := server.NewGRPCServer(searchConfig, logger, metricsCollector, searchService)

	// Start services
	if err := startServices(ctx, searchService, httpServer, grpcServer); err != nil {
		logger.Fatal("Failed to start services", zap.Error(err))
	}

	logger.Info("VxDB Search Service started successfully")

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutting down VxDB Search Service...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := stopServices(shutdownCtx, searchService, httpServer, grpcServer); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("VxDB Search Service stopped")
}

// loadConfig loads the search service configuration from a YAML file
func loadConfig(configPath string) (*searchconfig.SearchServiceConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := searchconfig.DefaultSearchServiceConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// createSearchEngine creates the actual search engine
func createSearchEngine(logger *zap.Logger, m *metrics.Collector) (*search.Engine, error) {
	storageMetrics := metrics.NewStorageMetrics(m, "search")
	return search.NewSearchEngine(nil, logger, storageMetrics)
}

// startServices starts all services
func startServices(ctx context.Context, searchService *service.SearchService, httpServer *server.HTTPServer, grpcServer *server.GRPCServer) error {
	// Start search service
	if err := searchService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start search service: %w", err)
	}

	// Start HTTP server
	if err := httpServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	// Start gRPC server
	if err := grpcServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	return nil
}

// stopServices stops all services
func stopServices(ctx context.Context, searchService *service.SearchService, httpServer *server.HTTPServer, grpcServer *server.GRPCServer) error {
	var firstErr error

	// Stop gRPC server
	if err := grpcServer.Stop(ctx); err != nil {
		firstErr = err
	}

	// Stop HTTP server
	if err := httpServer.Stop(ctx); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	// Stop search service
	if err := searchService.Stop(ctx); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
