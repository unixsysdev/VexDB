package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vexdb/internal/config"
	logging2 "vexdb/internal/logging"
	"vexdb/internal/metrics"
	searchconfig "vexdb/internal/search/config"
	"vexdb/internal/search/server"
	"vexdb/internal/search/service"
	"vexdb/internal/storage/search"

	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config/vexsearch.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize logger
	logger, err := logging2.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("Starting VexDB Search Service", zap.String("config", configPath))

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

	// TODO: Initialize storage engine and search components
	// For now, we'll create mock components
	searchEngine, err := createMockSearchEngine(logger, metricsCollector)
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

	logger.Info("VexDB Search Service started successfully")

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutting down VexDB Search Service...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := stopServices(shutdownCtx, searchService, httpServer, grpcServer); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("VexDB Search Service stopped")
}

// loadConfig loads the search service configuration
func loadConfig(configPath string) (*searchconfig.SearchServiceConfig, error) {
	// Try to load from file
	cfg, err := config.LoadConfig("search", configPath)
	if err != nil {
		// If file doesn't exist, use default configuration
		logger, _ := logging2.NewLogger()
		logger.Warn("Failed to load configuration file, using default configuration", zap.Error(err))
		return searchconfig.DefaultSearchServiceConfig(), nil
	}

	// Convert to search service configuration
	searchCfg, ok := cfg.(*searchconfig.SearchServiceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type")
	}

	return searchCfg, nil
}

// createMockSearchEngine creates a mock search engine for testing
func createMockSearchEngine(logger *zap.Logger, metrics *metrics.Collector) (*search.Engine, error) {
	// For now, return a mock engine
	// In a real implementation, this would create the actual search engine
	return &search.Engine{}, nil
}

// startServices starts all services
func startServices(ctx context.Context, searchService service.SearchService, httpServer *server.HTTPServer, grpcServer *server.GRPCServer) error {
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
func stopServices(ctx context.Context, searchService service.SearchService, httpServer *server.HTTPServer, grpcServer *server.GRPCServer) error {
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
