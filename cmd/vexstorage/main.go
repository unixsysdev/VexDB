package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vexdb/vexdb/internal/config"
	"github.com/vexdb/vexdb/internal/health"
	"github.com/vexdb/vexdb/internal/logging"
	"github.com/vexdb/vexdb/internal/metrics"
	"github.com/vexdb/vexdb/internal/service/grpc"
	"github.com/vexdb/vexdb/internal/service/health"
	"github.com/vexdb/vexdb/internal/service/metrics"
	"github.com/vexdb/vexdb/internal/service/shutdown"
	"github.com/vexdb/vexdb/internal/storage"
	"github.com/vexdb/vexdb/internal/storage/buffer"
	"github.com/vexdb/vexdb/internal/storage/compression"
	"github.com/vexdb/vexdb/internal/storage/hashing"
	"github.com/vexdb/vexdb/internal/storage/range"
	"github.com/vexdb/vexdb/internal/storage/search"
	"github.com/vexdb/vexdb/internal/storage/segment"
	"github.com/vexdb/vexdb/internal/types"
)

var (
	version   = "1.0.0"
	build     = "dev"
	commit    = "unknown"
	date      = "unknown"
	
	configFile = flag.String("config", "config.yaml", "Path to configuration file")
	logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logFormat  = flag.String("log-format", "json", "Log format (json, text)")
	showVersion = flag.Bool("version", false, "Show version information")
	showHelp    = flag.Bool("help", false, "Show help information")
)

func main() {
	flag.Parse()
	
	// Handle version and help flags
	if *showVersion {
		printVersion()
		os.Exit(0)
	}
	
	if *showHelp {
		printHelp()
		os.Exit(0)
	}
	
	// Initialize context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Initialize logger
	logger, err := logging.NewLogger(&logging.Config{
		Level:  *logLevel,
		Format: *logFormat,
		Output: "stdout",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	
	logger.Info("Starting VexStorage service",
		"version", version,
		"build", build,
		"commit", commit,
		"date", date)
	
	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}
	
	// Initialize metrics collector
	metricsCollector, err := metrics.NewMetricsCollector(cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize metrics collector", "error", err)
		os.Exit(1)
	}
	
	// Initialize service metrics
	serviceMetrics, err := metricsCollector.NewServiceMetrics()
	if err != nil {
		logger.Error("Failed to initialize service metrics", "error", err)
		os.Exit(1)
	}
	
	// Initialize storage metrics
	storageMetrics, err := metricsCollector.NewStorageMetrics()
	if err != nil {
		logger.Error("Failed to initialize storage metrics", "error", err)
		os.Exit(1)
	}
	
	// Initialize health checker
	healthChecker := health.NewHealthChecker()
	
	// Initialize shutdown handler
	shutdownHandler, err := shutdown.NewShutdownHandler(cfg, logger, serviceMetrics)
	if err != nil {
		logger.Error("Failed to initialize shutdown handler", "error", err)
		os.Exit(1)
	}
	
	// Initialize storage components
	storageComponents, err := initializeStorageComponents(cfg, logger, storageMetrics, healthChecker)
	if err != nil {
		logger.Error("Failed to initialize storage components", "error", err)
		os.Exit(1)
	}
	
	// Initialize gRPC server
	grpcServer, err := grpc.NewServer(cfg, logger, serviceMetrics, healthChecker)
	if err != nil {
		logger.Error("Failed to initialize gRPC server", "error", err)
		os.Exit(1)
	}
	
	// Initialize health endpoints
	healthEndpoints, err := health.NewHealthEndpoints(cfg, logger, serviceMetrics)
	if err != nil {
		logger.Error("Failed to initialize health endpoints", "error", err)
		os.Exit(1)
	}
	
	// Register health checks
	registerHealthChecks(healthEndpoints, storageComponents, healthChecker)
	
	// Register gRPC services
	registerGRPCServices(grpcServer, storageComponents)
	
	// Register shutdown hooks
	registerShutdownHooks(shutdownHandler, grpcServer, healthEndpoints, metricsCollector, storageComponents)
	
	// Start services
	if err := startServices(ctx, logger, metricsCollector, grpcServer, healthEndpoints, shutdownHandler); err != nil {
		logger.Error("Failed to start services", "error", err)
		os.Exit(1)
	}
	
	// Update service metrics
	updateServiceMetrics(serviceMetrics, version, build)
	
	logger.Info("VexStorage service started successfully")
	
	// Wait for shutdown signal
	waitForShutdown(logger, shutdownHandler)
	
	// Shutdown complete
	logger.Info("VexStorage service shutdown complete")
}

// initializeStorageComponents initializes all storage components
func initializeStorageComponents(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics, healthChecker health.HealthChecker) (*StorageComponents, error) {
	// Initialize segment manager
	segmentManager, err := segment.NewManager(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize segment manager: %w", err)
	}
	
	// Initialize buffer manager
	bufferManager, err := buffer.NewManager(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize buffer manager: %w", err)
	}
	
	// Initialize compression engine
	compressionEngine, err := compression.NewEngine(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize compression engine: %w", err)
	}
	
	// Initialize hashing engine
	hashingEngine, err := hashing.NewEngine(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize hashing engine: %w", err)
	}
	
	// Initialize range manager
	rangeManager, err := range.NewManager(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize range manager: %w", err)
	}
	
	// Initialize search engine
	searchEngine, err := search.NewEngine(cfg, logger, metrics, segmentManager, bufferManager)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize search engine: %w", err)
	}
	
	// Initialize storage service
	storageService, err := storage.NewService(cfg, logger, metrics, healthChecker,
		segmentManager, bufferManager, compressionEngine, hashingEngine, rangeManager, searchEngine)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage service: %w", err)
	}
	
	return &StorageComponents{
		SegmentManager:    segmentManager,
		BufferManager:     bufferManager,
		CompressionEngine: compressionEngine,
		HashingEngine:     hashingEngine,
		RangeManager:      rangeManager,
		SearchEngine:      searchEngine,
		StorageService:    storageService,
	}, nil
}

// StorageComponents holds all storage components
type StorageComponents struct {
	SegmentManager    *segment.Manager
	BufferManager     *buffer.Manager
	CompressionEngine *compression.Engine
	HashingEngine     *hashing.Engine
	RangeManager      *range.Manager
	SearchEngine      *search.Engine
	StorageService    *storage.Service
}

// registerHealthChecks registers health checks
func registerHealthChecks(healthEndpoints *health.HealthEndpoints, components *StorageComponents, healthChecker health.HealthChecker) {
	// Storage health check
	healthEndpoints.RegisterCheck("storage", func(ctx context.Context) error {
		return components.StorageService.HealthCheck(ctx)
	})
	
	// Buffer health check
	healthEndpoints.RegisterCheck("buffer", func(ctx context.Context) error {
		return components.BufferManager.HealthCheck(ctx)
	})
	
	// Search health check
	healthEndpoints.RegisterCheck("search", func(ctx context.Context) error {
		return components.SearchEngine.HealthCheck(ctx)
	})
	
	// Memory health check
	healthEndpoints.RegisterCheck("memory", func(ctx context.Context) error {
		return healthChecker.CheckMemory(ctx)
	})
	
	// Disk health check
	healthEndpoints.RegisterCheck("disk", func(ctx context.Context) error {
		return healthChecker.CheckDisk(ctx)
	})
}

// registerGRPCServices registers gRPC services
func registerGRPCServices(grpcServer *grpc.Server, components *StorageComponents) {
	// Register storage service
	if err := grpcServer.RegisterService("storage", components.StorageService); err != nil {
		// Log error but continue
		fmt.Printf("Failed to register storage service: %v\n", err)
	}
	
	// Register search service
	if err := grpcServer.RegisterService("search", components.SearchEngine); err != nil {
		// Log error but continue
		fmt.Printf("Failed to register search service: %v\n", err)
	}
}

// registerShutdownHooks registers shutdown hooks
func registerShutdownHooks(shutdownHandler *shutdown.ShutdownHandler, grpcServer *grpc.Server, healthEndpoints *health.HealthEndpoints, metricsCollector *metrics.MetricsCollector, components *StorageComponents) {
	// gRPC server shutdown hook
	shutdownHandler.RegisterHook("grpc_server", func(ctx context.Context) error {
		return grpcServer.Stop()
	}, shutdown.ShutdownHookConfig{
		Name:     "grpc_server",
		Order:    1,
		Timeout:  10 * time.Second,
		Critical: true,
	})
	
	// Health endpoints shutdown hook
	shutdownHandler.RegisterHook("health_endpoints", func(ctx context.Context) error {
		return healthEndpoints.Stop()
	}, shutdown.ShutdownHookConfig{
		Name:     "health_endpoints",
		Order:    2,
		Timeout:  5 * time.Second,
		Critical: false,
	})
	
	// Metrics collector shutdown hook
	shutdownHandler.RegisterHook("metrics_collector", func(ctx context.Context) error {
		return metricsCollector.Stop()
	}, shutdown.ShutdownHookConfig{
		Name:     "metrics_collector",
		Order:    3,
		Timeout:  5 * time.Second,
		Critical: false,
	})
	
	// Storage service shutdown hook
	shutdownHandler.RegisterHook("storage_service", func(ctx context.Context) error {
		return components.StorageService.Shutdown(ctx)
	}, shutdown.ShutdownHookConfig{
		Name:     "storage_service",
		Order:    4,
		Timeout:  15 * time.Second,
		Critical: true,
	})
	
	// Buffer manager shutdown hook
	shutdownHandler.RegisterHook("buffer_manager", func(ctx context.Context) error {
		return components.BufferManager.Shutdown(ctx)
	}, shutdown.ShutdownHookConfig{
		Name:     "buffer_manager",
		Order:    5,
		Timeout:  10 * time.Second,
		Critical: true,
	})
	
	// Search engine shutdown hook
	shutdownHandler.RegisterHook("search_engine", func(ctx context.Context) error {
		return components.SearchEngine.Shutdown(ctx)
	}, shutdown.ShutdownHookConfig{
		Name:     "search_engine",
		Order:    6,
		Timeout:  10 * time.Second,
		Critical: false,
	})
}

// startServices starts all services
func startServices(ctx context.Context, logger logging.Logger, metricsCollector *metrics.MetricsCollector, grpcServer *grpc.Server, healthEndpoints *health.HealthEndpoints, shutdownHandler *shutdown.ShutdownHandler) error {
	// Start metrics collector
	if err := metricsCollector.Start(); err != nil {
		return fmt.Errorf("failed to start metrics collector: %w", err)
	}
	
	// Start health endpoints
	if err := healthEndpoints.Start(); err != nil {
		return fmt.Errorf("failed to start health endpoints: %w", err)
	}
	
	// Start gRPC server
	if err := grpcServer.Start(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}
	
	// Start shutdown handler
	if err := shutdownHandler.Start(); err != nil {
		return fmt.Errorf("failed to start shutdown handler: %w", err)
	}
	
	return nil
}

// updateServiceMetrics updates service metrics
func updateServiceMetrics(serviceMetrics *metrics.ServiceMetrics, version, build string) {
	serviceMetrics.ServiceUptime.Set(0)
	serviceMetrics.ServiceVersion.WithLabelValues(version, build).Set(1)
	
	// Start uptime ticker
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		
		startTime := time.Now()
		for range ticker.C {
			serviceMetrics.ServiceUptime.Set(time.Since(startTime).Seconds())
		}
	}()
}

// waitForShutdown waits for shutdown signal
func waitForShutdown(logger logging.Logger, shutdownHandler *shutdown.ShutdownHandler) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	
	sig := <-signalChan
	logger.Info("Received shutdown signal", "signal", sig)
	
	// Initiate graceful shutdown
	if err := shutdownHandler.Shutdown(); err != nil {
		logger.Error("Graceful shutdown failed", "error", err)
		os.Exit(1)
	}
}

// printVersion prints version information
func printVersion() {
	fmt.Printf("VexStorage v%s\n", version)
	fmt.Printf("Build: %s\n", build)
	fmt.Printf("Commit: %s\n", commit)
	fmt.Printf("Date: %s\n", date)
}

// printHelp prints help information
func printHelp() {
	fmt.Println("VexStorage - Vector Storage Engine")
	fmt.Println()
	fmt.Println("Usage: vexstorage [options]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --config string     Path to configuration file (default: config.yaml)")
	fmt.Println("  --log-level string  Log level (debug, info, warn, error) (default: info)")
	fmt.Println("  --log-format string Log format (json, text) (default: json)")
	fmt.Println("  --version           Show version information")
	fmt.Println("  --help              Show help information")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  VEXSTORAGE_CONFIG   Path to configuration file")
	fmt.Println("  VEXSTORAGE_LOG_LEVEL Log level")
	fmt.Println("  VEXSTORAGE_LOG_FORMAT Log format")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  vexstorage --config /etc/vexstorage/config.yaml")
	fmt.Println("  vexstorage --log-level debug --log-format text")
	fmt.Println("  VEXSTORAGE_CONFIG=/etc/vexstorage/config.yaml vexstorage")
}