//go:build ignore

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
    "vexdb/internal/health"
    "vexdb/internal/logging"
    svcmetrics "vexdb/internal/service/metrics"
    grpcsvc "vexdb/internal/service/grpc"
    healthsvc "vexdb/internal/service/health"
    shutdownsvc "vexdb/internal/service/shutdown"
    "vexdb/internal/storage"
    "vexdb/internal/storage/buffer"
    "vexdb/internal/storage/compression"
    "vexdb/internal/storage/hashing"
    clusterrange "vexdb/internal/storage/clusterrange"
    "vexdb/internal/storage/search"
    "vexdb/internal/storage/segment"
    "go.uber.org/zap"
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
    
    if *showVersion {
        printVersion()
        os.Exit(0)
    }
    if *showHelp {
        printHelp()
        os.Exit(0)
    }
    
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    logger, err := logging.NewLogger()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
        os.Exit(1)
    }
    defer logger.Sync()
    
    logger.Info("Starting VexStorage service",
        zap.String("version", version),
        zap.String("build", build),
        zap.String("commit", commit),
        zap.String("date", date))

    // Load configuration
    cfg, err := config.LoadStorageConfig(*configFile)
    if err != nil {
        logger.Error("Failed to load configuration", zap.Error(err))
        os.Exit(1)
    }

    // Initialize metrics collector
    metricsCollector, err := svcmetrics.NewMetricsCollector(cfg, logger)
    if err != nil {
        logger.Error("Failed to initialize metrics collector", zap.Error(err))
        os.Exit(1)
    }
    if err := metricsCollector.Start(); err != nil {
        logger.Error("Failed to start metrics collector", zap.Error(err))
        os.Exit(1)
    }

    // Initialize service metrics
    serviceMetrics, err := metricsCollector.NewServiceMetrics()
    if err != nil {
        logger.Error("Failed to initialize service metrics", zap.Error(err))
        os.Exit(1)
    }

    // Initialize storage metrics
    storageMetrics, err := metricsCollector.NewStorageMetrics()
    if err != nil {
        logger.Error("Failed to initialize storage metrics", zap.Error(err))
        os.Exit(1)
    }

    // Initialize health checker
    healthChecker := health.NewHealthChecker()

    // Initialize shutdown handler
    shutdownHandler, err := shutdownsvc.NewShutdownHandler(cfg, logger, serviceMetrics)
    if err != nil {
        logger.Error("Failed to initialize shutdown handler", zap.Error(err))
        os.Exit(1)
    }

    // Initialize storage components
    components, err := initializeStorageComponents(cfg, logger, storageMetrics, healthChecker)
    if err != nil {
        logger.Error("Failed to initialize storage components", zap.Error(err))
        os.Exit(1)
    }

    // Initialize gRPC server
    grpcServer, err := grpcsvc.NewServer(cfg, logger, serviceMetrics, healthChecker)
    if err != nil {
        logger.Error("Failed to initialize gRPC server", zap.Error(err))
        os.Exit(1)
    }

    // Initialize health endpoints
    healthEndpoints, err := healthsvc.NewHealthEndpoints(cfg, logger, serviceMetrics)
    if err != nil {
        logger.Error("Failed to initialize health endpoints", zap.Error(err))
        os.Exit(1)
    }

    // Register health checks
    registerHealthChecks(healthEndpoints, components, healthChecker)

    // Register gRPC services
    registerGRPCServices(grpcServer, components)

    // Register shutdown hooks
    registerShutdownHooks(shutdownHandler, grpcServer, healthEndpoints, metricsCollector, components)

    // Start services
    if err := startServices(ctx, metricsCollector, grpcServer, healthEndpoints, shutdownHandler, logger); err != nil {
        logger.Error("Failed to start services", zap.Error(err))
        os.Exit(1)
    }

    logger.Info("VexStorage service started successfully")

    // Wait for shutdown signal
    waitForShutdown(logger, shutdownHandler)
}

// no-op helpers removed; zap used directly

// initializeStorageComponents initializes all storage components
func initializeStorageComponents(cfg *config.StorageConfig, logger logging.Logger, metrics *svcmetrics.StorageMetrics, healthChecker health.HealthChecker) (*StorageComponents, error) {
    // Initialize segment manager
    segmentManager, err := segment.NewManager(cfg, logger, metrics)
    if err != nil { return nil, fmt.Errorf("failed to initialize segment manager: %w", err) }

    // Initialize buffer manager
    bufferManager, err := buffer.NewManager(cfg, logger, metrics)
    if err != nil { return nil, fmt.Errorf("failed to initialize buffer manager: %w", err) }

    // Initialize compression engine
    compressionEngine, err := compression.NewEngine(cfg, logger, metrics)
    if err != nil { return nil, fmt.Errorf("failed to initialize compression engine: %w", err) }

    // Initialize hashing engine
    hashingEngine, err := hashing.NewEngine(cfg, logger, metrics)
    if err != nil { return nil, fmt.Errorf("failed to initialize hashing engine: %w", err) }

    // Initialize range manager
    rangeManager, err := clusterrange.NewManager(cfg, logger, metrics)
    if err != nil { return nil, fmt.Errorf("failed to initialize range manager: %w", err) }

    // Initialize search engine
    searchEngine, err := search.NewEngine(cfg, logger, metrics, segmentManager, bufferManager)
    if err != nil { return nil, fmt.Errorf("failed to initialize search engine: %w", err) }

    // Initialize storage service
    storageService, err := storage.NewService(cfg, logger, metrics, healthChecker,
        segmentManager, bufferManager, compressionEngine, hashingEngine, rangeManager, searchEngine)
    if err != nil { return nil, fmt.Errorf("failed to initialize storage service: %w", err) }

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
    RangeManager      *clusterrange.Manager
    SearchEngine      *search.Engine
    StorageService    *storage.Service
}

// registerHealthChecks registers health checks
func registerHealthChecks(healthEndpoints *healthsvc.HealthEndpoints, components *StorageComponents, healthChecker health.HealthChecker) {
    healthEndpoints.RegisterCheck("storage", func(ctx context.Context) error { return components.StorageService.HealthCheck(ctx) })
    healthEndpoints.RegisterCheck("buffer", func(ctx context.Context) error { return components.BufferManager.HealthCheck(ctx) })
    healthEndpoints.RegisterCheck("search", func(ctx context.Context) error { return components.SearchEngine.HealthCheck(ctx) })
    healthEndpoints.RegisterCheck("memory", func(ctx context.Context) error { return healthChecker.CheckMemory(ctx) })
    healthEndpoints.RegisterCheck("disk", func(ctx context.Context) error { return healthChecker.CheckDisk(ctx) })
}

// registerGRPCServices registers gRPC services
func registerGRPCServices(grpcServer *grpcsvc.Server, components *StorageComponents) {
    _ = grpcServer.RegisterService("storage", components.StorageService)
    _ = grpcServer.RegisterService("search", components.SearchEngine)
}

// registerShutdownHooks registers shutdown hooks
func registerShutdownHooks(shutdownHandler *shutdownsvc.ShutdownHandler, grpcServer *grpcsvc.Server, healthEndpoints *healthsvc.HealthEndpoints, metricsCollector *svcmetrics.MetricsCollector, components *StorageComponents) {
    shutdownHandler.RegisterHook("grpc_server", func(ctx context.Context) error { return grpcServer.Stop() }, shutdownsvc.ShutdownHookConfig{ Name: "grpc_server", Order: 1, Timeout: 10 * time.Second, Critical: true })
    shutdownHandler.RegisterHook("health_endpoints", func(ctx context.Context) error { return healthEndpoints.Stop() }, shutdownsvc.ShutdownHookConfig{ Name: "health_endpoints", Order: 2, Timeout: 5 * time.Second, Critical: false })
    shutdownHandler.RegisterHook("metrics_collector", func(ctx context.Context) error { return metricsCollector.Stop() }, shutdownsvc.ShutdownHookConfig{ Name: "metrics_collector", Order: 3, Timeout: 5 * time.Second, Critical: false })
    shutdownHandler.RegisterHook("storage_service", func(ctx context.Context) error { return components.StorageService.Shutdown(ctx) }, shutdownsvc.ShutdownHookConfig{ Name: "storage_service", Order: 4, Timeout: 15 * time.Second, Critical: true })
    shutdownHandler.RegisterHook("buffer_manager", func(ctx context.Context) error { return components.BufferManager.Shutdown(ctx) }, shutdownsvc.ShutdownHookConfig{ Name: "buffer_manager", Order: 5, Timeout: 10 * time.Second, Critical: true })
    shutdownHandler.RegisterHook("search_engine", func(ctx context.Context) error { return components.SearchEngine.Shutdown(ctx) }, shutdownsvc.ShutdownHookConfig{ Name: "search_engine", Order: 6, Timeout: 10 * time.Second, Critical: false })
}

// startServices starts all services
func startServices(ctx context.Context, metricsCollector *svcmetrics.MetricsCollector, grpcServer *grpcsvc.Server, healthEndpoints *healthsvc.HealthEndpoints, shutdownHandler *shutdownsvc.ShutdownHandler, logger logging.Logger) error {
    if err := healthEndpoints.Start(); err != nil { return fmt.Errorf("failed to start health endpoints: %w", err) }
    if err := grpcServer.Start(); err != nil { return fmt.Errorf("failed to start gRPC server: %w", err) }
    if err := shutdownHandler.Start(); err != nil { return fmt.Errorf("failed to start shutdown handler: %w", err) }
    return nil
}

// waitForShutdown waits for shutdown signal
func waitForShutdown(logger logging.Logger, shutdownHandler *shutdownsvc.ShutdownHandler) {
    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
    <-signalChan
    if err := shutdownHandler.Shutdown(); err != nil {
        fmt.Fprintf(os.Stderr, "Graceful shutdown failed: %v\n", err)
        os.Exit(1)
    }
}

func printVersion() {
    fmt.Printf("VexStorage v%s\nBuild: %s\nCommit: %s\nDate: %s\n", version, build, commit, date)
}

func printHelp() {
    fmt.Println("VexStorage - Vector Storage Engine")
    fmt.Println("\nUsage: vexstorage [options]")
    fmt.Println("\nOptions:")
    fmt.Println("  --config string     Path to configuration file (default: config.yaml)")
    fmt.Println("  --log-level string  Log level (debug, info, warn, error) (default: info)")
    fmt.Println("  --log-format string Log format (json, text) (default: json)")
    fmt.Println("  --version           Show version information")
    fmt.Println("  --help              Show help information")
}
