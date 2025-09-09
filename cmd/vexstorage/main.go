package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/storage"

	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "configs/vexstorage-production.yaml", "Path to configuration file")
	flag.Parse()

	logger, err := logging.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.LoadStorageConfig(configPath)
	if err != nil {
		logger.Fatal("load config", zap.Error(err))
	}

	metricsCollector, err := metrics.NewMetrics(cfg)
	if err != nil {
		logger.Fatal("init metrics", zap.Error(err))
	}
	storageMetrics := metrics.NewStorageMetrics(metricsCollector, "storage")

	store, err := storage.NewStorage(nil, logger, storageMetrics)
	if err != nil {
		logger.Fatal("init storage", zap.Error(err))
	}
	if err := store.Start(context.Background()); err != nil {
		logger.Fatal("start storage", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store.Stop(shutdownCtx)
}
