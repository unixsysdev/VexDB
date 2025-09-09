package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/storage"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

type insertServer struct {
	store  *storage.Storage
	logger *zap.Logger
}

func (s *insertServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	var v types.Vector
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := v.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.StoreVector(r.Context(), &v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "configs/vexinsert-production.yaml", "Path to configuration file")
	flag.Parse()

	logger, err := logging.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.LoadInsertConfig(configPath)
	if err != nil {
		logger.Fatal("load config", zap.Error(err))
	}

	metricsCollector, err := metrics.NewMetrics(cfg)
	if err != nil {
		logger.Fatal("init metrics", zap.Error(err))
	}
	storageMetrics := metrics.NewStorageMetrics(metricsCollector, "insert")

	store, err := storage.NewStorage(nil, logger, storageMetrics)
	if err != nil {
		logger.Fatal("init storage", zap.Error(err))
	}
	if err := store.Start(context.Background()); err != nil {
		logger.Fatal("start storage", zap.Error(err))
	}

	srv := &insertServer{store: store, logger: logger}
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler: http.HandlerFunc(srv.handleInsert),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", zap.Error(err))
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
	store.Stop(shutdownCtx)
}
