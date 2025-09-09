//go:build ignore

package main

import (
    "flag"
    "net/http"
    "os"
    "os/signal"
    "syscall"

    "vexdb/internal/logging"
    "go.uber.org/zap"
)

func main() {
    var configPath string
    flag.StringVar(&configPath, "config", "/etc/vexdb/config.yaml", "Path to configuration file")
    flag.Parse()

    logger, err := logging.NewLogger()
    if err != nil { panic(err) }
    defer logger.Sync()

    logger.Info("Starting VexDB Search (stub)", zap.String("config", configPath))

    // Minimal HTTP with health endpoint to keep container alive
    mux := http.NewServeMux()
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok")) })

    srv := &http.Server{ Addr: ":8083", Handler: mux }
    go func() { _ = srv.ListenAndServe() }()

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
    <-sig
    logger.Info("Shutting down VexDB Search (stub)")
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
