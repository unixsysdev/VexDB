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

    logger.Info("Starting VexDB Storage (stub)", zap.String("config", configPath))

    mux := http.NewServeMux()
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok")) })

    srv := &http.Server{ Addr: ":8082", Handler: mux }
    go func() { _ = srv.ListenAndServe() }()

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
    <-sig
    logger.Info("Shutting down VexDB Storage (stub)")
}

