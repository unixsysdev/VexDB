package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config/vexinsert.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize logger
	logger, err := logging.NewLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("Starting VexDB Insert Service", zap.String("config", configPath))

	// Load configuration
	_, err = config.LoadInsertConfig(configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Create context for graceful shutdown
	var cancel context.CancelFunc
	_, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// TODO: Initialize and start vexinsert service
	// This will be implemented in subsequent phases

	logger.Info("VexDB Insert Service started successfully")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutting down VexDB Insert Service...")

	// TODO: Graceful shutdown implementation

	logger.Info("VexDB Insert Service stopped")
}