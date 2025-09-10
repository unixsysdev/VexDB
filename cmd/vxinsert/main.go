package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"vxdb/internal/config"
	"vxdb/internal/logging"
	"vxdb/internal/types"
	pb "vxdb/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type insertServer struct {
	client pb.StorageServiceClient
	logger *zap.Logger
}

func (s *insertServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Determine if the request is a batch or single vector
	if len(body) > 0 && body[0] == '[' {
		var vectors []types.Vector
		if err := json.Unmarshal(body, &vectors); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pbVecs := make([]*pb.Vector, 0, len(vectors))
		for _, v := range vectors {
			if err := v.Validate(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			pbVecs = append(pbVecs, &pb.Vector{Id: v.ID, Data: v.Data})
		}
		req := &pb.InsertRequest{Request: &pb.InsertRequest_Batch{Batch: &pb.VectorBatch{Vectors: pbVecs}}}
		if _, err := s.client.InsertVector(r.Context(), req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		var v types.Vector
		if err := json.Unmarshal(body, &v); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := v.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p := &pb.Vector{Id: v.ID, Data: v.Data}
		req := &pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: p}}
		if _, err := s.client.InsertVector(r.Context(), req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusCreated)
}

func main() {
	var configPath, storageAddr string
	flag.StringVar(&configPath, "config", "configs/vxinsert-production.yaml", "Path to configuration file")
	flag.StringVar(&storageAddr, "storage-addr", "127.0.0.1:9096", "address of storage service")
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

	conn, err := grpc.Dial(storageAddr, grpc.WithInsecure())
	if err != nil {
		logger.Fatal("connect storage", zap.Error(err))
	}
	defer conn.Close()

	srv := &insertServer{client: pb.NewStorageServiceClient(conn), logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleInsert)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# VxDB Insert Service Metrics\n"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler: mux,
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
}
