package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	pb "vxdb/proto"
)

type searchServer struct {
	client pb.StorageServiceClient
	logger *zap.Logger
}

type searchRequest struct {
	Vector []float32 `json:"vector"`
	K      int32     `json:"k"`
}

type multiClusterRequest struct {
	Vector     []float32 `json:"vector"`
	K          int32     `json:"k"`
	ClusterIDs []string  `json:"cluster_ids"`
}

type batchQuery struct {
	Vector []float32 `json:"vector"`
	K      int32     `json:"k"`
}

type batchRequest struct {
	Queries []batchQuery `json:"queries"`
}

func (s *searchServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pbReq := &pb.SearchRequest{QueryVector: &pb.Vector{Data: req.Vector}, K: req.K}
	resp, err := s.client.Search(r.Context(), pbReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := protojson.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *searchServer) handleMultiCluster(w http.ResponseWriter, r *http.Request) {
	var req multiClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pbReq := &pb.SearchRequest{
		QueryVector: &pb.Vector{Data: req.Vector},
		K:           req.K,
		ClusterIds:  req.ClusterIDs,
	}
	resp, err := s.client.Search(r.Context(), pbReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := protojson.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *searchServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out []json.RawMessage
	for _, q := range req.Queries {
		pbReq := &pb.SearchRequest{QueryVector: &pb.Vector{Data: q.Vector}, K: q.K}
		resp, err := s.client.Search(r.Context(), pbReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b, err := protojson.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out = append(out, b)
	}
	data, err := json.Marshal(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func main() {
	var httpAddr, storageAddr string
	flag.StringVar(&httpAddr, "http-addr", "0.0.0.0:8083", "HTTP listen address")
	flag.StringVar(&storageAddr, "storage-addr", "127.0.0.1:9096", "address of storage service")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	conn, err := grpc.Dial(storageAddr, grpc.WithInsecure())
	if err != nil {
		logger.Fatal("connect storage", zap.Error(err))
	}
	defer conn.Close()

	srv := &searchServer{client: pb.NewStorageServiceClient(conn), logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/search", srv.handleSearch)
	mux.HandleFunc("/search/multi-cluster", srv.handleMultiCluster)
	mux.HandleFunc("/search/batch", srv.handleBatch)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# VxDB Search Service Metrics\n"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	server := &http.Server{Addr: httpAddr, Handler: mux}

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
