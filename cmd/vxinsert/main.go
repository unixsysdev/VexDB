package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"vxdb/internal/config"
	"vxdb/internal/logging"
	"vxdb/internal/types"
	pb "vxdb/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type insertServer struct {
	clients       []pb.StorageServiceClient
	logger        *zap.Logger
	totalClusters uint32
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
		for i := range vectors {
			v := &vectors[i]
			if err := v.Validate(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if s.totalClusters > 0 {
				v.ClusterID = computeClusterID(v.Data, s.totalClusters)
			}
			ts := timestamppb.Now()
			if v.Timestamp != 0 {
				ts = timestamppb.New(time.Unix(0, v.Timestamp))
			}
			pbVecs = append(pbVecs, &pb.Vector{Id: v.ID, Data: v.Data, ClusterId: v.ClusterID, Metadata: toProtoMetadata(v.Metadata), Timestamp: ts})
		}
		req := &pb.InsertRequest{Request: &pb.InsertRequest_Batch{Batch: &pb.VectorBatch{Vectors: pbVecs}}}
		if err := s.replicateInsert(r.Context(), req); err != nil {
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
		if s.totalClusters > 0 {
			v.ClusterID = computeClusterID(v.Data, s.totalClusters)
		}
		ts := timestamppb.Now()
		if v.Timestamp != 0 {
			ts = timestamppb.New(time.Unix(0, v.Timestamp))
		}
		p := &pb.Vector{Id: v.ID, Data: v.Data, ClusterId: v.ClusterID, Metadata: toProtoMetadata(v.Metadata), Timestamp: ts}
		req := &pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: p}}
		if err := s.replicateInsert(r.Context(), req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *insertServer) replicateInsert(ctx context.Context, req *pb.InsertRequest) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.clients))
	for _, c := range s.clients {
		wg.Add(1)
		go func(cli pb.StorageServiceClient) {
			defer wg.Done()
			if _, err := cli.InsertVector(ctx, req); err != nil {
				errCh <- err
			}
		}(c)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func toProtoMetadata(m map[string]interface{}) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func computeClusterID(data []float32, total uint32) uint32 {
	if total == 0 {
		return 0
	}
	h := fnv.New32a()
	var buf [4]byte
	for _, f := range data {
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f))
		h.Write(buf[:])
	}
	return h.Sum32() % total
}

func main() {
	var configPath, storageAddrs string
	var totalClusters uint
	flag.StringVar(&configPath, "config", "configs/vxinsert-production.yaml", "Path to configuration file")
	flag.StringVar(&storageAddrs, "storage-addrs", "127.0.0.1:9096", "comma-separated addresses of storage services")
	flag.UintVar(&totalClusters, "total-clusters", 1024, "total number of clusters")
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

	addrs := strings.Split(storageAddrs, ",")
	clients := make([]pb.StorageServiceClient, 0, len(addrs))
	conns := make([]*grpc.ClientConn, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			logger.Fatal("connect storage", zap.Error(err))
		}
		conns = append(conns, conn)
		clients = append(clients, pb.NewStorageServiceClient(conn))
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	srv := &insertServer{clients: clients, logger: logger, totalClusters: uint32(totalClusters)}
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
