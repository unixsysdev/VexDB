package main

import (
	"context"
	"flag"
	"net"
	"os/signal"
	"syscall"
	"time"

	"vxdb/internal/config"
	"vxdb/internal/logging"
	"vxdb/internal/metrics"
	"vxdb/internal/storage"
	"vxdb/internal/types"
	pb "vxdb/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	var configPath, grpcAddr string
	flag.StringVar(&configPath, "config", "configs/vxstorage-production.yaml", "Path to configuration file")
	flag.StringVar(&grpcAddr, "grpc-addr", "0.0.0.0:9096", "gRPC listen address")
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

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}
	grpcServer := grpc.NewServer()
	pb.RegisterStorageServiceServer(grpcServer, &storageServer{store: store})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc server", zap.Error(err))
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	store.Stop(shutdownCtx)
}

type storageServer struct {
	pb.UnimplementedStorageServiceServer
	store *storage.Storage
}

func (s *storageServer) InsertVector(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	v := req.GetVector()
	if v == nil {
		return &pb.InsertResponse{Success: false, Message: "vector required"}, nil
	}
	vec := &types.Vector{ID: v.Id, Data: v.Data}
	if err := s.store.StoreVector(ctx, vec); err != nil {
		return &pb.InsertResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.InsertResponse{Success: true, VectorIds: []string{vec.ID}}, nil
}

func (s *storageServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	qv := req.GetQueryVector()
	if qv == nil {
		return &pb.SearchResponse{Success: false, Message: "query vector required"}, nil
	}
	query := &types.Vector{ID: qv.Id, Data: qv.Data}
	results, err := s.store.SearchVectors(ctx, query, int(req.GetK()))
	if err != nil {
		return &pb.SearchResponse{Success: false, Message: err.Error()}, nil
	}
	pbRes := make([]*pb.SearchResult, 0, len(results))
	for _, r := range results {
		pbRes = append(pbRes, &pb.SearchResult{Vector: &pb.Vector{Id: r.Vector.ID, Data: r.Vector.Data}, Distance: float32(r.Distance), Score: r.Score})
	}
	return &pb.SearchResponse{Success: true, Results: pbRes, TotalResults: int64(len(pbRes))}, nil
}
