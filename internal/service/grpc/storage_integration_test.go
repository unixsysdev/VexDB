package grpc

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"

	"go.uber.org/zap"
	grpc2 "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"vxdb/internal/types"
	pb "vxdb/proto"
)

// testStorageService is a lightweight in-memory implementation of the
// StorageService used to exercise the gRPC server end-to-end.
type testStorageService struct {
	pb.UnimplementedStorageServiceServer
	mu      sync.Mutex
	vectors []*types.Vector
}

func (s *testStorageService) InsertVector(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	vec := pbToTypeVector(req.GetVector())
	s.mu.Lock()
	s.vectors = append(s.vectors, vec)
	s.mu.Unlock()
	return &pb.InsertResponse{Success: true, VectorIds: []string{vec.ID}}, nil
}

func (s *testStorageService) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	q := pbToTypeVector(req.QueryVector)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.vectors) == 0 {
		return &pb.SearchResponse{Success: true}, nil
	}
	best := s.vectors[0]
	bestDist, _ := q.Distance(best, "euclidean")
	for _, v := range s.vectors[1:] {
		d, _ := q.Distance(v, "euclidean")
		if d < bestDist {
			bestDist = d
			best = v
		}
	}
	return &pb.SearchResponse{
		Success:      true,
		Results:      []*pb.SearchResult{{Vector: typeToPBVector(best), Distance: float32(bestDist)}},
		TotalResults: 1,
	}, nil
}

func pbToTypeVector(v *pb.Vector) *types.Vector {
	if v == nil {
		return nil
	}
	data := make([]float32, len(v.Data))
	copy(data, v.Data)
	md := make(map[string]interface{}, len(v.Metadata))
	for k, val := range v.Metadata {
		md[k] = val
	}
	return &types.Vector{ID: v.Id, Data: data, Metadata: md}
}

func typeToPBVector(v *types.Vector) *pb.Vector {
	if v == nil {
		return nil
	}
	md := make(map[string]string, len(v.Metadata))
	for k, val := range v.Metadata {
		if s, ok := val.(string); ok {
			md[k] = s
		}
	}
	return &pb.Vector{Id: v.ID, Data: v.Data, Metadata: md}
}

// TestStorageServiceIntegration spins up the gRPC server and verifies a
// vector can be inserted and subsequently retrieved via a similarity search.
func TestStorageServiceIntegration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cfg := mapConfig{"grpc": map[string]interface{}{
		"host":                "127.0.0.1",
		"port":                port,
		"enable_reflection":   false,
		"enable_health_check": false,
	}}

	srv, err := NewServer(cfg, *zap.NewNop(), nil, nil)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	pb.RegisterStorageServiceServer(srv.server, &testStorageService{})
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer srv.Stop()

	conn, err := grpc2.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc2.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewStorageServiceClient(conn)

	vec := &pb.Vector{Id: "v1", Data: []float32{0.1, 0.2}}
	if _, err := client.InsertVector(context.Background(), &pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: vec}}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}

	res, err := client.Search(context.Background(), &pb.SearchRequest{QueryVector: vec, K: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Vector.Id != "v1" || math.Abs(float64(res.Results[0].Distance)) > 1e-6 {
		t.Fatalf("unexpected search result: %+v", res.Results)
	}
}
