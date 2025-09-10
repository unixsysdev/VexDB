package grpc

import (
	"context"
	"io"
	"net"
	"testing"

	grpc2 "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "vxdb/proto"
)

// insertServer proxies InsertService calls to an underlying storage service.
type insertServer struct {
	pb.UnimplementedInsertServiceServer
	store *testStorageService
}

func (s *insertServer) InsertVector(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	return s.store.InsertVector(ctx, req)
}

func (s *insertServer) InsertVectorStream(stream pb.InsertService_InsertVectorStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp, err := s.store.InsertVector(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// searchServer proxies SearchService calls to the same storage service.
type searchServer struct {
	pb.UnimplementedSearchServiceServer
	store *testStorageService
}

func (s *searchServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	return s.store.Search(ctx, req)
}

func (s *searchServer) SearchStream(stream pb.SearchService_SearchStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp, err := s.store.Search(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// TestFullFlow inserts a vector via the InsertService and retrieves it via the
// SearchService, ensuring both paths hit the same storage backend.
func TestFullFlow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	srv := grpc2.NewServer()
	pb.RegisterStorageServiceServer(srv, &testStorageService{})
	store := &testStorageService{}
	pb.RegisterInsertServiceServer(srv, &insertServer{store: store})
	pb.RegisterSearchServiceServer(srv, &searchServer{store: store})

	go srv.Serve(ln)
	defer srv.Stop()

	conn, err := grpc2.Dial(ln.Addr().String(), grpc2.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	insertClient := pb.NewInsertServiceClient(conn)
	searchClient := pb.NewSearchServiceClient(conn)

	vec := &pb.Vector{Id: "v1", Data: []float32{0.1, 0.2}}
	if _, err := insertClient.InsertVector(context.Background(), &pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: vec}}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}

	res, err := searchClient.Search(context.Background(), &pb.SearchRequest{QueryVector: vec, K: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Vector.Id != "v1" {
		t.Fatalf("unexpected search result: %+v", res.Results)
	}
}

// TestFullFlowStream exercises the streaming gRPC APIs for inserting and searching vectors.
func TestFullFlowStream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	srv := grpc2.NewServer()
	pb.RegisterStorageServiceServer(srv, &testStorageService{})
	store := &testStorageService{}
	pb.RegisterInsertServiceServer(srv, &insertServer{store: store})
	pb.RegisterSearchServiceServer(srv, &searchServer{store: store})

	go srv.Serve(ln)
	defer srv.Stop()

	conn, err := grpc2.Dial(ln.Addr().String(), grpc2.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	insertClient := pb.NewInsertServiceClient(conn)
	searchClient := pb.NewSearchServiceClient(conn)

	vec1 := &pb.Vector{Id: "v1", Data: []float32{0.1, 0.2}}
	vec2 := &pb.Vector{Id: "v2", Data: []float32{0.9, 0.9}}

	istream, err := insertClient.InsertVectorStream(context.Background())
	if err != nil {
		t.Fatalf("insert stream: %v", err)
	}
	if err := istream.Send(&pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: vec1}}); err != nil {
		t.Fatalf("send vec1: %v", err)
	}
	if _, err := istream.Recv(); err != nil {
		t.Fatalf("recv vec1: %v", err)
	}
	if err := istream.Send(&pb.InsertRequest{Request: &pb.InsertRequest_Vector{Vector: vec2}}); err != nil {
		t.Fatalf("send vec2: %v", err)
	}
	if _, err := istream.Recv(); err != nil {
		t.Fatalf("recv vec2: %v", err)
	}
	if err := istream.CloseSend(); err != nil {
		t.Fatalf("close insert stream: %v", err)
	}

	q := &pb.Vector{Data: []float32{0.1, 0.2}}
	sstream, err := searchClient.SearchStream(context.Background())
	if err != nil {
		t.Fatalf("search stream: %v", err)
	}
	if err := sstream.Send(&pb.SearchRequest{QueryVector: q, K: 1}); err != nil {
		t.Fatalf("send search: %v", err)
	}
	resp, err := sstream.Recv()
	if err != nil {
		t.Fatalf("recv search: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Vector.Id != "v1" {
		t.Fatalf("unexpected search result: %+v", resp.Results)
	}
	sstream.CloseSend()
}
