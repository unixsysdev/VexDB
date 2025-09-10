package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "vxdb/proto"
)

type searchServer struct {
	clients []pb.StorageServiceClient
	logger  *zap.Logger
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
	results, err := s.searchAll(r.Context(), pbReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := &pb.SearchResponse{Success: true, Results: results, TotalResults: int64(len(results))}
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
	results, err := s.searchAll(r.Context(), pbReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := &pb.SearchResponse{Success: true, Results: results, TotalResults: int64(len(results))}
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
		results, err := s.searchAll(r.Context(), pbReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.SearchResponse{Success: true, Results: results, TotalResults: int64(len(results))}
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

func (s *searchServer) searchAll(ctx context.Context, req *pb.SearchRequest) ([]*pb.SearchResult, error) {
	type result struct {
		res []*pb.SearchResult
		err error
	}
	ch := make(chan result, len(s.clients))
	for _, c := range s.clients {
		reqCopy := proto.Clone(req).(*pb.SearchRequest)
		go func(cli pb.StorageServiceClient, r *pb.SearchRequest) {
			resp, err := cli.Search(ctx, r)
			if err != nil {
				ch <- result{err: err}
				return
			}
			ch <- result{res: resp.Results}
		}(c, reqCopy)
	}
	merged := make(map[string]*pb.SearchResult)
	for i := 0; i < len(s.clients); i++ {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		for _, res := range r.res {
			if res.GetVector() == nil {
				continue
			}
			id := res.GetVector().GetId()
			if existing, ok := merged[id]; !ok || res.Distance < existing.Distance {
				merged[id] = res
			}
		}
	}
	all := make([]*pb.SearchResult, 0, len(merged))
	for _, res := range merged {
		all = append(all, res)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Distance < all[j].Distance })
	if req.K > 0 && len(all) > int(req.K) {
		all = all[:req.K]
	}
	return all, nil
}

func main() {
	var httpAddr, storageAddrs string
	flag.StringVar(&httpAddr, "http-addr", "0.0.0.0:8083", "HTTP listen address")
	flag.StringVar(&storageAddrs, "storage-addrs", "127.0.0.1:9096", "comma-separated addresses of storage services")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

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

	srv := &searchServer{clients: clients, logger: logger}
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
