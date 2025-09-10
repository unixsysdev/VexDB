# VxDB

VxDB is a distributed, high-throughput streaming vector database designed for real-time similarity search and scalable ingestion. It features a shared-nothing architecture with three core services: ingestion (`vxinsert`), storage (`vxstorage`), and query coordination (`vxsearch`).

## Features
- **Sub-second write latency** and immediate searchability
- **Horizontal scalability** for ingestion, storage, and search
- **Multi-protocol support:** HTTP, WebSocket, gRPC, Kafka, Redis
- **Append-only segment storage** with deterministic replication
- **Unified IVF (Inverted File) indexing** for fast approximate search
- **Stateless query coordination** with scatter-gather execution
- **Prometheus, Grafana, and Jaeger** integration for observability
- **Dedicated query planner configuration** with extensible search metrics

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                        VxDB Cluster                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  vxinsert  │  │  vxinsert  │  │  vxinsert  │          │
│  │  (Adapters) │  │  (Adapters) │  │  (Adapters) │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │               │
│  ┌──────┴─────────────────┴─────────────────┴──────┐        │
│  │              Load Distribution                  │        │
│  └──────┬─────────────────┬─────────────────┬──────┘        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ vxstorage  │  │ vxstorage  │  │ vxstorage  │          │
│  │ (Segments + │  │ (Segments + │  │ (Segments + │          │
│  │ IVF Index)  │  │ IVF Index)  │  │ IVF Index)  │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  vxsearch  │  │  vxsearch  │  │  vxsearch  │          │
│  │ (Stateless  │  │ (Stateless  │  │ (Stateless  │          │
│  │Coordinator) │  │Coordinator) │  │Coordinator) │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## Core Services

### vxinsert (Ingestion)
- Multi-protocol ingestion: WebSocket, HTTP, Kafka, Redis, gRPC
- Hash-based cluster assignment and deterministic replication
- Streaming-optimized, concurrent ingestion
- For writes, vxinsert forwards to the appropriate vxstorage nodes; the
  storage service is responsible for synchronizing data across the cluster
  according to its replication settings

### vxstorage (Storage & Indexing)
- Append-only segment files, in-memory buffer for fast writes
- Unified IVF index for approximate search
- Hybrid index update: incremental + periodic full rebuilds
- Dual search: memory buffer (exact) + IVF (approximate)

### vxsearch (Query Coordination)
- Stateless, scatter-gather query execution
- Cluster range calculation and node selection
- Result merging, ranking, and fault tolerance

## Data Model
- **Vector:** `{ "id": string, "vector": [float64], "metadata": { ... } }`
- **Max dimension:** 4096 (configurable)
- **Max metadata size:** 64KB per vector

## Deployment

### Docker Compose
VxDB provides a `docker-compose.yml` for local development and testing:

```sh
docker-compose up --build
```

This will start:
- `vxinsert` (ingestion)
- `vxstorage` (storage/index)
- `vxsearch` (query)
- `prometheus`, `grafana`, `jaeger` (monitoring)
- `redis` (optional), `nginx` (optional)

### Configuration
Service configs are in `configs/` (e.g., `vxinsert-production.yaml`).

## API Examples

### Ingest Vectors (HTTP)
```http
POST /vectors
Content-Type: application/json
{
  "vectors": [
    { "id": "vec_001", "vector": [0.1, 0.2, 0.3], "metadata": {"category": "doc"} }
  ]
}
```

### Search (HTTP)
```http
POST /search
Content-Type: application/json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 10,
  "filters": { "metadata.category": "doc" }
}
```

## Observability
- **Metrics:** `/metrics` endpoint (Prometheus format)
- **Health:** `/health` and `/ready` endpoints
- **Tracing:** Jaeger integration

## Testing

Run unit tests:

```
go test ./...
```

Run integration tests (requires additional services):

```
go test -tags integration ./...
```

## Code Graph

A lightweight Go AST walker can generate a project-wide JSON graph of structs,
functions (including their parameters and return types), and call
relationships in this repository:

```
go run scripts/go_code_graph.go . > go_graph.json
```

The resulting file contains node and edge lists with function signatures and
simple stats.

## Security
- mTLS for inter-service
- API key/token for client auth
- TLS 1.3 for all endpoints

## References
- See `SPEC_RFC.md` for full technical specification.
- [IVF Indexing](https://arxiv.org/abs/1702.08734), [WebSocket RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455), [gRPC](https://grpc.io/)

---

© 2025 VxDB Development Team
