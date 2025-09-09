# VexDB: Streaming Vector Database System Specification
**Version:** 1.0  
**Status:** Draft  
**Authors:** VexDB Development Team  
**Date:** January 2025

---

## Abstract

VexDB is a distributed vector database system optimized for high-throughput streaming ingestion and low-latency similarity search. The system employs a shared-nothing architecture with three independent services: vxinsert (ingestion), vxsearch (query coordination), and vxstorage (storage and indexing). VexDB prioritizes write performance through append-only segment storage and maintains search quality via unified IVF (Inverted File) indices with hybrid update strategies.

Key design principles include protocol compatibility for seamless integration with existing data infrastructure, deterministic replication for operational simplicity, and configurable performance tuning to accommodate diverse workload requirements.

---

## 1. Introduction

### 1.1 Problem Statement

Contemporary vector databases face fundamental limitations when handling high-velocity streaming workloads. Existing solutions typically optimize for read performance at the expense of write latency, employ complex consensus mechanisms that introduce operational overhead, and require specialized protocols that increase integration friction.

The demand for real-time vector similarity search in applications such as recommendation systems, content discovery, and retrieval-augmented generation necessitates a database architecture that can ingest vectors at sub-second latency while maintaining reasonable search quality and operational simplicity.

### 1.2 Design Goals

**Primary Objectives:**
- Sub-second write latency with immediate vector searchability
- Horizontal scalability without performance degradation
- Multi-protocol compatibility for ecosystem integration
- Operational simplicity through minimal coordination requirements

**Acceptable Trade-offs:**
- 85-90% similarity search recall versus 95%+ theoretical maximum
- Eventual consistency over strong consistency guarantees
- Single algorithm optimization (IVF) over universal algorithm support

### 1.3 System Overview

VexDB implements a three-tier architecture where each service operates independently:

- **vxinsert**: Multi-protocol ingestion layer with streaming optimization
- **vxstorage**: Distributed storage engine with unified vector indexing
- **vxsearch**: Stateless query coordination with scatter-gather execution

The system employs fixed cluster assignment for predictable routing, deterministic segment boundaries for replica consistency, and hybrid indexing strategies for balanced read/write performance.

---

## 2. Architecture Overview

### 2.1 System Components

```
┌─────────────────────────────────────────────────────────────┐
│                        VexDB Cluster                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  vxinsert  │  │  vxinsert  │  │  vxinsert  │          │
│  │   (Proto    │  │   (Proto    │  │   (Proto    │          │
│  │  Adapters)  │  │  Adapters)  │  │  Adapters)  │          │
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

### 2.2 Data Flow

**Write Path:**
1. Client connects via supported protocol (WebSocket, HTTP, Kafka, Redis, gRPC)
2. vxinsert calculates cluster assignment: `cluster_id = hash(vector) % total_clusters`
3. Vector routed to both replica vxstorage nodes concurrently
4. vxstorage nodes buffer vectors in memory with immediate searchability
5. Deterministic flush to segment files every N vectors (e.g., 10,000)

**Read Path:**
1. Client submits similarity query to any vxsearch instance
2. vxsearch determines relevant cluster ranges for query vector
3. Parallel search requests sent to all vxstorage nodes owning those ranges
4. Each vxstorage performs dual search: memory buffer + IVF index
5. vxsearch merges results and returns global top-k vectors

---

## 3. Service Specifications

### 3.1 vxinsert - Ingestion Service

**Service Responsibility:** Multi-protocol vector ingestion with cluster routing and replication management.

#### 3.1.1 Protocol Adapters

**WebSocket Streaming Interface:**
```
Endpoint: ws://host:port/stream
Message Format:
{
  "vector": [0.1, 0.2, 0.3, ...],
  "metadata": {
    "id": "vector_identifier", 
    "timestamp": 1640995200,
    "namespace": "collection_name"
  }
}
```

**HTTP REST Interface:**
```
POST /vectors
Content-Type: application/json

{
  "vectors": [
    {
      "id": "vec_001",
      "vector": [0.1, 0.2, 0.3],
      "metadata": {"category": "document"}
    }
  ]
}
```

**Kafka Connect Compatibility:**
```
connector.class=VexDBSinkConnector
topics=embeddings
vector.field=embedding
metadata.fields=id,timestamp,source
batch.size=1000
```

**Redis Protocol Commands:**
```
VECTOR.ADD key vector metadata
VECTOR.MADD key1 vec1 meta1 key2 vec2 meta2
```

**gRPC Interface:**
```protobuf
service VectorIngestion {
  rpc StreamVectors(stream VectorRequest) returns (stream VectorResponse);
  rpc BatchInsert(BatchVectorRequest) returns (BatchVectorResponse);
}
```

#### 3.1.2 Cluster Routing

**Hash-Based Assignment:**
- Fixed cluster count determined at deployment (e.g., 1024 clusters)
- Deterministic routing: `cluster_id = hash(vector_content) % cluster_count`
- Consistent hashing ensures identical routing across vxinsert instances
- Cluster-to-node mapping defined in static configuration

**Replication Strategy:**
- Each vector sent to exactly two vxstorage nodes (primary + replica)
- Concurrent transmission using Go goroutines for non-blocking performance
- No client acknowledgment required for streaming protocols
- Failed transmissions retried in background with exponential backoff

#### 3.1.3 Configuration

```yaml
vxinsert:
  protocols:
    websocket:
      enabled: true
      port: 8080
      max_connections: 10000
    http:
      enabled: true
      port: 8081
    kafka_connect:
      enabled: true
      port: 8082
    redis:
      enabled: true
      port: 6379
    grpc:
      enabled: true
      port: 9090
  
  cluster:
    total_clusters: 1024
    replication_factor: 2
    retry_attempts: 3
    retry_backoff: "100ms"
```

### 3.2 vxstorage - Storage Engine

**Service Responsibility:** Vector storage, indexing, and similarity search execution for assigned cluster ranges.

#### 3.2.1 Storage Architecture

**Segment-Based Storage:**
- Append-only segment files with deterministic boundaries
- Fixed vector count per segment (configurable, default: 10,000 vectors)
- Immutable segments ensure consistency and simplify replication
- Sequential I/O optimization for high write throughput

**File Structure:**
```
/data/node_id/
├── segments/
│   ├── cluster_001_seg_001.vex
│   ├── cluster_001_seg_002.vex
│   └── cluster_002_seg_001.vex
├── indices/
│   ├── ivf_snapshot_v123.idx
│   └── ivf_wal.log
└── metadata/
    ├── manifest.json
    └── cluster_assignments.json
```

**Memory Buffer Management:**
- In-memory buffer for recent vectors with immediate searchability
- Configurable buffer size (default: 1MB - 16MB)
- Linear similarity search over buffer contents
- Automatic flush to segment when vector count threshold reached

#### 3.2.2 IVF Index Implementation

**Unified Index Architecture:**
- Single IVF index per vxstorage node covering all assigned clusters
- In-memory index structure with disk-based persistence
- Segment pointers enable efficient vector retrieval from storage
- Hybrid update strategy balances performance and quality

**Index Update Strategy:**
```
Incremental Updates:
- Triggered after each segment creation
- New vectors added to existing inverted lists
- Fast operation (10-50ms for typical segment sizes)

Full Rebuilds:
- Scheduled at configurable intervals (default: 1 hour)
- Complete reconstruction from all segment data
- Ensures optimal cluster quality and index compression
- Background operation maintains service availability
```

**Persistence and Recovery:**
- Periodic index snapshots written to disk
- Write-ahead log captures incremental changes since last snapshot
- Crash recovery rebuilds index from snapshot + WAL replay
- Configurable snapshot frequency balances durability and performance

#### 3.2.3 Query Processing

**Dual Search Strategy:**
1. **Memory Buffer Search:** Linear scan over recent vectors for perfect recall
2. **IVF Index Search:** Approximate search over persisted segment data
3. **Result Merging:** Distance-based ranking across both result sets

**Search Pipeline:**
```
Query Vector Input
        ↓
┌─────────────────┐  ┌─────────────────┐
│ Memory Buffer   │  │ IVF Index       │
│ Linear Search   │  │ Cluster Search  │
└─────────────────┘  └─────────────────┘
        ↓                      ↓
┌─────────────────────────────────────────┐
│         Result Merging                  │
│     (Distance-based ranking)            │
└─────────────────────────────────────────┘
        ↓
   Top-K Results + Metadata
```

#### 3.2.4 Configuration

```yaml
vxstorage:
  storage:
    data_directory: "/var/lib/vexdb"
    vectors_per_segment: 10000
    memory_buffer_size: "8MB"
    
  ivf:
    clusters_per_node: 256
    update_mode: "hybrid"
    incremental_threshold: "1000_vectors"
    rebuild_interval: "1h"
    rebuild_threshold: 50
    
  performance:
    search_threads: 4
    io_threads: 2
    compression: "lz4"
```

### 3.3 vxsearch - Query Coordination

**Service Responsibility:** Stateless query coordination with scatter-gather execution across vxstorage nodes.

#### 3.3.1 Query Planning

**Cluster Range Determination:**
- Calculate target cluster for query vector using identical hash function
- Determine relevant vxstorage nodes based on cluster ownership
- Support for multi-cluster queries when similarity spans cluster boundaries
- Query optimization through early termination and result caching

**Execution Strategy:**
```
Query Analysis
     ↓
Cluster Range Calculation
     ↓
Node Selection (Primary/Replica)
     ↓
Parallel Query Dispatch
     ↓
Result Collection & Merging
     ↓
Top-K Response Generation
```

#### 3.3.2 API Specification

**HTTP REST Interface:**
```
POST /search
Content-Type: application/json

{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "filters": {
    "metadata.category": "document",
    "metadata.timestamp": {"$gte": 1640995200}
  },
  "options": {
    "recall_preference": "fast" | "accurate",
    "timeout": "5s"
  }
}

Response:
{
  "results": [
    {
      "id": "vec_001",
      "vector": [0.15, 0.18, 0.25, ...],
      "metadata": {"category": "document"},
      "similarity": 0.95
    }
  ],
  "total_results": 1,
  "query_time_ms": 23
}
```

**gRPC Interface:**
```protobuf
service VectorSearch {
  rpc Search(SearchRequest) returns (SearchResponse);
  rpc StreamingSearch(stream SearchRequest) returns (stream SearchResponse);
}
```

#### 3.3.3 Result Merging

**Distance-Based Ranking:**
- Collect results from all queried vxstorage nodes
- Global ranking using similarity scores across all sources
- Duplicate elimination based on vector IDs
- Configurable result count limits and timeout handling

**Fault Tolerance:**
- Partial results returned when subset of nodes unavailable
- Automatic failover between primary and replica nodes
- Graceful degradation with quality indicators in response metadata

---

## 4. Data Models and Formats

### 4.1 Vector Data Model

**Core Vector Structure:**
```json
{
  "id": "string",           // Unique identifier
  "vector": [float64],      // Dense vector representation
  "metadata": {             // Arbitrary key-value pairs
    "timestamp": int64,
    "source": "string",
    "category": "string",
    "custom_field": "any"
  }
}
```

**Constraints:**
- Vector dimensionality must be consistent within a cluster
- Vector ID must be unique within the cluster scope
- Metadata values support string, numeric, and boolean types
- Maximum vector dimension: 4096 (configurable)
- Maximum metadata size: 64KB per vector

### 4.2 Segment File Format

**Binary Layout:**
```
Segment Header (256 bytes)
├── Magic Number (8 bytes): "VEXSEG01"
├── Vector Count (8 bytes)
├── Vector Dimension (4 bytes)
├── Metadata Size (8 bytes)
├── Compression Type (4 bytes)
├── Checksum (32 bytes)
└── Reserved (192 bytes)

Vector Data Section
├── Vector 1: [dimensions * float64]
├── Vector 2: [dimensions * float64]
└── ...

Metadata Section
├── Metadata 1: Length-prefixed JSON
├── Metadata 2: Length-prefixed JSON
└── ...

Index Section
├── Vector ID → Offset Mapping
└── Metadata Offset Table
```

### 4.3 IVF Index Format

**Memory Structure:**
```go
type IVFIndex struct {
    Centroids     []Vector          // Cluster centroids
    InvertedLists [][]VectorRef     // Cluster → Vector mappings
    SegmentMap    map[string]int    // Segment file registry
    Metadata      IndexMetadata     // Build timestamp, stats
}

type VectorRef struct {
    SegmentID string
    Offset    uint64
    VectorID  string
}
```

---

## 5. Protocol Specifications

### 5.1 Internal Communication

**Service Discovery:**
- Static configuration-based peer discovery
- Health check endpoints on all services
- Graceful shutdown with connection draining
- No dynamic membership changes during operation

**Inter-Service Protocol:**
```protobuf
// vxinsert → vxstorage
message StoreVectorRequest {
  string cluster_id = 1;
  Vector vector = 2;
  map<string, string> metadata = 3;
  int64 sequence_number = 4;
}

// vxsearch → vxstorage  
message SearchRequest {
  Vector query_vector = 1;
  repeated string cluster_ranges = 2;
  int32 limit = 3;
  FilterExpression filters = 4;
}
```

### 5.2 External APIs

**WebSocket Protocol:**
```
Connection: ws://vxinsert:8080/stream
Subprotocol: vexdb-v1

Message Types:
- vector_insert: Single vector ingestion
- batch_insert: Multiple vector batch
- status_request: Connection health check

Error Handling:
- Connection-level errors trigger reconnection
- Message-level errors logged but do not disconnect
- Backpressure indicated through connection throttling
```

**Kafka Connect Integration:**
```json
{
  "connector.class": "io.vexdb.connect.VexDBSinkConnector",
  "tasks.max": "1",
  "topics": "vectors",
  "vexdb.endpoints": "vxinsert-1:8080,vxinsert-2:8080",
  "vexdb.vector.field": "embedding",
  "vexdb.metadata.fields": "id,timestamp,category",
  "vexdb.batch.size": "1000",
  "vexdb.retry.attempts": "3"
}
```

---

## 6. Deployment and Configuration

### 6.1 Cluster Configuration

**Static Topology Definition:**
```yaml
cluster:
  name: "vexdb-production"
  total_clusters: 1024
  replication_factor: 2
  
services:
  vxinsert:
    instances:
      - host: "10.0.1.10"
        port: 8080
        protocols: ["websocket", "http", "grpc"]
      - host: "10.0.1.11" 
        port: 8080
        protocols: ["websocket", "http", "grpc"]
        
  vxstorage:
    instances:
      - id: "storage-1"
        host: "10.0.2.10"
        port: 8090
        cluster_ranges: ["0-255", "512-767"]    # Primary + replica
        data_directory: "/var/lib/vexdb"
      - id: "storage-2"
        host: "10.0.2.11"
        port: 8090  
        cluster_ranges: ["256-511", "768-1023"]
        data_directory: "/var/lib/vexdb"
        
  vxsearch:
    instances:
      - host: "10.0.3.10"
        port: 8100
      - host: "10.0.3.11"
        port: 8100
```

### 6.2 Performance Tuning

**Memory Configuration:**
```yaml
vxstorage:
  memory:
    buffer_size: "16MB"        # Per-cluster write buffer
    ivf_cache_size: "2GB"      # IVF index memory limit
    segment_cache_size: "1GB"  # Hot segment cache
    
  performance:
    flush_interval: "30s"      # Maximum buffer retention
    vectors_per_segment: 10000 # Segment size control
    parallel_searches: 4       # Concurrent query threads
    compression_level: 1       # LZ4 fast compression
```

**Network Optimization:**
```yaml
vxinsert:
  network:
    max_connections: 10000
    connection_timeout: "30s"
    write_timeout: "5s"
    read_buffer_size: "64KB"
    
  routing:
    retry_attempts: 3
    retry_backoff: "100ms"
    circuit_breaker_threshold: 10
```

### 6.3 Monitoring and Observability

**Metrics Endpoints:**
```
GET /metrics (Prometheus format)

Key Metrics:
- vexdb_vectors_ingested_total
- vexdb_queries_executed_total  
- vexdb_query_duration_seconds
- vexdb_memory_buffer_utilization
- vexdb_segment_count_total
- vexdb_ivf_rebuild_duration_seconds
```

**Health Checks:**
```
GET /health
Response: {"status": "healthy|degraded|unhealthy", "details": {...}}

GET /ready  
Response: {"ready": true|false}
```

---

## 7. Performance Characteristics

### 7.1 Expected Performance Profile

**Write Performance:**
- Target write latency: < 100ms (99th percentile)
- Sustained throughput: 10,000+ vectors/second per vxinsert instance
- Memory usage: ~100MB per 1M vectors (excluding IVF index)
- Storage overhead: ~20% for indices and metadata

**Read Performance:**
- Target query latency: < 50ms (95th percentile) for fast mode
- Target query latency: < 200ms (95th percentile) for accurate mode
- Expected recall: 80-85% for fast mode, 85-90% for accurate mode
- Concurrent queries: 1,000+ per second per vxsearch instance

**Scaling Characteristics:**
- Linear write scaling with vxinsert instances
- Linear storage scaling with vxstorage instances  
- Linear query scaling with vxsearch instances
- Horizontal scalability limit: ~100 nodes per cluster

```

---

## 8. Security and Compliance

### 8.1 Authentication and Authorization

**Service-to-Service Authentication:**
- Mutual TLS (mTLS) for inter-service communication
- Certificate-based authentication with automatic rotation
- Service identity verification through certificate subject validation

**Client Authentication:**
- API key authentication for HTTP/gRPC endpoints
- Token-based authentication for WebSocket connections
- Integration with external identity providers (OAuth, LDAP)


**Encryption in Transit:**
- TLS 1.3 for all client communications
- mTLS for service-to-service communication
- Certificate pinning for enhanced security

### 8.3 Audit and Compliance

**Audit Logging:**
- Comprehensive operation logging with structured format
- Query audit trails with user attribution
- Data lineage tracking for regulatory compliance
- Configurable log retention policies


--

---

## 11. References and Standards

### 11.1 Technical References

- Approximate Nearest Neighbor (ANN) algorithms and benchmarks
- Inverted File (IVF) indexing methodology and optimization
- LSM-tree design patterns and storage optimization
- Distributed systems consensus and replication strategies

### 11.2 Protocol Standards

- WebSocket Protocol (RFC 6455)
- HTTP/1.1 (RFC 7230-7235) and HTTP/2 (RFC 7540)
- gRPC Protocol Specification
- Protocol Buffers Language Specification
- Kafka Connect API Specification

### 11.3 Security Standards

- Transport Layer Security (TLS) 1.3 (RFC 8446)
- JSON Web Tokens (JWT) (RFC 7519)
- OAuth 2.0 Authorization Framework (RFC 6749)
- Advanced Encryption Standard (AES) (FIPS 197)

--. For the latest version, consult the official VexDB documentation repository.*