package context

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"vexdb/internal/errors"
)

// Context keys for VexDB context values
type contextKey string

const (
	// RequestIDKey is the key for the request ID in the context
	RequestIDKey contextKey = "request_id"
	
	// TraceIDKey is the key for the trace ID in the context
	TraceIDKey contextKey = "trace_id"
	
	// SpanIDKey is the key for the span ID in the context
	SpanIDKey contextKey = "span_id"
	
	// UserIDKey is the key for the user ID in the context
	UserIDKey contextKey = "user_id"
	
	// ServiceNameKey is the key for the service name in the context
	ServiceNameKey contextKey = "service_name"
	
	// OperationNameKey is the key for the operation name in the context
	OperationNameKey contextKey = "operation_name"
	
	// StartTimeKey is the key for the start time in the context
	StartTimeKey contextKey = "start_time"
	
	// TimeoutKey is the key for the timeout in the context
	TimeoutKey contextKey = "timeout"
	
	// MetadataKey is the key for metadata in the context
	MetadataKey contextKey = "metadata"
	
	// ClusterIDKey is the key for the cluster ID in the context
	ClusterIDKey contextKey = "cluster_id"
	
	// NodeIDKey is the key for the node ID in the context
	NodeIDKey contextKey = "node_id"
	
	// VectorIDKey is the key for the vector ID in the context
	VectorIDKey contextKey = "vector_id"
	
	// QueryIDKey is the key for the query ID in the context
	QueryIDKey contextKey = "query_id"
	
	// BatchIDKey is the key for the batch ID in the context
	BatchIDKey contextKey = "batch_id"
	
	// ReplicationFactorKey is the key for the replication factor in the context
	ReplicationFactorKey contextKey = "replication_factor"
	
	// ConsistencyLevelKey is the key for the consistency level in the context
	ConsistencyLevelKey contextKey = "consistency_level"
	
	// RetryCountKey is the key for the retry count in the context
	RetryCountKey contextKey = "retry_count"
	
	// DeadlineKey is the key for the deadline in the context
	DeadlineKey contextKey = "deadline"
)

// Context represents a VexDB context with additional metadata
type Context struct {
	RequestID        string
	TraceID          string
	SpanID           string
	UserID           string
	ServiceName      string
	OperationName    string
	StartTime        time.Time
	Timeout          time.Duration
	Metadata         map[string]interface{}
	ClusterID        string
	NodeID           string
	VectorID         string
	QueryID          string
	BatchID          string
	ReplicationFactor int
	ConsistencyLevel string
	RetryCount       int
	Deadline         time.Time
}

// NewContext creates a new VexDB context
func NewContext(ctx context.Context) *Context {
	vexCtx := &Context{
		StartTime: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
	
	// Extract existing values from context if they exist
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		vexCtx.RequestID = requestID.(string)
	} else {
		vexCtx.RequestID = generateID()
	}
	
	if traceID := ctx.Value(TraceIDKey); traceID != nil {
		vexCtx.TraceID = traceID.(string)
	} else {
		vexCtx.TraceID = generateID()
	}
	
	if spanID := ctx.Value(SpanIDKey); spanID != nil {
		vexCtx.SpanID = spanID.(string)
	} else {
		vexCtx.SpanID = generateID()
	}
	
	if userID := ctx.Value(UserIDKey); userID != nil {
		vexCtx.UserID = userID.(string)
	}
	
	if serviceName := ctx.Value(ServiceNameKey); serviceName != nil {
		vexCtx.ServiceName = serviceName.(string)
	}
	
	if operationName := ctx.Value(OperationNameKey); operationName != nil {
		vexCtx.OperationName = operationName.(string)
	}
	
	if timeout := ctx.Value(TimeoutKey); timeout != nil {
		vexCtx.Timeout = timeout.(time.Duration)
	}
	
	if metadata := ctx.Value(MetadataKey); metadata != nil {
		if metaMap, ok := metadata.(map[string]interface{}); ok {
			vexCtx.Metadata = metaMap
		}
	}
	
	if clusterID := ctx.Value(ClusterIDKey); clusterID != nil {
		vexCtx.ClusterID = clusterID.(string)
	}
	
	if nodeID := ctx.Value(NodeIDKey); nodeID != nil {
		vexCtx.NodeID = nodeID.(string)
	}
	
	if vectorID := ctx.Value(VectorIDKey); vectorID != nil {
		vexCtx.VectorID = vectorID.(string)
	}
	
	if queryID := ctx.Value(QueryIDKey); queryID != nil {
		vexCtx.QueryID = queryID.(string)
	}
	
	if batchID := ctx.Value(BatchIDKey); batchID != nil {
		vexCtx.BatchID = batchID.(string)
	}
	
	if replicationFactor := ctx.Value(ReplicationFactorKey); replicationFactor != nil {
		vexCtx.ReplicationFactor = replicationFactor.(int)
	}
	
	if consistencyLevel := ctx.Value(ConsistencyLevelKey); consistencyLevel != nil {
		vexCtx.ConsistencyLevel = consistencyLevel.(string)
	}
	
	if retryCount := ctx.Value(RetryCountKey); retryCount != nil {
		vexCtx.RetryCount = retryCount.(int)
	}
	
	if deadline, ok := ctx.Deadline(); ok {
		vexCtx.Deadline = deadline
	}
	
	return vexCtx
}

// ToContext converts the VexDB context to a standard context
func (c *Context) ToContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, RequestIDKey, c.RequestID)
	ctx = context.WithValue(ctx, TraceIDKey, c.TraceID)
	ctx = context.WithValue(ctx, SpanIDKey, c.SpanID)
	ctx = context.WithValue(ctx, UserIDKey, c.UserID)
	ctx = context.WithValue(ctx, ServiceNameKey, c.ServiceName)
	ctx = context.WithValue(ctx, OperationNameKey, c.OperationName)
	ctx = context.WithValue(ctx, StartTimeKey, c.StartTime)
	ctx = context.WithValue(ctx, TimeoutKey, c.Timeout)
	ctx = context.WithValue(ctx, MetadataKey, c.Metadata)
	ctx = context.WithValue(ctx, ClusterIDKey, c.ClusterID)
	ctx = context.WithValue(ctx, NodeIDKey, c.NodeID)
	ctx = context.WithValue(ctx, VectorIDKey, c.VectorID)
	ctx = context.WithValue(ctx, QueryIDKey, c.QueryID)
	ctx = context.WithValue(ctx, BatchIDKey, c.BatchID)
	ctx = context.WithValue(ctx, ReplicationFactorKey, c.ReplicationFactor)
	ctx = context.WithValue(ctx, ConsistencyLevelKey, c.ConsistencyLevel)
	ctx = context.WithValue(ctx, RetryCountKey, c.RetryCount)
	ctx = context.WithValue(ctx, DeadlineKey, c.Deadline)
	
	return ctx
}

// WithRequestID sets the request ID in the context
func (c *Context) WithRequestID(requestID string) *Context {
	c.RequestID = requestID
	return c
}

// WithTraceID sets the trace ID in the context
func (c *Context) WithTraceID(traceID string) *Context {
	c.TraceID = traceID
	return c
}

// WithSpanID sets the span ID in the context
func (c *Context) WithSpanID(spanID string) *Context {
	c.SpanID = spanID
	return c
}

// WithUserID sets the user ID in the context
func (c *Context) WithUserID(userID string) *Context {
	c.UserID = userID
	return c
}

// WithServiceName sets the service name in the context
func (c *Context) WithServiceName(serviceName string) *Context {
	c.ServiceName = serviceName
	return c
}

// WithOperationName sets the operation name in the context
func (c *Context) WithOperationName(operationName string) *Context {
	c.OperationName = operationName
	return c
}

// WithTimeout sets the timeout in the context
func (c *Context) WithTimeout(timeout time.Duration) *Context {
	c.Timeout = timeout
	return c
}

// WithMetadata sets metadata in the context
func (c *Context) WithMetadata(metadata map[string]interface{}) *Context {
	c.Metadata = metadata
	return c
}

// WithMetadataValue adds a metadata value to the context
func (c *Context) WithMetadataValue(key string, value interface{}) *Context {
	if c.Metadata == nil {
		c.Metadata = make(map[string]interface{})
	}
	c.Metadata[key] = value
	return c
}

// WithClusterID sets the cluster ID in the context
func (c *Context) WithClusterID(clusterID string) *Context {
	c.ClusterID = clusterID
	return c
}

// WithNodeID sets the node ID in the context
func (c *Context) WithNodeID(nodeID string) *Context {
	c.NodeID = nodeID
	return c
}

// WithVectorID sets the vector ID in the context
func (c *Context) WithVectorID(vectorID string) *Context {
	c.VectorID = vectorID
	return c
}

// WithQueryID sets the query ID in the context
func (c *Context) WithQueryID(queryID string) *Context {
	c.QueryID = queryID
	return c
}

// WithBatchID sets the batch ID in the context
func (c *Context) WithBatchID(batchID string) *Context {
	c.BatchID = batchID
	return c
}

// WithReplicationFactor sets the replication factor in the context
func (c *Context) WithReplicationFactor(replicationFactor int) *Context {
	c.ReplicationFactor = replicationFactor
	return c
}

// WithConsistencyLevel sets the consistency level in the context
func (c *Context) WithConsistencyLevel(consistencyLevel string) *Context {
	c.ConsistencyLevel = consistencyLevel
	return c
}

// WithRetryCount sets the retry count in the context
func (c *Context) WithRetryCount(retryCount int) *Context {
	c.RetryCount = retryCount
	return c
}

// WithDeadline sets the deadline in the context
func (c *Context) WithDeadline(deadline time.Time) *Context {
	c.Deadline = deadline
	return c
}

// GetMetadataValue gets a metadata value from the context
func (c *Context) GetMetadataValue(key string) (interface{}, bool) {
	if c.Metadata == nil {
		return nil, false
	}
	value, exists := c.Metadata[key]
	return value, exists
}

// GetDuration returns the duration since the context was created
func (c *Context) GetDuration() time.Duration {
	return time.Since(c.StartTime)
}

// IsExpired checks if the context has expired
func (c *Context) IsExpired() bool {
	if c.Deadline.IsZero() {
		return false
	}
	return time.Now().After(c.Deadline)
}

// TimeRemaining returns the time remaining until the deadline
func (c *Context) TimeRemaining() time.Duration {
	if c.Deadline.IsZero() {
		return c.Timeout
	}
	return time.Until(c.Deadline)
}

// Validate validates the context
func (c *Context) Validate() error {
	if c.RequestID == "" {
		return errors.NewInvalidArgumentError("request ID is required")
	}
	if c.TraceID == "" {
		return errors.NewInvalidArgumentError("trace ID is required")
	}
	if c.SpanID == "" {
		return errors.NewInvalidArgumentError("span ID is required")
	}
	if c.ServiceName == "" {
		return errors.NewInvalidArgumentError("service name is required")
	}
	if c.OperationName == "" {
		return errors.NewInvalidArgumentError("operation name is required")
	}
	if c.Timeout <= 0 {
		return errors.NewInvalidArgumentError("timeout must be positive")
	}
	return nil
}

// Copy creates a copy of the context
func (c *Context) Copy() *Context {
	copy := &Context{
		RequestID:        c.RequestID,
		TraceID:          c.TraceID,
		SpanID:           c.SpanID,
		UserID:           c.UserID,
		ServiceName:      c.ServiceName,
		OperationName:    c.OperationName,
		StartTime:        c.StartTime,
		Timeout:          c.Timeout,
		ClusterID:        c.ClusterID,
		NodeID:           c.NodeID,
		VectorID:         c.VectorID,
		QueryID:          c.QueryID,
		BatchID:          c.BatchID,
		ReplicationFactor: c.ReplicationFactor,
		ConsistencyLevel: c.ConsistencyLevel,
		RetryCount:       c.RetryCount,
		Deadline:         c.Deadline,
	}
	
	if c.Metadata != nil {
		copy.Metadata = make(map[string]interface{})
		for k, v := range c.Metadata {
			copy.Metadata[k] = v
		}
	}
	
	return copy
}

// String returns a string representation of the context
func (c *Context) String() string {
	return fmt.Sprintf("Context{RequestID: %s, TraceID: %s, SpanID: %s, Service: %s, Operation: %s, Cluster: %s, Node: %s}",
		c.RequestID, c.TraceID, c.SpanID, c.ServiceName, c.OperationName, c.ClusterID, c.NodeID)
}

// generateID generates a unique ID
func generateID() string {
	return uuid.New().String()
}

// Helper functions for working with standard context

// WithRequestID adds a request ID to a standard context
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, requestID)
}

// WithTraceID adds a trace ID to a standard context
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// WithSpanID adds a span ID to a standard context
func WithSpanID(ctx context.Context, spanID string) context.Context {
	return context.WithValue(ctx, SpanIDKey, spanID)
}

// WithUserID adds a user ID to a standard context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// WithServiceName adds a service name to a standard context
func WithServiceName(ctx context.Context, serviceName string) context.Context {
	return context.WithValue(ctx, ServiceNameKey, serviceName)
}

// WithOperationName adds an operation name to a standard context
func WithOperationName(ctx context.Context, operationName string) context.Context {
	return context.WithValue(ctx, OperationNameKey, operationName)
}

// WithTimeout adds a timeout to a standard context
func WithTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, TimeoutKey, timeout)
}

// WithMetadata adds metadata to a standard context
func WithMetadata(ctx context.Context, metadata map[string]interface{}) context.Context {
	return context.WithValue(ctx, MetadataKey, metadata)
}

// WithMetadataValue adds a metadata value to a standard context
func WithMetadataValue(ctx context.Context, key string, value interface{}) context.Context {
	metadata := make(map[string]interface{})
	if existing := ctx.Value(MetadataKey); existing != nil {
		if metaMap, ok := existing.(map[string]interface{}); ok {
			for k, v := range metaMap {
				metadata[k] = v
			}
		}
	}
	metadata[key] = value
	return context.WithValue(ctx, MetadataKey, metadata)
}

// WithClusterID adds a cluster ID to a standard context
func WithClusterID(ctx context.Context, clusterID string) context.Context {
	return context.WithValue(ctx, ClusterIDKey, clusterID)
}

// WithNodeID adds a node ID to a standard context
func WithNodeID(ctx context.Context, nodeID string) context.Context {
	return context.WithValue(ctx, NodeIDKey, nodeID)
}

// WithVectorID adds a vector ID to a standard context
func WithVectorID(ctx context.Context, vectorID string) context.Context {
	return context.WithValue(ctx, VectorIDKey, vectorID)
}

// WithQueryID adds a query ID to a standard context
func WithQueryID(ctx context.Context, queryID string) context.Context {
	return context.WithValue(ctx, QueryIDKey, queryID)
}

// WithBatchID adds a batch ID to a standard context
func WithBatchID(ctx context.Context, batchID string) context.Context {
	return context.WithValue(ctx, BatchIDKey, batchID)
}

// WithReplicationFactor adds a replication factor to a standard context
func WithReplicationFactor(ctx context.Context, replicationFactor int) context.Context {
	return context.WithValue(ctx, ReplicationFactorKey, replicationFactor)
}

// WithConsistencyLevel adds a consistency level to a standard context
func WithConsistencyLevel(ctx context.Context, consistencyLevel string) context.Context {
	return context.WithValue(ctx, ConsistencyLevelKey, consistencyLevel)
}

// WithRetryCount adds a retry count to a standard context
func WithRetryCount(ctx context.Context, retryCount int) context.Context {
	return context.WithValue(ctx, RetryCountKey, retryCount)
}

// GetRequestID gets the request ID from a standard context
func GetRequestID(ctx context.Context) string {
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		return requestID.(string)
	}
	return ""
}

// GetTraceID gets the trace ID from a standard context
func GetTraceID(ctx context.Context) string {
	if traceID := ctx.Value(TraceIDKey); traceID != nil {
		return traceID.(string)
	}
	return ""
}

// GetSpanID gets the span ID from a standard context
func GetSpanID(ctx context.Context) string {
	if spanID := ctx.Value(SpanIDKey); spanID != nil {
		return spanID.(string)
	}
	return ""
}

// GetUserID gets the user ID from a standard context
func GetUserID(ctx context.Context) string {
	if userID := ctx.Value(UserIDKey); userID != nil {
		return userID.(string)
	}
	return ""
}

// GetServiceName gets the service name from a standard context
func GetServiceName(ctx context.Context) string {
	if serviceName := ctx.Value(ServiceNameKey); serviceName != nil {
		return serviceName.(string)
	}
	return ""
}

// GetOperationName gets the operation name from a standard context
func GetOperationName(ctx context.Context) string {
	if operationName := ctx.Value(OperationNameKey); operationName != nil {
		return operationName.(string)
	}
	return ""
}

// GetTimeout gets the timeout from a standard context
func GetTimeout(ctx context.Context) time.Duration {
	if timeout := ctx.Value(TimeoutKey); timeout != nil {
		return timeout.(time.Duration)
	}
	return 0
}

// GetMetadata gets the metadata from a standard context
func GetMetadata(ctx context.Context) map[string]interface{} {
	if metadata := ctx.Value(MetadataKey); metadata != nil {
		if metaMap, ok := metadata.(map[string]interface{}); ok {
			return metaMap
		}
	}
	return nil
}

// GetMetadataValue gets a metadata value from a standard context
func GetMetadataValue(ctx context.Context, key string) (interface{}, bool) {
	metadata := GetMetadata(ctx)
	if metadata == nil {
		return nil, false
	}
	value, exists := metadata[key]
	return value, exists
}

// GetClusterID gets the cluster ID from a standard context
func GetClusterID(ctx context.Context) string {
	if clusterID := ctx.Value(ClusterIDKey); clusterID != nil {
		return clusterID.(string)
	}
	return ""
}

// GetNodeID gets the node ID from a standard context
func GetNodeID(ctx context.Context) string {
	if nodeID := ctx.Value(NodeIDKey); nodeID != nil {
		return nodeID.(string)
	}
	return ""
}

// GetVectorID gets the vector ID from a standard context
func GetVectorID(ctx context.Context) string {
	if vectorID := ctx.Value(VectorIDKey); vectorID != nil {
		return vectorID.(string)
	}
	return ""
}

// GetQueryID gets the query ID from a standard context
func GetQueryID(ctx context.Context) string {
	if queryID := ctx.Value(QueryIDKey); queryID != nil {
		return queryID.(string)
	}
	return ""
}

// GetBatchID gets the batch ID from a standard context
func GetBatchID(ctx context.Context) string {
	if batchID := ctx.Value(BatchIDKey); batchID != nil {
		return batchID.(string)
	}
	return ""
}

// GetReplicationFactor gets the replication factor from a standard context
func GetReplicationFactor(ctx context.Context) int {
	if replicationFactor := ctx.Value(ReplicationFactorKey); replicationFactor != nil {
		return replicationFactor.(int)
	}
	return 0
}

// GetConsistencyLevel gets the consistency level from a standard context
func GetConsistencyLevel(ctx context.Context) string {
	if consistencyLevel := ctx.Value(ConsistencyLevelKey); consistencyLevel != nil {
		return consistencyLevel.(string)
	}
	return ""
}

// GetRetryCount gets the retry count from a standard context
func GetRetryCount(ctx context.Context) int {
	if retryCount := ctx.Value(RetryCountKey); retryCount != nil {
		return retryCount.(int)
	}
	return 0
}