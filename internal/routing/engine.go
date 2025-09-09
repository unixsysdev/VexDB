
package routing

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
	"go.uber.org/zap"
)

// RoutingEngine handles cluster routing and node selection
type RoutingEngine struct {
	logger          logging.Logger
	metrics         *metrics.IngestionMetrics
	config          *config.IngestionConfig
	clusterConfig   *types.ClusterConfig
	nodes           map[string]*NodeInfo
	ring            *ConsistentHashRing
	replicationMgr  *ReplicationManager
	mu              sync.RWMutex
	shutdown        chan struct{}
	wg              sync.WaitGroup
}

// NodeInfo represents information about a storage node
type NodeInfo struct {
	ID           string
	Address      string
	Port         int
	IsPrimary    bool
	ReplicaOf    string
	Replicas     []string
	ClusterRange ClusterRange
	Status       NodeStatus
	LastSeen     time.Time
	LoadScore    float64
	Capacity     NodeCapacity
	mu           sync.RWMutex
}

// ClusterRange represents the range of cluster IDs assigned to a node
type ClusterRange struct {
	Start uint32
	End   uint32
}

// NodeStatus represents the status of a node
type NodeStatus int

const (
	StatusUnknown NodeStatus = iota
	StatusHealthy
	StatusDegraded
	StatusUnhealthy
	StatusOffline
)

// String returns the string representation of NodeStatus
func (s NodeStatus) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusDegraded:
		return "degraded"
	case StatusUnhealthy:
		return "unhealthy"
	case StatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// NodeCapacity represents the capacity information of a node
type NodeCapacity struct {
	TotalVectors    int64
	AvailableMemory int64
	AvailableDisk   int64
	CPUUsage        float64
	NetworkLoad     float64
}

// RoutingResult represents the result of a routing operation
type RoutingResult struct {
	PrimaryNode    *NodeInfo
	ReplicaNodes   []*NodeInfo
	ClusterID      uint32
	RoutingPath    string
	SelectedNodes  []*NodeInfo
	FallbackNodes  []*NodeInfo
}

// RoutingConfig represents the configuration for the routing engine
type RoutingConfig struct {
	ReplicationFactor    int           `yaml:"replication_factor" json:"replication_factor"`
	HashRingVirtualNodes int           `yaml:"hash_ring_virtual_nodes" json:"hash_ring_virtual_nodes"`
	NodeTimeout         time.Duration `yaml:"node_timeout" json:"node_timeout"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval" json:"health_check_interval"`
	LoadBalanceStrategy string        `yaml:"load_balance_strategy" json:"load_balance_strategy"`
	EnableMetrics       bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultRoutingConfig returns the default routing configuration
func DefaultRoutingConfig() *RoutingConfig {
	return &RoutingConfig{
		ReplicationFactor:    3,
		HashRingVirtualNodes: 160,
		NodeTimeout:         30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		LoadBalanceStrategy: "round_robin",
		EnableMetrics:       true,
	}
}

// NewRoutingEngine creates a new routing engine
func NewRoutingEngine(config *RoutingConfig, clusterConfig *types.ClusterConfig, logger logging.Logger, metrics *metrics.IngestionMetrics) *RoutingEngine {
	if config == nil {
		config = DefaultRoutingConfig()
	}

	engine := &RoutingEngine{
		logger:        logger,
		metrics:       metrics,
		clusterConfig: clusterConfig,
		nodes:         make(map[string]*NodeInfo),
		ring:          NewConsistentHashRing(config.HashRingVirtualNodes),
		shutdown:      make(chan struct{}),
	}

	engine.replicationMgr = NewReplicationManager(config, logger, metrics)

	engine.logger.Info("Created routing engine",
		zap.Int("replication_factor", config.ReplicationFactor),
		zap.Int("hash_ring_virtual_nodes", config.HashRingVirtualNodes),
		zap.Duration("node_timeout", config.NodeTimeout),
		zap.Duration("health_check_interval", config.HealthCheckInterval),
		zap.String("load_balance_strategy", config.LoadBalanceStrategy))

	return engine
}

// Start starts the routing engine
func (e *RoutingEngine) Start(ctx context.Context) error {
	e.logger.Info("Starting routing engine")

	// Initialize nodes from cluster configuration
	if err := e.initializeNodes(); err != nil {
		return fmt.Errorf("failed to initialize nodes: %w", err)
	}

	// Start replication manager
	if err := e.replicationMgr.Start(ctx); err != nil {
		return fmt.Errorf("failed to start replication manager: %w", err)
	}

	// Start health monitoring
	e.wg.Add(1)
	go e.runHealthMonitoring(ctx)

	e.logger.Info("Routing engine started successfully")
	return nil
}

// Stop stops the routing engine
func (e *RoutingEngine) Stop() {
	e.logger.Info("Stopping routing engine")

	close(e.shutdown)
	e.wg.Wait()

	e.replicationMgr.Stop()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Mark all nodes as offline
	for _, node := range e.nodes {
		node.mu.Lock()
		node.Status = StatusOffline
		node.mu.Unlock()
	}

	e.logger.Info("Routing engine stopped")
}

// RouteVector routes a vector to the appropriate nodes
func (e *RoutingEngine) RouteVector(vector *types.Vector) (*RoutingResult, error) {
	// Calculate cluster ID for the vector
	clusterID := e.calculateClusterID(vector)

	// Get primary node for the cluster ID
	primaryNode, err := e.getPrimaryNode(clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary node for cluster %d: %w", clusterID, err)
	}

	// Get replica nodes
	replicaNodes, err := e.getReplicaNodes(primaryNode)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica nodes: %w", err)
	}

	// Get fallback nodes
	fallbackNodes, err := e.getFallbackNodes(primaryNode, replicaNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to get fallback nodes: %w", err)
	}

	// Build routing result
	result := &RoutingResult{
		PrimaryNode:   primaryNode,
		ReplicaNodes:  replicaNodes,
		ClusterID:     clusterID,
		RoutingPath:   e.buildRoutingPath(primaryNode, replicaNodes),
		SelectedNodes: append([]*NodeInfo{primaryNode}, replicaNodes...),
		FallbackNodes: fallbackNodes,
	}

	// Update metrics
	if e.metrics != nil {
		e.metrics.IncrementRoutingOperations(primaryNode.Address)
	}

	e.logger.Debug("Vector routed successfully",
		zap.String("vector_id", vector.ID),
		zap.Uint32("cluster_id", clusterID),
		zap.String("primary_node", primaryNode.ID),
		zap.Int("replica_count", len(replicaNodes)))

	return result, nil
}

// RouteBatch routes a batch of vectors to appropriate nodes
func (e *RoutingEngine) RouteBatch(vectors []*types.Vector) (map[string][]*types.Vector, error) {
	batches := make(map[string][]*types.Vector)

	for _, vector := range vectors {
		result, err := e.RouteVector(vector)
		if err != nil {
			return nil, fmt.Errorf("failed to route vector %s: %w", vector.ID, err)
		}

		// Add vector to primary node batch
		primaryAddr := fmt.Sprintf("%s:%d", result.PrimaryNode.Address, result.PrimaryNode.Port)
		batches[primaryAddr] = append(batches[primaryAddr], vector)

		// Add vector to replica node batches
		for _, replica := range result.ReplicaNodes {
			replicaAddr := fmt.Sprintf("%s:%d", replica.Address, replica.Port)
			batches[replicaAddr] = append(batches[replicaAddr], vector)
		}
	}

	return batches, nil
}

// AddNode adds a new node to the routing engine
func (e *RoutingEngine) AddNode(node *NodeInfo) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate node
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	if node.Address == "" {
		return fmt.Errorf("node address cannot be empty")
	}

	if node.Port <= 0 {
		return fmt.Errorf("node port must be positive")
	}

	// Check if node already exists
	if _, exists := e.nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}

	// Initialize node status
	node.Status = StatusHealthy
	node.LastSeen = time.Now()

	// Add node to nodes map
	e.nodes[node.ID] = node

	// Add node to consistent hash ring
	e.ring.AddNode(node.ID)

	e.logger.Info("Added node to routing engine",
		zap.String("node_id", node.ID),
		zap.String("address", node.Address),
		zap.Int("port", node.Port),
		zap.Uint32("cluster_start", node.ClusterRange.Start),
		zap.Uint32("cluster_end", node.ClusterRange.End))

	// Update metrics
	if e.metrics != nil {
		e.metrics.IncrementNodeCount()
	}

	return nil
}

// RemoveNode removes a node from the routing engine
func (e *RoutingEngine) RemoveNode(nodeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	node, exists := e.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Mark node as offline
	node.mu.Lock()
	node.Status = StatusOffline
	node.mu.Unlock()

	// Remove node from consistent hash ring
	e.ring.RemoveNode(nodeID)

	// Remove node from nodes map
	delete(e.nodes, nodeID)

	e.logger.Info("Removed node from routing engine",
		zap.String("node_id", nodeID),
		zap.String("address", node.Address),
		zap.Int("port", node.Port))

	// Update metrics
	if e.metrics != nil {
		e.metrics.DecrementNodeCount()
	}

	return nil
}

// GetNode returns information about a specific node
func (e *RoutingEngine) GetNode(nodeID string) (*NodeInfo, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	node, exists := e.nodes[nodeID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	node.mu.RLock()
	defer node.mu.RUnlock()

	copy := &NodeInfo{
		ID:           node.ID,
		Address:      node.Address,
		Port:         node.Port,
		IsPrimary:    node.IsPrimary,
		ReplicaOf:    node.ReplicaOf,
		Replicas:     make([]string, len(node.Replicas)),
		ClusterRange: node.ClusterRange,
		Status:       node.Status,
		LastSeen:     node.LastSeen,
		LoadScore:    node.LoadScore,
		Capacity:     node.Capacity,
	}

	copy.Replicas = append(copy.Replicas, node.Replicas...)

	return copy, true
}

// GetAllNodes returns information about all nodes
func (e *RoutingEngine) GetAllNodes() []*NodeInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(e.nodes))
	for _, node := range e.nodes {
		node.mu.RLock()
		copy := &NodeInfo{
			ID:           node.ID,
			Address:      node.Address,
			Port:         node.Port,
			IsPrimary:    node.IsPrimary,
			ReplicaOf:    node.ReplicaOf,
			Replicas:     make([]string, len(node.Replicas)),
			ClusterRange: node.ClusterRange,
			Status:       node.Status,
			LastSeen:     node.LastSeen,
			LoadScore:    node.LoadScore,
			Capacity:     node.Capacity,
		}
		copy.Replicas = append(copy.Replicas, node.Replicas...)
		node.mu.RUnlock()
		nodes = append(nodes, copy)
	}

	return nodes
}

// GetHealthyNodes returns all healthy nodes
func (e *RoutingEngine) GetHealthyNodes() []*NodeInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var healthyNodes []*NodeInfo
	for _, node := range e.nodes {
		node.mu.RLock()
		if node.Status == StatusHealthy {
			copy := &NodeInfo{
				ID:           node.ID,
				Address:      node.Address,
				Port:         node.Port,
				IsPrimary:    node.IsPrimary,
				ReplicaOf:    node.ReplicaOf,
				Replicas:     make([]string, len(node.Replicas)),
				ClusterRange: node.ClusterRange,
				Status:       node.Status,
				LastSeen:     node.LastSeen,
				LoadScore:    node.LoadScore,
				Capacity:     node.Capacity,
			}
			copy.Replicas = append(copy.Replicas, node.Replicas...)
			healthyNodes = append(healthyNodes, copy)
		}
		node.mu.RUnlock()
	}

	return healthyNodes
}

// UpdateNodeStatus updates the status of a node
func (e *RoutingEngine) UpdateNodeStatus(nodeID string, status NodeStatus) error {
	e.mu.RLock()
	node, exists := e.nodes[nodeID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.mu.Lock()
	oldStatus := node.Status
	node.Status = status
	node.LastSeen = time.Now()
	node.mu.Unlock()

	if oldStatus != status {
		e.logger.Info("Node status changed",
			zap.String("node_id", nodeID),
			zap.String("old_status", oldStatus.String()),
			zap.String("new_status", status.String()))

		// Update metrics
		if e.metrics != nil {
			if status == StatusHealthy {
				e.metrics.IncrementHealthyNodes()
			} else if oldStatus == StatusHealthy {
				e.metrics.DecrementHealthyNodes()
			}
		}
	}

	return nil
}

// UpdateNodeCapacity updates the capacity information of a node
func (e *RoutingEngine) UpdateNodeCapacity(nodeID string, capacity NodeCapacity) error {
	e.mu.RLock()
	node, exists := e.nodes[nodeID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.mu.Lock()
	node.Capacity = capacity
	node.LoadScore = e.calculateLoadScore(capacity)
	node.LastSeen = time.Now()
	node.mu.Unlock()

	return nil
}

// calculateClusterID calculates the cluster ID for a vector
func (e *RoutingEngine) calculateClusterID(vector *types.Vector) uint32 {
	if e.clusterConfig == nil || e.clusterConfig.TotalClusters == 0 {
		return 0
	}

	// Use FNV hash for consistent cluster assignment
	h := fnv.New32a()
	h.Write([]byte(vector.ID))
	hash := h.Sum32()

	return hash % e.clusterConfig.TotalClusters
}

// getPrimaryNode gets the primary node for a cluster ID
func (e *RoutingEngine) getPrimaryNode(clusterID uint32) (*NodeInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Find nodes that can handle this cluster ID
	var candidates []*NodeInfo
	for _, node := range e.nodes {
		node.mu.RLock()
		if node.Status == StatusHealthy && node.IsPrimary &&
			clusterID >= node.ClusterRange.Start && clusterID <= node.ClusterRange.End {
			candidates = append(candidates, node)
		}
		node.mu.RUnlock()
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy primary node found for cluster %d", clusterID)
	}

	// Select the best candidate based on load score
	return e.selectBestNode(candidates), nil
}

// getReplicaNodes gets the replica nodes for a primary node
func (e *RoutingEngine) getReplicaNodes(primaryNode *NodeInfo) ([]*NodeInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var replicas []*NodeInfo
	for _, node := range e.nodes {
		node.mu.RLock()
		if node.Status == StatusHealthy && !node.IsPrimary && node.ReplicaOf == primaryNode.ID {
			replicas = append(replicas, node)
		}
		node.mu.RUnlock()
	}

	return replicas, nil
}

// getFallbackNodes gets fallback nodes for when primary/replicas are unavailable
func (e *RoutingEngine) getFallbackNodes(primaryNode *NodeInfo, replicaNodes []*NodeInfo) ([]*NodeInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	excluded := make(map[string]bool)
	excluded[primaryNode.ID] = true
	for _, replica := range replicaNodes {
		excluded[replica.ID] = true
	}

	var fallbacks []*NodeInfo
	for _, node := range e.nodes {
		node.mu.RLock()
		if node.Status == StatusHealthy && !excluded[node.ID] {
			fallbacks = append(fallbacks, node)
		}
		node.mu.RUnlock()
	}

	return fallbacks, nil
}

// selectBestNode selects the best node from candidates based on load score
func (e *RoutingEngine) selectBestNode(candidates []*NodeInfo) *NodeInfo {
	if len(candidates) == 0 {
		return nil
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	// Sort candidates by load score (lower is better)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].LoadScore < candidates[j].LoadScore
	})

	return candidates[0]
}

// calculateLoadScore calculates a load score for a node
func (e *RoutingEngine) calculateLoadScore(capacity NodeCapacity) float64 {
	if capacity.TotalVectors == 0 {
		return 0.0
	}

	// Simple load calculation based on CPU and memory usage
	cpuScore := capacity.CPUUsage / 100.0
	memoryScore := float64(capacity.AvailableMemory) / float64(capacity.TotalVectors)
	networkScore := capacity.NetworkLoad / 100.0

	return (cpuScore + networkScore) * (1.0 - memoryScore)
}

// buildRoutingPath builds a string representation of the routing path
func (e *RoutingEngine) buildRoutingPath(primaryNode *NodeInfo, replicaNodes []*NodeInfo) string {
	path := fmt.Sprintf("primary:%s", primaryNode.ID)
	for _, replica := range replicaNodes {
		path += fmt.Sprintf(",replica:%s", replica.ID)
	}
	return path
}

// initializeNodes initializes nodes from cluster configuration
func (e *RoutingEngine) initializeNodes() error {
	if e.clusterConfig == nil {
		return fmt.Errorf("cluster configuration is required")
	}

	for _, nodeConfig := range e.clusterConfig.Nodes {
		node := &NodeInfo{
			ID:        nodeConfig.ID,
			Address:   nodeConfig.Address,
			Port:      nodeConfig.Port,
			IsPrimary: nodeConfig.IsPrimary,
			ReplicaOf: nodeConfig.ReplicaOf,
			ClusterRange: ClusterRange{
				Start: nodeConfig.ClusterStart,
				End:   nodeConfig.ClusterEnd,
			},
			Status:    StatusHealthy,
			LastSeen:  time.Now(),
			LoadScore: 0.0,
		}

		if err := e.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node %s: %w", node.ID, err)
		}
	}

	return nil
}

// runHealthMonitoring runs periodic health monitoring of nodes
func (e *RoutingEngine) runHealthMonitoring(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Health monitoring stopped due to context cancellation")
			return
		case <-e.shutdown:
			e.logger.Info("Health monitoring stopped due to shutdown")
			return
		case <-ticker.C:
			e.checkNodeHealth()
		}
	}
}

// checkNodeHealth checks the health of all nodes
func (e *RoutingEngine) checkNodeHealth() {
	e.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(e.nodes))
	for _, node := range e.nodes {
		nodes = append(nodes, node)
	}
	e.mu.RUnlock()

	for _, node := range nodes {
		e.checkSingleNodeHealth(node)
	}
}

// checkSingleNodeHealth checks the health of a single node
func (e *RoutingEngine) checkSingleNodeHealth(node *NodeInfo) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Skip offline nodes
	if node.Status == StatusOffline {
		return
	}

	// Check if node has been seen recently
	inactiveDuration := time.Since(node.LastSeen)
	if inactiveDuration > e.config.NodeTimeout {
		// Node is considered unhealthy
		if node.Status != StatusUnhealthy {
			e.logger.Warn("Node marked as unhealthy due to timeout",
				zap.String("node_id", node.ID),
				zap.Duration("inactive_duration", inactiveDuration))
			node.Status = StatusUnhealthy
		}
		return
	}

	// Check load score
	if node.LoadScore > 0.8 {
		// Node is under heavy load
		if node.Status != StatusDegraded {
			e.logger.Warn("Node marked as degraded due to high load",
				zap.String("node_id", node.ID),
				zap.Float64("load_score", node.LoadScore))
			node.Status = StatusDegraded
		}
		return

	}

	// Node is healthy
	if node.Status != StatusHealthy {
		e.logger.Info("Node marked as healthy",
			zap.String("node_id", node.ID),
			zap.Float64("load_score", node.LoadScore))
		node.Status = StatusHealthy
	}
}
