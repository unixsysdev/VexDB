package types

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// ClusterConfig represents the cluster configuration
type ClusterConfig struct {
	TotalClusters      uint32            `json:"total_clusters" yaml:"total_clusters"`
	ReplicationFactor  uint32            `json:"replication_factor" yaml:"replication_factor"`
	VirtualNodes       uint32            `json:"virtual_nodes" yaml:"virtual_nodes"` // For consistent hashing
	Nodes              []ClusterNode     `json:"nodes" yaml:"nodes"`
	HashRing          *ConsistentHash   `json:"-" yaml:"-"`
	RangeMap          map[uint32]string `json:"-" yaml:"-"` // cluster_id -> node_id
	mu                sync.RWMutex      `json:"-" yaml:"-"`
}

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	ID           string            `json:"id" yaml:"id"`
	Address      string            `json:"address" yaml:"address"`
	Port         int               `json:"port" yaml:"port"`
	IsPrimary    bool              `json:"is_primary" yaml:"is_primary"`
	ClusterRange []uint32          `json:"cluster_range" yaml:"cluster_range"` // [start, end]
	Metadata     map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Health       NodeHealth        `json:"health" yaml:"health"`
	Capacity     NodeCapacity      `json:"capacity" yaml:"capacity"`
}

// NodeHealth represents the health status of a node
type NodeHealth struct {
	Status      string    `json:"status" yaml:"status"`      // "healthy", "unhealthy", "degraded"
	LastCheck   int64     `json:"last_check" yaml:"last_check"`
	ResponseTime int64     `json:"response_time" yaml:"response_time"` // in milliseconds
	ErrorCount  int       `json:"error_count" yaml:"error_count"`
	LastError   string    `json:"last_error,omitempty" yaml:"last_error,omitempty"`
}

// NodeCapacity represents the capacity information of a node
type NodeCapacity struct {
	MaxVectors     uint64 `json:"max_vectors" yaml:"max_vectors"`
	UsedVectors    uint64 `json:"used_vectors" yaml:"used_vectors"`
	MaxMemory      uint64 `json:"max_memory" yaml:"max_memory"`      // in bytes
	UsedMemory     uint64 `json:"used_memory" yaml:"used_memory"`     // in bytes
	MaxDisk        uint64 `json:"max_disk" yaml:"max_disk"`          // in bytes
	UsedDisk       uint64 `json:"used_disk" yaml:"used_disk"`         // in bytes
	MaxConnections int    `json:"max_connections" yaml:"max_connections"`
	UsedConnections int   `json:"used_connections" yaml:"used_connections"`
}

// ConsistentHash implements consistent hashing for cluster node selection
type ConsistentHash struct {
	virtualNodes int
	ring         []uint32
	nodes        map[uint32]string // hash -> node_id
	sync.RWMutex
}

// ClusterAssignment represents the result of cluster assignment for a vector
type ClusterAssignment struct {
	ClusterID      uint32   `json:"cluster_id" yaml:"cluster_id"`
	PrimaryNode    string   `json:"primary_node" yaml:"primary_node"`
	ReplicaNodes   []string `json:"replica_nodes" yaml:"replica_nodes"`
	HashKey        string   `json:"hash_key" yaml:"hash_key"`
}

// NewClusterConfig creates a new cluster configuration
func NewClusterConfig(totalClusters, replicationFactor, virtualNodes uint32) (*ClusterConfig, error) {
	if totalClusters == 0 {
		return nil, errors.New("total clusters must be greater than 0")
	}
	
	if replicationFactor == 0 {
		return nil, errors.New("replication factor must be greater than 0")
	}
	
	if replicationFactor > 10 { // Reasonable upper limit
		return nil, errors.New("replication factor too large (max 10)")
	}
	
	if virtualNodes == 0 {
		virtualNodes = 100 // Default virtual nodes
	}
	
	config := &ClusterConfig{
		TotalClusters:     totalClusters,
		ReplicationFactor: replicationFactor,
		VirtualNodes:      virtualNodes,
		Nodes:             make([]ClusterNode, 0),
		RangeMap:          make(map[uint32]string),
		HashRing:          NewConsistentHash(int(virtualNodes)),
	}
	
	return config, nil
}

// AddNode adds a node to the cluster
func (c *ClusterConfig) AddNode(node ClusterNode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Validate node
	if node.ID == "" {
		return errors.New("node ID cannot be empty")
	}
	
	if node.Address == "" {
		return errors.New("node address cannot be empty")
	}
	
	if node.Port <= 0 {
		return errors.New("node port must be greater than 0")
	}
	
	// Check if node already exists
	for _, existingNode := range c.Nodes {
		if existingNode.ID == node.ID {
			return fmt.Errorf("node with ID '%s' already exists", node.ID)
		}
	}
	
	// Set default health status if not set
	if node.Health.Status == "" {
		node.Health.Status = "healthy"
		node.Health.LastCheck = 0
	}
	
	// Add node to list
	c.Nodes = append(c.Nodes, node)
	
	// Add to hash ring
	c.HashRing.AddNode(node.ID)
	
	// Rebalance cluster ranges
	if err := c.rebalanceClusterRanges(); err != nil {
		// Rollback node addition
		c.Nodes = c.Nodes[:len(c.Nodes)-1]
		c.HashRing.RemoveNode(node.ID)
		return err
	}
	
	return nil
}

// RemoveNode removes a node from the cluster
func (c *ClusterConfig) RemoveNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Find and remove node
	found := false
	for i, node := range c.Nodes {
		if node.ID == nodeID {
			c.Nodes = append(c.Nodes[:i], c.Nodes[i+1:]...)
			found = true
			break
		}
	}
	
	if !found {
		return fmt.Errorf("node with ID '%s' not found", nodeID)
	}
	
	// Remove from hash ring
	c.HashRing.RemoveNode(nodeID)
	
	// Rebalance cluster ranges
	if err := c.rebalanceClusterRanges(); err != nil {
		return err
	}
	
	return nil
}

// GetNode returns a node by ID
func (c *ClusterConfig) GetNode(nodeID string) (*ClusterNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	for _, node := range c.Nodes {
		if node.ID == nodeID {
			return &node, nil
		}
	}
	
	return nil, fmt.Errorf("node with ID '%s' not found", nodeID)
}

// GetHealthyNodes returns all healthy nodes
func (c *ClusterConfig) GetHealthyNodes() []ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	healthyNodes := make([]ClusterNode, 0)
	for _, node := range c.Nodes {
		if node.Health.Status == "healthy" {
			healthyNodes = append(healthyNodes, node)
		}
	}
	
	return healthyNodes
}

// GetPrimaryNodes returns all primary nodes
func (c *ClusterConfig) GetPrimaryNodes() []ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	primaryNodes := make([]ClusterNode, 0)
	for _, node := range c.Nodes {
		if node.IsPrimary {
			primaryNodes = append(primaryNodes, node)
		}
	}
	
	return primaryNodes
}

// AssignCluster assigns a cluster ID to a vector
func (c *ClusterConfig) AssignCluster(vectorID string, vectorData []float32) (*ClusterAssignment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if len(c.Nodes) == 0 {
		return nil, errors.New("no nodes available in cluster")
	}
	
	// Generate hash key for the vector
	hashKey := c.generateVectorHash(vectorID, vectorData)
	
	// Get cluster ID using hash
	clusterID := c.hashToClusterID(hashKey)
	
	// Get primary node for this cluster
	primaryNodeID, exists := c.RangeMap[clusterID]
	if !exists {
		return nil, fmt.Errorf("no node assigned to cluster %d", clusterID)
	}
	
	// Get replica nodes
	replicaNodes := c.getReplicaNodes(primaryNodeID)
	
	return &ClusterAssignment{
		ClusterID:    clusterID,
		PrimaryNode:  primaryNodeID,
		ReplicaNodes: replicaNodes,
		HashKey:      hashKey,
	}, nil
}

// GetNodesForCluster returns all nodes responsible for a cluster
func (c *ClusterConfig) GetNodesForCluster(clusterID uint32) ([]ClusterNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if clusterID >= c.TotalClusters {
		return nil, fmt.Errorf("cluster ID %d exceeds total clusters %d", clusterID, c.TotalClusters)
	}
	
	nodes := make([]ClusterNode, 0)
	for _, node := range c.Nodes {
		if node.ClusterRange != nil && len(node.ClusterRange) == 2 {
			if clusterID >= node.ClusterRange[0] && clusterID <= node.ClusterRange[1] {
				nodes = append(nodes, node)
			}
		}
	}
	
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes found for cluster %d", clusterID)
	}
	
	return nodes, nil
}

// rebalanceClusterRanges rebalances cluster ranges across nodes
func (c *ClusterConfig) rebalanceClusterRanges() error {
	// Clear existing range map
	c.RangeMap = make(map[uint32]string)
	
	// Get healthy primary nodes
	healthyPrimaries := make([]ClusterNode, 0)
	for _, node := range c.Nodes {
		if node.IsPrimary && node.Health.Status == "healthy" {
			healthyPrimaries = append(healthyPrimaries, node)
		}
	}
	
	if len(healthyPrimaries) == 0 {
		return errors.New("no healthy primary nodes available")
	}
	
	// Calculate clusters per node
	clustersPerNode := c.TotalClusters / uint32(len(healthyPrimaries))
	remainder := c.TotalClusters % uint32(len(healthyPrimaries))
	
	// Assign cluster ranges
	startCluster := uint32(0)
	for i, node := range healthyPrimaries {
		endCluster := startCluster + clustersPerNode - 1
		if i < int(remainder) {
			endCluster++
		}
		
		// Update node cluster range
		node.ClusterRange = []uint32{startCluster, endCluster}
		
		// Update range map
		for clusterID := startCluster; clusterID <= endCluster; clusterID++ {
			c.RangeMap[clusterID] = node.ID
		}
		
		startCluster = endCluster + 1
	}
	
	return nil
}

// generateVectorHash generates a hash key for a vector
func (c *ClusterConfig) generateVectorHash(vectorID string, vectorData []float32) string {
	// Combine vector ID and data for hashing
	h := sha1.New()
	
	// Write vector ID
	h.Write([]byte(vectorID))
	
	// Write vector data
	for _, val := range vectorData {
		bits := math.Float32bits(val)
		h.Write([]byte{
			byte(bits >> 24),
			byte(bits >> 16),
			byte(bits >> 8),
			byte(bits),
		})
	}
	
	return fmt.Sprintf("%x", h.Sum(nil))
}

// hashToClusterID converts a hash to a cluster ID
func (c *ClusterConfig) hashToClusterID(hashKey string) uint32 {
	// Simple hash to cluster ID mapping
	// In practice, you might want a more sophisticated approach
	hash := uint32(0)
	for i, c := range hashKey {
		hash += uint32(c) << (uint(i) % 32)
	}
	
	return hash % c.TotalClusters
}

// getReplicaNodes gets replica nodes for a primary node
func (c *ClusterConfig) getReplicaNodes(primaryNodeID string) []string {
	if c.ReplicationFactor <= 1 {
		return nil
	}
	
	// Get all healthy nodes except the primary
	healthyNodes := make([]string, 0)
	for _, node := range c.Nodes {
		if node.ID != primaryNodeID && node.Health.Status == "healthy" {
			healthyNodes = append(healthyNodes, node.ID)
		}
	}
	
	// Select replica nodes using consistent hashing
	replicaCount := int(c.ReplicationFactor) - 1
	if replicaCount > len(healthyNodes) {
		replicaCount = len(healthyNodes)
	}
	
	if replicaCount == 0 {
		return nil
	}
	
	// Use consistent hashing to select replica nodes
	replicaNodes := make([]string, 0)
	used := make(map[string]bool)
	used[primaryNodeID] = true
	
	for i := 0; i < replicaCount; i++ {
		// Generate a hash for this replica selection
		hashKey := fmt.Sprintf("%s_replica_%d", primaryNodeID, i)
		_ = c.HashRing.GetNode(hashKey) // Get node from hash ring (result ignored for now)
		
		// Find a healthy node that hasn't been used
		for _, healthyNode := range healthyNodes {
			if !used[healthyNode] {
				replicaNodes = append(replicaNodes, healthyNode)
				used[healthyNode] = true
				break
			}
		}
	}
	
	return replicaNodes
}

// UpdateNodeHealth updates the health status of a node
func (c *ClusterConfig) UpdateNodeHealth(nodeID string, health NodeHealth) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for i, node := range c.Nodes {
		if node.ID == nodeID {
			c.Nodes[i].Health = health
			return nil
		}
	}
	
	return fmt.Errorf("node with ID '%s' not found", nodeID)
}

// UpdateNodeCapacity updates the capacity information of a node
func (c *ClusterConfig) UpdateNodeCapacity(nodeID string, capacity NodeCapacity) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for i, node := range c.Nodes {
		if node.ID == nodeID {
			c.Nodes[i].Capacity = capacity
			return nil
		}
	}
	
	return fmt.Errorf("node with ID '%s' not found", nodeID)
}

// GetClusterStats returns cluster statistics
func (c *ClusterConfig) GetClusterStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	stats := make(map[string]interface{})
	
	// Node statistics
	stats["total_nodes"] = len(c.Nodes)
	stats["healthy_nodes"] = len(c.GetHealthyNodes())
	stats["primary_nodes"] = len(c.GetPrimaryNodes())
	
	// Capacity statistics
	totalVectors := uint64(0)
	usedVectors := uint64(0)
	totalMemory := uint64(0)
	usedMemory := uint64(0)
	totalDisk := uint64(0)
	usedDisk := uint64(0)
	
	for _, node := range c.Nodes {
		totalVectors += node.Capacity.MaxVectors
		usedVectors += node.Capacity.UsedVectors
		totalMemory += node.Capacity.MaxMemory
		usedMemory += node.Capacity.UsedMemory
		totalDisk += node.Capacity.MaxDisk
		usedDisk += node.Capacity.UsedDisk
	}
	
	stats["vector_usage"] = map[string]interface{}{
		"total": totalVectors,
		"used":  usedVectors,
		"free":  totalVectors - usedVectors,
		"usage_percent": float64(usedVectors) / float64(totalVectors) * 100,
	}
	
	stats["memory_usage"] = map[string]interface{}{
		"total": totalMemory,
		"used":  usedMemory,
		"free":  totalMemory - usedMemory,
		"usage_percent": float64(usedMemory) / float64(totalMemory) * 100,
	}
	
	stats["disk_usage"] = map[string]interface{}{
		"total": totalDisk,
		"used":  usedDisk,
		"free":  totalDisk - usedDisk,
		"usage_percent": float64(usedDisk) / float64(totalDisk) * 100,
	}
	
	return stats
}

// NewConsistentHash creates a new consistent hash ring
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	return &ConsistentHash{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		nodes:        make(map[uint32]string),
	}
}

// AddNode adds a node to the consistent hash ring
func (ch *ConsistentHash) AddNode(nodeID string) {
	ch.Lock()
	defer ch.Unlock()
	
	// Add virtual nodes for this node
	for i := 0; i < ch.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s_virtual_%d", nodeID, i)
		hash := ch.hashKey(virtualKey)
		ch.ring = append(ch.ring, hash)
		ch.nodes[hash] = nodeID
	}
	
	// Sort the ring for binary search
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	})
}

// RemoveNode removes a node from the consistent hash ring
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.Lock()
	defer ch.Unlock()
	
	// Remove all virtual nodes for this node
	newRing := make([]uint32, 0)
	for _, hash := range ch.ring {
		if ch.nodes[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(ch.nodes, hash)
		}
	}
	ch.ring = newRing
}

// GetNode gets the node responsible for a given key
func (ch *ConsistentHash) GetNode(key string) string {
	ch.RLock()
	defer ch.RUnlock()
	
	if len(ch.ring) == 0 {
		return ""
	}
	
	hash := ch.hashKey(key)
	
	// Find the first node in the ring with hash >= key hash
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})
	
	if idx == len(ch.ring) {
		idx = 0 // Wrap around to the first node
	}
	
	return ch.nodes[ch.ring[idx]]
}

// hashKey generates a hash for a key
func (ch *ConsistentHash) hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	hash := h.Sum(nil)
	return binary.BigEndian.Uint32(hash[:4])
}

// ClusterInfo represents information about a cluster
type ClusterInfo struct {
	ID          string            `json:"id" yaml:"id"`
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description" yaml:"description"`
	Config      ClusterConfig     `json:"config" yaml:"config"`
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	CreatedAt   int64             `json:"created_at" yaml:"created_at"`
	UpdatedAt   int64             `json:"updated_at" yaml:"updated_at"`
}

// ClusterStatus represents the status of a cluster
type ClusterStatus struct {
	ID        string    `json:"id" yaml:"id"`
	Status    string    `json:"status" yaml:"status"` // "active", "inactive", "degraded", "maintenance"
	Health    string    `json:"health" yaml:"health"` // "healthy", "unhealthy", "warning"
	Message   string    `json:"message,omitempty" yaml:"message,omitempty"`
	Timestamp int64     `json:"timestamp" yaml:"timestamp"`
	Nodes     []string  `json:"nodes" yaml:"nodes"`
	Metrics   map[string]interface{} `json:"metrics,omitempty" yaml:"metrics,omitempty"`
}

// HealthStatus represents the health status of a service or component
type HealthStatus struct {
    Healthy   bool              `json:"healthy" yaml:"healthy"`
    Status    string            `json:"status" yaml:"status"` // "healthy", "unhealthy", "degraded"
    Message   string            `json:"message,omitempty" yaml:"message,omitempty"`
    Details   map[string]string `json:"details,omitempty" yaml:"details,omitempty"`
    Timestamp int64             `json:"timestamp" yaml:"timestamp"`
    Checks    []HealthCheck     `json:"checks,omitempty" yaml:"checks,omitempty"`
}

// HealthCheck represents a single health check result
type HealthCheck struct {
	Name     string            `json:"name" yaml:"name"`
	Status   string            `json:"status" yaml:"status"` // "pass", "fail", "warn"
	Message  string            `json:"message,omitempty" yaml:"message,omitempty"`
	Duration int64             `json:"duration,omitempty" yaml:"duration"` // in milliseconds
	Details  map[string]string `json:"details,omitempty" yaml:"details,omitempty"`
}

// NodeInfo represents information about a node
type NodeInfo struct {
	ID          string            `json:"id" yaml:"id"`
	Address     string            `json:"address" yaml:"address"`
	Port        int               `json:"port" yaml:"port"`
	Role        string            `json:"role" yaml:"role"` // "primary", "replica", "standalone"
	Status      string            `json:"status" yaml:"status"` // "online", "offline", "maintenance"
	Health      string            `json:"health" yaml:"health"` // "healthy", "unhealthy", "degraded"
	Capacity    NodeCapacity      `json:"capacity" yaml:"capacity"`
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	LastSeen    int64             `json:"last_seen" yaml:"last_seen"`
	Version     string            `json:"version,omitempty" yaml:"version,omitempty"`
}
