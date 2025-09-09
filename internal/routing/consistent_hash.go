
package routing

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHashRing implements a consistent hashing ring for distributed node selection
type ConsistentHashRing struct {
	virtualNodes int
	ring         []uint32
	nodes        map[uint32]string
	nodeMap      map[string]bool
	mu           sync.RWMutex
}

// NewConsistentHashRing creates a new consistent hashing ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		nodes:        make(map[uint32]string),
		nodeMap:      make(map[string]bool),
	}
}

// AddNode adds a node to the hash ring
func (r *ConsistentHashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip if node already exists
	if r.nodeMap[nodeID] {
		return
	}

	// Add virtual nodes for this node
	for i := 0; i < r.virtualNodes; i++ {
		virtualNodeID := r.generateVirtualNodeID(nodeID, i)
		hash := r.hashKey(virtualNodeID)
		r.ring = append(r.ring, hash)
		r.nodes[hash] = nodeID
	}

	// Mark node as added
	r.nodeMap[nodeID] = true

	// Sort the ring for binary search
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

// RemoveNode removes a node from the hash ring
func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip if node doesn't exist
	if !r.nodeMap[nodeID] {
		return
	}

	// Remove all virtual nodes for this node
	var newRing []uint32
	for _, hash := range r.ring {
		if r.nodes[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(r.nodes, hash)
		}
	}

	r.ring = newRing
	delete(r.nodeMap, nodeID)

	// Ring is already sorted since we only removed elements
}

// GetNode gets the node responsible for a given key
func (r *ConsistentHashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	hash := r.hashKey(key)

	// Find the first node in the ring with hash >= key hash
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= hash
	})

	// If we reached the end, wrap around to the first node
	if idx == len(r.ring) {
		idx = 0
	}

	return r.nodes[r.ring[idx]]
}

// GetNodes gets the N nodes responsible for a given key (for replication)
func (r *ConsistentHashRing) GetNodes(key string, count int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 || count <= 0 {
		return nil
	}

	hash := r.hashKey(key)

	// Find the first node in the ring with hash >= key hash
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= hash
	})

	// If we reached the end, wrap around to the first node
	if idx == len(r.ring) {
		idx = 0
	}

	// Collect unique nodes
	nodes := make(map[string]bool)
	result := make([]string, 0)

	// Start from the found position and walk around the ring
	for i := 0; i < len(r.ring) && len(result) < count; i++ {
		currentIdx := (idx + i) % len(r.ring)
		nodeID := r.nodes[r.ring[currentIdx]]

		if !nodes[nodeID] {
			nodes[nodeID] = true
			result = append(result, nodeID)
		}
	}

	return result
}

// GetNodeCount returns the number of nodes in the ring
func (r *ConsistentHashRing) GetNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r
	nodeMap)
}

// GetAllNodes returns all nodes in the ring
func (r *ConsistentHashRing) GetAllNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodeMap))
	for nodeID := range r.nodeMap {
		nodes = append(nodes, nodeID)
	}

	return nodes
}

// ContainsNode checks if a node exists in the ring
func (r *ConsistentHashRing) ContainsNode(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.nodeMap[nodeID]
}

// GetRingSize returns the size of the ring (number of virtual nodes)
func (r *ConsistentHashRing) GetRingSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.ring)
}

// generateVirtualNodeID generates a virtual node ID for a given node and index
func (r *ConsistentHashRing) generateVirtualNodeID(nodeID string, index int) string {
	return nodeID + "#" + strconv.Itoa(index)
}

// hashKey hashes a key using FNV-1a
func (r *ConsistentHashRing) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}