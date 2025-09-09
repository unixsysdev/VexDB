package clusterrange

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"go.uber.org/zap"
)

var (
	ErrInvalidRange      = errors.New("invalid range")
	ErrRangeNotFound     = errors.New("range not found")
	ErrRangeOverlap      = errors.New("range overlap detected")
	ErrRangeGap          = errors.New("range gap detected")
	ErrInvalidNodeID     = errors.New("invalid node ID")
	ErrNodeNotFound      = errors.New("node not found")
	ErrAssignmentFailed  = errors.New("assignment failed")
	ErrConfigInvalid     = errors.New("invalid configuration")
)

// Range represents a cluster range
type Range struct {
	Start      uint32            `json:"start"`
	End        uint32            `json:"end"`
	NodeID     string            `json:"node_id"`
	Replicas   []string          `json:"replicas"`
	Status     string            `json:"status"` // active, readonly, draining, offline
	CreatedAt  time.Time         `json:"created_at"`
	ModifiedAt time.Time         `json:"modified_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// RangeConfig represents the range management configuration
type RangeConfig struct {
	ClusterCount      uint32          `yaml:"cluster_count" json:"cluster_count"`
	ReplicationFactor int             `yaml:"replication_factor" json:"replication_factor"`
	RangeSize         uint32          `yaml:"range_size" json:"range_size"`
	EnableAutoBalance bool            `yaml:"enable_auto_balance" json:"enable_auto_balance"`
	BalanceThreshold  float64         `yaml:"balance_threshold" json:"balance_threshold"`
	Nodes             []string        `yaml:"nodes" json:"nodes"`
	Strategy          string          `yaml:"strategy" json:"strategy"` // uniform, custom, dynamic
	EnableValidation  bool            `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultRangeConfig returns the default range configuration
func DefaultRangeConfig() *RangeConfig {
	return &RangeConfig{
		ClusterCount:      1024,
		ReplicationFactor: 1,
		RangeSize:         64, // 64 clusters per range
		EnableAutoBalance: true,
		BalanceThreshold:  0.1, // 10% imbalance threshold
		Nodes:             []string{"node1"},
		Strategy:          "uniform",
		EnableValidation:  true,
	}
}

// RangeManager represents a cluster range manager
type RangeManager struct {
	config      *RangeConfig
	ranges      []*Range
	nodeRanges  map[string][]*Range // node_id -> ranges
	mu          sync.RWMutex
	logger      logging.Logger
	metrics     *metrics.StorageMetrics
}

// RangeStats represents range management statistics
type RangeStats struct {
	TotalRanges       int64     `json:"total_ranges"`
	ActiveRanges      int64     `json:"active_ranges"`
	ReadOnlyRanges    int64     `json:"readonly_ranges"`
	DrainingRanges    int64     `json:"draining_ranges"`
	OfflineRanges     int64     `json:"offline_ranges"`
	TotalNodes        int64     `json:"total_nodes"`
	BalanceFactor     float64   `json:"balance_factor"`
	LastBalanceAt     time.Time `json:"last_balance_at"`
	RebalanceCount    int64     `json:"rebalance_count"`
}

// NewRangeManager creates a new range manager
func NewRangeManager(cfg config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*RangeManager, error) {
	rangeConfig := DefaultRangeConfig()
	
	if cfg != nil {
		if rangeCfg, ok := cfg.Get("range"); ok {
			if cfgMap, ok := rangeCfg.(map[string]interface{}); ok {
				if clusterCount, ok := cfgMap["cluster_count"].(int); ok {
					rangeConfig.ClusterCount = uint32(clusterCount)
				}
				if replicationFactor, ok := cfgMap["replication_factor"].(int); ok {
					rangeConfig.ReplicationFactor = replicationFactor
				}
				if rangeSize, ok := cfgMap["range_size"].(int); ok {
					rangeConfig.RangeSize = uint32(rangeSize)
				}
				if enableAutoBalance, ok := cfgMap["enable_auto_balance"].(bool); ok {
					rangeConfig.EnableAutoBalance = enableAutoBalance
				}
				if balanceThreshold, ok := cfgMap["balance_threshold"].(float64); ok {
					rangeConfig.BalanceThreshold = balanceThreshold
				}
				if nodes, ok := cfgMap["nodes"].([]interface{}); ok {
					rangeConfig.Nodes = make([]string, len(nodes))
					for i, node := range nodes {
						if nodeStr, ok := node.(string); ok {
							rangeConfig.Nodes[i] = nodeStr
						}
					}
				}
				if strategy, ok := cfgMap["strategy"].(string); ok {
					rangeConfig.Strategy = strategy
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					rangeConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateRangeConfig(rangeConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfigInvalid, err)
	}
	
	manager := &RangeManager{
		config:     rangeConfig,
		ranges:     make([]*Range, 0),
		nodeRanges: make(map[string][]*Range),
		logger:     logger,
		metrics:    metrics,
	}
	
	// Initialize ranges based on strategy
	if err := manager.initializeRanges(); err != nil {
		return nil, fmt.Errorf("failed to initialize ranges: %w", err)
	}
	
	manager.logger.Info("Created range manager",
		zap.Uint32("cluster_count", rangeConfig.ClusterCount),
		zap.Int("replication_factor", rangeConfig.ReplicationFactor),
		zap.Uint32("range_size", rangeConfig.RangeSize),
		zap.Strings("nodes", rangeConfig.Nodes),
		zap.String("strategy", rangeConfig.Strategy))
	
	return manager, nil
}

// validateRangeConfig validates the range configuration
func validateRangeConfig(cfg *RangeConfig) error {
	if cfg.ClusterCount == 0 {
		return errors.New("cluster count must be positive")
	}
	
	if cfg.ReplicationFactor < 1 {
		return errors.New("replication factor must be at least 1")
	}
	
	if cfg.RangeSize == 0 {
		return errors.New("range size must be positive")
	}
	
	if cfg.BalanceThreshold < 0 || cfg.BalanceThreshold > 1 {
		return errors.New("balance threshold must be between 0 and 1")
	}
	
	if len(cfg.Nodes) == 0 {
		return errors.New("at least one node must be specified")
	}
	
	if cfg.Strategy != "uniform" && cfg.Strategy != "custom" && cfg.Strategy != "dynamic" {
		return errors.New("strategy must be uniform, custom, or dynamic")
	}
	
	return nil
}

// initializeRanges initializes ranges based on the configured strategy
func (m *RangeManager) initializeRanges() error {
	switch m.config.Strategy {
	case "uniform":
		return m.initializeUniformRanges()
	case "custom":
		return m.initializeCustomRanges()
	case "dynamic":
		return m.initializeDynamicRanges()
	default:
		return fmt.Errorf("unsupported strategy: %s", m.config.Strategy)
	}
}

// initializeUniformRanges initializes ranges with uniform distribution
func (m *RangeManager) initializeUniformRanges() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Clear existing ranges
	m.ranges = make([]*Range, 0)
	m.nodeRanges = make(map[string][]*Range)
	
	// Calculate ranges per node
	rangesPerNode := int(m.config.ClusterCount / m.config.RangeSize)
	if rangesPerNode == 0 {
		rangesPerNode = 1
	}
	
	// Distribute ranges evenly across nodes
	nodeIndex := 0
	for i := uint32(0); i < m.config.ClusterCount; i += m.config.RangeSize {
		end := i + m.config.RangeSize - 1
		if end >= m.config.ClusterCount {
			end = m.config.ClusterCount - 1
		}
		
		nodeID := m.config.Nodes[nodeIndex%len(m.config.Nodes)]
		
		rng := &Range{
			Start:      i,
			End:        end,
			NodeID:     nodeID,
			Replicas:   m.getReplicas(nodeID),
			Status:     "active",
			CreatedAt:  time.Now(),
			ModifiedAt: time.Now(),
		}
		
		m.ranges = append(m.ranges, rng)
		m.nodeRanges[nodeID] = append(m.nodeRanges[nodeID], rng)
		
		nodeIndex++
	}
	
	return nil
}

// initializeCustomRanges initializes ranges with custom distribution
func (m *RangeManager) initializeCustomRanges() error {
	// For custom strategy, we'll use uniform as a fallback
	// In a real implementation, this would load custom ranges from configuration
	return m.initializeUniformRanges()
}

// initializeDynamicRanges initializes ranges with dynamic distribution
func (m *RangeManager) initializeDynamicRanges() error {
	// For dynamic strategy, we'll use uniform as a fallback
	// In a real implementation, this would analyze current load and distribute accordingly
	return m.initializeUniformRanges()
}

// getReplicas returns replica nodes for a given primary node
func (m *RangeManager) getReplicas(primaryNode string) []string {
	if m.config.ReplicationFactor <= 1 {
		return nil
	}
	
	replicas := make([]string, 0, m.config.ReplicationFactor-1)
	
	// Find other nodes to use as replicas
	for _, node := range m.config.Nodes {
		if node != primaryNode {
			replicas = append(replicas, node)
			if len(replicas) >= m.config.ReplicationFactor-1 {
				break
			}
		}
	}
	
	return replicas
}

// GetRange returns the range containing the given cluster ID
func (m *RangeManager) GetRange(clusterID uint32) (*Range, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	for _, rng := range m.ranges {
		if clusterID >= rng.Start && clusterID <= rng.End {
			return rng, nil
		}
	}
	
	return nil, ErrRangeNotFound
}

// GetRanges returns all ranges
func (m *RangeManager) GetRanges() []*Range {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy of ranges
	ranges := make([]*Range, len(m.ranges))
	copy(ranges, m.ranges)
	return ranges
}

// GetNodeRanges returns all ranges for a given node
func (m *RangeManager) GetNodeRanges(nodeID string) ([]*Range, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	ranges, exists := m.nodeRanges[nodeID]
	if !exists {
		return nil, ErrNodeNotFound
	}
	
	// Return a copy of ranges
	result := make([]*Range, len(ranges))
	copy(result, ranges)
	return result, nil
}

// AddNode adds a new node to the cluster
func (m *RangeManager) AddNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check if node already exists
	for _, existingNode := range m.config.Nodes {
		if existingNode == nodeID {
			return fmt.Errorf("node %s already exists", nodeID)
		}
	}
	
	// Add node to configuration
	m.config.Nodes = append(m.config.Nodes, nodeID)
	
	// Initialize node ranges
	m.nodeRanges[nodeID] = make([]*Range, 0)
	
	// Rebalance ranges if auto-balance is enabled
	if m.config.EnableAutoBalance {
		if err := m.rebalanceRanges(); err != nil {
			m.logger.Error("Failed to rebalance ranges after adding node", zap.String("node_id", nodeID), zap.Error(err))
		}
	}
	
	m.logger.Info("Added node to range manager", zap.String("node_id", nodeID))
	
	return nil
}

// RemoveNode removes a node from the cluster
func (m *RangeManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check if node exists
	nodeExists := false
	for i, existingNode := range m.config.Nodes {
		if existingNode == nodeID {
			// Remove node from configuration
			m.config.Nodes = append(m.config.Nodes[:i], m.config.Nodes[i+1:]...)
			nodeExists = true
			break
		}
	}
	
	if !nodeExists {
		return ErrNodeNotFound
	}
	
	// Mark ranges as draining
	for _, rng := range m.nodeRanges[nodeID] {
		rng.Status = "draining"
		rng.ModifiedAt = time.Now()
	}
	
	// Rebalance ranges if auto-balance is enabled
	if m.config.EnableAutoBalance {
		if err := m.rebalanceRanges(); err != nil {
			m.logger.Error("Failed to rebalance ranges after removing node", zap.String("node_id", nodeID), zap.Error(err))
		}
	}
	
	m.logger.Info("Removed node from range manager", zap.String("node_id", nodeID))
	
	return nil
}

// UpdateRangeStatus updates the status of a range
func (m *RangeManager) UpdateRangeStatus(start, end uint32, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	for _, rng := range m.ranges {
		if rng.Start == start && rng.End == end {
			rng.Status = status
			rng.ModifiedAt = time.Now()
			return nil
		}
	}
	
	return ErrRangeNotFound
}

// MoveRange moves a range to a different node
func (m *RangeManager) MoveRange(start, end uint32, newNodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Find the range
	var rng *Range
	var rngIndex int
	for i, r := range m.ranges {
		if r.Start == start && r.End == end {
			rng = r
			rngIndex = i
			break
		}
	}
	
	if rng == nil {
		return ErrRangeNotFound
	}
	
	// Validate new node
	nodeExists := false
	for _, node := range m.config.Nodes {
		if node == newNodeID {
			nodeExists = true
			break
		}
	}
	
	if !nodeExists {
		return ErrNodeNotFound
	}
	
	// Remove from old node ranges
	oldNodeRanges := m.nodeRanges[rng.NodeID]
	for i, r := range oldNodeRanges {
		if r.Start == start && r.End == end {
			m.nodeRanges[rng.NodeID] = append(oldNodeRanges[:i], oldNodeRanges[i+1:]...)
			break
		}
	}
	
	// Update range
	oldNodeID := rng.NodeID
	rng.NodeID = newNodeID
	rng.Replicas = m.getReplicas(newNodeID)
	rng.ModifiedAt = time.Now()
	
	// Add to new node ranges
	m.nodeRanges[newNodeID] = append(m.nodeRanges[newNodeID], rng)
	
	m.logger.Info("Moved range",
		zap.Uint32("start", start),
		zap.Uint32("end", end),
		zap.String("old_node", oldNodeID),
		zap.String("new_node", newNodeID))
	
	return nil
}

// SplitRange splits a range into two ranges
func (m *RangeManager) SplitRange(start, end uint32, splitPoint uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate split point
	if splitPoint <= start || splitPoint >= end {
		return ErrInvalidRange
	}
	
	// Find the range
	var rng *Range
	var rngIndex int
	for i, r := range m.ranges {
		if r.Start == start && r.End == end {
			rng = r
			rngIndex = i
			break
		}
	}
	
	if rng == nil {
		return ErrRangeNotFound
	}
	
	// Create two new ranges
	range1 := &Range{
		Start:      start,
		End:        splitPoint,
		NodeID:     rng.NodeID,
		Replicas:   m.getReplicas(rng.NodeID),
		Status:     rng.Status,
		CreatedAt:  time.Now(),
		ModifiedAt: time.Now(),
	}
	
	range2 := &Range{
		Start:      splitPoint + 1,
		End:        end,
		NodeID:     rng.NodeID,
		Replicas:   m.getReplicas(rng.NodeID),
		Status:     rng.Status,
		CreatedAt:  time.Now(),
		ModifiedAt: time.Now(),
	}
	
	// Remove old range from node ranges
	nodeRanges := m.nodeRanges[rng.NodeID]
	for i, r := range nodeRanges {
		if r.Start == start && r.End == end {
			m.nodeRanges[rng.NodeID] = append(nodeRanges[:i], nodeRanges[i+1:]...)
			break
		}
	}
	
	// Replace old range with new ranges
	m.ranges = append(m.ranges[:rngIndex], m.ranges[rngIndex+1:]...)
	m.ranges = append(m.ranges, range1, range2)
	
	// Add new ranges to node ranges
	m.nodeRanges[rng.NodeID] = append(m.nodeRanges[rng.NodeID], range1, range2)
	
	m.logger.Info("Split range",
		zap.Uint32("original_start", start),
		zap.Uint32("original_end", end),
		zap.Uint32("split_point", splitPoint),
		zap.Uint32("new_range1_start", range1.Start),
		zap.Uint32("new_range1_end", range1.End),
		zap.Uint32("new_range2_start", range2.Start),
		zap.Uint32("new_range2_end", range2.End))
	
	return nil
}

// MergeRanges merges two adjacent ranges
func (m *RangeManager) MergeRanges(start1, end1, start2, end2 uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate ranges are adjacent
	if end1+1 != start2 {
		return ErrInvalidRange
	}
	
	// Find the ranges
	var range1, range2 *Range
	var range1Index, range2Index int
	
	for i, r := range m.ranges {
		if r.Start == start1 && r.End == end1 {
			range1 = r
			range1Index = i
		}
		if r.Start == start2 && r.End == end2 {
			range2 = r
			range2Index = i
		}
	}
	
	if range1 == nil || range2 == nil {
		return ErrRangeNotFound
	}
	
	// Validate ranges are on the same node
	if range1.NodeID != range2.NodeID {
		return ErrInvalidRange
	}
	
	// Create merged range
	mergedRange := &Range{
		Start:      start1,
		End:        end2,
		NodeID:     range1.NodeID,
		Replicas:   m.getReplicas(range1.NodeID),
		Status:     range1.Status,
		CreatedAt:  time.Now(),
		ModifiedAt: time.Now(),
	}
	
	// Remove old ranges from node ranges
	nodeRanges := m.nodeRanges[range1.NodeID]
	newNodeRanges := make([]*Range, 0, len(nodeRanges))
	for _, r := range nodeRanges {
		if !((r.Start == start1 && r.End == end1) || (r.Start == start2 && r.End == end2)) {
			newNodeRanges = append(newNodeRanges, r)
		}
	}
	m.nodeRanges[range1.NodeID] = newNodeRanges
	
	// Replace old ranges with merged range
	// Remove ranges (higher index first to avoid index shifting)
	if range1Index > range2Index {
		m.ranges = append(m.ranges[:range1Index], m.ranges[range1Index+1:]...)
		m.ranges = append(m.ranges[:range2Index], m.ranges[range2Index+1:]...)
	} else {
		m.ranges = append(m.ranges[:range2Index], m.ranges[range2Index+1:]...)
		m.ranges = append(m.ranges[:range1Index], m.ranges[range1Index+1:]...)
	}
	m.ranges = append(m.ranges, mergedRange)
	
	// Add merged range to node ranges
	m.nodeRanges[range1.NodeID] = append(m.nodeRanges[range1.NodeID], mergedRange)
	
	m.logger.Info("Merged ranges",
		zap.Uint32("range1_start", start1),
		zap.Uint32("range1_end", end1),
		zap.Uint32("range2_start", start2),
		zap.Uint32("range2_end", end2),
		zap.Uint32("merged_start", mergedRange.Start),
		zap.Uint32("merged_end", mergedRange.End))
	
	return nil
}

// rebalanceRanges rebalances ranges across nodes
func (m *RangeManager) rebalanceRanges() error {
	// Calculate ideal ranges per node
	idealRanges := len(m.ranges) / len(m.config.Nodes)
	
	// Get current ranges per node
	currentRanges := make(map[string]int)
	for _, node := range m.config.Nodes {
		currentRanges[node] = len(m.nodeRanges[node])
	}
	
	// Find nodes that need more ranges and nodes that have too many
	underloaded := make([]string, 0)
	overloaded := make([]string, 0)
	
	for _, node := range m.config.Nodes {
		if currentRanges[node] < idealRanges {
			underloaded = append(underloaded, node)
		} else if currentRanges[node] > idealRanges {
			overloaded = append(overloaded, node)
		}
	}
	
	// Move ranges from overloaded to underloaded nodes
	for len(overloaded) > 0 && len(underloaded) > 0 {
		overloadedNode := overloaded[0]
		underloadedNode := underloaded[0]
		
		// Find a range to move
		ranges := m.nodeRanges[overloadedNode]
		if len(ranges) > 0 {
			rng := ranges[0]
			
			// Move range
			if err := m.MoveRange(rng.Start, rng.End, underloadedNode); err != nil {
				return err
			}
			
			// Update counts
			currentRanges[overloadedNode]--
			currentRanges[underloadedNode]++
			
			// Update lists
			if currentRanges[overloadedNode] <= idealRanges {
				overloaded = overloaded[1:]
			}
			if currentRanges[underloadedNode] >= idealRanges {
				underloaded = underloaded[1:]
			}
		} else {
			overloaded = overloaded[1:]
		}
	}
	
	return nil
}

// GetBalanceFactor returns the balance factor of range distribution
func (m *RangeManager) GetBalanceFactor() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if len(m.config.Nodes) == 0 {
		return 0.0
	}
	
	// Calculate ideal ranges per node
	idealRanges := float64(len(m.ranges)) / float64(len(m.config.Nodes))
	
	// Calculate variance
	var variance float64
	for _, node := range m.config.Nodes {
		actualRanges := float64(len(m.nodeRanges[node]))
		diff := actualRanges - idealRanges
		variance += diff * diff
	}
	variance /= float64(len(m.config.Nodes))
	
	// Calculate balance factor
	stdDev := math.Sqrt(variance)
	balanceFactor := 1.0 - (stdDev / idealRanges)
	if balanceFactor < 0.0 {
		balanceFactor = 0.0
	}
	
	return balanceFactor
}

// GetStats returns range management statistics
func (m *RangeManager) GetStats() *RangeStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	stats := &RangeStats{
		TotalRanges:    int64(len(m.ranges)),
		TotalNodes:     int64(len(m.config.Nodes)),
		BalanceFactor:  m.GetBalanceFactor(),
	}
	
	// Count ranges by status
	for _, rng := range m.ranges {
		switch rng.Status {
		case "active":
			stats.ActiveRanges++
		case "readonly":
			stats.ReadOnlyRanges++
		case "draining":
			stats.DrainingRanges++
		case "offline":
			stats.OfflineRanges++
		}
	}
	
	return stats
}

// GetConfig returns the range configuration
func (m *RangeManager) GetConfig() *RangeConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy of config
	config := *m.config
	return &config
}

// UpdateConfig updates the range configuration
func (m *RangeManager) UpdateConfig(config *RangeConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate new configuration
	if err := validateRangeConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrConfigInvalid, err)
	}
	
	// Check if cluster count changed
	if config.ClusterCount != m.config.ClusterCount {
		// Reinitialize ranges
		m.config = config
		if err := m.initializeRanges(); err != nil {
			return fmt.Errorf("failed to reinitialize ranges: %w", err)
		}
	} else {
		m.config = config
	}
	
	m.logger.Info("Updated range configuration", zap.Any("config", config))
	
	return nil
}

// Validate validates the range manager state
func (m *RangeManager) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Validate configuration
	if err := validateRangeConfig(m.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate ranges
	for i, rng := range m.ranges {
		if rng.Start > rng.End {
			return fmt.Errorf("%w: range %d has start > end", ErrInvalidRange, i)
		}
		
		// Check for overlaps
		for j, other := range m.ranges {
			if i != j {
				if (rng.Start >= other.Start && rng.Start <= other.End) ||
					(rng.End >= other.Start && rng.End <= other.End) ||
					(other.Start >= rng.Start && other.Start <= rng.End) ||
					(other.End >= rng.Start && other.End <= rng.End) {
					return fmt.Errorf("%w: ranges %d and %d overlap", ErrRangeOverlap, i, j)
				}
			}
		}
	}
	
	// Validate node ranges
	for nodeID, ranges := range m.nodeRanges {
		nodeExists := false
		for _, node := range m.config.Nodes {
			if node == nodeID {
				nodeExists = true
				break
			}
		}
		if !nodeExists {
			return fmt.Errorf("%w: node %s not in configuration", ErrNodeNotFound, nodeID)
		}
		
		// Validate all ranges belong to this node
		for _, rng := range ranges {
			if rng.NodeID != nodeID {
				return fmt.Errorf("range %d-%d belongs to node %s but is in node %s ranges",
					rng.Start, rng.End, rng.NodeID, nodeID)
			}
		}
	}
	
	// Check for gaps in ranges
	if len(m.ranges) > 0 {
		// Sort ranges by start
		sortedRanges := make([]*Range, len(m.ranges))
		copy(sortedRanges, m.ranges)
		sort.Slice(sortedRanges, func(i, j int) bool {
			return sortedRanges[i].Start < sortedRanges[j].Start
		})
		
		// Check for gaps
		for i := 1; i < len(sortedRanges); i++ {
			if sortedRanges[i].Start > sortedRanges[i-1].End+1 {
				return fmt.Errorf("%w: gap between ranges %d-%d and %d-%d",
					ErrRangeGap,
					sortedRanges[i-1].Start, sortedRanges[i-1].End,
					sortedRanges[i].Start, sortedRanges[i].End)
			}
		}
		
		// Check coverage
		if sortedRanges[0].Start != 0 {
			return fmt.Errorf("%w: ranges don't start at 0", ErrRangeGap)
		}
		if sortedRanges[len(sortedRanges)-1].End != m.config.ClusterCount-1 {
			return fmt.Errorf("%w: ranges don't end at %d", ErrRangeGap, m.config.ClusterCount-1)
		}
	}
	
	return nil
}

// GetNodeInfo returns information about a node
func (m *RangeManager) GetNodeInfo(nodeID string) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	ranges, exists := m.nodeRanges[nodeID]
	if !exists {
		return nil, ErrNodeNotFound
	}
	
	info := make(map[string]interface{})
	info["node_id"] = nodeID
	info["range_count"] = len(ranges)
	
	// Count ranges by status
	statusCounts := make(map[string]int)
	for _, rng := range ranges {
		statusCounts[rng.Status]++
	}
	info["status_counts"] = statusCounts
	
	// Calculate total clusters
	var totalClusters uint32
	for _, rng := range ranges {
		totalClusters += (rng.End - rng.Start + 1)
	}
	info["total_clusters"] = totalClusters
	info["cluster_percentage"] = float64(totalClusters) / float64(m.config.ClusterCount) * 100
	
	return info, nil
}

// GetClusterDistribution returns the distribution of clusters across nodes
func (m *RangeManager) GetClusterDistribution() map[string]uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	distribution := make(map[string]uint32)
	for nodeID, ranges := range m.nodeRanges {
		var total uint32
		for _, rng := range ranges {
			total += (rng.End - rng.Start + 1)
		}
		distribution[nodeID] = total
	}
	
	return distribution
}
