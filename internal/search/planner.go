package search

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/routing"
	"vexdb/internal/types"
)

// QueryPlanner handles query planning and optimization for similarity searches
type QueryPlanner struct {
	logger        logging.Logger
	metrics       *metrics.SearchMetrics
	config        *QueryPlannerConfig
	routingEngine *routing.RoutingEngine
	clusterConfig *types.ClusterConfig
	queryCache    *QueryCache
	nodeSelector  *NodeSelector
	dispatcher    *QueryDispatcher
	merger        *ResultMerger
	mu            sync.RWMutex
	shutdown      chan struct{}
	wg            sync.WaitGroup
}

// QueryPlan represents a query execution plan
type QueryPlan struct {
	ID                string
	QueryVector       *types.Vector
	TopK              int
	MetadataFilter    *types.MetadataFilter
	SelectedNodes     []*routing.NodeInfo
	FallbackNodes     []*routing.NodeInfo
	ExecutionStrategy ExecutionStrategy
	EstimatedCost     float64
	EstimatedLatency  time.Duration
	CreatedAt         time.Time
	Timeout           time.Duration
}

// ExecutionStrategy represents the strategy for executing a query
type ExecutionStrategy int

const (
	StrategySingle ExecutionStrategy = iota
	StrategyParallel
	StrategyBroadcast
	StrategyHybrid
)

// String returns the string representation of ExecutionStrategy
func (s ExecutionStrategy) String() string {
	switch s {
	case StrategySingle:
		return "single"
	case StrategyParallel:
		return "parallel"
	case StrategyBroadcast:
		return "broadcast"
	case StrategyHybrid:
		return "hybrid"
	default:
		return "unknown"
	}
}

// QueryCache caches query plans for reuse
type QueryCache struct {
	entries map[string]*CacheEntry
	maxSize int
	mu      sync.RWMutex
}

// CacheEntry represents a cached query plan
type CacheEntry struct {
	Plan      *QueryPlan
	CreatedAt time.Time
	Accessed  time.Time
	HitCount  int
}

// NodeSelector handles node selection for query execution
type NodeSelector struct {
	logger        logging.Logger
	metrics       *metrics.SearchMetrics
	routingEngine *routing.RoutingEngine
	config        *QueryPlannerConfig
}

// QueryDispatcher handles dispatching queries to selected nodes
type QueryDispatcher struct {
	logger     logging.Logger
	metrics    *metrics.SearchMetrics
	config     *QueryPlannerConfig
	workerPool *WorkerPool
}

// WorkerPool manages a pool of workers for concurrent query execution
type WorkerPool struct {
	workers   []*QueryWorker
	taskQueue chan *QueryTask
	mu        sync.RWMutex
}

// QueryWorker represents a worker that executes queries
type QueryWorker struct {
	id      int
	pool    *WorkerPool
	logger  logging.Logger
	metrics *metrics.SearchMetrics
	active  bool
}

// QueryTask represents a query execution task
type QueryTask struct {
	ID          string
	QueryVector *types.Vector
	Node        *routing.NodeInfo
	TopK        int
	Filter      *types.MetadataFilter
	Timeout     time.Duration
	Context     context.Context
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	TaskID      string
	NodeID      string
	Success     bool
	Results     []*types.SearchResult
	Error       error
	Duration    time.Duration
	VectorCount int
}

// QueryPlannerConfig represents the configuration for the query planner
type QueryPlannerConfig struct {
	CacheSize            int           `yaml:"cache_size" json:"cache_size"`
	CacheTTL             time.Duration `yaml:"cache_ttl" json:"cache_ttl"`
	MaxConcurrentQueries int           `yaml:"max_concurrent_queries" json:"max_concurrent_queries"`
	QueryTimeout         time.Duration `yaml:"query_timeout" json:"query_timeout"`
	NodeTimeout          time.Duration `yaml:"node_timeout" json:"node_timeout"`
	EnableMetrics        bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableCaching        bool          `yaml:"enable_caching" json:"enable_caching"`
	EnableOptimization   bool          `yaml:"enable_optimization" json:"enable_optimization"`
}

// DefaultQueryPlannerConfig returns the default query planner configuration
func DefaultQueryPlannerConfig() *QueryPlannerConfig {
	return &QueryPlannerConfig{
		CacheSize:            1000,
		CacheTTL:             5 * time.Minute,
		MaxConcurrentQueries: 100,
		QueryTimeout:         30 * time.Second,
		NodeTimeout:          10 * time.Second,
		EnableMetrics:        true,
		EnableCaching:        true,
		EnableOptimization:   true,
	}
}

// NewQueryPlanner creates a new query planner
func NewQueryPlanner(config *QueryPlannerConfig, clusterConfig *types.ClusterConfig, logger logging.Logger, metrics *metrics.SearchMetrics) *QueryPlanner {
	if config == nil {
		config = DefaultQueryPlannerConfig()
	}

	planner := &QueryPlanner{
		logger:        logger,
		metrics:       metrics,
		config:        config,
		clusterConfig: clusterConfig,
		shutdown:      make(chan struct{}),
	}

	// Initialize query cache
	if config.EnableCaching {
		planner.queryCache = NewQueryCache(config.CacheSize)
	}

	// Initialize node selector
	planner.nodeSelector = NewNodeSelector(logger, metrics, config)

	// Initialize query dispatcher
	planner.dispatcher = NewQueryDispatcher(config, logger, metrics)

	// Initialize result merger
	planner.merger = NewResultMerger(logger, metrics)

	planner.logger.Info("Created query planner",
		zap.Int("cache_size", config.CacheSize),
		zap.Duration("cache_ttl", config.CacheTTL),
		zap.Int("max_concurrent_queries", config.MaxConcurrentQueries),
		zap.Duration("query_timeout", config.QueryTimeout),
		zap.Duration("node_timeout", config.NodeTimeout))

	return planner
}

// SetRoutingEngine sets the routing engine for the query planner
func (p *QueryPlanner) SetRoutingEngine(engine *routing.RoutingEngine) {
	p.routingEngine = engine
	p.nodeSelector.SetRoutingEngine(engine)
}

// Start starts the query planner
func (p *QueryPlanner) Start(ctx context.Context) error {
	p.logger.Info("Starting query planner")

	// Start query dispatcher
	if err := p.dispatcher.Start(ctx); err != nil {
		return fmt.Errorf("failed to start query dispatcher: %w", err)
	}

	// Start cache cleanup
	if p.queryCache != nil {
		p.wg.Add(1)
		go p.runCacheCleanup(ctx)
	}

	p.logger.Info("Query planner started successfully")
	return nil
}

// Stop stops the query planner
func (p *QueryPlanner) Stop() {
	p.logger.Info("Stopping query planner")

	close(p.shutdown)
	p.wg.Wait()

	p.dispatcher.Stop()

	p.logger.Info("Query planner stopped")
}

// PlanQuery creates a query execution plan
func (p *QueryPlanner) PlanQuery(ctx context.Context, queryVector *types.Vector, topK int, filter *types.MetadataFilter) (*QueryPlan, error) {
	if p.routingEngine == nil {
		return nil, fmt.Errorf("routing engine not set")
	}

	// Generate query cache key
	cacheKey := p.generateCacheKey(queryVector, topK, filter)

	// Check cache first
	if p.queryCache != nil {
		if cachedPlan := p.queryCache.Get(cacheKey); cachedPlan != nil {
			p.logger.Debug("Using cached query plan",
				zap.String("query_id", cachedPlan.ID),
				zap.String("cache_key", cacheKey))
			return cachedPlan, nil
		}
	}

	// Create new query plan
	plan := &QueryPlan{
		ID:             fmt.Sprintf("query-%d", time.Now().UnixNano()),
		QueryVector:    queryVector,
		TopK:           topK,
		MetadataFilter: filter,
		CreatedAt:      time.Now(),
		Timeout:        p.config.QueryTimeout,
	}

	// Select nodes for query execution
	selectedNodes, fallbackNodes, err := p.nodeSelector.SelectNodes(ctx, queryVector)
	if err != nil {
		return nil, fmt.Errorf("failed to select nodes: %w", err)
	}

	plan.SelectedNodes = selectedNodes
	plan.FallbackNodes = fallbackNodes

	// Determine execution strategy
	plan.ExecutionStrategy = p.determineExecutionStrategy(plan)

	// Estimate cost and latency
	plan.EstimatedCost = p.estimateCost(plan)
	plan.EstimatedLatency = p.estimateLatency(plan)

	// Cache the plan
	if p.queryCache != nil {
		p.queryCache.Put(cacheKey, plan)
	}

	p.logger.Debug("Created query plan",
		zap.String("query_id", plan.ID),
		zap.String("strategy", plan.ExecutionStrategy.String()),
		zap.Int("selected_nodes", len(plan.SelectedNodes)),
		zap.Int("fallback_nodes", len(plan.FallbackNodes)),
		zap.Float64("estimated_cost", plan.EstimatedCost),
		zap.Duration("estimated_latency", plan.EstimatedLatency))

	return plan, nil
}

// ExecuteQuery executes a query plan
func (p *QueryPlanner) ExecuteQuery(ctx context.Context, plan *QueryPlan) ([]*types.SearchResult, error) {
	if plan == nil {
		return nil, fmt.Errorf("query plan is nil")
	}

	// Update metrics
	if p.metrics != nil {
		p.metrics.IncrementQueryCount()
		p.metrics.ObserveQueryPlanCost(plan.EstimatedCost)
	}

	startTime := time.Now()

	// Execute query based on strategy
	var results []*types.SearchResult
	var err error

	switch plan.ExecutionStrategy {
	case StrategySingle:
		results, err = p.executeSingleQuery(ctx, plan)
	case StrategyParallel:
		results, err = p.executeParallelQuery(ctx, plan)
	case StrategyBroadcast:
		results, err = p.executeBroadcastQuery(ctx, plan)
	case StrategyHybrid:
		results, err = p.executeHybridQuery(ctx, plan)
	default:
		return nil, fmt.Errorf("unknown execution strategy: %v", plan.ExecutionStrategy)
	}

	duration := time.Since(startTime)

	// Update metrics
	if p.metrics != nil {
		if err != nil {
			p.metrics.IncrementQueryFailure()
		} else {
			p.metrics.IncrementQuerySuccess()
			p.metrics.ObserveQueryLatency(duration)
		}
	}

	p.logger.Debug("Executed query",
		zap.String("query_id", plan.ID),
		zap.String("strategy", plan.ExecutionStrategy.String()),
		zap.Duration("duration", duration),
		zap.Int("result_count", len(results)),
		zap.Error(err))

	return results, err
}

// executeSingleQuery executes a query on a single node
func (p *QueryPlanner) executeSingleQuery(ctx context.Context, plan *QueryPlan) ([]*types.SearchResult, error) {
	if len(plan.SelectedNodes) == 0 {
		return nil, fmt.Errorf("no nodes selected for query execution")
	}

	// Select the best node (lowest load)
	node := p.selectBestNode(plan.SelectedNodes)

	// Execute query on selected node
	result, err := p.dispatcher.ExecuteQuery(ctx, &QueryTask{
		ID:          plan.ID + "-single",
		QueryVector: plan.QueryVector,
		Node:        node,
		TopK:        plan.TopK,
		Filter:      plan.MetadataFilter,
		Timeout:     plan.Timeout,
		Context:     ctx,
	})

	if err != nil {
		// Try fallback nodes if primary fails
		for _, fallbackNode := range plan.FallbackNodes {
			fallbackResult, fallbackErr := p.dispatcher.ExecuteQuery(ctx, &QueryTask{
				ID:          plan.ID + "-fallback-" + fallbackNode.ID,
				QueryVector: plan.QueryVector,
				Node:        fallbackNode,
				TopK:        plan.TopK,
				Filter:      plan.MetadataFilter,
				Timeout:     plan.Timeout,
				Context:     ctx,
			})

			if fallbackErr == nil {
				return fallbackResult.Results, nil
			}
		}
		return nil, fmt.Errorf("failed to execute query on primary and fallback nodes: %w", err)
	}

	return result.Results, nil
}

// executeParallelQuery executes a query in parallel on multiple nodes
func (p *QueryPlanner) executeParallelQuery(ctx context.Context, plan *QueryPlan) ([]*types.SearchResult, error) {
	if len(plan.SelectedNodes) == 0 {
		return nil, fmt.Errorf("no nodes selected for query execution")
	}

	// Execute queries in parallel
	results := make(chan *QueryResult, len(plan.SelectedNodes))

	for _, node := range plan.SelectedNodes {
		go func(n *routing.NodeInfo) {
			task := &QueryTask{
				ID:          plan.ID + "-parallel-" + n.ID,
				QueryVector: plan.QueryVector,
				Node:        n,
				TopK:        plan.TopK,
				Filter:      plan.MetadataFilter,
				Timeout:     plan.Timeout,
				Context:     ctx,
			}

			result, err := p.dispatcher.ExecuteQuery(ctx, task)
			if err != nil {
				results <- &QueryResult{
					TaskID:  task.ID,
					NodeID:  n.ID,
					Success: false,
					Error:   err,
				}
			} else {
				results <- result
			}
		}(node)
	}

	// Collect results
	var allResults []*types.SearchResult
	var errors []error

	for i := 0; i < len(plan.SelectedNodes); i++ {
		select {
		case result := <-results:
			if result.Success {
				allResults = append(allResults, result.Results...)
			} else {
				errors = append(errors, result.Error)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("query execution cancelled")
		}
	}

	// If all nodes failed, return error
	if len(errors) == len(plan.SelectedNodes) {
		return nil, fmt.Errorf("all nodes failed to execute query: %v", errors)
	}

	// Merge results from all nodes
	mergedResults, err := p.merger.MergeResults(allResults, plan.TopK)
	if err != nil {
		return nil, fmt.Errorf("failed to merge results: %w", err)
	}

	return mergedResults, nil
}

// executeBroadcastQuery executes a query on all nodes (broadcast)
func (p *QueryPlanner) executeBroadcastQuery(ctx context.Context, plan *QueryPlan) ([]*types.SearchResult, error) {
	allNodes := p.routingEngine.GetAllNodes()
	if len(allNodes) == 0 {
		return nil, fmt.Errorf("no nodes available for broadcast query")
	}

	// Execute queries on all nodes
	results := make(chan *QueryResult, len(allNodes))

	for _, node := range allNodes {
		go func(n *routing.NodeInfo) {
			task := &QueryTask{
				ID:          plan.ID + "-broadcast-" + n.ID,
				QueryVector: plan.QueryVector,
				Node:        n,
				TopK:        plan.TopK,
				Filter:      plan.MetadataFilter,
				Timeout:     plan.Timeout,
				Context:     ctx,
			}

			result, err := p.dispatcher.ExecuteQuery(ctx, task)
			if err != nil {
				results <- &QueryResult{
					TaskID:  task.ID,
					NodeID:  n.ID,
					Success: false,
					Error:   err,
				}
			} else {
				results <- result
			}
		}(node)
	}

	// Collect results
	var allResults []*types.SearchResult
	var successCount int

	for i := 0; i < len(allNodes); i++ {
		select {
		case result := <-results:
			if result.Success {
				allResults = append(allResults, result.Results...)
				successCount++
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("broadcast query execution cancelled")
		}
	}

	// If no nodes succeeded, return error
	if successCount == 0 {
		return nil, fmt.Errorf("no nodes responded to broadcast query")
	}

	// Merge results from all nodes
	mergedResults, err := p.merger.MergeResults(allResults, plan.TopK)
	if err != nil {
		return nil, fmt.Errorf("failed to merge broadcast results: %w", err)
	}

	return mergedResults, nil
}

// executeHybridQuery executes a query using a hybrid approach
func (p *QueryPlanner) executeHybridQuery(ctx context.Context, plan *QueryPlan) ([]*types.SearchResult, error) {
	// First try parallel execution on selected nodes
	results, err := p.executeParallelQuery(ctx, plan)
	if err == nil && len(results) > 0 {
		return results, nil
	}

	// If parallel execution failed, try broadcast
	p.logger.Warn("Parallel execution failed, falling back to broadcast",
		zap.String("query_id", plan.ID),
		zap.Error(err))

	return p.executeBroadcastQuery(ctx, plan)
}

// selectBestNode selects the best node from a list of candidates
func (p *QueryPlanner) selectBestNode(nodes []*routing.NodeInfo) *routing.NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	if len(nodes) == 1 {
		return nodes[0]
	}

	// Select node with lowest load score
	bestNode := nodes[0]
	for _, node := range nodes[1:] {
		if node.LoadScore < bestNode.LoadScore {
			bestNode = node
		}
	}

	return bestNode
}

// determineExecutionStrategy determines the best execution strategy for a query plan
func (p *QueryPlanner) determineExecutionStrategy(plan *QueryPlan) ExecutionStrategy {
	// Simple strategy selection based on number of nodes and query complexity
	nodeCount := len(plan.SelectedNodes)

	if nodeCount == 1 {
		return StrategySingle
	}

	if nodeCount <= 3 {
		return StrategyParallel
	}

	if nodeCount > 10 {
		return StrategyBroadcast
	}

	return StrategyHybrid
}

// estimateCost estimates the cost of executing a query plan
func (p *QueryPlanner) estimateCost(plan *QueryPlan) float64 {
	// Simple cost estimation based on number of nodes and query complexity
	baseCost := 1.0
	nodeCost := float64(len(plan.SelectedNodes)) * 0.5
	filterCost := 0.0

	if plan.MetadataFilter != nil {
		filterCost = 0.3
	}

	return baseCost + nodeCost + filterCost
}

// estimateLatency estimates the latency of executing a query plan
func (p *QueryPlanner) estimateLatency(plan *QueryPlan) time.Duration {
	// Simple latency estimation based on execution strategy
	baseLatency := 10 * time.Millisecond

	switch plan.ExecutionStrategy {
	case StrategySingle:
		return baseLatency
	case StrategyParallel:
		return baseLatency + 5*time.Millisecond
	case StrategyBroadcast:
		return baseLatency + 20*time.Millisecond
	case StrategyHybrid:
		return baseLatency + 15*time.Millisecond
	default:
		return baseLatency
	}
}

// generateCacheKey generates a cache key for a query
func (p *QueryPlanner) generateCacheKey(queryVector *types.Vector, topK int, filter *types.MetadataFilter) string {
	// Simple cache key generation based on vector ID and parameters
	key := fmt.Sprintf("%s:%d", queryVector.ID, topK)
	if filter != nil {
		key += fmt.Sprintf(":filter-%v", filter)
	}
	return key
}

// runCacheCleanup runs periodic cleanup of the query cache
func (p *QueryPlanner) runCacheCleanup(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Cache cleanup stopped due to context cancellation")
			return
		case <-p.shutdown:
			p.logger.Info("Cache cleanup stopped due to shutdown")
			return
		case <-ticker.C:
			p.queryCache.Cleanup()
		}
	}
}

// NewQueryCache creates a new query cache
func NewQueryCache(maxSize int) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*CacheEntry),
		maxSize: maxSize,
	}
}

// Get retrieves a query plan from the cache
func (c *QueryCache) Get(key string) *QueryPlan {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		return nil
	}

	// Check if entry has expired
	if time.Since(entry.CreatedAt) > 5*time.Minute {
		return nil
	}

	// Update access time and hit count
	entry.Accessed = time.Now()
	entry.HitCount++

	return entry.Plan
}

// Put stores a query plan in the cache
func (c *QueryCache) Put(key string, plan *QueryPlan) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If cache is full, remove least recently used entries
	if len(c.entries) >= c.maxSize {
		c.evictLRU()
	}

	c.entries[key] = &CacheEntry{
		Plan:      plan,
		CreatedAt: time.Now(),
		Accessed:  time.Now(),
		HitCount:  0,
	}
}

// Cleanup removes expired entries from the cache
func (c *QueryCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if now.Sub(entry.CreatedAt) > 5*time.Minute {
			delete(c.entries, key)
		}
	}
}

// evictLRU evicts the least recently used entry from the cache
func (c *QueryCache) evictLRU() {
	if len(c.entries) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.Accessed.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.Accessed
		}
	}

	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// NewNodeSelector creates a new node selector
func NewNodeSelector(logger logging.Logger, metrics *metrics.SearchMetrics, config *QueryPlannerConfig) *NodeSelector {
	return &NodeSelector{
		logger:  logger,
		metrics: metrics,
		config:  config,
	}
}

// SetRoutingEngine sets the routing engine for the node selector
func (ns *NodeSelector) SetRoutingEngine(engine *routing.RoutingEngine) {
	ns.routingEngine = engine
}

// SelectNodes selects nodes for query execution
func (ns *NodeSelector) SelectNodes(ctx context.Context, queryVector *types.Vector) ([]*routing.NodeInfo, []*routing.NodeInfo, error) {
	if ns.routingEngine == nil {
		return nil, nil, fmt.Errorf("routing engine not set")
	}

	// Get routing result for the query vector
	routingResult, err := ns.routingEngine.RouteVector(queryVector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to route vector: %w", err)
	}

	// Get healthy nodes
	healthyNodes := ns.routingEngine.GetHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, nil, fmt.Errorf("no healthy nodes available")
	}

	// Select primary and replica nodes
	selectedNodes := []*routing.NodeInfo{routingResult.PrimaryNode}
	selectedNodes = append(selectedNodes, routingResult.ReplicaNodes...)

	// Get fallback nodes (other healthy nodes)
	fallbackNodes := make([]*routing.NodeInfo, 0)
	for _, node := range healthyNodes {
		isSelected := false
		for _, selected := range selectedNodes {
			if node.ID == selected.ID {
				isSelected = true
				break
			}
		}
		if !isSelected {
			fallbackNodes = append(fallbackNodes, node)
		}
	}

	return selectedNodes, fallbackNodes, nil
}

// NewQueryDispatcher creates a new query dispatcher
func NewQueryDispatcher(config *QueryPlannerConfig, logger logging.Logger, metrics *metrics.SearchMetrics) *QueryDispatcher {
	dispatcher := &QueryDispatcher{
		logger:     logger,
		metrics:    metrics,
		workerPool: NewWorkerPool(config.MaxConcurrentQueries, logger, metrics),
	}

	return dispatcher
}

// Start starts the query dispatcher
func (d *QueryDispatcher) Start(ctx context.Context) error {
	return d.workerPool.Start(ctx)
}

// Stop stops the query dispatcher
func (d *QueryDispatcher) Stop() {
	d.workerPool.Stop()
}

// ExecuteQuery executes a query on a specific node
func (d *QueryDispatcher) ExecuteQuery(ctx context.Context, task *QueryTask) (*QueryResult, error) {
	return d.workerPool.ExecuteTask(ctx, task)
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workerCount int, logger logging.Logger, metrics *metrics.SearchMetrics) *WorkerPool {
	pool := &WorkerPool{
		taskQueue: make(chan *QueryTask, 1000),
	}

	// Create workers
	for i := 0; i < workerCount; i++ {
		worker := &QueryWorker{
			id:      i,
			pool:    pool,
			logger:  logger,
			metrics: metrics,
			active:  true,
		}
		pool.workers = append(pool.workers, worker)
	}

	return pool
}

// Start starts the worker pool
func (p *WorkerPool) Start(ctx context.Context) error {
	for _, worker := range p.workers {
		go worker.run(ctx)
	}
	return nil
}

// Stop stops the worker pool
func (p *WorkerPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Deactivate all workers
	for _, worker := range p.workers {
		worker.active = false
	}

	// Close task queue
	close(p.taskQueue)
}

// ExecuteTask executes a query task
func (p *WorkerPool) ExecuteTask(ctx context.Context, task *QueryTask) (*QueryResult, error) {
	select {
	case p.taskQueue <- task:
		// Task submitted successfully
		resultChan := make(chan *QueryResult, 1)
		go func() {
			// This is a placeholder for actual query execution
			// In a real implementation, this would send the query to the node via gRPC
			result := &QueryResult{
				TaskID:      task.ID,
				NodeID:      task.Node.ID,
				Success:     true,
				Results:     make([]*types.SearchResult, 0),
				Duration:    10 * time.Millisecond,
				VectorCount: 0,
			}
			resultChan <- result
		}()

		select {
		case result := <-resultChan:
			return result, nil
		case <-time.After(task.Timeout):
			return nil, fmt.Errorf("query execution timeout")
		case <-ctx.Done():
			return nil, fmt.Errorf("query execution cancelled")
		}
	default:
		return nil, fmt.Errorf("worker pool is full")
	}
}

// run runs the query worker
func (w *QueryWorker) run(ctx context.Context) {
	w.logger.Info("Started query worker", zap.Int("worker_id", w.id))

	for w.active {
		select {
		case <-ctx.Done():
			w.logger.Info("Query worker stopped due to context cancellation", zap.Int("worker_id", w.id))
			return
		case task, ok := <-w.pool.taskQueue:
			if !ok {
				w.logger.Info("Query worker stopped due to closed task queue", zap.Int("worker_id", w.id))
				return
			}

			result := w.executeTask(task)
			// In a real implementation, this would send the result back to the caller
			// For now, we'll just log it
			w.logger.Debug("Executed query task",
				zap.String("task_id", task.ID),
				zap.String("node_id", task.Node.ID),
				zap.Bool("success", result.Success),
				zap.Duration("duration", result.Duration))
		}
	}
}

// executeTask executes a query task
func (w *QueryWorker) executeTask(task *QueryTask) *QueryResult {
	startTime := time.Now()

	// This is a placeholder for actual query execution
	// In a real implementation, this would send the query to the node via gRPC or HTTP
	time.Sleep(10 * time.Millisecond)

	// Simulate occasional failures (5% failure rate)
	success := time.Now().UnixNano()%20 != 0

	result := &QueryResult{
		TaskID:      task.ID,
		NodeID:      task.Node.ID,
		Success:     success,
		Results:     make([]*types.SearchResult, 0),
		Duration:    time.Since(startTime),
		VectorCount: 0,
	}

	if !success {
		result.Error = fmt.Errorf("simulated query execution failure")
	}

	// Update metrics
	if w.metrics != nil {
		if result.Success {
			w.metrics.IncrementNodeQuerySuccess(task.Node.ID)
		} else {
			w.metrics.IncrementNodeQueryFailure(task.Node.ID)
		}
		w.metrics.ObserveNodeQueryLatency(task.Node.ID, result.Duration)
	}

	return result
}
