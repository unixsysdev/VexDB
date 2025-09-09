package routing

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

// ReplicationManager handles replication of vectors across multiple nodes
type ReplicationManager struct {
	logger          logging.Logger
	metrics         *metrics.IngestionMetrics
	config          *RoutingConfig
	engine          *RoutingEngine
	replicationPool *ReplicationPool
	circuitBreakers map[string]*CircuitBreaker
	mu              sync.RWMutex
	shutdown        chan struct{}
	wg              sync.WaitGroup
}

// ReplicationPool manages a pool of workers for concurrent replication
type ReplicationPool struct {
        workers    []*ReplicationWorker
        taskQueue  chan *ReplicationTask
        resultChan chan *ReplicationResult
        logger     logging.Logger
        mu         sync.RWMutex
}

// ReplicationWorker represents a worker that handles replication tasks
type ReplicationWorker struct {
	id        int
	pool      *ReplicationPool
	logger    logging.Logger
	metrics   *metrics.IngestionMetrics
	active    bool
	taskCount int64
}

// ReplicationTask represents a replication task
type ReplicationTask struct {
	Vector     *types.Vector
	Nodes      []*NodeInfo
	Timeout    time.Duration
	RetryCount int
	MaxRetries int
	CreatedAt  time.Time
	Context    context.Context
}

// ReplicationResult represents the result of a replication task
type ReplicationResult struct {
	TaskID       string
	VectorID     string
	Success      bool
	ReplicatedTo []string
	FailedNodes  []string
	Error        error
	Duration     time.Duration
	Retries      int
}

// CircuitBreaker implements the circuit breaker pattern for node failures
type CircuitBreaker struct {
	nodeID       string
	state        CircuitState
	failureCount int
	successCount int
	lastFailure  time.Time
	lastSuccess  time.Time
	config       *CircuitBreakerConfig
	mu           sync.RWMutex
}

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// String returns the string representation of CircuitState
func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig represents the configuration for a circuit breaker
type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold" json:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold" json:"success_threshold"`
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`
	HalfOpenMaxCalls int           `yaml:"half_open_max_calls" json:"half_open_max_calls"`
	EnableMetrics    bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultCircuitBreakerConfig returns the default circuit breaker configuration
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          60 * time.Second,
		HalfOpenMaxCalls: 3,
		EnableMetrics:    true,
	}
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(config *RoutingConfig, logger logging.Logger, metrics *metrics.IngestionMetrics) *ReplicationManager {
	if config == nil {
		config = DefaultRoutingConfig()
	}

	manager := &ReplicationManager{
		logger:          logger,
		metrics:         metrics,
		config:          config,
		circuitBreakers: make(map[string]*CircuitBreaker),
		shutdown:        make(chan struct{}),
	}

	// Initialize replication pool
	manager.replicationPool = NewReplicationPool(config.ReplicationFactor, logger, metrics)

	manager.logger.Info("Created replication manager",
		zap.Int("replication_factor", config.ReplicationFactor),
		zap.Int("failure_threshold", DefaultCircuitBreakerConfig().FailureThreshold),
		zap.Duration("timeout", DefaultCircuitBreakerConfig().Timeout))

	return manager
}

// SetRoutingEngine sets the routing engine for the replication manager
func (m *ReplicationManager) SetRoutingEngine(engine *RoutingEngine) {
	m.engine = engine
}

// Start starts the replication manager
func (m *ReplicationManager) Start(ctx context.Context) error {
	m.logger.Info("Starting replication manager")

	// Start replication pool
	if err := m.replicationPool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start replication pool: %w", err)
	}

	// Start circuit breaker monitoring
	m.wg.Add(1)
	go m.runCircuitBreakerMonitoring(ctx)

	m.logger.Info("Replication manager started successfully")
	return nil
}

// Stop stops the replication manager
func (m *ReplicationManager) Stop() {
	m.logger.Info("Stopping replication manager")

	close(m.shutdown)
	m.wg.Wait()

	m.replicationPool.Stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all circuit breakers
	for _, cb := range m.circuitBreakers {
		cb.mu.Lock()
		cb.state = StateOpen
		cb.mu.Unlock()
	}

	m.logger.Info("Replication manager stopped")
}

// ReplicateVector replicates a vector to multiple nodes
func (m *ReplicationManager) ReplicateVector(ctx context.Context, vector *types.Vector, nodes []*NodeInfo) (*ReplicationResult, error) {
	if m.engine == nil {
		return nil, fmt.Errorf("routing engine not set")
	}

	// Filter out nodes that are blocked by circuit breakers
	availableNodes := m.filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("no available nodes for replication")
	}

	// Create replication task
	task := &ReplicationTask{
		Vector:     vector,
		Nodes:      availableNodes,
		Timeout:    m.config.NodeTimeout,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		Context:    ctx,
	}

	// Submit task to replication pool
	result, err := m.replicationPool.SubmitTask(task)
	if err != nil {
		return nil, fmt.Errorf("failed to submit replication task: %w", err)
	}

	return result, nil
}

// ReplicateBatch replicates a batch of vectors to multiple nodes
func (m *ReplicationManager) ReplicateBatch(ctx context.Context, vectors []*types.Vector, nodes []*NodeInfo) ([]*ReplicationResult, error) {
	results := make([]*ReplicationResult, 0, len(vectors))

	for _, vector := range vectors {
		result, err := m.ReplicateVector(ctx, vector, nodes)
		if err != nil {
			m.logger.Error("Failed to replicate vector",
				zap.String("vector_id", vector.ID),
				zap.Error(err))
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// GetCircuitBreaker gets or creates a circuit breaker for a node
func (m *ReplicationManager) GetCircuitBreaker(nodeID string) *CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, exists := m.circuitBreakers[nodeID]; exists {
		return cb
	}

	cb := NewCircuitBreaker(nodeID, DefaultCircuitBreakerConfig(), m.logger, m.metrics)
	m.circuitBreakers[nodeID] = cb

	return cb
}

// filterAvailableNodes filters out nodes that are blocked by circuit breakers
func (m *ReplicationManager) filterAvailableNodes(nodes []*NodeInfo) []*NodeInfo {
	var available []*NodeInfo

	for _, node := range nodes {
		cb := m.GetCircuitBreaker(node.ID)
		if cb.AllowRequest() {
			available = append(available, node)
		} else {
			m.logger.Debug("Node blocked by circuit breaker",
				zap.String("node_id", node.ID),
				zap.String("circuit_state", cb.GetState().String()))
		}
	}

	return available
}

// runCircuitBreakerMonitoring runs periodic monitoring of circuit breakers
func (m *ReplicationManager) runCircuitBreakerMonitoring(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Circuit breaker monitoring stopped due to context cancellation")
			return
		case <-m.shutdown:
			m.logger.Info("Circuit breaker monitoring stopped due to shutdown")
			return
		case <-ticker.C:
			m.monitorCircuitBreakers()
		}
	}
}

// monitorCircuitBreakers monitors the state of all circuit breakers
func (m *ReplicationManager) monitorCircuitBreakers() {
	m.mu.RLock()
	breakers := make([]*CircuitBreaker, 0, len(m.circuitBreakers))
	for _, cb := range m.circuitBreakers {
		breakers = append(breakers, cb)
	}
	m.mu.RUnlock()

	for _, cb := range breakers {
		state := cb.GetState()
		failureCount := cb.GetFailureCount()
		successCount := cb.GetSuccessCount()

		m.logger.Debug("Circuit breaker state",
			zap.String("node_id", cb.nodeID),
			zap.String("state", state.String()),
			zap.Int("failure_count", failureCount),
			zap.Int("success_count", successCount))

		// Update metrics
		if m.metrics != nil {
			if state == StateOpen {
				m.metrics.IncrementCircuitBreakerOpenCount()
			}
		}
	}
}

// NewReplicationPool creates a new replication pool
func NewReplicationPool(workerCount int, logger logging.Logger, metrics *metrics.IngestionMetrics) *ReplicationPool {
        pool := &ReplicationPool{
                taskQueue:  make(chan *ReplicationTask, 1000),
                resultChan: make(chan *ReplicationResult, 1000),
                logger:     logger,
        }

	// Create workers
	for i := 0; i < workerCount; i++ {
		worker := &ReplicationWorker{
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

// Start starts the replication pool
func (p *ReplicationPool) Start(ctx context.Context) error {
	p.logger.Info("Starting replication pool", zap.Int("worker_count", len(p.workers)))

	for _, worker := range p.workers {
		go worker.run(ctx)
	}

	return nil
}

// Stop stops the replication pool
func (p *ReplicationPool) Stop() {
	p.logger.Info("Stopping replication pool")

	p.mu.Lock()
	defer p.mu.Unlock()

	// Deactivate all workers
	for _, worker := range p.workers {
		worker.active = false
	}

	// Close channels
	close(p.taskQueue)
	close(p.resultChan)

	p.logger.Info("Replication pool stopped")
}

// SubmitTask submits a task to the replication pool
func (p *ReplicationPool) SubmitTask(task *ReplicationTask) (*ReplicationResult, error) {
	select {
	case p.taskQueue <- task:
		// Task submitted successfully
		select {
		case result := <-p.resultChan:
			return result, nil
		case <-time.After(task.Timeout):
			return nil, fmt.Errorf("replication task timeout")
		}
	default:
		return nil, fmt.Errorf("replication pool is full")
	}
}

// run runs the replication worker
func (w *ReplicationWorker) run(ctx context.Context) {
	w.logger.Info("Started replication worker", zap.Int("worker_id", w.id))

	for w.active {
		select {
		case <-ctx.Done():
			w.logger.Info("Replication worker stopped due to context cancellation", zap.Int("worker_id", w.id))
			return
		case task, ok := <-w.pool.taskQueue:
			if !ok {
				w.logger.Info("Replication worker stopped due to closed task queue", zap.Int("worker_id", w.id))
				return
			}

			result := w.processTask(task)
			w.pool.resultChan <- result
		}
	}
}

// processTask processes a replication task
func (w *ReplicationWorker) processTask(task *ReplicationTask) *ReplicationResult {
	startTime := time.Now()
	result := &ReplicationResult{
		TaskID:       fmt.Sprintf("%s-%d", task.Vector.ID, time.Now().UnixNano()),
		VectorID:     task.Vector.ID,
		Success:      false,
		ReplicatedTo: make([]string, 0),
		FailedNodes:  make([]string, 0),
		Retries:      task.RetryCount,
	}

	// Process replication with retry logic
	for attempt := 0; attempt <= task.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			select {
			case <-task.Context.Done():
				result.Error = fmt.Errorf("replication cancelled")
				return result
			case <-time.After(backoff):
				// Continue with retry
			}
		}

		// Try to replicate to all nodes
		success := true
		for _, node := range task.Nodes {
			if err := w.replicateToNode(task, node); err != nil {
				w.logger.Error("Failed to replicate to node",
					zap.String("vector_id", task.Vector.ID),
					zap.String("node_id", node.ID),
					zap.Error(err),
					zap.Int("attempt", attempt+1))

				result.FailedNodes = append(result.FailedNodes, node.ID)
				success = false
			} else {
				result.ReplicatedTo = append(result.ReplicatedTo, node.ID)
			}
		}

		if success {
			result.Success = true
			break
		}

		task.RetryCount++
	}

	result.Duration = time.Since(startTime)

	// Update metrics
	if w.metrics != nil {
		if result.Success {
			w.metrics.IncrementReplicationSuccess()
		} else {
			w.metrics.IncrementReplicationFailure()
		}
		w.metrics.ObserveReplicationLatency(result.Duration)
	}

	w.taskCount++
	w.logger.Debug("Processed replication task",
		zap.String("vector_id", task.Vector.ID),
		zap.Bool("success", result.Success),
		zap.Int("replicated_to_count", len(result.ReplicatedTo)),
		zap.Int("failed_nodes_count", len(result.FailedNodes)),
		zap.Duration("duration", result.Duration),
		zap.Int("retries", result.Retries))

	return result
}

// replicateToNode replicates a vector to a specific node
func (w *ReplicationWorker) replicateToNode(task *ReplicationTask, node *NodeInfo) error {
	// This is a placeholder for the actual replication logic
	// In a real implementation, this would send the vector to the node via gRPC or HTTP

	// Simulate network delay
	time.Sleep(10 * time.Millisecond)

	// Simulate occasional failures (10% failure rate)
	if time.Now().UnixNano()%10 == 0 {
		return fmt.Errorf("simulated network error")
	}

	w.logger.Debug("Replicated vector to node",
		zap.String("vector_id", task.Vector.ID),
		zap.String("node_id", node.ID))

	return nil
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(nodeID string, config *CircuitBreakerConfig, logger logging.Logger, metrics *metrics.IngestionMetrics) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}

	return &CircuitBreaker{
		nodeID: nodeID,
		state:  StateClosed,
		config: config,
	}
}

// AllowRequest checks if a request is allowed through the circuit breaker
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if timeout has elapsed
		if now.Sub(cb.lastFailure) > cb.config.Timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
			cb.failureCount = 0
			return true
		}
		return false
	case StateHalfOpen:
		// Allow limited requests in half-open state
		if cb.successCount+cb.failureCount < cb.config.HalfOpenMaxCalls {
			return true
		}
		return false
	default:
		return false
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastSuccess = now
	cb.successCount++

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failureCount = 0
	case StateHalfOpen:
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.state = StateClosed
			cb.failureCount = 0
		}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastFailure = now
	cb.failureCount++

	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		if cb.failureCount >= 1 {
			cb.state = StateOpen
		}
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.state
}

// GetFailureCount returns the current failure count
func (cb *CircuitBreaker) GetFailureCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.failureCount
}

// GetSuccessCount returns the current success count
func (cb *CircuitBreaker) GetSuccessCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.successCount
}
