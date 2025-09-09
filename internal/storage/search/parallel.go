package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrInvalidParallelSearch = errors.New("invalid parallel search")
	ErrSearchTimeout        = errors.New("search timeout")
	ErrSearchCancelled      = errors.New("search cancelled")
	ErrWorkerPoolExhausted  = errors.New("worker pool exhausted")
	ErrResultCollection     = errors.New("result collection failed")
	ErrTaskSubmission       = errors.New("task submission failed")
)

// ParallelSearchConfig represents the parallel search configuration
type ParallelSearchConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	WorkerCount       int           `yaml:"worker_count" json:"worker_count"`
	QueueSize         int           `yaml:"queue_size" json:"queue_size"`
	BatchSize         int           `yaml:"batch_size" json:"batch_size"`
	Timeout           time.Duration `yaml:"timeout" json:"timeout"`
	EnableLoadBalance bool          `yaml:"enable_load_balance" json:"enable_load_balance"`
	EnableBackoff     bool          `yaml:"enable_backoff" json:"enable_backoff"`
	BackoffFactor     float64       `yaml:"backoff_factor" json:"backoff_factor"`
	MaxRetries        int           `yaml:"max_retries" json:"max_retries"`
	EnableValidation  bool          `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultParallelSearchConfig returns the default parallel search configuration
func DefaultParallelSearchConfig() *ParallelSearchConfig {
	return &ParallelSearchConfig{
		Enabled:           true,
		WorkerCount:       4,
		QueueSize:         100,
		BatchSize:         10,
		Timeout:           30 * time.Second,
		EnableLoadBalance: true,
		EnableBackoff:     true,
		BackoffFactor:     2.0,
		MaxRetries:        3,
		EnableValidation:  true,
	}
}

// SearchTask represents a search task
type SearchTask struct {
	ID        string        `json:"id"`
	Query     *SearchQuery  `json:"query"`
	Vectors   []*types.Vector `json:"vectors"`
	Context   context.Context `json:"-"`
	Result    chan *SearchResult `json:"-"`
	Error     chan error    `json:"-"`
	Retries   int           `json:"retries"`
	Priority  int           `json:"priority"`
	CreatedAt time.Time     `json:"created_at"`
	StartedAt time.Time     `json:"started_at"`
}

// ParallelSearchStats represents parallel search statistics
type ParallelSearchStats struct {
	TotalTasks        int64         `json:"total_tasks"`
	CompletedTasks    int64         `json:"completed_tasks"`
	FailedTasks       int64         `json:"failed_tasks"`
	TimeoutTasks      int64         `json:"timeout_tasks"`
	CancelledTasks    int64         `json:"cancelled_tasks"`
	RetriedTasks      int64         `json:"retried_tasks"`
	AverageLatency    time.Duration `json:"average_latency"`
	QueueSize         int           `json:"queue_size"`
	ActiveWorkers     int           `json:"active_workers"`
	LastTaskAt        time.Time     `json:"last_task_at"`
	FailureCount      int64         `json:"failure_count"`
}

// ParallelSearch represents a parallel search processor
type ParallelSearch struct {
	config         *ParallelSearchConfig
	logger         logging.Logger
	metrics        *metrics.StorageMetrics
	searchEngine   *SearchEngine
	
	// Worker pool
	taskQueue      chan *SearchTask
	workers        []*Worker
	workerWg       sync.WaitGroup
	
	// Statistics
	stats          *ParallelSearchStats
	statsMu        sync.RWMutex
	
	// Lifecycle
	started        bool
	stopped        bool
	mu             sync.RWMutex
}

// Worker represents a search worker
type Worker struct {
	ID           int
	Parallel     *ParallelSearch
	Task         *SearchTask
	Active       bool
	LastTaskAt   time.Time
	TaskCount    int64
	ErrorCount   int64
}

// NewParallelSearch creates a new parallel search processor
func NewParallelSearch(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics, searchEngine *SearchEngine) (*ParallelSearch, error) {
	parallelConfig := DefaultParallelSearchConfig()
	
	if cfg != nil {
		if parallelCfg, ok := cfg.Get("parallel_search"); ok {
			if cfgMap, ok := parallelCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					parallelConfig.Enabled = enabled
				}
				if workerCount, ok := cfgMap["worker_count"].(int); ok {
					parallelConfig.WorkerCount = workerCount
				}
				if queueSize, ok := cfgMap["queue_size"].(int); ok {
					parallelConfig.QueueSize = queueSize
				}
				if batchSize, ok := cfgMap["batch_size"].(int); ok {
					parallelConfig.BatchSize = batchSize
				}
				if timeout, ok := cfgMap["timeout"].(string); ok {
					if timeoutDur, err := time.ParseDuration(timeout); err == nil {
						parallelConfig.Timeout = timeoutDur
					}
				}
				if enableLoadBalance, ok := cfgMap["enable_load_balance"].(bool); ok {
					parallelConfig.EnableLoadBalance = enableLoadBalance
				}
				if enableBackoff, ok := cfgMap["enable_backoff"].(bool); ok {
					parallelConfig.EnableBackoff = enableBackoff
				}
				if backoffFactor, ok := cfgMap["backoff_factor"].(float64); ok {
					parallelConfig.BackoffFactor = backoffFactor
				}
				if maxRetries, ok := cfgMap["max_retries"].(int); ok {
					parallelConfig.MaxRetries = maxRetries
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					parallelConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateParallelSearchConfig(parallelConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidParallelSearch, err)
	}
	
	parallel := &ParallelSearch{
		config:       parallelConfig,
		logger:       logger,
		metrics:      metrics,
		searchEngine: searchEngine,
		taskQueue:    make(chan *SearchTask, parallelConfig.QueueSize),
		stats: &ParallelSearchStats{
			QueueSize: parallelConfig.QueueSize,
		},
	}
	
	// Initialize workers
	if err := parallel.initializeWorkers(); err != nil {
		return nil, fmt.Errorf("failed to initialize workers: %w", err)
	}
	
	parallel.logger.Info("Created parallel search processor",
		"enabled", parallelConfig.Enabled,
		"worker_count", parallelConfig.WorkerCount,
		"queue_size", parallelConfig.QueueSize,
		"batch_size", parallelConfig.BatchSize,
		"timeout", parallelConfig.Timeout,
		"enable_load_balance", parallelConfig.EnableLoadBalance,
		"enable_backoff", parallelConfig.EnableBackoff,
		"backoff_factor", parallelConfig.BackoffFactor,
		"max_retries", parallelConfig.MaxRetries)
	
	return parallel, nil
}

// validateParallelSearchConfig validates the parallel search configuration
func validateParallelSearchConfig(cfg *ParallelSearchConfig) error {
	if cfg.WorkerCount <= 0 {
		return errors.New("worker count must be positive")
	}
	
	if cfg.QueueSize <= 0 {
		return errors.New("queue size must be positive")
	}
	
	if cfg.BatchSize <= 0 {
		return errors.New("batch size must be positive")
	}
	
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	
	if cfg.BackoffFactor <= 1.0 {
		return errors.New("backoff factor must be greater than 1.0")
	}
	
	if cfg.MaxRetries < 0 {
		return errors.New("max retries must be non-negative")
	}
	
	return nil
}

// initializeWorkers initializes search workers
func (p *ParallelSearch) initializeWorkers() error {
	p.workers = make([]*Worker, p.config.WorkerCount)
	
	for i := 0; i < p.config.WorkerCount; i++ {
		worker := &Worker{
			ID:       i,
			Parallel: p,
		}
		p.workers = append(p.workers, worker)
	}
	
	return nil
}

// Start starts the parallel search processor
func (p *ParallelSearch) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.started {
		return nil
	}
	
	// Start workers
	for _, worker := range p.workers {
		p.workerWg.Add(1)
		go worker.run()
	}
	
	p.started = true
	p.stopped = false
	
	p.logger.Info("Started parallel search processor", "worker_count", len(p.workers))
	
	return nil
}

// Stop stops the parallel search processor
func (p *ParallelSearch) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.stopped {
		return nil
	}
	
	// Close task queue
	close(p.taskQueue)
	
	// Wait for workers to finish
	p.workerWg.Wait()
	
	p.stopped = true
	p.started = false
	
	p.logger.Info("Stopped parallel search processor")
	
	return nil
}

// IsRunning checks if the parallel search processor is running
func (p *ParallelSearch) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	return p.started && !p.stopped
}

// Search performs a parallel search
func (p *ParallelSearch) Search(ctx context.Context, query *SearchQuery, vectors []*types.Vector) ([]*SearchResult, error) {
	if !p.config.Enabled {
		// Fallback to sequential search
		return p.searchEngine.Search(ctx, query)
	}
	
	if p.config.EnableValidation {
		if err := query.Validate(); err != nil {
			p.metrics.Errors.Inc("parallel_search", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidParallelSearch, err)
		}
	}
	
	start := time.Now()
	
	// Create search task
	task := &SearchTask{
		ID:        generateTaskID(),
		Query:     query,
		Vectors:   vectors,
		Context:   ctx,
		Result:    make(chan *SearchResult, 1),
		Error:     make(chan error, 1),
		Priority:  0,
		CreatedAt: time.Now(),
	}
	
	// Apply timeout if set
	if p.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.Timeout)
		defer cancel()
		task.Context = ctx
	}
	
	// Submit task
	if err := p.submitTask(task); err != nil {
		p.metrics.Errors.Inc("parallel_search", "task_submission_failed")
		return nil, fmt.Errorf("%w: %v", ErrTaskSubmission, err)
	}
	
	// Wait for result
	select {
	case result := <-task.Result:
		duration := time.Since(start)
		
		// Update metrics
		p.metrics.ParallelSearchOperations.Inc("parallel_search", "search")
		p.metrics.ParallelSearchLatency.Observe(duration.Seconds())
		
		// Update statistics
		p.updateStats(func(stats *ParallelSearchStats) {
			stats.CompletedTasks++
			stats.AverageLatency = time.Duration(
				(int64(stats.AverageLatency)*stats.CompletedTasks + int64(duration)) / (stats.CompletedTasks + 1),
			)
			stats.LastTaskAt = time.Now()
		})
		
		return []*SearchResult{result}, nil
		
	case err := <-task.Error:
		duration := time.Since(start)
		
		// Update metrics
		p.metrics.Errors.Inc("parallel_search", "search_failed")
		
		// Update statistics
		p.updateStats(func(stats *ParallelSearchStats) {
			stats.FailedTasks++
			stats.FailureCount++
		})
		
		return nil, fmt.Errorf("parallel search failed: %w", err)
		
	case <-ctx.Done():
		duration := time.Since(start)
		
		// Update metrics
		p.metrics.Errors.Inc("parallel_search", "search_timeout")
		
		// Update statistics
		p.updateStats(func(stats *ParallelSearchStats) {
			stats.TimeoutTasks++
		})
		
		return nil, fmt.Errorf("%w: %v", ErrSearchTimeout, ctx.Err())
	}
}

// BatchSearch performs parallel batch search
func (p *ParallelSearch) BatchSearch(ctx context.Context, queries []*SearchQuery, vectorsList [][]*types.Vector) ([][]*SearchResult, error) {
	if !p.config.Enabled {
		// Fallback to sequential batch search
		return p.searchEngine.BatchSearch(ctx, queries)
	}
	
	if len(queries) != len(vectorsList) {
		return nil, fmt.Errorf("%w: queries and vectors list length mismatch", ErrInvalidParallelSearch)
	}
	
	if p.config.EnableValidation {
		for i, query := range queries {
			if err := query.Validate(); err != nil {
				p.metrics.Errors.Inc("parallel_search", "validation_failed")
				return nil, fmt.Errorf("%w: query %d: %v", ErrInvalidParallelSearch, i, err)
			}
		}
	}
	
	start := time.Now()
	
	// Create search tasks
	tasks := make([]*SearchTask, len(queries))
	for i, query := range queries {
		task := &SearchTask{
			ID:        generateTaskID(),
			Query:     query,
			Vectors:   vectorsList[i],
			Context:   ctx,
			Result:    make(chan *SearchResult, 1),
			Error:     make(chan error, 1),
			Priority:  i,
			CreatedAt: time.Now(),
		}
		
		// Apply timeout if set
		if p.config.Timeout > 0 {
			taskCtx, cancel := context.WithTimeout(ctx, p.config.Timeout)
			task.Context = taskCtx
			defer cancel()
		}
		
		tasks[i] = task
	}
	
	// Submit tasks
	for _, task := range tasks {
		if err := p.submitTask(task); err != nil {
			p.metrics.Errors.Inc("parallel_search", "task_submission_failed")
			return nil, fmt.Errorf("%w: %v", ErrTaskSubmission, err)
		}
	}
	
	// Collect results
	results := make([][]*SearchResult, len(queries))
	errors := make([]error, len(queries))
	
	for i, task := range tasks {
		select {
		case result := <-task.Result:
			results[i] = []*SearchResult{result}
			
		case err := <-task.Error:
			errors[i] = err
			
		case <-ctx.Done():
			errors[i] = fmt.Errorf("%w: %v", ErrSearchTimeout, ctx.Err())
		}
	}
	
	duration := time.Since(start)
	
	// Update metrics
	p.metrics.ParallelSearchOperations.Inc("parallel_search", "batch_search")
	p.metrics.ParallelSearchLatency.Observe(duration.Seconds())
	
	// Check for errors
	for i, err := range errors {
		if err != nil {
			p.metrics.Errors.Inc("parallel_search", "batch_search_failed")
			return nil, fmt.Errorf("batch search failed at task %d: %w", i, err)
		}
	}
	
	return results, nil
}

// submitTask submits a search task to the worker pool
func (p *ParallelSearch) submitTask(task *SearchTask) error {
	select {
	case p.taskQueue <- task:
		// Update statistics
		p.updateStats(func(stats *ParallelSearchStats) {
			stats.TotalTasks++
		})
		return nil
	default:
		return ErrWorkerPoolExhausted
	}
}

// updateStats updates statistics atomically
func (p *ParallelSearch) updateStats(update func(*ParallelSearchStats)) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	
	update(p.stats)
}

// GetStats returns parallel search statistics
func (p *ParallelSearch) GetStats() *ParallelSearchStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	
	// Update active workers count
	activeWorkers := 0
	for _, worker := range p.workers {
		if worker.Active {
			activeWorkers++
		}
	}
	
	// Return a copy of stats
	stats := *p.stats
	stats.ActiveWorkers = activeWorkers
	stats.QueueSize = len(p.taskQueue)
	
	return &stats
}

// GetConfig returns the parallel search configuration
func (p *ParallelSearch) GetConfig() *ParallelSearchConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	// Return a copy of config
	config := *p.config
	return &config
}

// UpdateConfig updates the parallel search configuration
func (p *ParallelSearch) UpdateConfig(config *ParallelSearchConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Validate new configuration
	if err := validateParallelSearchConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidParallelSearch, err)
	}
	
	// Handle worker count changes
	if config.WorkerCount != p.config.WorkerCount {
		if err := p.updateWorkerCount(config.WorkerCount); err != nil {
			return fmt.Errorf("failed to update worker count: %w", err)
		}
	}
	
	// Handle queue size changes
	if config.QueueSize != p.config.QueueSize {
		if err := p.updateQueueSize(config.QueueSize); err != nil {
			return fmt.Errorf("failed to update queue size: %w", err)
		}
	}
	
	p.config = config
	
	p.logger.Info("Updated parallel search configuration", "config", config)
	
	return nil
}

// updateWorkerCount updates the number of workers
func (p *ParallelSearch) updateWorkerCount(newCount int) error {
	if p.started {
		return errors.New("cannot update worker count while running")
	}
	
	return p.initializeWorkers()
}

// updateQueueSize updates the task queue size
func (p *ParallelSearch) updateQueueSize(newSize int) error {
	if p.started {
		return errors.New("cannot update queue size while running")
	}
	
	p.taskQueue = make(chan *SearchTask, newSize)
	
	return nil
}

// GetWorkerStats returns worker statistics
func (p *ParallelSearch) GetWorkerStats() []map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	stats := make([]map[string]interface{}, len(p.workers))
	for i, worker := range p.workers {
		workerStats := map[string]interface{}{
			"id":          worker.ID,
			"active":      worker.Active,
			"last_task_at": worker.LastTaskAt,
			"task_count":  worker.TaskCount,
			"error_count": worker.ErrorCount,
		}
		stats[i] = workerStats
	}
	
	return stats
}

// Validate validates the parallel search processor state
func (p *ParallelSearch) Validate() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	// Validate configuration
	if err := validateParallelSearchConfig(p.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate search engine
	if p.searchEngine == nil {
		return errors.New("search engine is nil")
	}
	
	return nil
}

// run is the main worker loop
func (w *Worker) run() {
	defer w.Parallel.workerWg.Done()
	
	for task := range w.Parallel.taskQueue {
		w.processTask(task)
	}
}

// processTask processes a search task
func (w *Worker) processTask(task *SearchTask) {
	w.Active = true
	w.Task = task
	task.StartedAt = time.Now()
	
	defer func() {
		w.Active = false
		w.LastTaskAt = time.Now()
		w.TaskCount++
	}()
	
	// Execute search with retry logic
	var result *SearchResult
	var err error
	
	for retry := 0; retry <= w.Parallel.config.MaxRetries; retry++ {
		task.Retries = retry
		
		// Perform search
		result, err = w.performSearch(task)
		if err == nil {
			break
		}
		
		// Handle retry
		if retry < w.Parallel.config.MaxRetries && w.Parallel.config.EnableBackoff {
			backoffDuration := w.calculateBackoff(retry)
			select {
			case <-time.After(backoffDuration):
				continue
			case <-task.Context.Done():
				err = fmt.Errorf("%w: %v", ErrSearchCancelled, task.Context.Err())
				break
			}
		}
	}
	
	// Send result
	if err != nil {
		w.ErrorCount++
		task.Error <- err
	} else {
		task.Result <- result
	}
}

// performSearch performs the actual search
func (w *Worker) performSearch(task *SearchTask) (*SearchResult, error) {
	// Create a copy of the query for this worker
	query := &SearchQuery{
		Vector:        task.Query.Vector,
		Limit:         task.Query.Limit,
		Metric:        task.Query.Metric,
		MetadataFilter: task.Query.MetadataFilter,
		IncludeVector:  task.Query.IncludeVector,
		IncludeMetadata: task.Query.IncludeMetadata,
	}
	
	// Perform search using the search engine
	results, err := w.Parallel.searchEngine.Search(task.Context, query)
	if err != nil {
		return nil, err
	}
	
	if len(results) == 0 {
		return nil, errors.New("no results found")
	}
	
	// Return the best result
	return results[0], nil
}

// calculateBackoff calculates backoff duration
func (w *Worker) calculateBackoff(retry int) time.Duration {
	baseDelay := time.Second
	backoff := baseDelay * time.Duration(w.Parallel.config.BackoffFactor*float64(retry))
	
	// Cap the backoff to a reasonable maximum
	maxBackoff := 30 * time.Second
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	
	return backoff
}

// generateTaskID generates a unique task ID
func generateTaskID() string {
	return fmt.Sprintf("task_%d", time.Now().UnixNano())
}