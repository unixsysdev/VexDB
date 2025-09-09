package search

import (
	"context"
	"time"
)

// SearchTimeout provides timeout functionality for search operations
type SearchTimeout struct {
	defaultTimeout time.Duration
}

// NewSearchTimeout creates a new search timeout handler
func NewSearchTimeout(defaultTimeout time.Duration) *SearchTimeout {
	return &SearchTimeout{
		defaultTimeout: defaultTimeout,
	}
}

// WithTimeout creates a context with timeout for search operations
func (st *SearchTimeout) WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, st.defaultTimeout)
}

// WithCustomTimeout creates a context with custom timeout
func (st *SearchTimeout) WithCustomTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}

// GetDefaultTimeout returns the default timeout
func (st *SearchTimeout) GetDefaultTimeout() time.Duration {
	return st.defaultTimeout
}

// SetDefaultTimeout sets the default timeout
func (st *SearchTimeout) SetDefaultTimeout(timeout time.Duration) {
	st.defaultTimeout = timeout
}
	Cancelled, fmt.Errorf("query cancelled by user"))

	m.logger.Info("Cancelled query",
		zap.String("query_id", queryID))

	return true
}

// GetActiveQueries returns all active queries
func (m *TimeoutManager) GetActiveQueries() map[string]*QueryContext {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queries := make(map[string]*QueryContext)
	for id, queryCtx := range m.activeQueries {
		queries[id] = queryCtx
	}

	return queries
}

// GetQueryStats returns statistics about queries
func (m *TimeoutManager) GetQueryStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"total_queries": len(m.activeQueries),
		"status_counts": make(map[string]int),
	}

	statusCounts := stats["status_counts"].(map[string]int)

	for _, queryCtx := range m.activeQueries {
		queryCtx.mu.RLock()
		status := queryCtx.Status.String()
		statusCounts[status]++
		queryCtx.mu.RUnlock()
	}

	return stats
}

// runCleanup runs periodic cleanup of completed queries
func (m *TimeoutManager) runCleanup(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Cleanup stopped due to context cancellation")
			return
		case <-m.shutdown:
			m.logger.Info("Cleanup stopped due to shutdown")
			return
		case <-ticker.C:
			m.cleanupCompletedQueries()
		}
	}
}

// cleanupCompletedQueries removes completed queries from active tracking
func (m *TimeoutManager) cleanupCompletedQueries() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	completedQueries := make([]string, 0)

	for queryID, queryCtx := range m.activeQueries {
		queryCtx.mu.RLock()
		isCompleted := queryCtx.Status == StatusCompleted || 
			queryCtx.Status == StatusFailed || 
			queryCtx.Status == StatusCancelled || 
			queryCtx.Status == StatusTimeout
		
		// Remove queries that completed more than 5 minutes ago
		shouldRemove := isCompleted && now.Sub(queryCtx.CompletedAt) > 5*time.Minute
		queryCtx.mu.RUnlock()

		if shouldRemove {
			completedQueries = append(completedQueries, queryID)
		}
	}

	for _, queryID := range completedQueries {
		delete(m.activeQueries, queryID)
		m.logger.Debug("Cleaned up completed query",
			zap.String("query_id", queryID))
	}

	if len(completedQueries) > 0 {
		m.logger.Debug("Cleaned up completed queries",
			zap.Int("count", len(completedQueries)))
	}
}

// runHeartbeatMonitoring monitors query heartbeats
func (m *TimeoutManager) runHeartbeatMonitoring(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(DefaultTimeoutConfig().HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Heartbeat monitoring stopped due to context cancellation")
			return
		case <-m.shutdown:
			m.logger.Info("Heartbeat monitoring stopped due to shutdown")
			return
		case <-ticker.C:
			m.monitorQueryHeartbeats()
		}
	}
}

// monitorQueryHeartbeats checks for queries that have exceeded their timeout
func (m *TimeoutManager) monitorQueryHeartbeats() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	timeoutQueries := make([]string, 0)

	for queryID, queryCtx := range m.activeQueries {
		queryCtx.mu.RLock()
		isRunning := queryCtx.Status == StatusRunning
		elapsed := now.Sub(queryCtx.StartTime)
		isTimeout := elapsed > queryCtx.Timeout
		queryCtx.mu.RUnlock()

		if isRunning && isTimeout {
			timeoutQueries = append(timeoutQueries, queryID)
		}
	}

	for _, queryID := range timeoutQueries {
		m.logger.Warn("Query timeout detected",
			zap.String("query_id", queryID))
		
		// Cancel the query and update status
		if queryCtx, exists := m.activeQueries[queryID]; exists {
			queryCtx.Cancel()
			m.UpdateQueryStatus(queryID, StatusTimeout, fmt.Errorf("query timeout after %v", queryCtx.Timeout))
		}
	}
}

// runProgressMonitoring monitors query progress
func (m *TimeoutManager) runProgressMonitoring(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(DefaultTimeoutConfig().ProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Progress monitoring stopped due to context cancellation")
			return
		case <-m.shutdown:
			m.logger.Info("Progress monitoring stopped due to shutdown")
			return
		case <-ticker.C:
			m.monitorQueryProgress()
		}
	}
}

// monitorQueryProgress logs progress of running queries
func (m *TimeoutManager) monitorQueryProgress() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	runningQueries := make([]string, 0)

	for queryID, queryCtx := range m.activeQueries {
		queryCtx.mu.RLock()
		isRunning := queryCtx.Status == StatusRunning
		elapsed := now.Sub(queryCtx.StartTime)
		progress := queryCtx.Progress
		queryCtx.mu.RUnlock()

		if isRunning {
			runningQueries = append(runningQueries, queryID)
			m.logger.Debug("Query progress",
				zap.String("query_id", queryID),
				zap.Duration("elapsed", elapsed),
				zap.Float64("progress", progress))
		}
	}

	if len(runningQueries) > 0 {
		m.logger.Debug("Progress monitoring",
			zap.Int("running_queries", len(runningQueries)))
	}
}

// WaitForCompletion waits for a query to complete or timeout
func (m *TimeoutManager) WaitForCompletion(queryID string, timeout time.Duration) (*QueryContext, error) {
	if timeout <= 0 {
		timeout = DefaultTimeoutConfig().DefaultQueryTimeout
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return nil, fmt.Errorf("wait for query completion timeout")
		case <-ticker.C:
			if queryCtx, exists := m.GetQueryContext(queryID); exists {
				queryCtx.mu.RLock()
				status := queryCtx.Status
				queryCtx.mu.RUnlock()

				if status == StatusCompleted || status == StatusFailed || status == StatusCancelled || status == StatusTimeout {
					return queryCtx, nil
				}
			} else {
				return nil, fmt.Errorf("query context not found")
			}
		}
	}
}

// CreateChildContext creates a child context with node-specific timeout
func (m *TimeoutManager) CreateChildContext(parentCtx *QueryContext, nodeID string) (context.Context, context.CancelFunc) {
	nodeTimeout := DefaultTimeoutConfig().NodeTimeout
	if nodeTimeout <= 0 {
		nodeTimeout = parentCtx.Timeout
	}

	return context.WithTimeout(parentCtx.Ctx, nodeTimeout)
}

// ExtendTimeout extends the timeout for a query
func (m *TimeoutManager) ExtendTimeout(queryID string, additionalTimeout time.Duration) error {
	m.mu.RLock()
	queryCtx, exists := m.activeQueries[queryID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("query context not found")
	}

	queryCtx.mu.Lock()
	defer queryCtx.mu.Unlock()

	// Cancel the old context
	queryCtx.Cancel()

	// Create new context with extended timeout
	newTimeout := queryCtx.Timeout + additionalTimeout
	ctx, cancel := context.WithTimeout(context.Background(), newTimeout)

	queryCtx.Ctx = ctx
	queryCtx.Cancel = cancel
	queryCtx.Timeout = newTimeout

	m.logger.Info("Extended query timeout",
		zap.String("query_id", queryID),
		zap.Duration("old_timeout", queryCtx.Timeout-additionalTimeout),
		zap.Duration("new_timeout", newTimeout))

	return nil
}

// GetQueryDuration returns the duration of a query
func (m *TimeoutManager) GetQueryDuration(queryID string) (time.Duration, error) {
	m.mu.RLock()
	queryCtx, exists := m.activeQueries[queryID]
	m.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("query context not found")
	}

	queryCtx.mu.RLock()
	defer queryCtx.mu.RUnlock()

	if queryCtx.Status == StatusCompleted || 
	   queryCtx.Status == StatusFailed || 
	   queryCtx.Status == StatusCancelled || 
	   queryCtx.Status == StatusTimeout {
		return queryCtx.CompletedAt.Sub(queryCtx.StartTime), nil
	}

	return time.Since(queryCtx.StartTime), nil
}

// IsQueryActive checks if a query is still active
func (m *TimeoutManager) IsQueryActive(queryID string) bool {
	m.mu.RLock()
	queryCtx, exists := m.activeQueries[queryID]
	m.mu.RUnlock()

	if !exists {
		return false
	}

	queryCtx.mu.RLock()
	defer queryCtx.mu.RUnlock()

	return queryCtx.Status == StatusPending || queryCtx.Status == StatusRunning
}

// GetQueryError returns the error associated with a query
func (m *TimeoutManager) GetQueryError(queryID string) error {
	m.mu.RLock()
	queryCtx, exists := m.activeQueries[queryID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("query context not found")
	}

	queryCtx.mu.RLock()
	defer queryCtx.mu.RUnlock()

	return queryCtx.Error
}