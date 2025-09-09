package search

import (
	"context"
	"time"
)

// SearchTimeout provides timeout functionality for search operations.
type SearchTimeout struct {
	defaultTimeout time.Duration
}

// NewSearchTimeout creates a new search timeout handler.
func NewSearchTimeout(defaultTimeout time.Duration) *SearchTimeout {
	return &SearchTimeout{defaultTimeout: defaultTimeout}
}

// WithTimeout creates a context with the default timeout for search operations.
func (st *SearchTimeout) WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, st.defaultTimeout)
}

// WithCustomTimeout creates a context with a custom timeout.
func (st *SearchTimeout) WithCustomTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}

// GetDefaultTimeout returns the default timeout.
func (st *SearchTimeout) GetDefaultTimeout() time.Duration {
	return st.defaultTimeout
}

// SetDefaultTimeout sets the default timeout.
func (st *SearchTimeout) SetDefaultTimeout(timeout time.Duration) {
	st.defaultTimeout = timeout
}
