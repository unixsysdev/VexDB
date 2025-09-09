package search

import (
	"context"
	"testing"
	"time"
)

func TestNewSearchTimeout(t *testing.T) {
	defaultTimeout := 30 * time.Second
	timeout := NewSearchTimeout(defaultTimeout)
	
	if timeout == nil {
		t.Fatal("Expected timeout handler to be created, got nil")
	}
	
	if timeout.defaultTimeout != defaultTimeout {
		t.Errorf("Expected default timeout to be %v, got %v", defaultTimeout, timeout.defaultTimeout)
	}
}

func TestSearchTimeout_WithTimeout(t *testing.T) {
	timeout := NewSearchTimeout(1 * time.Second)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithTimeout(ctx)
	defer cancel()
	
	if ctxWithTimeout == nil {
		t.Fatal("Expected context with timeout to be created, got nil")
	}
	
	// Verify that the context has a deadline
	deadline, ok := ctxWithTimeout.Deadline()
	if !ok {
		t.Fatal("Expected context to have a deadline")
	}
	
	// Check that the deadline is approximately 1 second from now
	expectedDeadline := time.Now().Add(1 * time.Second)
	diff := deadline.Sub(expectedDeadline)
	if diff < -100*time.Millisecond || diff > 100*time.Millisecond {
		t.Errorf("Expected deadline to be approximately 1 second from now, got %v (diff: %v)", deadline, diff)
	}
}

func TestSearchTimeout_WithCustomTimeout(t *testing.T) {
	timeout := NewSearchTimeout(30 * time.Second)
	
	ctx := context.Background()
	customTimeout := 5 * time.Second
	ctxWithTimeout, cancel := timeout.WithCustomTimeout(ctx, customTimeout)
	defer cancel()
	
	if ctxWithTimeout == nil {
		t.Fatal("Expected context with custom timeout to be created, got nil")
	}
	
	// Verify that the context has a deadline
	deadline, ok := ctxWithTimeout.Deadline()
	if !ok {
		t.Fatal("Expected context to have a deadline")
	}
	
	// Check that the deadline is approximately 5 seconds from now
	expectedDeadline := time.Now().Add(5 * time.Second)
	diff := deadline.Sub(expectedDeadline)
	if diff < -100*time.Millisecond || diff > 100*time.Millisecond {
		t.Errorf("Expected deadline to be approximately 5 seconds from now, got %v (diff: %v)", deadline, diff)
	}
}

func TestSearchTimeout_GetDefaultTimeout(t *testing.T) {
	defaultTimeout := 45 * time.Second
	timeout := NewSearchTimeout(defaultTimeout)
	
	retrievedTimeout := timeout.GetDefaultTimeout()
	
	if retrievedTimeout != defaultTimeout {
		t.Errorf("Expected GetDefaultTimeout to return %v, got %v", defaultTimeout, retrievedTimeout)
	}
}

func TestSearchTimeout_SetDefaultTimeout(t *testing.T) {
	timeout := NewSearchTimeout(30 * time.Second)
	
	newTimeout := 60 * time.Second
	timeout.SetDefaultTimeout(newTimeout)
	
	retrievedTimeout := timeout.GetDefaultTimeout()
	
	if retrievedTimeout != newTimeout {
		t.Errorf("Expected GetDefaultTimeout to return %v after setting, got %v", newTimeout, retrievedTimeout)
	}
}

func TestSearchTimeout_ContextCancellation(t *testing.T) {
	timeout := NewSearchTimeout(100 * time.Millisecond)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithTimeout(ctx)
	defer cancel()
	
	// Wait for the context to be cancelled
	select {
	case <-ctxWithTimeout.Done():
		// Context was cancelled as expected
	case <-time.After(200 * time.Millisecond):
		t.Error("Expected context to be cancelled within 200ms, but it wasn't")
	}
	
	if ctxWithTimeout.Err() != context.DeadlineExceeded {
		t.Errorf("Expected context error to be DeadlineExceeded, got %v", ctxWithTimeout.Err())
	}
}

func TestSearchTimeout_ContextCancellationWithCustomTimeout(t *testing.T) {
	timeout := NewSearchTimeout(30 * time.Second)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithCustomTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	
	// Wait for the context to be cancelled
	select {
	case <-ctxWithTimeout.Done():
		// Context was cancelled as expected
	case <-time.After(150 * time.Millisecond):
		t.Error("Expected context to be cancelled within 150ms, but it wasn't")
	}
	
	if ctxWithTimeout.Err() != context.DeadlineExceeded {
		t.Errorf("Expected context error to be DeadlineExceeded, got %v", ctxWithTimeout.Err())
	}
}

func TestSearchTimeout_ManualCancellation(t *testing.T) {
	timeout := NewSearchTimeout(30 * time.Second)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithTimeout(ctx)
	
	// Cancel the context manually
	cancel()
	
	// Wait a bit to ensure cancellation is processed
	time.Sleep(10 * time.Millisecond)
	
	if ctxWithTimeout.Err() != context.Canceled {
		t.Errorf("Expected context error to be Canceled, got %v", ctxWithTimeout.Err())
	}
}

func TestSearchTimeout_NestedContexts(t *testing.T) {
	timeout := NewSearchTimeout(1 * time.Second)
	
	// Create a parent context with a shorter timeout
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer parentCancel()
	
	// Create a child context with a longer timeout
	childCtx, childCancel := timeout.WithCustomTimeout(parentCtx, 2*time.Second)
	defer childCancel()
	
	// The child context should be cancelled when the parent context expires
	select {
	case <-childCtx.Done():
		// Context was cancelled as expected
	case <-time.After(1 * time.Second):
		t.Error("Expected child context to be cancelled when parent expires, but it wasn't")
	}
	
	if childCtx.Err() != context.DeadlineExceeded {
		t.Errorf("Expected child context error to be DeadlineExceeded, got %v", childCtx.Err())
	}
}

func TestSearchTimeout_ConcurrentUsage(t *testing.T) {
	timeout := NewSearchTimeout(100 * time.Millisecond)
	
	// Test concurrent creation of contexts
	done := make(chan bool, 10)
	
	for i := 0; i < 10; i++ {
		go func() {
			ctx, cancel := timeout.WithTimeout(context.Background())
			defer cancel()
			
			select {
			case <-ctx.Done():
				done <- true
			case <-time.After(200 * time.Millisecond):
				done <- false
			}
		}()
	}
	
	// Wait for all goroutines to complete
	successCount := 0
	for i := 0; i < 10; i++ {
		if <-done {
			successCount++
		}
	}
	
	if successCount != 10 {
		t.Errorf("Expected all 10 contexts to be cancelled, got %d", successCount)
	}
}

func TestSearchTimeout_ZeroTimeout(t *testing.T) {
	timeout := NewSearchTimeout(0)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithTimeout(ctx)
	defer cancel()
	
	// Context with zero timeout should be cancelled immediately
	select {
	case <-ctxWithTimeout.Done():
		// Context was cancelled as expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected context with zero timeout to be cancelled immediately")
	}
	
	if ctxWithTimeout.Err() != context.DeadlineExceeded {
		t.Errorf("Expected context error to be DeadlineExceeded, got %v", ctxWithTimeout.Err())
	}
}

func TestSearchTimeout_VeryLongTimeout(t *testing.T) {
	timeout := NewSearchTimeout(24 * time.Hour)
	
	ctx := context.Background()
	ctxWithTimeout, cancel := timeout.WithTimeout(ctx)
	defer cancel()
	
	// Context should not be cancelled within a short time
	select {
	case <-ctxWithTimeout.Done():
		t.Error("Expected context with 24h timeout not to be cancelled within 100ms")
	case <-time.After(100 * time.Millisecond):
		// Context is still active as expected
	}
	
	if ctxWithTimeout.Err() != nil {
		t.Errorf("Expected context with long timeout to have no error yet, got %v", ctxWithTimeout.Err())
	}
}