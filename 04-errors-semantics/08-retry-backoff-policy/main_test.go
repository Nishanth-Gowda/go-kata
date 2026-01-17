package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Mock Timer for Deterministic Testing
// ============================================================================

// fakeTimer is a mock Timer that allows tests to control time.
type fakeTimer struct {
	c        chan time.Time
	active   atomic.Bool
	delays   []time.Duration // Track all delays (from creation and Reset)
	autoFire bool            // If true, immediately fires when Reset is called
}

func newFakeTimer() *fakeTimer {
	ft := &fakeTimer{
		c:        make(chan time.Time, 1),
		delays:   make([]time.Duration, 0),
		autoFire: false,
	}
	ft.active.Store(true)
	return ft
}

func newAutoFireTimer(d time.Duration) *fakeTimer {
	ft := &fakeTimer{
		c:        make(chan time.Time, 1),
		delays:   []time.Duration{d},
		autoFire: true,
	}
	ft.active.Store(true)
	// Auto-fire immediately
	ft.c <- time.Now()
	return ft
}

func (f *fakeTimer) C() <-chan time.Time {
	return f.c
}

func (f *fakeTimer) Reset(d time.Duration) {
	f.delays = append(f.delays, d)
	f.active.Store(true)
	if f.autoFire {
		select {
		case f.c <- time.Now():
		default:
		}
	}
}

func (f *fakeTimer) Stop() {
	f.active.Store(false)
	// Drain channel to prevent stale fires
	select {
	case <-f.c:
	default:
	}
}

func (f *fakeTimer) GetDelays() []time.Duration {
	return f.delays
}

// Fire simulates the timer firing.
func (f *fakeTimer) Fire() {
	if f.active.Load() {
		select {
		case f.c <- time.Now():
		default:
		}
	}
}

// ============================================================================
// Test Error Types
// ============================================================================

// httpError is a mock HTTP error with status code.
type httpError struct {
	code int
}

func (e *httpError) Error() string {
	return fmt.Sprintf("http error: %d", e.code)
}

func (e *httpError) StatusCode() int {
	return e.code
}

// timeoutError is a mock net.Error with timeout.
type timeoutError struct {
	timeout bool
}

func (e *timeoutError) Error() string {
	return "timeout error"
}

func (e *timeoutError) Timeout() bool {
	return e.timeout
}

func (e *timeoutError) Temporary() bool {
	return e.timeout
}

var _ net.Error = (*timeoutError)(nil)

// ============================================================================
// Tests for Idiomatic Constraints
// ============================================================================

// Test 1: Must use time.Timer and Reset it (timer reuse)
func TestRetryer_TimerReuse(t *testing.T) {
	var timerCreations int

	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			timerCreations++
			return fakeT
		},
	}

	callCount := 0
	fn := func(ctx context.Context) error {
		callCount++
		if callCount < 3 {
			// Fire timer immediately to proceed
			go fakeT.Fire()
			return ErrTransient
		}
		return nil
	}

	ctx := context.Background()
	err := r.Do(ctx, fn)

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	// Timer should be created only ONCE and then Reset for subsequent waits
	// With 3 attempts and 2 waits (after attempt 1 and 2), we should only have 1 timer creation
	if timerCreations != 1 {
		t.Errorf("expected 1 timer creation (timer reuse via Reset), got %d", timerCreations)
	}

	// Verify we actually had multiple attempts (proving Reset was used)
	if callCount != 3 {
		t.Errorf("expected 3 calls (proving timer was reset), got %d", callCount)
	}
}

// Test 2: Must wrap final error with context (attempt count) using %w
func TestRetryer_ErrorWrappingWithAttemptCount(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	originalErr := fmt.Errorf("service unavailable: %w", ErrTransient)

	fn := func(ctx context.Context) error {
		go fakeT.Fire() // Fire immediately
		return originalErr
	}

	ctx := context.Background()
	err := r.Do(ctx, fn)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Check that original error is wrapped (errors.Is should work)
	if !errors.Is(err, ErrTransient) {
		t.Errorf("expected error to wrap ErrTransient, got: %v", err)
	}

	// Check that attempt count is included in error message
	errMsg := err.Error()
	if !strings.Contains(errMsg, "attempt") {
		t.Errorf("expected error message to contain 'attempt', got: %s", errMsg)
	}
}

// Test 3: Must classify errors using errors.Is / errors.As
func TestRetryer_ErrorClassification_TransientSentinel(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		go fakeT.Fire()
		return ErrTransient
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	// Should have retried all 3 attempts for transient error
	if attempts != 3 {
		t.Errorf("expected 3 attempts for ErrTransient, got %d", attempts)
	}
}

func TestRetryer_ErrorClassification_NonTransient_NoRetry(t *testing.T) {
	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return newFakeTimer()
		},
	}

	attempts := 0
	permanentErr := errors.New("permanent database error")
	fn := func(ctx context.Context) error {
		attempts++
		return permanentErr
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	// Should NOT retry for non-transient error
	if attempts != 1 {
		t.Errorf("expected 1 attempt for non-transient error (no retry), got %d", attempts)
	}
}

func TestRetryer_ErrorClassification_HTTP429_Retries(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		go fakeT.Fire()
		return &httpError{code: http.StatusTooManyRequests}
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	if attempts != 3 {
		t.Errorf("expected 3 attempts for HTTP 429, got %d", attempts)
	}
}

func TestRetryer_ErrorClassification_HTTP503_Retries(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		go fakeT.Fire()
		return &httpError{code: http.StatusServiceUnavailable}
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	if attempts != 3 {
		t.Errorf("expected 3 attempts for HTTP 503, got %d", attempts)
	}
}

func TestRetryer_ErrorClassification_NetTimeout_Retries(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		go fakeT.Fire()
		return &timeoutError{timeout: true}
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	if attempts != 3 {
		t.Errorf("expected 3 attempts for net.Error timeout, got %d", attempts)
	}
}

// Test 4: Context cancellation stops IMMEDIATELY (not after sleep)
func TestRetryer_ContextCancellation_StopsImmediately(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   5,
		baseDelay:     1 * time.Hour, // Very long - we should NOT wait
		maxDelay:      10 * time.Hour,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			// Cancel context while waiting for retry
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()
		}
		return ErrTransient
	}

	start := time.Now()
	err := r.Do(ctx, fn)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	// Should have returned almost immediately, not waited for the hour-long delay
	if elapsed > 1*time.Second {
		t.Errorf("context cancellation took too long (%v), should be immediate", elapsed)
	}
}

// Test 5: Verify NO time.Sleep is used - tests run fast with fake timers
func TestRetryer_NoRealTimeSleep(t *testing.T) {
	fakeT := newFakeTimer()

	r := &Retryer{
		maxAttempts:   10,
		baseDelay:     1 * time.Hour, // Would take forever with real sleep
		maxDelay:      100 * time.Hour,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		go fakeT.Fire() // Immediately fire the timer
		if attempts < 10 {
			return ErrTransient
		}
		return nil
	}

	start := time.Now()
	ctx := context.Background()
	err := r.Do(ctx, fn)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	if attempts != 10 {
		t.Errorf("expected 10 attempts, got %d", attempts)
	}

	// Should complete near-instantly with fake timers
	if elapsed > 1*time.Second {
		t.Errorf("test took %v - likely using real time.Sleep instead of Timer", elapsed)
	}
}

// Test 6: Success on first attempt - no timer needed
func TestRetryer_SuccessOnFirstAttempt(t *testing.T) {
	timerCreated := false

	r := &Retryer{
		maxAttempts:   3,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			timerCreated = true
			return newFakeTimer()
		},
	}

	fn := func(ctx context.Context) error {
		return nil // Immediate success
	}

	ctx := context.Background()
	err := r.Do(ctx, fn)

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	// Timer should NOT be created if we succeed on first try
	if timerCreated {
		t.Error("timer should not be created for immediate success")
	}
}

// Test 7: Exponential backoff calculation
func TestRetryer_ExponentialBackoff(t *testing.T) {
	var fakeT *fakeTimer

	r := &Retryer{
		maxAttempts:   4,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      10 * time.Second,
		jitterPercent: 0, // No jitter for deterministic test
		timerFactory: func(d time.Duration) Timer {
			fakeT = newAutoFireTimer(d)
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		if attempts < 4 {
			return ErrTransient
		}
		return nil
	}

	ctx := context.Background()
	err := r.Do(ctx, fn)

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	delays := fakeT.GetDelays()

	// Expected: 100ms (2^0), 200ms (2^1), 400ms (2^2)
	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}

	if len(delays) != len(expected) {
		t.Fatalf("expected %d delays, got %d: %v", len(expected), len(delays), delays)
	}

	for i, exp := range expected {
		if delays[i] != exp {
			t.Errorf("delay[%d] = %v, expected %v", i, delays[i], exp)
		}
	}
}

// Test 8: Max delay cap
func TestRetryer_MaxDelayCap(t *testing.T) {
	var fakeT *fakeTimer

	r := &Retryer{
		maxAttempts:   6,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      500 * time.Millisecond, // Cap at 500ms
		jitterPercent: 0,
		timerFactory: func(d time.Duration) Timer {
			fakeT = newAutoFireTimer(d)
			return fakeT
		},
	}

	attempts := 0
	fn := func(ctx context.Context) error {
		attempts++
		return ErrTransient
	}

	ctx := context.Background()
	_ = r.Do(ctx, fn)

	delays := fakeT.GetDelays()

	// Check that no delay exceeds maxDelay
	for i, d := range delays {
		if d > 500*time.Millisecond {
			t.Errorf("delay[%d] = %v, exceeds maxDelay (500ms)", i, d)
		}
	}
}
