package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"time"
)

// Sentinel error for explicit transient failures.
var ErrTransient = errors.New("transient failure")

type Retryer struct {
	maxAttempts   int
	baseDelay     time.Duration
	maxDelay      time.Duration
	jitterPercent float64

	// timerFactory allows injecting fake timers for deterministic testing.
	timerFactory func(d time.Duration) Timer
}

type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration)
	Stop()
}

// realTimer wraps the standard time.Timer.
type realTimer struct {
	t *time.Timer
}

func (r *realTimer) C() <-chan time.Time {
	return r.t.C
}

func (r *realTimer) Reset(d time.Duration) {
	r.t.Reset(d)
}

func (r *realTimer) Stop() {
	r.t.Stop()
}

func NewRetryer(maxAttempts int, base time.Duration) *Retryer {
	return &Retryer{
		maxAttempts:   maxAttempts,
		baseDelay:     base,
		maxDelay:      base * 32, // Cap at some reasonable multiple
		jitterPercent: 0.1,       // 10% jitter
		timerFactory: func(d time.Duration) Timer {
			return &realTimer{t: time.NewTimer(d)}
		},
	}
}
func (r *Retryer) Do(ctx context.Context, fn func(context.Context) error) error {
	var last_err error
	var timer Timer

	// Ensure timer is cleaned up if function exits early
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for attempt := 0; attempt < r.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		if !r.isTransient(err) {
			return last_err
		}

		last_err = fmt.Errorf("attempt %d: %w", attempt+1, err)
		if attempt == r.maxAttempts-1 {
			return last_err
		}

		wait_duration := r.calculateBackoff(attempt)

		if timer == nil {
			timer = r.timerFactory(wait_duration)
		} else {
			timer.Stop()
			select {
			case <-timer.C():
			default:
			}
			timer.Reset(wait_duration)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C():
			continue
		}

	}
	return last_err
}

func (r *Retryer) isTransient(err error) bool {
	if errors.Is(err, ErrTransient) {
		return true
	}

	// B. HTTP Check (429/503)
	// We use an interface to avoid depending on a specific struct implementation
	type statusCoder interface {
		StatusCode() int
	}
	var sc statusCoder
	if errors.As(err, &sc) {
		if sc.StatusCode() == http.StatusTooManyRequests || sc.StatusCode() == http.StatusServiceUnavailable {
			return true
		}
	}

	// C. Net Error (Timeout)
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}

	return false
}

func (r *Retryer) calculateBackoff(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt))
	backoff := float64(r.baseDelay) * exp

	if backoff > float64(r.maxDelay) {
		backoff = float64(r.maxDelay)
	}

	// Apply Jitter: +/- jitterPercent
	// Note: In real tests, seed math/rand or inject a source to make this deterministic.
	jitter := (rand.Float64()*2 - 1) * r.jitterPercent * backoff
	backoff += jitter

	return time.Duration(backoff)
}
