package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"
)

type Retryer struct {
	maxAttempts int
	jitter      time.Duration
	maxBackoff  time.Duration
	base        time.Duration
}

func NewRetryer(maxAttempts int, base time.Duration) *Retryer {
	return &Retryer{
		maxAttempts: maxAttempts,
		jitter:      base / 10,
		maxBackoff:  base * 2,
		base:        base,
	}
}

func (r *Retryer) Do(ctx context.Context, fn func(context.Context) error) error {
	var last_err error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		if err := fn(ctx); err != nil {
			last_err = fmt.Errorf("attempt %d: %w", attempt, err)
			if attempt < r.maxAttempts {
				backoff := r.base * (2 << attempt)
				if backoff > r.maxBackoff {
					backoff = r.maxBackoff
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff + time.Duration(rand.Int63n(int64(r.jitter)))):
					continue
				}
			}
		} else {
			// success
			return nil
		}
	}
	return last_err
}

func main() {
	ctx := context.Background()
	retryer := NewRetryer(3, 100*time.Millisecond)
	if err := retryer.Do(ctx, func(ctx context.Context) error {
		return nil
	}); err != nil {
		panic(err)
	}

	err := net.Error{}
	if err.Timeout() {

	}
}
