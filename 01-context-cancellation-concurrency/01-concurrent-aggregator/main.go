package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

type ProfileService struct {
	timeout time.Duration
	err     error
}

type OrderService struct {
	timeout time.Duration
	err     error
}

func (ps *ProfileService) Fetch(ctx context.Context, id int) (string, error) {
	select {
	case <-time.After(ps.timeout):
		if ps.err != nil {
			return "", ps.err
		}
		return "Alice", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (os *OrderService) Fetch(ctx context.Context, id int) (string, error) {
	select {
	case <-time.After(os.timeout):
		if os.err != nil {
			return "", os.err
		}
		return "5", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

type UserAggregator struct {
	profileService *ProfileService
	orderService   *OrderService
	timeout        time.Duration
	logger         *slog.Logger
}

type UserAggregatorOption func(*UserAggregator)

// WithTimeout sets the timeout for aggregation operations
func WithTimeout(duration time.Duration) UserAggregatorOption {
	return func(ua *UserAggregator) {
		ua.timeout = duration
	}
}

// WithLogger sets a custom logger for the aggregator
func WithLogger(logger *slog.Logger) UserAggregatorOption {
	return func(ua *UserAggregator) {
		ua.logger = logger
	}
}

// NewUserAggregator creates a new UserAggregator with the given options
func NewUserAggregator(profileSvc *ProfileService, orderSvc *OrderService, opts ...UserAggregatorOption) *UserAggregator {
	ua := &UserAggregator{
		profileService: profileSvc,
		orderService:   orderSvc,
		timeout:        2 * time.Second,
		logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	// Apply functional options
	for _, opt := range opts {
		opt(ua)
	}

	return ua
}

func (ua *UserAggregator) Aggregate(ctx context.Context, id int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, ua.timeout)
	defer cancel()

	ua.logger.Info("starting aggregation", "user_id", id, "timeout", ua.timeout)
	g, ctx := errgroup.WithContext(ctx)

	var profileData, orderData string

	// Fetch profile data in a goroutine
	g.Go(func() error {
		ua.logger.Info("fetching profile data", "user_id", id)
		data, err := ua.profileService.Fetch(ctx, id)
		if err != nil {
			ua.logger.Error("profile fetch failed", "error", err)
			return fmt.Errorf("profile service: %w", err)
		}
		profileData = data
		ua.logger.Info("profile fetch succeeded", "data", data)
		return nil
	})

	// Fetch order data in a goroutine
	g.Go(func() error {
		ua.logger.Info("fetching order data", "user_id", id)
		data, err := ua.orderService.Fetch(ctx, id)
		if err != nil {
			ua.logger.Error("order fetch failed", "error", err)
			return fmt.Errorf("order service: %w", err)
		}
		orderData = data
		ua.logger.Info("order fetch succeeded", "data", data)
		return nil
	})

	// Wait for all goroutines to complete or first error
	if err := g.Wait(); err != nil {
		ua.logger.Error("aggregation failed", "error", err)
		return "", err
	}

	// Combine results
	result := fmt.Sprintf("User: %s | Orders: %s", profileData, orderData)
	ua.logger.Info("aggregation completed", "result", result)
	return result, nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	fmt.Println("\n=== Test 1: Success Case ===")
	mockSuccessCase(logger)

	fmt.Println("\n=== Test 2: Slow Poke (Timeout) ===")
	mockSlowService(logger)

	fmt.Println("\n=== Test 3: Domino Effect (Immediate Failure) ===")
	mockDominoEffect(logger)
}

func mockSuccessCase(logger *slog.Logger) {
	profileSvc := &ProfileService{timeout: 100 * time.Millisecond}
	orderSvc := &OrderService{timeout: 100 * time.Millisecond}

	aggregator := NewUserAggregator(
		profileSvc,
		orderSvc,
		WithTimeout(2*time.Second),
		WithLogger(logger),
	)

	start := time.Now()
	result, err := aggregator.Aggregate(context.Background(), 1)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("FAILED: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: %s (took %v)\n", result, elapsed)
	}
}

func mockSlowService(logger *slog.Logger) {
	profileSvc := &ProfileService{timeout: 2 * time.Second}
	orderSvc := &OrderService{timeout: 100 * time.Millisecond}

	aggregator := NewUserAggregator(
		profileSvc,
		orderSvc,
		WithTimeout(1*time.Second), // Short timeout
		WithLogger(logger),
	)

	start := time.Now()
	result, err := aggregator.Aggregate(context.Background(), 1)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("PASSED: Got expected timeout error after %v: %v\n", elapsed, err)
		if elapsed < 1100*time.Millisecond && elapsed > 900*time.Millisecond {
			fmt.Printf("TIMING CORRECT: Returned around 1s as expected\n")
		} else {
			fmt.Printf("WARNING: Timing was %v, expected ~1s\n", elapsed)
		}
	} else {
		fmt.Printf("FAILED: Should have timed out, got result: %s\n", result)
	}
}

func mockDominoEffect(logger *slog.Logger) {
	profileSvc := &ProfileService{
		timeout: 0,
		err:     fmt.Errorf("profile service unavailable"),
	}
	orderSvc := &OrderService{timeout: 10 * time.Second}

	aggregator := NewUserAggregator(
		profileSvc,
		orderSvc,
		WithTimeout(15*time.Second), // Long timeout
		WithLogger(logger),
	)

	start := time.Now()
	result, err := aggregator.Aggregate(context.Background(), 1)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("PASSED: Got expected error: %v\n", err)
		if elapsed < 500*time.Millisecond {
			fmt.Printf("FAST FAIL CORRECT: Returned in %v (not waiting for slow service)\n", elapsed)
		} else {
			fmt.Printf("FAILED: Took too long (%v), should fail immediately\n", elapsed)
		}
	} else {
		fmt.Printf("FAILED: Should have errored, got result: %s\n", result)
	}
}
