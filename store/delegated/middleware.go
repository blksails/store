package delegated

import (
	"context"
	"log"
	"time"
)

// LoggingMiddleware 返回一个记录操作的中间件
func LoggingMiddleware[K comparable, V any](logf func(format string, v ...interface{})) Middleware[K, V] {
	if logf == nil {
		logf = log.Printf
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, key K, value V) (V, error) {
			logf("store: key=%v", key)
			result, err := next(ctx, key, value)
			if err != nil {
				logf("store: key=%v, error=%v", key, err)
			} else {
				logf("store: key=%v, success", key)
			}
			return result, err
		}
	}
}

// MetricsMiddleware 返回一个用于收集性能指标的中间件
func MetricsMiddleware[K comparable, V any](
	recordLatency func(operation string, duration time.Duration),
) Middleware[K, V] {
	if recordLatency == nil {
		recordLatency = func(string, time.Duration) {}
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, key K, value V) (V, error) {
			operation := "unknown"
			if op := ctx.Value("operation"); op != nil {
				if opStr, ok := op.(string); ok {
					operation = opStr
				}
			}

			start := time.Now()
			result, err := next(ctx, key, value)
			duration := time.Since(start)

			recordLatency(operation, duration)
			return result, err
		}
	}
}

// RetryMiddleware 返回一个在操作失败时重试的中间件
func RetryMiddleware[K comparable, V any](attempts int, shouldRetry func(err error) bool) Middleware[K, V] {
	if shouldRetry == nil {
		shouldRetry = func(err error) bool { return err != nil }
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, key K, value V) (V, error) {
			var result V
			var err error

			for i := 0; i < attempts; i++ {
				result, err = next(ctx, key, value)
				if !shouldRetry(err) {
					log.Printf("RetryMiddleware: 尝试 %d 成功或不需要重试: %v", i+1, err)
					return result, err
				}

				log.Printf("RetryMiddleware: 尝试 %d 失败: %v, 将重试", i+1, err)

				// 如果这是最后一次尝试，则返回当前结果
				if i == attempts-1 {
					log.Printf("RetryMiddleware: 达到最大尝试次数 %d, 返回最后结果", attempts)
					return result, err
				}

				// 简单的指数退避
				backoff := time.Duration(1<<uint(i)) * time.Millisecond
				if i > 0 {
					log.Printf("RetryMiddleware: 等待 %v 后重试", backoff)
					time.Sleep(backoff)
				}
			}

			return result, err
		}
	}
}

// WithOperationContext 返回一个带有操作名称的上下文
func WithOperationContext(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, "operation", operation)
}
