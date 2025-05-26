package delegated

import (
	"context"
	"log"
	"time"
)

// LoggingMiddleware 返回一个记录操作的中间件
// 现在可以根据操作类型记录不同的日志信息
func LoggingMiddleware[K comparable, V any](logf func(format string, v ...interface{})) Middleware[K, V] {
	if logf == nil {
		logf = log.Printf
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, op OperationType, key K, value V) (V, error) {
			logf("store: %s operation, key=%v", op.String(), key)
			result, err := next(ctx, op, key, value)
			if err != nil {
				logf("store: %s operation failed, key=%v, error=%v", op.String(), key, err)
			} else {
				logf("store: %s operation success, key=%v", op.String(), key)
			}
			return result, err
		}
	}
}

// MetricsMiddleware 返回一个用于收集性能指标的中间件
// 现在可以根据操作类型收集不同的指标
func MetricsMiddleware[K comparable, V any](
	recordLatency func(operation string, duration time.Duration),
) Middleware[K, V] {
	if recordLatency == nil {
		recordLatency = func(string, time.Duration) {}
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, op OperationType, key K, value V) (V, error) {
			start := time.Now()
			result, err := next(ctx, op, key, value)
			duration := time.Since(start)

			// 使用操作类型字符串作为指标标签
			recordLatency(op.String(), duration)
			return result, err
		}
	}
}

// RetryMiddleware 返回一个在操作失败时重试的中间件
// 现在可以根据操作类型决定是否需要重试
func RetryMiddleware[K comparable, V any](attempts int, shouldRetry func(op OperationType, err error) bool) Middleware[K, V] {
	if shouldRetry == nil {
		shouldRetry = func(op OperationType, err error) bool {
			// 默认只对读操作进行重试，写操作可能有副作用
			return err != nil && (op == OperationGet || op == OperationHas)
		}
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, op OperationType, key K, value V) (V, error) {
			var result V
			var err error

			for i := 0; i < attempts; i++ {
				result, err = next(ctx, op, key, value)
				if !shouldRetry(op, err) {
					log.Printf("RetryMiddleware: %s operation attempt %d success or no retry needed: %v", op.String(), i+1, err)
					return result, err
				}

				log.Printf("RetryMiddleware: %s operation attempt %d failed: %v, will retry", op.String(), i+1, err)

				// 如果这是最后一次尝试，则返回当前结果
				if i == attempts-1 {
					log.Printf("RetryMiddleware: reached max attempts %d for %s operation, returning last result", attempts, op.String())
					return result, err
				}

				// 简单的指数退避
				backoff := time.Duration(1<<uint(i)) * time.Millisecond
				if i > 0 {
					log.Printf("RetryMiddleware: waiting %v before retry for %s operation", backoff, op.String())
					time.Sleep(backoff)
				}
			}

			return result, err
		}
	}
}

// CacheStatsMiddleware 返回一个收集缓存统计的中间件
// 根据操作类型和结果收集缓存命中率等统计信息
func CacheStatsMiddleware[K comparable, V any](
	recordStats func(op OperationType, hit bool, duration time.Duration),
) Middleware[K, V] {
	if recordStats == nil {
		recordStats = func(OperationType, bool, time.Duration) {}
	}

	return func(next OperationFunc[K, V]) OperationFunc[K, V] {
		return func(ctx context.Context, op OperationType, key K, value V) (V, error) {
			start := time.Now()
			result, err := next(ctx, op, key, value)
			duration := time.Since(start)

			// 根据操作类型和结果判断是否命中
			var hit bool
			switch op {
			case OperationGet:
				hit = (err == nil)
			case OperationHas:
				hit = (err == nil)
			default:
				// 对于写操作，不统计命中率
				hit = false
			}

			recordStats(op, hit, duration)
			return result, err
		}
	}
}
