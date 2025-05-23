package delegated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddleware(t *testing.T) {
	ctx := context.Background()
	store1 := newMockStore[string, string](0)
	store2 := newMockStore[string, string](0)

	t.Run("LoggingMiddleware", func(t *testing.T) {
		var logs []string
		logger := func(format string, v ...interface{}) {
			logs = append(logs, fmt.Sprintf(format, v...))
		}

		ds := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   store1,
				TTL:     time.Minute,
				Primary: false,
			},
			{
				Store:   store2,
				Primary: true,
			},
		})
		ds.Use(LoggingMiddleware[string, string](logger))

		// 测试 Set
		logs = nil
		err := ds.Set(ctx, "log-key", "log-value")
		assert.NoError(t, err)
		assert.Len(t, logs, 2)
		assert.True(t, strings.Contains(logs[0], "log-key"))

		// 测试 Get
		logs = nil
		_, err = ds.Get(ctx, "non-existing")
		assert.Error(t, err)
		assert.Len(t, logs, 2)
		assert.True(t, strings.Contains(logs[1], "error"))
	})

	t.Run("MetricsMiddleware", func(t *testing.T) {
		var operations []string
		var durations []time.Duration

		recorder := func(op string, d time.Duration) {
			operations = append(operations, op)
			durations = append(durations, d)
		}

		ds := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   store1,
				Primary: true,
			},
		})
		ds.Use(MetricsMiddleware[string, string](recorder))

		// 测试带操作上下文的 Set
		operations = nil
		durations = nil
		opCtx := WithOperationContext(ctx, "set-operation")
		err := ds.Set(opCtx, "metrics-key", "metrics-value")
		assert.NoError(t, err)
		assert.Len(t, operations, 1)
		assert.Equal(t, "set-operation", operations[0])
		assert.True(t, durations[0] > 0)

		// 测试带操作上下文的 Get
		operations = nil
		durations = nil
		opCtx = WithOperationContext(ctx, "get-operation")
		_, err = ds.Get(opCtx, "metrics-key")
		assert.NoError(t, err)
		assert.Len(t, operations, 1)
		assert.Equal(t, "get-operation", operations[0])
	})

	t.Run("RetryMiddleware", func(t *testing.T) {
		// 创建一个包含自定义操作函数的存储
		// 而不是使用mockErrorStore
		var count int32

		ds := NewDelegatedStore([]Layer[string, string]{
			{
				Store: &directStore[string, string]{
					// 同时实现 GetSet、Get 和 Set 方法

					getFn: func(ctx context.Context, key string) (string, error) {
						currentCount := atomic.AddInt32(&count, 1)
						t.Logf("GetSet调用 #%d", currentCount)

						if currentCount < 3 {
							return "", errors.New("temporary error")
						}
						return "success", nil
					},
					setFn: func(ctx context.Context, key string, value string) error {
						// Set 方法不需要计数，直接通过
						return nil
					},
				},
				Primary: true,
			},
		})

		shouldRetry := func(err error) bool {
			return err != nil && err.Error() == "temporary error"
		}

		ds.Use(RetryMiddleware[string, string](5, shouldRetry))

		// 重置计数器
		atomic.StoreInt32(&count, 0)

		// 执行GetSet
		val, err := ds.GetSet(ctx, "retry-key", "retry-value")

		// 验证结果
		require.NoError(t, err, "预期没有错误，但得到：%v", err)
		assert.Equal(t, "success", val, "预期返回值为 'success'")
		assert.Equal(t, int32(3), atomic.LoadInt32(&count), "预期调用次数为 3")
	})

	t.Run("Middleware Chain", func(t *testing.T) {
		var logs []string
		logger := func(format string, v ...interface{}) {
			logs = append(logs, format)
		}

		var operations []string
		recorder := func(op string, d time.Duration) {
			operations = append(operations, op)
		}

		ds := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   store1,
				Primary: true,
			},
		})
		ds.Use(
			LoggingMiddleware[string, string](logger),
			MetricsMiddleware[string, string](recorder),
		)

		logs = nil
		operations = nil
		opCtx := WithOperationContext(ctx, "chained-operation")
		ds.Set(opCtx, "chain-key", "chain-value")

		assert.Len(t, logs, 2)
		assert.Len(t, operations, 1)
		assert.Equal(t, "chained-operation", operations[0])
	})
}

// directStore 可以直接指定操作函数
type directStore[K comparable, V any] struct {
	getFn    func(context.Context, K) (V, error)
	setFn    func(context.Context, K, V) error
	deleteFn func(context.Context, K) error
	hasFn    func(context.Context, K) bool
	clearFn  func(context.Context) error
	keysFn   func(context.Context) []K
	getSetFn func(context.Context, K, V) (V, error)
}

func (s *directStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	if s.getFn != nil {
		return s.getFn(ctx, key)
	}
	var zero V
	return zero, errors.New("not implemented")
}

func (s *directStore[K, V]) Set(ctx context.Context, key K, value V) error {
	if s.setFn != nil {
		return s.setFn(ctx, key, value)
	}
	return errors.New("not implemented")
}

func (s *directStore[K, V]) Delete(ctx context.Context, key K) error {
	if s.deleteFn != nil {
		return s.deleteFn(ctx, key)
	}
	return errors.New("not implemented")
}

func (s *directStore[K, V]) Has(ctx context.Context, key K) bool {
	if s.hasFn != nil {
		return s.hasFn(ctx, key)
	}
	return false
}

func (s *directStore[K, V]) Clear(ctx context.Context) error {
	if s.clearFn != nil {
		return s.clearFn(ctx)
	}
	return errors.New("not implemented")
}

func (s *directStore[K, V]) Keys(ctx context.Context) []K {
	if s.keysFn != nil {
		return s.keysFn(ctx)
	}
	return nil
}

func (s *directStore[K, V]) GetSet(ctx context.Context, key K, value V) (V, error) {
	if s.getSetFn != nil {
		return s.getSetFn(ctx, key, value)
	}
	var zero V
	return zero, errors.New("not implemented")
}

// mockErrorStore 专门用于测试中间件的错误处理
type mockErrorStore[K comparable, V any] struct {
	err          error
	customGetSet func(context.Context, K, V) (V, error)
}

func (s *mockErrorStore[K, V]) Set(_ context.Context, _ K, _ V) error { return s.err }
func (s *mockErrorStore[K, V]) Get(_ context.Context, _ K) (V, error) { var zero V; return zero, s.err }
func (s *mockErrorStore[K, V]) Delete(_ context.Context, _ K) error   { return s.err }
func (s *mockErrorStore[K, V]) Has(_ context.Context, _ K) bool       { return false }
func (s *mockErrorStore[K, V]) Clear(_ context.Context) error         { return s.err }
func (s *mockErrorStore[K, V]) Keys(_ context.Context) []K            { return nil }
func (s *mockErrorStore[K, V]) GetSet(ctx context.Context, k K, v V) (V, error) {
	if s.customGetSet != nil {
		return s.customGetSet(ctx, k, v)
	}
	var zero V
	return zero, s.err
}

// TestRetryMiddleware 专门测试重试中间件的细节
func TestRetryMiddleware(t *testing.T) {
	ctx := context.Background()

	t.Run("简单重试", func(t *testing.T) {
		// 跟踪失败次数
		var failures int32

		// 创建一个简单的操作函数，前两次失败，第三次成功
		op := func(ctx context.Context, key string, value string) (string, error) {
			count := atomic.AddInt32(&failures, 1)
			t.Logf("尝试 #%d", count)

			if count < 3 {
				return "", errors.New("临时错误")
			}
			return "成功", nil
		}

		// 应用重试中间件
		retryOp := RetryMiddleware[string, string](5, func(err error) bool {
			return err != nil
		})(op)

		// 重置计数器
		atomic.StoreInt32(&failures, 0)

		// 执行操作
		result, err := retryOp(ctx, "test-key", "test-value")

		// 验证结果
		require.NoError(t, err, "操作应该成功")
		assert.Equal(t, "成功", result, "返回值应该是'成功'")
		assert.Equal(t, int32(3), atomic.LoadInt32(&failures), "应该有3次尝试")
	})

	t.Run("达到最大重试次数", func(t *testing.T) {
		// 跟踪失败次数
		var attempts int32

		// 创建一个总是失败的操作函数
		op := func(ctx context.Context, key string, value string) (string, error) {
			count := atomic.AddInt32(&attempts, 1)
			t.Logf("尝试 #%d", count)
			return "", errors.New("持续错误")
		}

		// 应用重试中间件，最多重试3次
		retryOp := RetryMiddleware[string, string](3, func(err error) bool {
			return err != nil
		})(op)

		// 重置计数器
		atomic.StoreInt32(&attempts, 0)

		// 执行操作
		_, err := retryOp(ctx, "test-key", "test-value")

		// 验证结果
		require.Error(t, err, "操作应该失败")
		assert.Equal(t, "持续错误", err.Error(), "应该返回预期的错误消息")
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts), "应该有3次尝试")
	})

	t.Run("自定义重试判断", func(t *testing.T) {
		// 跟踪调用次数
		var attempts int32

		// 创建一个会返回不同错误的操作函数
		op := func(ctx context.Context, key string, value string) (string, error) {
			count := atomic.AddInt32(&attempts, 1)
			t.Logf("尝试 #%d", count)

			if count == 1 {
				return "", errors.New("可重试错误")
			} else if count == 2 {
				return "", errors.New("不可重试错误")
			}
			return "不应该到达这里", nil
		}

		// 应用重试中间件，只重试特定错误
		retryOp := RetryMiddleware[string, string](5, func(err error) bool {
			return err != nil && err.Error() == "可重试错误"
		})(op)

		// 重置计数器
		atomic.StoreInt32(&attempts, 0)

		// 执行操作
		_, err := retryOp(ctx, "test-key", "test-value")

		// 验证结果
		require.Error(t, err, "操作应该失败")
		assert.Equal(t, "不可重试错误", err.Error(), "应该返回不可重试的错误")
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts), "应该只有2次尝试")
	})
}
