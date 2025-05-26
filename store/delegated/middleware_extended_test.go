package delegated

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStoreForMiddleware 为中间件测试的模拟存储
type mockStoreForMiddleware[K comparable, V any] struct {
	mu    sync.RWMutex
	data  map[K]V
	delay time.Duration // 模拟延迟
}

func newMockStoreForMiddleware[K comparable, V any](delay time.Duration) *mockStoreForMiddleware[K, V] {
	return &mockStoreForMiddleware[K, V]{
		data:  make(map[K]V),
		delay: delay,
	}
}

func (s *mockStoreForMiddleware[K, V]) Set(_ context.Context, key K, value V) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Get(_ context.Context, key K) (V, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, exists := s.data[key]; exists {
		return value, nil
	}
	var zero V
	return zero, errors.New("not found")
}

func (s *mockStoreForMiddleware[K, V]) Delete(_ context.Context, key K) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Has(_ context.Context, key K) bool {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.data[key]
	return exists
}

func (s *mockStoreForMiddleware[K, V]) Clear(_ context.Context) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[K]V)
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Keys(_ context.Context) []K {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]K, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *mockStoreForMiddleware[K, V]) GetSet(_ context.Context, key K, value V) (V, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.data[key]
	s.data[key] = value
	if !exists {
		var zero V
		return zero, errors.New("not found")
	}
	return old, nil
}

func defaultStore() *DelegatedStore[string, string] {
	store1 := newMockStoreForMiddleware[string, string](0)
	store2 := newMockStoreForMiddleware[string, string](0)

	layers := []Layer[string, string]{
		{Store: store1, Primary: false},
		{Store: store2, Primary: true},
	}

	return NewDelegatedStore(layers)
}

// directStoreForMiddleware 可以直接指定操作函数的存储
type directStoreForMiddleware[K comparable, V any] struct {
	getFn    func(context.Context, K) (V, error)
	setFn    func(context.Context, K, V) error
	deleteFn func(context.Context, K) error
	hasFn    func(context.Context, K) bool
	clearFn  func(context.Context) error
	keysFn   func(context.Context) []K
	getSetFn func(context.Context, K, V) (V, error)
}

func (s *directStoreForMiddleware[K, V]) Get(ctx context.Context, key K) (V, error) {
	if s.getFn != nil {
		return s.getFn(ctx, key)
	}
	var zero V
	return zero, errors.New("not implemented")
}

func (s *directStoreForMiddleware[K, V]) Set(ctx context.Context, key K, value V) error {
	if s.setFn != nil {
		return s.setFn(ctx, key, value)
	}
	return errors.New("not implemented")
}

func (s *directStoreForMiddleware[K, V]) Delete(ctx context.Context, key K) error {
	if s.deleteFn != nil {
		return s.deleteFn(ctx, key)
	}
	return errors.New("not implemented")
}

func (s *directStoreForMiddleware[K, V]) Has(ctx context.Context, key K) bool {
	if s.hasFn != nil {
		return s.hasFn(ctx, key)
	}
	return false
}

func (s *directStoreForMiddleware[K, V]) Clear(ctx context.Context) error {
	if s.clearFn != nil {
		return s.clearFn(ctx)
	}
	return errors.New("not implemented")
}

func (s *directStoreForMiddleware[K, V]) Keys(ctx context.Context) []K {
	if s.keysFn != nil {
		return s.keysFn(ctx)
	}
	return nil
}

func (s *directStoreForMiddleware[K, V]) GetSet(ctx context.Context, key K, value V) (V, error) {
	if s.getSetFn != nil {
		return s.getSetFn(ctx, key, value)
	}
	var zero V
	return zero, errors.New("not implemented")
}

func TestMiddlewareWithOperationType(t *testing.T) {
	ctx := context.Background()

	t.Run("CounterMiddleware", func(t *testing.T) {
		var getCount, setCount, deleteCount, hasCount int32

		// 创建计数中间件，现在可以直接使用操作类型
		counterMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				// 根据操作类型计数
				switch op {
				case OperationGet:
					atomic.AddInt32(&getCount, 1)
				case OperationSet:
					atomic.AddInt32(&setCount, 1)
				case OperationDelete:
					atomic.AddInt32(&deleteCount, 1)
				case OperationHas:
					atomic.AddInt32(&hasCount, 1)
				}
				return next(ctx, op, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(counterMiddleware)

		// 执行不同类型的操作
		ds.Get(ctx, "key1")
		ds.Get(ctx, "key2")
		ds.Set(ctx, "key3", "value3")
		ds.Delete(ctx, "key1")
		ds.Has(ctx, "key2")

		// 验证计数
		assert.Equal(t, int32(2), atomic.LoadInt32(&getCount), "Get操作计数不正确")
		assert.Equal(t, int32(1), atomic.LoadInt32(&setCount), "Set操作计数不正确")
		assert.Equal(t, int32(1), atomic.LoadInt32(&deleteCount), "Delete操作计数不正确")
		assert.Equal(t, int32(1), atomic.LoadInt32(&hasCount), "Has操作计数不正确")
	})

	t.Run("TransformMiddleware", func(t *testing.T) {
		// 创建转换中间件 - 根据操作类型修改值
		transformMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				// 对写入的值加前缀
				if op == OperationSet {
					return next(ctx, op, key, "transformed-"+value)
				}
				return next(ctx, op, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(transformMiddleware)

		// 测试值转换
		err := ds.Set(ctx, "transform-key", "original")
		require.NoError(t, err)

		// 验证转换后的值
		value, err := ds.Get(ctx, "transform-key")
		require.NoError(t, err)
		assert.Equal(t, "transformed-original", value, "值未被正确转换")
	})

	t.Run("KeyFilterMiddleware", func(t *testing.T) {
		// 创建密钥过滤中间件 - 根据操作类型进行不同的过滤
		keyFilterMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				// 对写操作进行严格过滤
				if op == OperationSet || op == OperationDelete {
					if len(key) >= 7 && key[:7] == "secret-" {
						var zero string
						return zero, errors.New("access denied to secret keys for write operations")
					}
				}
				// 对读操作放松限制
				return next(ctx, op, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(keyFilterMiddleware)

		// 测试允许的键
		err := ds.Set(ctx, "public-key", "public-value")
		assert.NoError(t, err)

		// 测试禁止的写操作
		err = ds.Set(ctx, "secret-key", "secret-value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")

		// 先设置一个 secret- 键（通过直接操作底层存储）
		err = ds.GetLayer(1).Store.Set(ctx, "secret-test", "secret-value")
		require.NoError(t, err)

		// 测试读操作仍然被允许（即使对 secret- 键）
		_, err = ds.Get(ctx, "secret-test")
		assert.NoError(t, err) // 读操作应该被允许
	})

	t.Run("OperationLoggingMiddleware", func(t *testing.T) {
		var logs []string

		// 创建操作日志中间件
		loggingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				logs = append(logs, "BEFORE: "+op.String()+" "+key)
				result, err := next(ctx, op, key, value)
				if err != nil {
					logs = append(logs, "ERROR: "+op.String()+" "+key+" "+err.Error())
				} else {
					logs = append(logs, "SUCCESS: "+op.String()+" "+key)
				}
				return result, err
			}
		}

		ds := defaultStore()
		ds.Use(loggingMiddleware)

		// 执行一些操作
		ds.Set(ctx, "log-key", "log-value")
		ds.Get(ctx, "log-key")
		ds.Delete(ctx, "log-key")

		// 验证日志
		expectedLogs := []string{
			"BEFORE: SET log-key",
			"SUCCESS: SET log-key",
			"BEFORE: GET log-key",
			"SUCCESS: GET log-key",
			"BEFORE: DELETE log-key",
			"SUCCESS: DELETE log-key",
		}

		assert.Equal(t, expectedLogs, logs, "日志记录不正确")
	})

	t.Run("ConditionalRetryMiddleware", func(t *testing.T) {
		var retryCount int32

		// 创建条件重试中间件 - 只对读操作重试
		conditionalRetryMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				maxRetries := 3
				for attempt := 0; attempt < maxRetries; attempt++ {
					result, err := next(ctx, op, key, value)

					// 如果成功或者不是需要重试的操作类型，直接返回
					if err == nil || !(op == OperationGet || op == OperationHas) {
						return result, err
					}

					// 如果这是最后一次尝试，返回结果
					if attempt == maxRetries-1 {
						return result, err
					}

					// 记录重试次数
					atomic.AddInt32(&retryCount, 1)
				}

				// 这行不应该被执行到，但为了安全起见保留
				var zero string
				return zero, errors.New("unexpected error")
			}
		}

		// 创建一个总是返回错误的存储
		failingStore := &directStoreForMiddleware[string, string]{
			getFn: func(ctx context.Context, key string) (string, error) {
				return "", errors.New("always fails")
			},
			setFn: func(ctx context.Context, key string, value string) error {
				return nil // Set 操作总是成功
			},
		}

		ds := NewDelegatedStore([]Layer[string, string]{
			{Store: failingStore, Primary: true},
		})
		ds.Use(conditionalRetryMiddleware)

		// 测试 Get 操作（应该被重试）
		_, err := ds.Get(ctx, "nonexistent-key")
		assert.Error(t, err) // 最终仍然失败
		assert.Equal(t, int32(2), atomic.LoadInt32(&retryCount), "Get 操作应该被重试")

		// 重置计数器
		atomic.StoreInt32(&retryCount, 0)

		// 测试 Set 操作（不应该被重试）
		err = ds.Set(ctx, "test-key", "test-value")
		assert.NoError(t, err) // Set 应该成功
		assert.Equal(t, int32(0), atomic.LoadInt32(&retryCount), "Set 操作不应该被重试")
	})
}
