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
	"pkg.blksails.net/x/store"
)

// MockStore 是在 middleware_test_extended.go 中使用的内存存储
// 与 store_test.go 中定义的 mockStore 相同，但需要复制过来以避免循环引用
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
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Get(_ context.Context, key K) (V, error) {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, ok := s.data[key]; ok {
		return value, nil
	}
	var zero V
	return zero, store.ErrNotFound
}

func (s *mockStoreForMiddleware[K, V]) Delete(_ context.Context, key K) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return store.ErrNotFound
	}
	delete(s.data, key)
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Has(_ context.Context, key K) bool {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

func (s *mockStoreForMiddleware[K, V]) Clear(_ context.Context) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[K]V)
	return nil
}

func (s *mockStoreForMiddleware[K, V]) Keys(_ context.Context) []K {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]K, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *mockStoreForMiddleware[K, V]) GetSet(_ context.Context, key K, value V) (V, error) {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()

	var oldValue V
	var err error

	if old, ok := s.data[key]; ok {
		oldValue = old
	} else {
		err = store.ErrNotFound
	}

	s.data[key] = value
	return oldValue, err
}

// 测试各种中间件组合和高级场景
func TestMiddlewareExtended(t *testing.T) {
	ctx := context.Background()
	store1 := newMockStoreForMiddleware[string, string](0)
	store2 := newMockStoreForMiddleware[string, string](0)

	// 设置一些初始数据
	store2.Set(ctx, "key1", "value1")
	store2.Set(ctx, "key2", "value2")

	// 创建默认存储
	defaultStore := func() *DelegatedStore[string, string] {
		return NewDelegatedStore([]Layer[string, string]{
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
	}

	t.Run("CounterMiddleware", func(t *testing.T) {
		var getCount, setCount, otherCount int32

		// 创建计数中间件
		counterMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				// 通过上下文判断操作类型
				if op := ctx.Value("operation"); op != nil {
					switch op {
					case "get":
						atomic.AddInt32(&getCount, 1)
					case "set":
						atomic.AddInt32(&setCount, 1)
					default:
						atomic.AddInt32(&otherCount, 1)
					}
				} else {
					atomic.AddInt32(&otherCount, 1)
				}
				return next(ctx, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(counterMiddleware)

		// 执行不同类型的操作
		getCtx := WithOperationContext(ctx, "get")
		setCtx := WithOperationContext(ctx, "set")
		deleteCtx := WithOperationContext(ctx, "delete")

		// 执行操作
		ds.Get(getCtx, "key1")
		ds.Get(getCtx, "key2")
		ds.Set(setCtx, "key3", "value3")
		ds.Delete(deleteCtx, "key1")
		ds.Has(ctx, "key2") // 没有操作上下文

		// 验证计数
		assert.Equal(t, int32(2), atomic.LoadInt32(&getCount), "Get操作计数不正确")
		assert.Equal(t, int32(1), atomic.LoadInt32(&setCount), "Set操作计数不正确")
		assert.Equal(t, int32(2), atomic.LoadInt32(&otherCount), "其他操作计数不正确")
	})

	t.Run("TransformMiddleware", func(t *testing.T) {
		// 创建转换中间件 - 修改传入值
		transformMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				// 对写入的值加前缀
				if ctx.Value("operation") == "set" {
					return next(ctx, key, "transformed-"+value)
				}
				return next(ctx, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(transformMiddleware)

		// 测试值转换
		setCtx := WithOperationContext(ctx, "set")
		err := ds.Set(setCtx, "transform-key", "original")
		require.NoError(t, err)

		// 验证转换后的值
		value, err := ds.Get(ctx, "transform-key")
		require.NoError(t, err)
		assert.Equal(t, "transformed-original", value, "值未被正确转换")
	})

	t.Run("KeyFilterMiddleware", func(t *testing.T) {
		// 创建密钥过滤中间件 - 拒绝特定前缀的键
		keyFilterMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				// 拒绝以 "secret-" 开头的键
				if len(key) >= 7 && key[:7] == "secret-" {
					var zero string
					return zero, errors.New("access denied to secret keys")
				}
				return next(ctx, key, value)
			}
		}

		ds := defaultStore()
		ds.Use(keyFilterMiddleware)

		// 测试允许的键
		err := ds.Set(ctx, "public-key", "public-value")
		assert.NoError(t, err)

		// 测试禁止的键
		err = ds.Set(ctx, "secret-key", "secret-value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")

		// 测试 GetSet
		_, err = ds.GetSet(ctx, "secret-key", "new-secret")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("CachingMiddleware", func(t *testing.T) {
		slowStore := newMockStoreForMiddleware[string, string](100 * time.Millisecond)
		slowStore.Set(ctx, "cached-key", "cached-value")

		// 创建支持缓存的存储
		ds := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   slowStore,
				Primary: true,
			},
		})

		// 创建缓存中间件
		cache := make(map[string]string)
		cachingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				// 只对 Get 操作进行缓存
				if ctx.Value("operation") == "get" {
					// 尝试从缓存获取
					if cachedValue, ok := cache[key]; ok {
						return cachedValue, nil
					}

					// 缓存未命中，调用存储
					result, err := next(ctx, key, value)
					if err == nil {
						// 成功获取，更新缓存
						cache[key] = result
					}
					return result, err
				} else if ctx.Value("operation") == "set" {
					// 更新缓存
					result, err := next(ctx, key, value)
					if err == nil {
						cache[key] = value
					}
					return result, err
				} else if ctx.Value("operation") == "delete" {
					// 删除缓存
					delete(cache, key)
					return next(ctx, key, value)
				}
				return next(ctx, key, value)
			}
		}

		ds.Use(cachingMiddleware)

		// 第一次获取，应该很慢并填充缓存
		getCtx := WithOperationContext(ctx, "get")
		start := time.Now()
		val1, err := ds.Get(getCtx, "cached-key")
		duration1 := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, "cached-value", val1)
		assert.GreaterOrEqual(t, duration1.Milliseconds(), int64(90), "第一次获取应该很慢")

		// 第二次获取，应该从缓存获取，很快
		start = time.Now()
		val2, err := ds.Get(getCtx, "cached-key")
		duration2 := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, "cached-value", val2)
		assert.Less(t, duration2.Milliseconds(), int64(10), "第二次获取应该很快")

		// 修改值，应该更新缓存
		setCtx := WithOperationContext(ctx, "set")
		ds.Set(setCtx, "cached-key", "new-cached-value")

		// 获取新值，应该从缓存获取
		start = time.Now()
		val3, err := ds.Get(getCtx, "cached-key")
		duration3 := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, "new-cached-value", val3)
		assert.Less(t, duration3.Milliseconds(), int64(10), "更新后的获取应该很快")
	})

	t.Run("OperationBatchingMiddleware", func(t *testing.T) {
		ds := defaultStore()

		batchCount := 0
		operations := make(map[string]int)

		// 创建批处理中间件
		batchingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				// 记录操作
				if op := ctx.Value("operation"); op != nil {
					if opStr, ok := op.(string); ok {
						operations[opStr]++
					}
				}

				// 记录批处理
				batchCount++

				// 继续处理
				return next(ctx, key, value)
			}
		}

		ds.Use(batchingMiddleware)

		// 执行多个操作
		getCtx := WithOperationContext(ctx, "get")
		setCtx := WithOperationContext(ctx, "set")
		deleteCtx := WithOperationContext(ctx, "delete")

		// 获取全部键
		keysCtx := WithOperationContext(ctx, "keys")
		keys := ds.Keys(keysCtx)

		// 其他操作
		ds.Get(getCtx, "key1")
		ds.Set(setCtx, "new-key", "new-value")
		ds.Delete(deleteCtx, "key1")
		ds.Has(ctx, "key2")

		// 检查批处理计数和操作类型
		assert.Equal(t, 5, batchCount, "操作总数应为5")
		assert.Equal(t, 1, operations["keys"], "Keys操作数应为1")
		assert.Equal(t, 1, operations["get"], "Get操作数应为1")
		assert.Equal(t, 1, operations["set"], "Set操作数应为1")
		assert.Equal(t, 1, operations["delete"], "Delete操作数应为1")

		// Keys操作应该返回预期的键
		assert.GreaterOrEqual(t, len(keys), 1, "Keys应该返回至少一个键")
	})

	t.Run("GetSetWithMiddleware", func(t *testing.T) {
		ds := defaultStore()

		var logs []string
		logger := func(format string, v ...interface{}) {
			logs = append(logs, format)
		}

		var operations []string
		recorder := func(op string, d time.Duration) {
			operations = append(operations, op)
		}

		// 使用多个中间件
		ds.Use(
			LoggingMiddleware[string, string](logger),
			MetricsMiddleware[string, string](recorder),
		)

		// 初始化测试数据
		store1.Clear(ctx)
		store2.Clear(ctx)
		store2.Set(ctx, "getset-key", "old-value")

		// 执行GetSet
		opCtx := WithOperationContext(ctx, "getset-operation")
		oldValue, err := ds.GetSet(opCtx, "getset-key", "new-value")

		// 验证结果
		require.NoError(t, err)
		assert.Equal(t, "old-value", oldValue, "应返回旧值")

		// 验证中间件被调用
		assert.GreaterOrEqual(t, len(logs), 2, "日志中间件应被调用")
		assert.Contains(t, logs[0], "store: key=getset-key", "日志应包含键名")

		assert.Len(t, operations, 1, "度量中间件应被调用")
		assert.Equal(t, "getset-operation", operations[0], "操作名称应正确记录")

		// 验证新值已被设置
		newValue, err := ds.Get(ctx, "getset-key")
		require.NoError(t, err)
		assert.Equal(t, "new-value", newValue, "新值应被正确设置")
	})

	t.Run("ErrorTransformMiddleware", func(t *testing.T) {
		// 创建错误转换中间件
		errorTransform := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, key string, value string) (string, error) {
				result, err := next(ctx, key, value)
				if err == store.ErrNotFound {
					// 将"未找到"错误转换为自定义错误
					return result, errors.New("custom not found: " + key)
				}
				return result, err
			}
		}

		ds := defaultStore()
		ds.Use(errorTransform)

		// 尝试获取不存在的键
		_, err := ds.Get(ctx, "non-existent-key")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "custom not found")
		assert.Contains(t, err.Error(), "non-existent-key")
	})
}
