package delegated

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pkg.blksails.net/x/store"
	gstore "pkg.blksails.net/x/store"
)

// mockStore 是用于测试的内存存储实现
type mockStore[K comparable, V any] struct {
	mu    sync.RWMutex
	data  map[K]V
	delay time.Duration // 模拟延迟
}

func newMockStore[K comparable, V any](delay time.Duration) *mockStore[K, V] {
	return &mockStore[K, V]{
		data:  make(map[K]V),
		delay: delay,
	}
}

func (s *mockStore[K, V]) Set(_ context.Context, key K, value V) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *mockStore[K, V]) Get(_ context.Context, key K) (V, error) {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, ok := s.data[key]; ok {
		return value, nil
	}
	var zero V
	return zero, gstore.ErrNotFound
}

func (s *mockStore[K, V]) Delete(_ context.Context, key K) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return gstore.ErrNotFound
	}
	delete(s.data, key)
	return nil
}

func (s *mockStore[K, V]) Has(_ context.Context, key K) bool {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

func (s *mockStore[K, V]) Clear(_ context.Context) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[K]V)
	return nil
}

func (s *mockStore[K, V]) Keys(_ context.Context) []K {
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]K, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *mockStore[K, V]) GetSet(_ context.Context, key K, value V) (V, error) {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()

	var oldValue V
	var err error

	if old, ok := s.data[key]; ok {
		oldValue = old
	} else {
		err = gstore.ErrNotFound
	}

	s.data[key] = value
	return oldValue, err
}

// 单元测试
func TestDelegatedStore(t *testing.T) {
	ctx := context.Background()
	fastStore := newMockStore[string, string](0)
	slowStore := newMockStore[string, string](100 * time.Millisecond)

	store := NewDelegatedStore([]Layer[string, string]{
		{
			Store:   fastStore,
			TTL:     time.Minute,
			Primary: false,
		},
		{
			Store:   slowStore,
			Primary: true,
		},
	})

	t.Run("Set and Get", func(t *testing.T) {
		err := store.Set(ctx, "key1", "value1")
		require.NoError(t, err)

		value, err := store.Get(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)
	})

	t.Run("Get from slow store", func(t *testing.T) {
		err := slowStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)

		start := time.Now()
		value, err := store.Get(ctx, "key2")
		duration := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, "value2", value)

		// 检查第一次获取是否从慢存储获取（应该在 90-200ms 之间）
		assert.GreaterOrEqual(t, duration.Milliseconds(), int64(90), "第一次获取应该从慢存储读取")
		assert.Less(t, duration.Milliseconds(), int64(200), "慢存储读取不应该超时")

		// 第二次获取应该从快存储中获取
		start = time.Now()
		value, err = store.Get(ctx, "key2")
		duration = time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, "value2", value)
		assert.Less(t, duration.Milliseconds(), int64(90), "第二次获取应该从快存储读取") // 放宽快存储的时间限制
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Set(ctx, "key3", "value3")
		require.NoError(t, err)

		err = store.Delete(ctx, "key3")
		require.NoError(t, err)

		_, err = store.Get(ctx, "key3")
		assert.ErrorIs(t, err, gstore.ErrNotFound)
	})

	t.Run("Clear", func(t *testing.T) {
		err := store.Set(ctx, "key4", "value4")
		require.NoError(t, err)

		err = store.Clear(ctx)
		require.NoError(t, err)

		_, err = store.Get(ctx, "key4")
		assert.ErrorIs(t, err, gstore.ErrNotFound)
	})
}

// 模糊测试
func FuzzDelegatedStore(f *testing.F) {
	f.Add("key", "value")
	f.Fuzz(func(t *testing.T, key, value string) {
		ctx := context.Background()
		store := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		})

		err := store.Set(ctx, key, value)
		if err != nil {
			t.Skip()
		}

		got, err := store.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%q) error: %v", key, err)
		}
		if got != value {
			t.Errorf("Get(%q) = %q, want %q", key, got, value)
		}
	})
}

// 基准测试
func BenchmarkDelegatedStore(b *testing.B) {
	ctx := context.Background()
	fastStore := newMockStore[string, string](0)
	slowStore := newMockStore[string, string](100 * time.Microsecond)

	store := NewDelegatedStore([]Layer[string, string]{
		{
			Store:   fastStore,
			TTL:     time.Minute,
			Primary: false,
		},
		{
			Store:   slowStore,
			Primary: true,
		},
	})

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			store.Set(ctx, key, value)
		}
	})

	b.Run("Get/Cache Hit", func(b *testing.B) {
		key := "benchmark_key"
		store.Set(ctx, key, "benchmark_value")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store.Get(ctx, key)
		}
	})

	b.Run("Get/Cache Miss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("missing_key%d", i)
			store.Get(ctx, key)
		}
	})

	b.Run("Parallel Get", func(b *testing.B) {
		key := "parallel_key"
		store.Set(ctx, key, "parallel_value")
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				store.Get(ctx, key)
			}
		})
	})
}

// 测试并发安全性
func TestDelegatedStoreConcurrency(t *testing.T) {
	ctx := context.Background()
	store := NewDelegatedStore([]Layer[string, string]{
		{
			Store:   newMockStore[string, string](0),
			Primary: true,
		},
	})

	const goroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key%d-%d", id, j)
				value := fmt.Sprintf("value%d-%d", id, j)

				err := store.Set(ctx, key, value)
				assert.NoError(t, err)

				got, err := store.Get(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, value, got)
			}
		}(i)
	}

	wg.Wait()
}

// 测试错误情况
type errorStore[K comparable, V any] struct {
	err error
}

func (s *errorStore[K, V]) Set(_ context.Context, _ K, _ V) error { return s.err }
func (s *errorStore[K, V]) Get(_ context.Context, _ K) (V, error) { var zero V; return zero, s.err }
func (s *errorStore[K, V]) Delete(_ context.Context, _ K) error   { return s.err }
func (s *errorStore[K, V]) Has(_ context.Context, _ K) bool       { return false }
func (s *errorStore[K, V]) Clear(_ context.Context) error         { return s.err }
func (s *errorStore[K, V]) Keys(_ context.Context) []K            { return nil }
func (s *errorStore[K, V]) GetSet(_ context.Context, _ K, _ V) (V, error) {
	var zero V
	return zero, s.err
}

func TestDelegatedStoreErrors(t *testing.T) {
	ctx := context.Background()
	expectedErr := fmt.Errorf("storage error")

	store := NewDelegatedStore([]Layer[string, string]{
		{
			Store:   &errorStore[string, string]{err: expectedErr},
			Primary: true,
		},
	})

	t.Run("Set Error", func(t *testing.T) {
		err := store.Set(ctx, "key", "value")
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Get Error", func(t *testing.T) {
		_, err := store.Get(ctx, "key")
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Delete Error", func(t *testing.T) {
		err := store.Delete(ctx, "key")
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestDelegatedStore_GetSet(t *testing.T) {
	store1 := newMockStore[string, string](0)
	store2 := newMockStore[string, string](0)

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

	ctx := context.Background()

	tests := []struct {
		name       string
		setup      func()
		key        string
		value      string
		wantOld    string
		wantErr    error
		checkAfter func() bool
	}{
		{
			name:    "set new value",
			setup:   func() {},
			key:     "key1",
			value:   "new1",
			wantOld: "",
			wantErr: store.ErrNotFound,
			checkAfter: func() bool {
				// 验证新值已经写入两个层
				v1, _ := store1.Get(ctx, "key1")
				v2, _ := store2.Get(ctx, "key1")
				return v1 == "new1" && v2 == "new1"
			},
		},
		{
			name: "update existing value",
			setup: func() {
				store2.Set(ctx, "key2", "old2")
			},
			key:     "key2",
			value:   "new2",
			wantOld: "old2",
			wantErr: nil,
			checkAfter: func() bool {
				// 验证新值已经更新到两个层
				v1, _ := store1.Get(ctx, "key2")
				v2, _ := store2.Get(ctx, "key2")
				return v1 == "new2" && v2 == "new2"
			},
		},
		{
			name: "value exists in first layer",
			setup: func() {
				store1.Set(ctx, "key3", "old3")
			},
			key:     "key3",
			value:   "new3",
			wantOld: "old3",
			wantErr: nil,
			checkAfter: func() bool {
				// 验证新值已经更新到两个层
				v1, _ := store1.Get(ctx, "key3")
				v2, _ := store2.Get(ctx, "key3")
				return v1 == "new3" && v2 == "new3"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 清理之前的数据
			store1.Clear(ctx)
			store2.Clear(ctx)

			// 设置测试数据
			tt.setup()

			// 执行 GetSet
			gotOld, err := ds.GetSet(ctx, tt.key, tt.value)

			// 检查返回的旧值和错误
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("GetSet() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("GetSet() unexpected error = %v", err)
			}

			if gotOld != tt.wantOld {
				t.Errorf("GetSet() gotOld = %v, want %v", gotOld, tt.wantOld)
			}

			// 检查后续状态
			if !tt.checkAfter() {
				t.Error("GetSet() failed post-condition check")
			}
		})
	}
}

func TestOnBackFillCallback(t *testing.T) {
	ctx := context.Background()

	// 创建三层存储：缓存层（非主存储），主存储层，持久存储层（非主存储）
	cacheStore := newMockStore[string, string](0)   // 优先级 0 - 缓存层
	primaryStore := newMockStore[string, string](0) // 优先级 1 - 主存储层
	persistStore := newMockStore[string, string](0) // 优先级 2 - 持久存储层

	ds := NewDelegatedStore([]Layer[string, string]{
		{
			Store:   cacheStore,
			TTL:     time.Minute,
			Primary: false,
		},
		{
			Store:   primaryStore,
			Primary: true,
		},
		{
			Store:   persistStore,
			TTL:     time.Hour,
			Primary: false,
		},
	})

	// 记录回调调用
	var callbackCalls []struct {
		key        string
		value      string
		layerIndex int
		isPrimary  bool
	}

	// 设置回调函数
	ds.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
		callbackCalls = append(callbackCalls, struct {
			key        string
			value      string
			layerIndex int
			isPrimary  bool
		}{key, value, layerIndex, isPrimary})
		return "", false
	})

	t.Run("Primary Layer Backfill Callback", func(t *testing.T) {
		// 清空回调记录
		callbackCalls = nil

		// 首先在持久存储层设置值（绕过缓存和主存储）
		err := persistStore.Set(ctx, "test-key", "test-value")
		require.NoError(t, err)

		// 通过 Get 操作触发 backfill
		value, err := ds.Get(ctx, "test-key")
		require.NoError(t, err)
		assert.Equal(t, "test-value", value)

		// 等待异步 backfill 完成
		time.Sleep(10 * time.Millisecond)

		// 验证回调被调用
		require.Len(t, callbackCalls, 2, "应该有两次回调调用：缓存层和主存储层")

		// 验证缓存层回调（优先级 0，非主存储）
		assert.Equal(t, "test-key", callbackCalls[0].key)
		assert.Equal(t, "test-value", callbackCalls[0].value)
		assert.Equal(t, 0, callbackCalls[0].layerIndex)
		assert.False(t, callbackCalls[0].isPrimary)

		// 验证主存储层回调（优先级 1，主存储）
		assert.Equal(t, "test-key", callbackCalls[1].key)
		assert.Equal(t, "test-value", callbackCalls[1].value)
		assert.Equal(t, 1, callbackCalls[1].layerIndex)
		assert.True(t, callbackCalls[1].isPrimary)
	})

	t.Run("No Callback When Found In Primary Layer", func(t *testing.T) {
		// 清空回调记录
		callbackCalls = nil

		// 直接在缓存层设置值（优先级最高的层）
		err := cacheStore.Set(ctx, "cache-key", "cache-value")
		require.NoError(t, err)

		// 通过 Get 操作获取值
		value, err := ds.Get(ctx, "cache-key")
		require.NoError(t, err)
		assert.Equal(t, "cache-value", value)

		// 等待可能的异步操作
		time.Sleep(10 * time.Millisecond)

		// 验证没有回调被调用（因为值在最高优先级层找到，无需 backfill）
		assert.Len(t, callbackCalls, 0, "不应该有回调调用，因为值在最高优先级层找到")
	})

	t.Run("Multiple Keys Backfill", func(t *testing.T) {
		// 清空回调记录和所有存储
		callbackCalls = nil
		cacheStore.Clear(ctx)
		primaryStore.Clear(ctx)
		persistStore.Clear(ctx)

		// 在持久存储层设置多个值
		keys := []string{"key1", "key2", "key3"}
		for _, key := range keys {
			err := persistStore.Set(ctx, key, "value-"+key)
			require.NoError(t, err)
		}

		// 依次获取值触发 backfill，并且在每次获取后稍作等待
		for _, key := range keys {
			value, err := ds.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, "value-"+key, value)
			// 等待当前 backfill 完成再进行下一个
			time.Sleep(20 * time.Millisecond)
		}

		// 等待所有异步 backfill 完成
		time.Sleep(50 * time.Millisecond)

		// 验证回调被调用（每个 key 应该有两次回调：缓存层和主存储层）
		expectedCalls := len(keys) * 2
		require.Len(t, callbackCalls, expectedCalls, "每个 key 应该有两次回调调用")

		// 验证每个 key 的回调
		for i, key := range keys {
			// 缓存层回调
			cacheCallIndex := i * 2
			if cacheCallIndex < len(callbackCalls) {
				assert.Equal(t, key, callbackCalls[cacheCallIndex].key)
				assert.Equal(t, "value-"+key, callbackCalls[cacheCallIndex].value)
				assert.Equal(t, 0, callbackCalls[cacheCallIndex].layerIndex)
				assert.False(t, callbackCalls[cacheCallIndex].isPrimary)
			}

			// 主存储层回调
			primaryCallIndex := i*2 + 1
			if primaryCallIndex < len(callbackCalls) {
				assert.Equal(t, key, callbackCalls[primaryCallIndex].key)
				assert.Equal(t, "value-"+key, callbackCalls[primaryCallIndex].value)
				assert.Equal(t, 1, callbackCalls[primaryCallIndex].layerIndex)
				assert.True(t, callbackCalls[primaryCallIndex].isPrimary)
			}
		}
	})

	t.Run("No Callback When Not Set", func(t *testing.T) {
		// 创建一个没有设置回调的新存储
		dsNoCallback := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   newMockStore[string, string](0),
				Primary: false,
			},
			{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		})

		// 在第二层设置值
		err := dsNoCallback.GetLayer(1).Store.Set(ctx, "no-callback-key", "no-callback-value")
		require.NoError(t, err)

		// 通过 Get 操作触发 backfill（应该不会崩溃）
		value, err := dsNoCallback.Get(ctx, "no-callback-key")
		require.NoError(t, err)
		assert.Equal(t, "no-callback-value", value)
	})

	t.Run("Multiple Callbacks", func(t *testing.T) {
		// 创建新的存储来测试多个回调
		dsMultiple := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   newMockStore[string, string](0),
				Primary: false,
			},
			{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		})

		// 用于记录不同回调的调用
		var callback1Calls, callback2Calls, callback3Calls []string

		// 添加第一个回调
		dsMultiple.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callback1Calls = append(callback1Calls, fmt.Sprintf("cb1-%s-%d", key, layerIndex))
			return "", false
		})

		// 添加第二个回调
		dsMultiple.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callback2Calls = append(callback2Calls, fmt.Sprintf("cb2-%s-%d", key, layerIndex))
			return "", false
		})

		// 添加第三个回调
		dsMultiple.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callback3Calls = append(callback3Calls, fmt.Sprintf("cb3-%s-%d", key, layerIndex))
			return "", false
		})

		// 在第二层设置值
		err := dsMultiple.GetLayer(1).Store.Set(ctx, "multi-key", "multi-value")
		require.NoError(t, err)

		// 触发 backfill
		value, err := dsMultiple.Get(ctx, "multi-key")
		require.NoError(t, err)
		assert.Equal(t, "multi-value", value)

		// 等待异步 backfill 完成
		time.Sleep(20 * time.Millisecond)

		// 验证所有回调都被调用
		assert.Len(t, callback1Calls, 1)
		assert.Len(t, callback2Calls, 1)
		assert.Len(t, callback3Calls, 1)

		// 验证回调参数正确
		assert.Equal(t, "cb1-multi-key-0", callback1Calls[0])
		assert.Equal(t, "cb2-multi-key-0", callback2Calls[0])
		assert.Equal(t, "cb3-multi-key-0", callback3Calls[0])
	})

	t.Run("Clear Callbacks", func(t *testing.T) {
		// 创建新的存储来测试清除回调
		dsClear := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   newMockStore[string, string](0),
				Primary: false,
			},
			{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		})

		var callbackCalls []string

		// 添加回调
		dsClear.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callbackCalls = append(callbackCalls, key)
			return "", false
		})

		// 在第二层设置值
		err := dsClear.GetLayer(1).Store.Set(ctx, "clear-key1", "clear-value1")
		require.NoError(t, err)

		// 触发 backfill
		_, err = dsClear.Get(ctx, "clear-key1")
		require.NoError(t, err)

		// 等待异步 backfill 完成
		time.Sleep(20 * time.Millisecond)

		// 验证回调被调用
		assert.Len(t, callbackCalls, 1)
		assert.Equal(t, "clear-key1", callbackCalls[0])

		// 清除所有回调
		dsClear.ClearBackFillCallbacks()

		// 重置调用记录
		callbackCalls = nil

		// 在第二层设置新值
		err = dsClear.GetLayer(1).Store.Set(ctx, "clear-key2", "clear-value2")
		require.NoError(t, err)

		// 再次触发 backfill
		_, err = dsClear.Get(ctx, "clear-key2")
		require.NoError(t, err)

		// 等待可能的异步操作
		time.Sleep(20 * time.Millisecond)

		// 验证回调不再被调用
		assert.Len(t, callbackCalls, 0, "清除回调后不应该有回调被调用")
	})

	t.Run("Callback Execution Order", func(t *testing.T) {
		// 创建新的存储来测试回调执行顺序
		dsOrder := NewDelegatedStore([]Layer[string, string]{
			{
				Store:   newMockStore[string, string](0),
				Primary: false,
			},
			{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		})

		var executionOrder []string

		// 按顺序添加回调
		dsOrder.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			executionOrder = append(executionOrder, "first")
			return "", false
		})

		dsOrder.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			executionOrder = append(executionOrder, "second")
			return "", false
		})

		dsOrder.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			executionOrder = append(executionOrder, "third")
			return "", false
		})

		// 在第二层设置值
		err := dsOrder.GetLayer(1).Store.Set(ctx, "order-key", "order-value")
		require.NoError(t, err)

		// 触发 backfill
		_, err = dsOrder.Get(ctx, "order-key")
		require.NoError(t, err)

		// 等待异步 backfill 完成
		time.Sleep(20 * time.Millisecond)

		// 验证回调按注册顺序执行
		require.Len(t, executionOrder, 3)
		assert.Equal(t, "first", executionOrder[0])
		assert.Equal(t, "second", executionOrder[1])
		assert.Equal(t, "third", executionOrder[2])
	})
}

func TestNewDelegatedStoreWithOptions(t *testing.T) {
	ctx := context.Background()
	store1 := newMockStore[string, string](0)
	store2 := newMockStore[string, string](0)

	layers := []Layer[string, string]{
		{Store: store1, Primary: false},
		{Store: store2, Primary: true},
	}

	t.Run("Basic Creation Without Options", func(t *testing.T) {
		ds := NewDelegatedStore(layers)

		// 验证基本功能
		err := ds.Set(ctx, "test-key", "test-value")
		require.NoError(t, err)

		value, err := ds.Get(ctx, "test-key")
		require.NoError(t, err)
		assert.Equal(t, "test-value", value)
	})

	t.Run("With BackFill Callback Option", func(t *testing.T) {
		var callbackCalled bool
		var callbackKey string

		callback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callbackCalled = true
			callbackKey = key
			return "", true
		}

		ds := NewDelegatedStore(layers, WithBackFillCallback(callback))

		// 在第二层设置值
		err := store2.Set(ctx, "callback-key", "callback-value")
		require.NoError(t, err)

		// 触发回填
		_, err = ds.Get(ctx, "callback-key")
		require.NoError(t, err)

		// 等待异步回填
		time.Sleep(10 * time.Millisecond)

		// 验证回调被调用
		assert.True(t, callbackCalled)
		assert.Equal(t, "callback-key", callbackKey)
	})

	t.Run("With Multiple BackFill Callbacks Option", func(t *testing.T) {
		var callback1Called, callback2Called bool

		callback1 := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callback1Called = true
			return "", false
		}

		callback2 := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callback2Called = true
			return "", false
		}

		ds := NewDelegatedStore(layers, WithBackFillCallbacks(callback1, callback2))

		// 在第二层设置值
		err := store2.Set(ctx, "multi-callback-key", "multi-callback-value")
		require.NoError(t, err)

		// 触发回填
		_, err = ds.Get(ctx, "multi-callback-key")
		require.NoError(t, err)

		// 等待异步回填
		time.Sleep(10 * time.Millisecond)

		// 验证两个回调都被调用
		assert.True(t, callback1Called)
		assert.True(t, callback2Called)
	})

	t.Run("With Middleware Option", func(t *testing.T) {
		var logs []string

		loggingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				logs = append(logs, "before: "+key)
				result, err := next(ctx, op, key, value)
				logs = append(logs, "after: "+key)
				return result, err
			}
		}

		ds := NewDelegatedStore(layers, WithMiddleware(loggingMiddleware))

		// 执行操作
		err := ds.Set(ctx, "middleware-key", "middleware-value")
		require.NoError(t, err)

		// 验证中间件被调用
		assert.Len(t, logs, 2)
		assert.Equal(t, "before: middleware-key", logs[0])
		assert.Equal(t, "after: middleware-key", logs[1])
	})

	t.Run("With Multiple Middlewares Option", func(t *testing.T) {
		var logs []string

		middleware1 := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				logs = append(logs, "middleware1-before")
				result, err := next(ctx, op, key, value)
				logs = append(logs, "middleware1-after")
				return result, err
			}
		}

		middleware2 := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				logs = append(logs, "middleware2-before")
				result, err := next(ctx, op, key, value)
				logs = append(logs, "middleware2-after")
				return result, err
			}
		}

		ds := NewDelegatedStore(layers, WithMiddlewares(middleware1, middleware2))

		// 执行操作
		err := ds.Set(ctx, "multi-middleware-key", "multi-middleware-value")
		require.NoError(t, err)

		// 验证中间件按倒序执行（中间件链是从后向前构建的）
		assert.Len(t, logs, 4)
		assert.Equal(t, "middleware1-before", logs[0]) // 实际执行顺序
		assert.Equal(t, "middleware2-before", logs[1])
		assert.Equal(t, "middleware2-after", logs[2])
		assert.Equal(t, "middleware1-after", logs[3])
	})

	t.Run("With Combined Options", func(t *testing.T) {
		var callbackCalled bool
		var logs []string

		callback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
			callbackCalled = true
			return "", false
		}

		middleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
			return func(ctx context.Context, op OperationType, key string, value string) (string, error) {
				logs = append(logs, "middleware-called")
				return next(ctx, op, key, value)
			}
		}

		ds := NewDelegatedStore(layers,
			WithBackFillCallback(callback),
			WithMiddleware(middleware),
		)

		// 在第二层设置值
		err := store2.Set(ctx, "combined-key", "combined-value")
		require.NoError(t, err)

		// 触发回填和中间件
		_, err = ds.Get(ctx, "combined-key")
		require.NoError(t, err)

		// 等待异步回填
		time.Sleep(10 * time.Millisecond)

		// 验证回调和中间件都被调用
		assert.True(t, callbackCalled)
		assert.Contains(t, logs, "middleware-called")
	})
}
