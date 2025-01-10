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

	store := NewDelegatedStore(
		Layer[string, string]{
			Store:   fastStore,
			TTL:     time.Minute,
			Primary: false,
		},
		Layer[string, string]{
			Store:   slowStore,
			Primary: true,
		},
	)

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
		store := NewDelegatedStore(
			Layer[string, string]{
				Store:   newMockStore[string, string](0),
				Primary: true,
			},
		)

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

	store := NewDelegatedStore(
		Layer[string, string]{
			Store:   fastStore,
			TTL:     time.Minute,
			Primary: false,
		},
		Layer[string, string]{
			Store:   slowStore,
			Primary: true,
		},
	)

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
	store := NewDelegatedStore(
		Layer[string, string]{
			Store:   newMockStore[string, string](0),
			Primary: true,
		},
	)

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

	store := NewDelegatedStore(
		Layer[string, string]{
			Store:   &errorStore[string, string]{err: expectedErr},
			Primary: true,
		},
	)

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

	ds := NewDelegatedStore(
		Layer[string, string]{
			Store:   store1,
			TTL:     time.Minute,
			Primary: false,
		},
		Layer[string, string]{
			Store:   store2,
			Primary: true,
		},
	)

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
