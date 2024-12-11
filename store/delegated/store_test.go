package delegated

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	gstore "blksails.net/pkg/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		// Debugging output
		fmt.Printf("Duration for slow store Get: %v\n", duration)

		assert.True(t, duration >= 100*time.Millisecond)

		// 第二次获取应该从快存储中获取
		start = time.Now()
		value, err = store.Get(ctx, "key2")
		duration = time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, "value2", value)
		assert.True(t, duration < 100*time.Millisecond)
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
