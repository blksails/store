package memory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pkg.blksails.net/x/store"
)

// 测试 LRUStore
func TestLRUStore(t *testing.T) {
	ctx := context.Background()
	lruStore, err := NewLRUStore[string, string](100)
	require.NoError(t, err)

	t.Run("Basic CRUD", func(t *testing.T) {
		// Set and Get
		err := lruStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)

		value, err := lruStore.Get(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)

		// Has
		assert.True(t, lruStore.Has(ctx, "key1"))
		assert.False(t, lruStore.Has(ctx, "nonexistent"))

		// Delete
		err = lruStore.Delete(ctx, "key1")
		require.NoError(t, err)
		assert.False(t, lruStore.Has(ctx, "key1"))

		// Get after delete
		_, err = lruStore.Get(ctx, "key1")
		assert.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("LRU Eviction", func(t *testing.T) {
		smallStore, err := NewLRUStore[string, string](2)
		require.NoError(t, err)

		// Add three items to a cache with size 2
		err = smallStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)
		err = smallStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)
		err = smallStore.Set(ctx, "key3", "value3")
		require.NoError(t, err)

		// The first item should be evicted
		assert.False(t, smallStore.Has(ctx, "key1"))
		assert.True(t, smallStore.Has(ctx, "key2"))
		assert.True(t, smallStore.Has(ctx, "key3"))
	})

	t.Run("Clear", func(t *testing.T) {
		err := lruStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)
		err = lruStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)

		err = lruStore.Clear(ctx)
		require.NoError(t, err)

		assert.False(t, lruStore.Has(ctx, "key1"))
		assert.False(t, lruStore.Has(ctx, "key2"))
	})

	t.Run("Keys", func(t *testing.T) {
		err := lruStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)
		err = lruStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)

		keys := lruStore.Keys(ctx)
		assert.ElementsMatch(t, []string{"key1", "key2"}, keys)
	})

	t.Run("GetSet", func(t *testing.T) {
		// Test when key doesn't exist
		oldValue, err := lruStore.GetSet(ctx, "newkey", "newvalue")
		assert.ErrorIs(t, err, store.ErrNotFound)
		assert.Empty(t, oldValue)

		value, err := lruStore.Get(ctx, "newkey")
		require.NoError(t, err)
		assert.Equal(t, "newvalue", value)

		// Test when key exists
		oldValue, err = lruStore.GetSet(ctx, "newkey", "updatedvalue")
		require.NoError(t, err)
		assert.Equal(t, "newvalue", oldValue)

		value, err = lruStore.Get(ctx, "newkey")
		require.NoError(t, err)
		assert.Equal(t, "updatedvalue", value)
	})
}

// 测试 ExpirableStore
func TestExpirableStore(t *testing.T) {
	ctx := context.Background()
	expStore, err := NewExpirableStore[string, string](100, time.Second, nil)
	require.NoError(t, err)

	t.Run("Basic CRUD", func(t *testing.T) {
		// Set and Get
		err := expStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)

		value, err := expStore.Get(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)

		// Has
		assert.True(t, expStore.Has(ctx, "key1"))
		assert.False(t, expStore.Has(ctx, "nonexistent"))

		// Delete
		err = expStore.Delete(ctx, "key1")
		require.NoError(t, err)
		assert.False(t, expStore.Has(ctx, "key1"))
	})

	t.Run("Expiration", func(t *testing.T) {
		shortStore, err := NewExpirableStore[string, string](100, 100*time.Millisecond, nil)
		require.NoError(t, err)

		err = shortStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)

		// Value should exist immediately
		assert.True(t, shortStore.Has(ctx, "key1"))

		// Wait for expiration
		time.Sleep(200 * time.Millisecond)

		// Value should be expired
		assert.False(t, shortStore.Has(ctx, "key1"))
		_, err = shortStore.Get(ctx, "key1")
		assert.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("Clear", func(t *testing.T) {
		err := expStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)
		err = expStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)

		err = expStore.Clear(ctx)
		require.NoError(t, err)

		assert.False(t, expStore.Has(ctx, "key1"))
		assert.False(t, expStore.Has(ctx, "key2"))
	})

	t.Run("Keys", func(t *testing.T) {
		err := expStore.Set(ctx, "key1", "value1")
		require.NoError(t, err)
		err = expStore.Set(ctx, "key2", "value2")
		require.NoError(t, err)

		keys := expStore.Keys(ctx)
		assert.ElementsMatch(t, []string{"key1", "key2"}, keys)
	})

	t.Run("GetSet", func(t *testing.T) {
		// Test when key doesn't exist
		oldValue, err := expStore.GetSet(ctx, "newkey", "newvalue")
		assert.ErrorIs(t, err, store.ErrNotFound)
		assert.Empty(t, oldValue)

		value, err := expStore.Get(ctx, "newkey")
		require.NoError(t, err)
		assert.Equal(t, "newvalue", value)

		// Test when key exists
		oldValue, err = expStore.GetSet(ctx, "newkey", "updatedvalue")
		require.NoError(t, err)
		assert.Equal(t, "newvalue", oldValue)

		value, err = expStore.Get(ctx, "newkey")
		require.NoError(t, err)
		assert.Equal(t, "updatedvalue", value)
	})

	t.Run("GetSet with Expiration", func(t *testing.T) {
		shortStore, err := NewExpirableStore[string, string](100, 100*time.Millisecond, nil)
		require.NoError(t, err)

		oldValue, err := shortStore.GetSet(ctx, "expkey", "expvalue")
		assert.ErrorIs(t, err, store.ErrNotFound)
		assert.Empty(t, oldValue)

		// Wait for expiration
		time.Sleep(200 * time.Millisecond)

		// Key should be expired
		oldValue, err = shortStore.GetSet(ctx, "expkey", "newvalue")
		assert.ErrorIs(t, err, store.ErrNotFound)
		assert.Empty(t, oldValue)
	})
}

// 并发测试
func TestConcurrency(t *testing.T) {
	t.Run("LRUStore", func(t *testing.T) {
		store, err := NewLRUStore[string, string](1000)
		require.NoError(t, err)
		testConcurrentAccess(t, store)
	})

	t.Run("ExpirableStore", func(t *testing.T) {
		store, err := NewExpirableStore[string, string](1000, time.Minute, nil)
		require.NoError(t, err)
		testConcurrentAccess(t, store)
	})
}

func testConcurrentAccess(t *testing.T, s store.Store[string, string]) {
	ctx := context.Background()
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

				err := s.Set(ctx, key, value)
				assert.NoError(t, err)

				got, err := s.Get(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, value, got)
			}
		}(i)
	}

	wg.Wait()
}

// 基准测试
func BenchmarkStores(b *testing.B) {
	ctx := context.Background()

	b.Run("LRUStore", func(b *testing.B) {
		store, _ := NewLRUStore[string, string](10000)
		benchmarkStore(b, ctx, store)
	})

	b.Run("ExpirableStore", func(b *testing.B) {
		store, _ := NewExpirableStore[string, string](10000, time.Minute, nil)
		benchmarkStore(b, ctx, store)
	})
}

func benchmarkStore(b *testing.B, ctx context.Context, s store.Store[string, string]) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			s.Set(ctx, key, value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		key := "benchmark_key"
		s.Set(ctx, key, "benchmark_value")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			s.Get(ctx, key)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		key := "parallel_key"
		s.Set(ctx, key, "parallel_value")
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.Get(ctx, key)
			}
		})
	})
}

// 模糊测试
func FuzzLRUStore(f *testing.F) {
	f.Add("key", "value")
	f.Fuzz(func(t *testing.T, key, value string) {
		ctx := context.Background()
		store, err := NewLRUStore[string, string](100)
		if err != nil {
			t.Skip()
		}

		err = store.Set(ctx, key, value)
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

func FuzzExpirableStore(f *testing.F) {
	f.Add("key", "value")
	f.Fuzz(func(t *testing.T, key, value string) {
		ctx := context.Background()
		store, err := NewExpirableStore[string, string](100, time.Minute, nil)
		if err != nil {
			t.Skip()
		}

		err = store.Set(ctx, key, value)
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
