package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gstore "pkg.blksails.net/x/store"
)

func setupTestRedis(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // 使用测试数据库
	})

	// 清空测试数据库
	err := client.FlushDB(context.Background()).Err()
	require.NoError(t, err)

	return client
}

func TestRedisStore(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	ctx := context.Background()
	store := NewRedisStore[string, string](client, Options{
		KeyPrefix:  "test:",
		DefaultTTL: time.Minute,
	})

	t.Run("Basic CRUD", func(t *testing.T) {
		// Set and Get
		err := store.Set(ctx, "key1", "value1")
		require.NoError(t, err)

		value, err := store.Get(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)

		// Has
		assert.True(t, store.Has(ctx, "key1"))
		assert.False(t, store.Has(ctx, "nonexistent"))

		// Delete
		err = store.Delete(ctx, "key1")
		require.NoError(t, err)
		assert.False(t, store.Has(ctx, "key1"))

		// Get after delete
		_, err = store.Get(ctx, "key1")
		assert.ErrorIs(t, err, gstore.ErrNotFound)
	})

	t.Run("TTL", func(t *testing.T) {
		err := store.SetWithTTL(ctx, "key2", "value2", 100*time.Millisecond)
		require.NoError(t, err)

		// Value should exist
		assert.True(t, store.Has(ctx, "key2"))

		// Wait for expiration
		time.Sleep(200 * time.Millisecond)

		// Value should be expired
		assert.False(t, store.Has(ctx, "key2"))
		_, err = store.Get(ctx, "key2")
		assert.ErrorIs(t, err, gstore.ErrNotFound)
	})

	t.Run("Clear", func(t *testing.T) {
		err := store.Set(ctx, "key3", "value3")
		require.NoError(t, err)
		err = store.Set(ctx, "key4", "value4")
		require.NoError(t, err)

		err = store.Clear(ctx)
		require.NoError(t, err)

		assert.False(t, store.Has(ctx, "key3"))
		assert.False(t, store.Has(ctx, "key4"))
	})

	t.Run("Keys", func(t *testing.T) {
		err := store.Set(ctx, "key5", "value5")
		require.NoError(t, err)
		err = store.Set(ctx, "key6", "value6")
		require.NoError(t, err)

		keys := store.Keys(ctx)
		assert.ElementsMatch(t, []string{"key5", "key6"}, keys)
	})
}

func BenchmarkRedisStore(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer client.Close()

	ctx := context.Background()
	store := NewRedisStore[string, string](client, Options{
		KeyPrefix:  "bench:",
		DefaultTTL: time.Minute,
	})

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			store.Set(ctx, key, value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		key := "benchmark_key"
		store.Set(ctx, key, "benchmark_value")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
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
