package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"pkg.blksails.net/store"
)

// RedisStore 是基于 Redis 的存储实现
type RedisStore[K comparable, V any] struct {
	client     *redis.Client
	keyPrefix  string        // 键前缀，用于隔离不同的存储实例
	defaultTTL time.Duration // 默认的过期时间，0 表示永不过期
}

// Options 配置 RedisStore 的选项
type Options struct {
	KeyPrefix  string
	DefaultTTL time.Duration
}

// NewRedisStore 创建一个新的 Redis 存储
func NewRedisStore[K comparable, V any](client *redis.Client, opts Options) *RedisStore[K, V] {
	return &RedisStore[K, V]{
		client:     client,
		keyPrefix:  opts.KeyPrefix,
		defaultTTL: opts.DefaultTTL,
	}
}

// formatKey 格式化存储键
func (s *RedisStore[K, V]) formatKey(key K) string {
	keyJSON, _ := json.Marshal(key)
	return s.keyPrefix + string(keyJSON)
}

// Set 存储键值对
func (s *RedisStore[K, V]) Set(ctx context.Context, key K, value V) error {
	return s.SetWithTTL(ctx, key, value, s.defaultTTL)
}

// SetWithTTL 存储键值对并设置过期时间
func (s *RedisStore[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return s.client.Set(ctx, s.formatKey(key), valueJSON, ttl).Err()
}

// Get 获取指定键的值
func (s *RedisStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	var zero V
	valueJSON, err := s.client.Get(ctx, s.formatKey(key)).Bytes()
	if err == redis.Nil {
		return zero, store.ErrNotFound
	}
	if err != nil {
		return zero, err
	}

	var value V
	if err := json.Unmarshal(valueJSON, &value); err != nil {
		return zero, err
	}
	return value, nil
}

// Delete 删除指定键的值
func (s *RedisStore[K, V]) Delete(ctx context.Context, key K) error {
	result, err := s.client.Del(ctx, s.formatKey(key)).Result()
	if err != nil {
		return err
	}
	if result == 0 {
		return store.ErrNotFound
	}
	return nil
}

// Has 检查键是否存在
func (s *RedisStore[K, V]) Has(ctx context.Context, key K) bool {
	result, err := s.client.Exists(ctx, s.formatKey(key)).Result()
	return err == nil && result > 0
}

// Clear 清空所有键值对
func (s *RedisStore[K, V]) Clear(ctx context.Context) error {
	pattern := s.keyPrefix + "*"
	iter := s.client.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		if err := s.client.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}

	return iter.Err()
}

// Keys 返回所有键
func (s *RedisStore[K, V]) Keys(ctx context.Context) []K {
	pattern := s.keyPrefix + "*"
	iter := s.client.Scan(ctx, 0, pattern, 0).Iterator()

	var keys []K
	for iter.Next(ctx) {
		key := iter.Val()
		if len(key) > len(s.keyPrefix) {
			var k K
			keyJSON := []byte(key[len(s.keyPrefix):])
			if err := json.Unmarshal(keyJSON, &k); err == nil {
				keys = append(keys, k)
			}
		}
	}

	return keys
}

// TTL 获取键的剩余生存时间
func (s *RedisStore[K, V]) TTL(ctx context.Context, key K) (time.Duration, error) {
	duration, err := s.client.TTL(ctx, s.formatKey(key)).Result()
	if err == redis.Nil {
		return 0, store.ErrNotFound
	}
	return duration, err
}

// Close 关闭 Redis 连接
func (s *RedisStore[K, V]) Close() error {
	return s.client.Close()
}
