package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"pkg.blksails.net/x/store"
)

// RedisStore 是基于 Redis 的存储实现
type RedisStore[K comparable, V any] struct {
	client     *redis.Client
	keyPrefix  string        // 键前缀，用于隔离不同的存储实例
	defaultTTL time.Duration // 默认的过期时间，0 表示永不过期
	onChange   []store.OnChangeCallback[K, V]
	onDelete   []store.OnDeleteCallback[K, V]
}

// Options 配置 RedisStore 的选项
type Options struct {
	KeyPrefix  string
	DefaultTTL time.Duration
}

// StoreOption 配置 RedisStore 的选项函数
type StoreOption[K comparable, V any] func(*RedisStore[K, V])

// WithOnChange 添加值变更回调函数
func WithOnChange[K comparable, V any](callback store.OnChangeCallback[K, V]) StoreOption[K, V] {
	return func(s *RedisStore[K, V]) {
		s.onChange = append(s.onChange, callback)
	}
}

// WithOnDelete 添加值删除回调函数
func WithOnDelete[K comparable, V any](callback store.OnDeleteCallback[K, V]) StoreOption[K, V] {
	return func(s *RedisStore[K, V]) {
		s.onDelete = append(s.onDelete, callback)
	}
}

// NewRedisStore 创建一个新的 Redis 存储
func NewRedisStore[K comparable, V any](client *redis.Client, opts Options, storeOpts ...StoreOption[K, V]) *RedisStore[K, V] {
	store := &RedisStore[K, V]{
		client:     client,
		keyPrefix:  opts.KeyPrefix,
		defaultTTL: opts.DefaultTTL,
	}

	// 应用选项
	for _, opt := range storeOpts {
		opt(store)
	}

	return store
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
	// 获取旧值用于变更回调
	var oldValue V
	oldValueJSON, err := s.client.Get(ctx, s.formatKey(key)).Bytes()
	hasOldValue := err == nil
	if hasOldValue {
		if err := json.Unmarshal(oldValueJSON, &oldValue); err != nil {
			return err
		}
	}

	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = s.client.Set(ctx, s.formatKey(key), valueJSON, ttl).Err()
	if err != nil {
		return err
	}

	// 执行变更回调
	if hasOldValue {
		for _, callback := range s.onChange {
			if err := callback(ctx, key, oldValue, value); err != nil {
				return err
			}
		}
	}

	return nil
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
	// 获取要删除的值用于回调
	var value V
	valueJSON, err := s.client.Get(ctx, s.formatKey(key)).Bytes()
	if err == redis.Nil {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}

	if err := json.Unmarshal(valueJSON, &value); err != nil {
		return err
	}

	result, err := s.client.Del(ctx, s.formatKey(key)).Result()
	if err != nil {
		return err
	}
	if result == 0 {
		return store.ErrNotFound
	}

	// 执行删除回调
	for _, callback := range s.onDelete {
		if err := callback(ctx, key, value); err != nil {
			return err
		}
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
