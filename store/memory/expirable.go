package memory

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"pkg.blksails.net/store"
)

// ExpirableStore 是基于 LRU 的可过期存储实现
type ExpirableStore[K comparable, V any] struct {
	cache *lru.LRU[K, V]
}

// NewExpirableStore 创建一个新的可过期存储
// size: 缓存的最大容量
// ttl: 默认的过期时间，如果为 0 则表示永不过期
func NewExpirableStore[K comparable, V any](size int, ttl time.Duration) (*ExpirableStore[K, V], error) {
	cache := expirable.NewLRU[K, V](size, nil, ttl)
	return &ExpirableStore[K, V]{
		cache: cache,
	}, nil
}

// Set 存储键值对（使用默认过期时间）
func (s *ExpirableStore[K, V]) Set(_ context.Context, key K, value V) error {
	s.cache.Add(key, value)
	return nil
}

// Get 获取指定键的值
func (s *ExpirableStore[K, V]) Get(_ context.Context, key K) (V, error) {
	if value, ok := s.cache.Get(key); ok {
		return value, nil
	}
	var zero V
	return zero, store.ErrNotFound
}

// Delete 删除指定键的值
func (s *ExpirableStore[K, V]) Delete(_ context.Context, key K) error {
	if s.cache.Contains(key) {
		s.cache.Remove(key)
		return nil
	}
	return store.ErrNotFound
}

// Has 检查键是否存在且未过期
func (s *ExpirableStore[K, V]) Has(_ context.Context, key K) bool {
	return s.cache.Contains(key)
}

// Clear 清空所有键值对
func (s *ExpirableStore[K, V]) Clear(_ context.Context) error {
	s.cache.Purge()
	return nil
}

// Keys 返回所有未过期的键
func (s *ExpirableStore[K, V]) Keys(_ context.Context) []K {
	return s.cache.Keys()
}

// Len 返回当前存储的键值对数量
func (s *ExpirableStore[K, V]) Len() int {
	return s.cache.Len()
}
