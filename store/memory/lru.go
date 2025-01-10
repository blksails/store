package memory

import (
	"context"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"pkg.blksails.net/x/store"
)

// LRUStore 是基于 LRU 缓存的存储实现
type LRUStore[K comparable, V any] struct {
	cache *lru.Cache[K, V]
	mu    sync.RWMutex
}

// NewLRUStore 创建一个新的 LRU 缓存存储
func NewLRUStore[K comparable, V any](size int) (*LRUStore[K, V], error) {
	cache, err := lru.New[K, V](size)
	if err != nil {
		return nil, err
	}
	return &LRUStore[K, V]{
		cache: cache,
	}, nil
}

func (s *LRUStore[K, V]) Set(_ context.Context, key K, value V) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache.Add(key, value)
	return nil
}

func (s *LRUStore[K, V]) Get(_ context.Context, key K) (V, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if value, ok := s.cache.Get(key); ok {
		return value, nil
	}
	var zero V
	return zero, store.ErrNotFound
}

func (s *LRUStore[K, V]) Delete(_ context.Context, key K) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cache.Contains(key) {
		s.cache.Remove(key)
		return nil
	}
	return store.ErrNotFound
}

func (s *LRUStore[K, V]) Has(_ context.Context, key K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache.Contains(key)
}

func (s *LRUStore[K, V]) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache.Purge()
	return nil
}

func (s *LRUStore[K, V]) Keys(_ context.Context) []K {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache.Keys()
}

// GetSet 获取旧值并设置新值
func (s *LRUStore[K, V]) GetSet(_ context.Context, key K, value V) (V, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var oldValue V
	var err error

	if old, ok := s.cache.Get(key); ok {
		oldValue = old
	} else {
		err = store.ErrNotFound
	}

	s.cache.Add(key, value)
	return oldValue, err
}
