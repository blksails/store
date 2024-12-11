package delegated

import (
	"context"
	"time"

	"pkg.blksails.net/store"
)

// Layer 表示存储层配置
type Layer[K comparable, V any] struct {
	Store   store.Store[K, V] // 存储实现
	TTL     time.Duration     // 缓存时间，仅对非最后一层有效
	Primary bool              // 是否为主存储（写操作必须同步到主存储）
}

// DelegatedStore 是分层存储的委派实现
type DelegatedStore[K comparable, V any] struct {
	layers []Layer[K, V]
}

// NewDelegatedStore 创建一个新的分层存储
// layers 按优先级顺序排列，索引 0 具有最高优先级
func NewDelegatedStore[K comparable, V any](layers ...Layer[K, V]) *DelegatedStore[K, V] {
	return &DelegatedStore[K, V]{
		layers: layers,
	}
}

// Set 存储值到所有层
func (s *DelegatedStore[K, V]) Set(ctx context.Context, key K, value V) error {
	// 首先写入所有主存储
	for _, layer := range s.layers {
		if layer.Primary {
			if err := layer.Store.Set(ctx, key, value); err != nil {
				return err
			}
		}
	}

	// 然后写入其他层并设置 TTL（如果支持）
	for _, layer := range s.layers {
		if !layer.Primary {
			if expirable, ok := layer.Store.(interface {
				SetWithTTL(context.Context, K, V, time.Duration) error
			}); ok && layer.TTL > 0 {
				if err := expirable.SetWithTTL(ctx, key, value, layer.TTL); err != nil {
					return err
				}
			} else {
				if err := layer.Store.Set(ctx, key, value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Get 按优先级顺序获取值
func (s *DelegatedStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	var lastErr error

	// 按优先级顺序查找
	for i, layer := range s.layers {
		value, err := layer.Store.Get(ctx, key)
		if err == nil {
			// 找到值后，异步更新之前的层
			if i > 0 {
				go s.backfill(context.Background(), key, value, i)
			}
			return value, nil
		}
		if err != store.ErrNotFound {
			lastErr = err
		}
	}

	var zero V
	if lastErr != nil {
		return zero, lastErr
	}
	return zero, store.ErrNotFound
}

// backfill 将值回填到更高优先级的层
func (s *DelegatedStore[K, V]) backfill(ctx context.Context, key K, value V, foundAt int) {
	for i := 0; i < foundAt; i++ {
		layer := s.layers[i]
		if expirable, ok := layer.Store.(interface {
			SetWithTTL(context.Context, K, V, time.Duration) error
		}); ok && layer.TTL > 0 {
			expirable.SetWithTTL(ctx, key, value, layer.TTL)
		} else {
			layer.Store.Set(ctx, key, value)
		}
	}
}

// Delete 从所有层中删除值
func (s *DelegatedStore[K, V]) Delete(ctx context.Context, key K) error {
	var lastErr error
	for _, layer := range s.layers {
		if err := layer.Store.Delete(ctx, key); err != nil && err != store.ErrNotFound {
			lastErr = err
		}
	}
	return lastErr
}

// Has 检查任意层是否存在值
func (s *DelegatedStore[K, V]) Has(ctx context.Context, key K) bool {
	for _, layer := range s.layers {
		if layer.Store.Has(ctx, key) {
			return true
		}
	}
	return false
}

// Clear 清空所有层
func (s *DelegatedStore[K, V]) Clear(ctx context.Context) error {
	var lastErr error
	for _, layer := range s.layers {
		if err := layer.Store.Clear(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Keys 返回所有主存储的键
func (s *DelegatedStore[K, V]) Keys(ctx context.Context) []K {
	keyMap := make(map[K]struct{})
	var keys []K

	// 只从主存储获取键
	for _, layer := range s.layers {
		if layer.Primary {
			for _, key := range layer.Store.Keys(ctx) {
				if _, exists := keyMap[key]; !exists {
					keyMap[key] = struct{}{}
					keys = append(keys, key)
				}
			}
		}
	}

	return keys
}
