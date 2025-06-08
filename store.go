package store

import (
	"context"
	"errors"
)

// OnChangeCallback 定义值变更的回调函数类型
// 参数：ctx 上下文, key 键, oldValue 旧值, newValue 新值
type OnChangeCallback[K comparable, V any] func(ctx context.Context, key K, oldValue V, newValue V) error

// OnDeleteCallback 定义值删除的回调函数类型
// 参数：ctx 上下文, key 键, value 被删除的值
type OnDeleteCallback[K comparable, V any] func(ctx context.Context, key K, value V) error

// Store 定义了一个通用的键值存储接口
// K 表示键的类型，必须是可比较的
// V 表示值的类型
type Store[K comparable, V any] interface {
	// Set 存储键值对
	Set(ctx context.Context, key K, value V) error

	// Get 获取指定键的值
	// 如果键不存在，返回零值和 ErrNotFound
	Get(ctx context.Context, key K) (V, error)

	// GetSet 获取旧值并设置新值
	// 如果键不存在，oldValue 将是零值且 err 为 ErrNotFound，但仍会设置新值
	GetSet(ctx context.Context, key K, value V) (oldValue V, err error)

	// Delete 删除指定键的值
	// 如果键不存在，返回 ErrNotFound
	Delete(ctx context.Context, key K) error

	// Has 检查键是否存在
	Has(ctx context.Context, key K) bool

	// Clear 清空所有键值对
	Clear(ctx context.Context) error

	// Keys 返回所有键的切片
	Keys(ctx context.Context) []K
}

// ErrNotFound 表示键不存在的错误
var ErrNotFound = errors.New("key not found")
