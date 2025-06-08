package delegated

import (
	"context"
	"time"

	"pkg.blksails.net/x/store"
)

// OperationType 定义存储操作的类型枚举
type OperationType int

const (
	// OperationGet 获取操作
	OperationGet OperationType = iota
	// OperationSet 设置操作
	OperationSet
	// OperationDelete 删除操作
	OperationDelete
	// OperationHas 检查存在操作
	OperationHas
	// OperationKeys 获取所有键操作
	OperationKeys
	// OperationClear 清空操作
	OperationClear
	// OperationGetSet 获取并设置操作
	OperationGetSet
)

// String 返回操作类型的字符串表示
func (op OperationType) String() string {
	switch op {
	case OperationGet:
		return "GET"
	case OperationSet:
		return "SET"
	case OperationDelete:
		return "DELETE"
	case OperationHas:
		return "HAS"
	case OperationKeys:
		return "KEYS"
	case OperationClear:
		return "CLEAR"
	case OperationGetSet:
		return "GETSET"
	default:
		return "UNKNOWN"
	}
}

// Middleware 定义存储操作的中间件
type Middleware[K comparable, V any] func(next OperationFunc[K, V]) OperationFunc[K, V]

// OperationFunc 表示存储操作的函数类型
type OperationFunc[K comparable, V any] func(ctx context.Context, op OperationType, key K, value V) (V, error)

// BackFillCallback 定义 backfill 操作的回调函数类型
// 参数：ctx 上下文, key 键, value 值, layerIndex 被 backfill 的层索引, isPrimary 是否为主存储层
// 返回值：(newValue V, useReturnValue bool) - newValue 是要回填的新值，useReturnValue 表示是否使用返回值
type BackFillCallback[K comparable, V any] func(ctx context.Context, key K, value V, layerIndex int, isPrimary bool) (V, bool)

// DelegatedStoreOpt 定义 DelegatedStore 的选项函数类型
type DelegatedStoreOpt[K comparable, V any] func(*DelegatedStore[K, V])

// WithBackFillCallback 设置 backfill 回调函数选项
func WithBackFillCallback[K comparable, V any](callback BackFillCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onBackFillFuncs = append(ds.onBackFillFuncs, callback)
	}
}

// WithBackFillCallbacks 设置多个 backfill 回调函数选项
func WithBackFillCallbacks[K comparable, V any](callbacks ...BackFillCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onBackFillFuncs = append(ds.onBackFillFuncs, callbacks...)
	}
}

// WithMiddleware 设置中间件选项
func WithMiddleware[K comparable, V any](middleware Middleware[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.middlewares = append(ds.middlewares, middleware)
	}
}

// WithMiddlewares 设置多个中间件选项
func WithMiddlewares[K comparable, V any](middlewares ...Middleware[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.middlewares = append(ds.middlewares, middlewares...)
	}
}

// WithOnChange 设置值变更回调函数选项
func WithOnChange[K comparable, V any](callback store.OnChangeCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onChange = append(ds.onChange, callback)
	}
}

// WithOnChangeCallbacks 设置多个值变更回调函数选项
func WithOnChangeCallbacks[K comparable, V any](callbacks ...store.OnChangeCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onChange = append(ds.onChange, callbacks...)
	}
}

// WithOnDelete 设置值删除回调函数选项
func WithOnDelete[K comparable, V any](callback store.OnDeleteCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onDelete = append(ds.onDelete, callback)
	}
}

// WithOnDeleteCallbacks 设置多个值删除回调函数选项
func WithOnDeleteCallbacks[K comparable, V any](callbacks ...store.OnDeleteCallback[K, V]) DelegatedStoreOpt[K, V] {
	return func(ds *DelegatedStore[K, V]) {
		ds.onDelete = append(ds.onDelete, callbacks...)
	}
}

// Layer 表示存储层配置
type Layer[K comparable, V any] struct {
	Store   store.Store[K, V] // 存储实现
	TTL     time.Duration     // 缓存时间，仅对非最后一层有效
	Primary bool              // 是否为主存储（写操作必须同步到主存储）
}

// DelegatedStore 是分层存储的委派实现
type DelegatedStore[K comparable, V any] struct {
	layers          []Layer[K, V]
	middlewares     []Middleware[K, V]
	onBackFillFuncs []BackFillCallback[K, V]
	onChange        []store.OnChangeCallback[K, V]
	onDelete        []store.OnDeleteCallback[K, V]
}

// NewDelegatedStore 创建一个新的分层存储
// layers 按优先级顺序排列，索引 0 具有最高优先级
// opts 可选的配置选项，支持设置回调函数、中间件等
//
// 使用示例：
//
//	// 基础创建
//	ds := NewDelegatedStore(layers)
//
//	// 带回调和中间件的创建
//	ds := NewDelegatedStore(layers,
//	    WithBackFillCallback(myCallback),
//	    WithMiddleware(loggingMiddleware),
//	)
func NewDelegatedStore[K comparable, V any](layers []Layer[K, V], opts ...DelegatedStoreOpt[K, V]) *DelegatedStore[K, V] {
	ds := &DelegatedStore[K, V]{
		layers:          layers,
		middlewares:     []Middleware[K, V]{},
		onBackFillFuncs: []BackFillCallback[K, V]{},
	}

	// 应用所有选项
	for _, opt := range opts {
		opt(ds)
	}

	return ds
}

// Use 添加中间件到存储
func (s *DelegatedStore[K, V]) Use(middlewares ...Middleware[K, V]) {
	s.middlewares = append(s.middlewares, middlewares...)
}

// applyMiddlewares 应用所有中间件到操作函数
func (s *DelegatedStore[K, V]) applyMiddlewares(operation OperationFunc[K, V]) OperationFunc[K, V] {
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		operation = s.middlewares[i](operation)
	}
	return operation
}

// Set 存储值到所有层
func (s *DelegatedStore[K, V]) Set(ctx context.Context, key K, value V) error {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, k K, v V) (V, error) {
		// 获取旧值用于变更回调
		var oldValue V
		var hasOldValue bool
		for _, layer := range s.layers {
			val, err := layer.Store.Get(ctx, k)
			if err == nil {
				oldValue = val
				hasOldValue = true
				break
			}
		}

		// 首先写入所有主存储
		for _, layer := range s.layers {
			if layer.Primary {
				if err := layer.Store.Set(ctx, k, v); err != nil {
					var zero V
					return zero, err
				}
			}
		}

		// 然后写入其他层并设置 TTL（如果支持）
		for _, layer := range s.layers {
			if !layer.Primary {
				if expirable, ok := layer.Store.(interface {
					SetWithTTL(context.Context, K, V, time.Duration) error
				}); ok && layer.TTL > 0 {
					if err := expirable.SetWithTTL(ctx, k, v, layer.TTL); err != nil {
						var zero V
						return zero, err
					}
				} else {
					if err := layer.Store.Set(ctx, k, v); err != nil {
						var zero V
						return zero, err
					}
				}
			}
		}

		// 执行变更回调
		if hasOldValue {
			for _, callback := range s.onChange {
				if err := callback(ctx, k, oldValue, v); err != nil {
					var zero V
					return zero, err
				}
			}
		}

		var zero V
		return zero, nil
	})

	_, err := op(ctx, OperationSet, key, value)
	return err
}

// Get 按优先级顺序获取值
func (s *DelegatedStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, k K, _ V) (V, error) {
		var lastErr error

		// 按优先级顺序查找
		for i, layer := range s.layers {
			value, err := layer.Store.Get(ctx, k)
			if err == nil {
				// 找到值后，异步更新之前的层
				if i > 0 {
					go s.backfill(context.Background(), k, value, i)
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
	})

	var zero V
	return op(ctx, OperationGet, key, zero)
}

// backfill 将值回填到更高优先级的层
func (s *DelegatedStore[K, V]) backfill(ctx context.Context, key K, value V, foundAt int) {
	for i := 0; i < foundAt; i++ {
		layer := s.layers[i]
		backfillValue := value // 默认使用原始值

		// 如果设置了回调函数，触发所有回调（使用 panic 恢复机制）
		for _, callback := range s.onBackFillFuncs {
			func(cb BackFillCallback[K, V]) {
				defer func() {
					if r := recover(); r != nil {
						// 记录 panic 但不影响 backfill 过程
						// 在生产环境中，这里应该使用适当的日志记录
						// log.Printf("OnBackFill callback panic recovered: %v", r)
					}
				}()
				if newValue, useReturnValue := cb(ctx, key, value, i, layer.Primary); useReturnValue {
					backfillValue = newValue
				}
			}(callback)
		}

		if expirable, ok := layer.Store.(interface {
			SetWithTTL(context.Context, K, V, time.Duration) error
		}); ok && layer.TTL > 0 {
			expirable.SetWithTTL(ctx, key, backfillValue, layer.TTL)
		} else {
			layer.Store.Set(ctx, key, backfillValue)
		}
	}
}

// Delete 删除指定键的值
func (s *DelegatedStore[K, V]) Delete(ctx context.Context, key K) error {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, k K, _ V) (V, error) {
		// 获取要删除的值用于回调
		var value V
		var hasValue bool
		for _, layer := range s.layers {
			val, err := layer.Store.Get(ctx, k)
			if err == nil {
				value = val
				hasValue = true
				break
			}
		}

		var lastErr error
		for _, layer := range s.layers {
			if err := layer.Store.Delete(ctx, k); err != nil && err != store.ErrNotFound {
				lastErr = err
			}
		}

		// 执行删除回调
		if hasValue {
			for _, callback := range s.onDelete {
				if err := callback(ctx, k, value); err != nil {
					var zero V
					return zero, err
				}
			}
		}

		var zero V
		return zero, lastErr
	})

	_, err := op(ctx, OperationDelete, key, *new(V))
	return err
}

// Has 检查任意层是否存在值
func (s *DelegatedStore[K, V]) Has(ctx context.Context, key K) bool {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, k K, _ V) (V, error) {
		for _, layer := range s.layers {
			if layer.Store.Has(ctx, k) {
				var zero V
				return zero, nil
			}
		}
		var zero V
		return zero, store.ErrNotFound
	})

	_, err := op(ctx, OperationHas, key, *new(V))
	return err == nil
}

// Clear 清空所有层
func (s *DelegatedStore[K, V]) Clear(ctx context.Context) error {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, _ K, _ V) (V, error) {
		var lastErr error
		for _, layer := range s.layers {
			if err := layer.Store.Clear(ctx); err != nil {
				lastErr = err
			}
		}
		var zero V
		return zero, lastErr
	})

	_, err := op(ctx, OperationClear, *new(K), *new(V))
	return err
}

// Keys 返回所有主存储的键
func (s *DelegatedStore[K, V]) Keys(ctx context.Context) []K {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, _ K, _ V) (V, error) {
		var zero V
		return zero, nil
	})

	// 为了保持接口一致，调用中间件但不使用其结果
	op(ctx, OperationKeys, *new(K), *new(V))

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

// GetLayer 获取指定索引的层
func (s *DelegatedStore[K, V]) GetLayer(index int) Layer[K, V] {
	return s.layers[index]
}

// GetSet 获取旧值并设置新值
func (s *DelegatedStore[K, V]) GetSet(ctx context.Context, key K, value V) (V, error) {
	op := s.applyMiddlewares(func(ctx context.Context, op OperationType, k K, v V) (V, error) {
		var oldValue V
		var foundAt = -1
		var lastErr error

		// 按优先级顺序查找旧值
		for i, layer := range s.layers {
			val, err := layer.Store.Get(ctx, k)
			if err == nil {
				oldValue = val
				foundAt = i
				break
			}
			if err != store.ErrNotFound {
				lastErr = err
			}
		}

		// 设置新值 - 使用内部实现而不是调用 s.Set 以避免重复应用中间件
		// 首先写入所有主存储
		for _, layer := range s.layers {
			if layer.Primary {
				if err := layer.Store.Set(ctx, k, v); err != nil {
					return oldValue, err
				}
			}
		}

		// 然后写入其他层并设置 TTL（如果支持）
		for _, layer := range s.layers {
			if !layer.Primary {
				if expirable, ok := layer.Store.(interface {
					SetWithTTL(context.Context, K, V, time.Duration) error
				}); ok && layer.TTL > 0 {
					if err := expirable.SetWithTTL(ctx, k, v, layer.TTL); err != nil {
						return oldValue, err
					}
				} else {
					if err := layer.Store.Set(ctx, k, v); err != nil {
						return oldValue, err
					}
				}
			}
		}

		// 如果找到了旧值，返回它
		if foundAt >= 0 {
			return oldValue, nil
		}

		// 如果没找到旧值，返回适当的错误
		if lastErr != nil {
			return oldValue, lastErr
		}
		return oldValue, store.ErrNotFound
	})

	return op(ctx, OperationGetSet, key, value)
}

// OnBackFill 添加 backfill 回调函数
// 当数据从低优先级层回填到高优先级层时，会触发所有已注册的回调
// 支持添加多个回调函数，它们会按注册顺序依次执行
//
// 回调参数说明：
//   - ctx: 上下文
//   - key: 被回填的键
//   - value: 被回填的值
//   - layerIndex: 被回填的层的索引（0 为最高优先级）
//   - isPrimary: 被回填的层是否为主存储层
//
// 回调返回值说明：
//   - newValue: 要回填的新值（如果 useReturnValue 为 true）
//   - useReturnValue: 是否使用返回的新值进行回填，false 表示使用原始值
//
// 使用场景：
//   - 监控缓存命中率和数据流动
//   - 记录主存储层的回填事件（可能表明缓存失效）
//   - 收集性能指标和访问模式
//   - 触发相关数据的预热
//   - 在回填时转换或修改数据值
//
// 使用示例：
//
//	// 添加监控回调（不修改值）
//	ds.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
//	    if isPrimary {
//	        log.Printf("警告: 主存储层回填 key=%s layer=%d", key, layerIndex)
//	    }
//	    return value, false // 不修改原始值
//	})
//
//	// 添加数据转换回调
//	ds.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
//	    // 在回填时对数据进行压缩或转换
//	    if layerIndex == 0 && strings.HasPrefix(key, "cache:") {
//	        compressedValue := compress(value)
//	        return compressedValue, true // 使用压缩后的值
//	    }
//	    return value, false // 使用原始值
//	})
//
//	// 添加指标收集回调
//	ds.OnBackFill(func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) (string, bool) {
//	    metrics.BackfillCounter.WithLabels("layer", fmt.Sprintf("%d", layerIndex)).Inc()
//	    return value, false // 不修改值，仅收集指标
//	})
func (s *DelegatedStore[K, V]) OnBackFill(callback BackFillCallback[K, V]) {
	s.onBackFillFuncs = append(s.onBackFillFuncs, callback)
}

// ClearBackFillCallbacks 清除所有 backfill 回调函数
func (s *DelegatedStore[K, V]) ClearBackFillCallbacks() {
	s.onBackFillFuncs = s.onBackFillFuncs[:0]
}
