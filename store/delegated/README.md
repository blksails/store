# Delegated Store

## 概述

`DelegatedStore` 是一个分层存储实现，支持多层缓存、回调机制和中间件系统。使用选项模式提供灵活的配置方式。

## 核心功能

- **分层存储**: 支持多层存储架构，按优先级自动回填数据
- **回调机制**: 监控数据在各存储层之间的流动情况 
- **中间件系统**: 支持操作拦截和自定义处理逻辑
- **选项模式**: 灵活的配置选项，支持组合使用
- **异步回填**: 非阻塞的数据回填操作

## 创建 DelegatedStore

### 基础创建

```go
// 定义存储层
layers := []Layer[string, string]{
    {Store: cacheStore, Primary: false, TTL: time.Minute},
    {Store: primaryStore, Primary: true},
    {Store: persistStore, Primary: false, TTL: time.Hour},
}

// 创建基础存储
ds := NewDelegatedStore(layers)
```

### 使用选项模式创建

```go
// 创建带回调和中间件的存储
ds := NewDelegatedStore(layers,
    WithBackFillCallback(monitoringCallback),
    WithMiddleware(loggingMiddleware),
    WithMiddleware(metricsMiddleware),
)
```

## 可用选项

### WithBackFillCallback

设置单个 backfill 回调函数：

```go
callback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    if isPrimary {
        log.Printf("主存储层回填: key=%s layer=%d", key, layerIndex)
    }
}

ds := NewDelegatedStore(layers, WithBackFillCallback(callback))
```

### WithBackFillCallbacks

设置多个 backfill 回调函数：

```go
monitoringCallback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    // 监控逻辑
}

metricsCallback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    // 指标收集逻辑  
}

ds := NewDelegatedStore(layers, WithBackFillCallbacks(monitoringCallback, metricsCallback))
```

### WithMiddleware

设置单个中间件：

```go
loggingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
    return func(ctx context.Context, key string, value string) (string, error) {
        log.Printf("操作: key=%s", key)
        return next(ctx, key, value)
    }
}

ds := NewDelegatedStore(layers, WithMiddleware(loggingMiddleware))
```

### WithMiddlewares

设置多个中间件：

```go
ds := NewDelegatedStore(layers, WithMiddlewares(
    loggingMiddleware,
    metricsMiddleware,
    retryMiddleware,
))
```

## 完整使用示例

### 监控和指标收集

```go
// 回调函数 - 用于监控和告警
monitoringCallback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    if isPrimary {
        // 主存储层回填可能表明缓存问题
        alerting.SendAlert("primary_layer_backfill", map[string]interface{}{
            "key":   key,
            "layer": layerIndex,
        })
    }
    
    // 发送指标
    metrics.Counter("backfill_total").Inc()
    metrics.Counter("backfill_by_layer").WithLabels(
        "layer", fmt.Sprintf("%d", layerIndex),
        "is_primary", fmt.Sprintf("%t", isPrimary),
    ).Inc()
}

// 指标收集回调
metricsCallback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    // 记录访问模式
    metricsCollector.RecordBackfill(key, layerIndex, isPrimary)
}

// 日志中间件
loggingMiddleware := func(next OperationFunc[string, string]) OperationFunc[string, string] {
    return func(ctx context.Context, key string, value string) (string, error) {
        start := time.Now()
        log.Printf("开始操作: key=%s", key)
        
        result, err := next(ctx, key, value)
        
        if err != nil {
            log.Printf("操作失败: key=%s, error=%v, duration=%v", key, err, time.Since(start))
        } else {
            log.Printf("操作成功: key=%s, duration=%v", key, time.Since(start))
        }
        
        return result, err
    }
}

// 重试中间件
retryMiddleware := RetryMiddleware[string, string](3, func(err error) bool {
    return err != nil && !errors.Is(err, store.ErrNotFound)
})

// 创建完整配置的存储
ds := NewDelegatedStore(layers,
    WithBackFillCallbacks(monitoringCallback, metricsCallback),
    WithMiddlewares(loggingMiddleware, retryMiddleware),
)
```

### 数据预热示例

```go
// 热点数据预热回调
preheatCallback := func(ctx context.Context, key string, value string, layerIndex int, isPrimary bool) {
    // 如果是用户数据的回填，预热相关数据
    if strings.HasPrefix(key, "user:") {
        userID := strings.TrimPrefix(key, "user:")
        
        // 异步预热用户的其他相关数据
        go func() {
            relatedKeys := []string{
                "user:profile:" + userID,
                "user:settings:" + userID,
                "user:permissions:" + userID,
            }
            
            for _, relatedKey := range relatedKeys {
                ds.Get(context.Background(), relatedKey)
            }
        }()
    }
}

ds := NewDelegatedStore(layers, WithBackFillCallback(preheatCallback))
```

## OnBackFill 回调功能

回调在以下情况下会被触发：

1. **Get 操作**: 当在低优先级层找到数据时，会触发对所有高优先级层的回填
2. **异步执行**: backfill 操作是异步执行的，不会阻塞主要的 Get 操作
3. **层级顺序**: 只有当数据在非第一层找到时才会触发回填
4. **多回调执行**: 所有注册的回调函数会按注册顺序依次执行

### 回调函数签名

```go
type BackFillCallback[K comparable, V any] func(
    ctx context.Context,
    key K,           // 被回填的键
    value V,         // 被回填的值
    layerIndex int,  // 被回填的层索引（0为最高优先级）
    isPrimary bool   // 是否为主存储层
)
```

## API 方法

### 构造函数

```go
func NewDelegatedStore[K comparable, V any](
    layers []Layer[K, V], 
    opts ...DelegatedStoreOpt[K, V]
) *DelegatedStore[K, V]
```

### 选项函数

```go
func WithBackFillCallback[K comparable, V any](callback BackFillCallback[K, V]) DelegatedStoreOpt[K, V]
func WithBackFillCallbacks[K comparable, V any](callbacks ...BackFillCallback[K, V]) DelegatedStoreOpt[K, V]
func WithMiddleware[K comparable, V any](middleware Middleware[K, V]) DelegatedStoreOpt[K, V]
func WithMiddlewares[K comparable, V any](middlewares ...Middleware[K, V]) DelegatedStoreOpt[K, V]
```

### 运行时方法

```go
func (s *DelegatedStore[K, V]) OnBackFill(callback BackFillCallback[K, V])
func (s *DelegatedStore[K, V]) ClearBackFillCallbacks()
func (s *DelegatedStore[K, V]) Use(middlewares ...Middleware[K, V])
```

## 注意事项

- 回调函数应该是轻量级的，避免阻塞 backfill 过程
- 如果需要执行重量级操作，建议在回调中启动新的 goroutine
- 回调函数中的错误不会影响 backfill 操作的执行
- 每个层的回填都会触发所有注册的回调函数
- 回调函数按注册顺序执行，早注册的先执行
- 中间件按倒序执行（后注册的先执行）
- 使用 `ClearBackFillCallbacks()` 可以清除所有回调函数

## 测试示例

查看 `store_test.go` 中的 `TestNewDelegatedStoreWithOptions` 和 `TestOnBackFillCallback` 函数了解完整的测试用例。 