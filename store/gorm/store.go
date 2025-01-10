package gorm

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"pkg.blksails.net/x/store"
)

// QueryOption 是查询选项函数
type QueryOption func(*gorm.DB) *gorm.DB

// KeyConverter 定义键转换器接口
type KeyConverter[K comparable] interface {
	// ToDBKey 将键转换为数据库查询条件
	ToDBKey(key K) interface{}
}

// DefaultKeyConverter 默认的键转换器，直接使用键值
type DefaultKeyConverter[K comparable] struct{}

func (c DefaultKeyConverter[K]) ToDBKey(key K) interface{} {
	return key
}

// FieldKeyConverter 字段键转换器，将键映射到指定字段
type FieldKeyConverter[K comparable] struct {
	Field string
}

func (c FieldKeyConverter[K]) ToDBKey(key K) interface{} {
	return map[string]interface{}{
		c.Field: key,
	}
}

// CompositeKeyConverter 复合键转换器，支持多字段组合
type CompositeKeyConverter[K comparable] struct {
	Convert func(key K) map[string]interface{}
}

func (c CompositeKeyConverter[K]) ToDBKey(key K) interface{} {
	return c.Convert(key)
}

// GormStore 是基于 GORM 的存储实现
type GormStore[K comparable, V any] struct {
	db           *gorm.DB
	keyConv      KeyConverter[K]
	beforeGet    []func(ctx context.Context, key K) error
	afterGet     []func(ctx context.Context, key K, value V) error
	beforeSet    []func(ctx context.Context, key K, value V) error
	afterSet     []func(ctx context.Context, key K, value V) error
	beforeDel    []func(ctx context.Context, key K) error
	afterDel     []func(ctx context.Context, key K) error
	getScopes    []func(*gorm.DB) *gorm.DB
	setScopes    []func(*gorm.DB) *gorm.DB
	deleteScopes []func(*gorm.DB) *gorm.DB
}

// StoreOption 配置 GormStore 的选项
type StoreOption[K comparable, V any] func(*GormStore[K, V])

// WithBeforeGet 添加 Get 操作前的钩子
func WithBeforeGet[K comparable, V any](hook func(ctx context.Context, key K) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.beforeGet = append(s.beforeGet, hook)
	}
}

// WithAfterGet 添加 Get 操作后的钩子
func WithAfterGet[K comparable, V any](hook func(ctx context.Context, key K, value V) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.afterGet = append(s.afterGet, hook)
	}
}

// WithBeforeSet 添加 Set 操作前的钩子
func WithBeforeSet[K comparable, V any](hook func(ctx context.Context, key K, value V) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.beforeSet = append(s.beforeSet, hook)
	}
}

// WithAfterSet 添加 Set 操作后的钩子
func WithAfterSet[K comparable, V any](hook func(ctx context.Context, key K, value V) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.afterSet = append(s.afterSet, hook)
	}
}

// WithBeforeDelete 添加 Delete 操作前的钩子
func WithBeforeDelete[K comparable, V any](hook func(ctx context.Context, key K) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.beforeDel = append(s.beforeDel, hook)
	}
}

// WithAfterDelete 添加 Delete 操作后的钩子
func WithAfterDelete[K comparable, V any](hook func(ctx context.Context, key K) error) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.afterDel = append(s.afterDel, hook)
	}
}

// WithKeyConverter 配置键转换器
func WithKeyConverter[K comparable, V any](conv KeyConverter[K]) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.keyConv = conv
	}
}

// WithGetScope 添加 Get 操作的查询范围
func WithGetScope[K comparable, V any](scope func(*gorm.DB) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.getScopes = append(s.getScopes, scope)
	}
}

// WithSetScope 添加 Set 操作的查询范围
func WithSetScope[K comparable, V any](scope func(*gorm.DB) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.setScopes = append(s.setScopes, scope)
	}
}

// WithDeleteScope 添加 Delete 操作的查询范围
func WithDeleteScope[K comparable, V any](scope func(*gorm.DB) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.deleteScopes = append(s.deleteScopes, scope)
	}
}

// NewGormStore 创建一个新的 GORM 存储
func NewGormStore[K comparable, V any](db *gorm.DB, opts ...StoreOption[K, V]) (*GormStore[K, V], error) {
	store := &GormStore[K, V]{
		db:      db,
		keyConv: DefaultKeyConverter[K]{}, // 默认使用直接转换
	}

	// 应用选项
	for _, opt := range opts {
		opt(store)
	}

	// 自动迁移表结构
	if err := db.AutoMigrate(new(V)); err != nil {
		return nil, err
	}

	return store, nil
}

// DB 返回底层的 GORM DB 实例
func (s *GormStore[K, V]) DB() *gorm.DB {
	return s.db
}

// Set 存储值
func (s *GormStore[K, V]) Set(ctx context.Context, key K, value V) error {
	// 执行前置钩子
	for _, hook := range s.beforeSet {
		if err := hook(ctx, key, value); err != nil {
			return err
		}
	}

	query := s.db.WithContext(ctx)
	// 应用 set scopes
	for _, scope := range s.setScopes {
		query = scope(query)
	}

	err := query.Save(&value).Error
	if err != nil {
		return err
	}

	// 执行后置钩子
	for _, hook := range s.afterSet {
		if err := hook(ctx, key, value); err != nil {
			return err
		}
	}

	return nil
}

// Get 获取指定键的值
func (s *GormStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	return s.GetWithOpts(ctx, key)
}

// GetWithOpts 带查询选项的 Get 方法
func (s *GormStore[K, V]) GetWithOpts(ctx context.Context, key K, opts ...QueryOption) (V, error) {
	var value V

	// 执行前置钩子
	for _, hook := range s.beforeGet {
		if err := hook(ctx, key); err != nil {
			return value, err
		}
	}

	query := s.db.WithContext(ctx)
	// 应用 get scopes
	for _, scope := range s.getScopes {
		query = scope(query)
	}
	// 应用查询选项
	for _, opt := range opts {
		query = opt(query)
	}

	err := query.First(&value, s.keyConv.ToDBKey(key)).Error
	if err == gorm.ErrRecordNotFound {
		return value, store.ErrNotFound
	}
	if err != nil {
		return value, err
	}

	// 执行后置钩子
	for _, hook := range s.afterGet {
		if err := hook(ctx, key, value); err != nil {
			return value, err
		}
	}

	return value, nil
}

// Delete 删除指定键的值
func (s *GormStore[K, V]) Delete(ctx context.Context, key K) error {
	// 执行前置钩子
	for _, hook := range s.beforeDel {
		if err := hook(ctx, key); err != nil {
			return err
		}
	}

	query := s.db.WithContext(ctx)
	// 应用 delete scopes
	for _, scope := range s.deleteScopes {
		query = scope(query)
	}

	result := query.Delete(new(V), s.keyConv.ToDBKey(key))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return store.ErrNotFound
	}

	// 执行后置钩子
	for _, hook := range s.afterDel {
		if err := hook(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

// Has 检查键是否存在
func (s *GormStore[K, V]) Has(ctx context.Context, key K) bool {
	return s.HasWithOpts(ctx, key)
}

// HasWithOpts 带查询选项的 Has 方法
func (s *GormStore[K, V]) HasWithOpts(ctx context.Context, key K, opts ...QueryOption) bool {
	var count int64
	query := s.db.WithContext(ctx).Model(new(V))

	// 应用查询选项
	for _, opt := range opts {
		query = opt(query)
	}

	err := query.Where(s.keyConv.ToDBKey(key)).Count(&count).Error
	return err == nil && count > 0
}

// Clear 清空所有值
func (s *GormStore[K, V]) Clear(ctx context.Context) error {
	return s.db.WithContext(ctx).Session(&gorm.Session{AllowGlobalUpdate: true}).
		Delete(new(V)).Error
}

// Keys 返回所有键
func (s *GormStore[K, V]) Keys(ctx context.Context) []K {
	var keys []K
	s.db.WithContext(ctx).Model(new(V)).Pluck("id", &keys)
	return keys
}

// SetGetBeforeHook 设置获取前置钩子
func (s *GormStore[K, V]) SetGetBeforeHook(hook func(ctx context.Context, key K) error) {
	s.beforeGet = append(s.beforeGet, hook)
}

// SetGetAfterHook 设置获取后置钩子
func (s *GormStore[K, V]) SetGetAfterHook(hook func(ctx context.Context, key K, value V) error) {
	s.afterGet = append(s.afterGet, hook)
}

// SetUpdateBeforeHook 设置更新前置钩子
func (s *GormStore[K, V]) SetUpdateBeforeHook(hook func(ctx context.Context, key K, value V) error) {
	s.beforeSet = append(s.beforeSet, hook)
}

// SetUpdateAfterHook 设置更新后置钩子
func (s *GormStore[K, V]) SetUpdateAfterHook(hook func(ctx context.Context, key K, value V) error) {
	s.afterSet = append(s.afterSet, hook)
}

// SetBeforeDeleteHook 设置删除前置钩子
func (s *GormStore[K, V]) SetBeforeDeleteHook(hook func(ctx context.Context, key K) error) {
	s.beforeDel = append(s.beforeDel, hook)
}

// SetAfterDeleteHook 设置删除后置钩子
func (s *GormStore[K, V]) SetAfterDeleteHook(hook func(ctx context.Context, key K) error) {
	s.afterDel = append(s.afterDel, hook)
}

// 一些常用的查询选项
func WithPreload(query string, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Preload(query, args...)
	}
}

func WithJoins(query string, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Joins(query, args...)
	}
}

func WithSelect(query interface{}, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Select(query, args...)
	}
}

func WithOrder(value interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Order(value)
	}
}

// WithWhere 添加 Where 条件
func WithWhere(query interface{}, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(query, args...)
	}
}

// WithOr 添加 Or 条件
func WithOr(query interface{}, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Or(query, args...)
	}
}

// WithGroup 添加 Group 条件
func WithGroup(name string) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Group(name)
	}
}

// WithHaving 添加 Having 条件
func WithHaving(query interface{}, args ...interface{}) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Having(query, args...)
	}
}

// WithLock 添加锁
func WithLock(lock clause.Locking) QueryOption {
	return func(db *gorm.DB) *gorm.DB {
		return db.Clauses(lock)
	}
}

// 一些预定义的锁模式
var (
	ForUpdate = clause.Locking{Strength: "UPDATE"}
	ForShare  = clause.Locking{Strength: "SHARE"}
)
