package gorm

import (
	"context"
	"reflect"
	"unsafe"

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
	db            *gorm.DB
	keyConv       KeyConverter[K]
	entityToModel func(entity V) any // 将实体转换为模型
	modelToEntity func(model any) V  // 将模型转换为实体
	beforeGet     []func(ctx context.Context, key K) error
	afterGet      []func(ctx context.Context, key K, value V) error
	beforeSet     []func(ctx context.Context, key K, value V) error
	afterSet      []func(ctx context.Context, key K, value V) error
	beforeDel     []func(ctx context.Context, key K) error
	afterDel      []func(ctx context.Context, key K) error
	getScopes     []func(*gorm.DB, K) *gorm.DB
	setScopes     []func(*gorm.DB, K, V) *gorm.DB
	deleteScopes  []func(*gorm.DB, K) *gorm.DB
	isptr         bool         // 是否是指针类型
	autoMigrate   bool         // 是否自动迁移表结构
	vType         reflect.Type // 值类型
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
func WithGetScope[K comparable, V any](scope func(*gorm.DB, K) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.getScopes = append(s.getScopes, scope)
	}
}

// WithSetScope 添加 Set 操作的查询范围
func WithSetScope[K comparable, V any](scope func(*gorm.DB, K, V) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.setScopes = append(s.setScopes, scope)
	}
}

// WithDeleteScope 添加 Delete 操作的查询范围
func WithDeleteScope[K comparable, V any](scope func(*gorm.DB, K) *gorm.DB) StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.deleteScopes = append(s.deleteScopes, scope)
	}
}

// WithModelConverter 配置模型转换器
func WithModelConverter[K comparable, V any](toModel func(V) any, fromModel func(any) V) StoreOption[K, V] {
	// 验证 toModel 和 fromModel 的参数类型
	if toModel == nil {
		panic("toModel is nil")
	}

	if fromModel == nil {
		panic("fromModel is nil")
	}

	m := toModel(newValue[V]())
	if reflect.TypeOf(m).Kind() != reflect.Ptr {
		panic("toModel must return a pointer")
	}

	fromModel(m)

	return func(s *GormStore[K, V]) {
		s.entityToModel = toModel
		s.modelToEntity = fromModel
	}
}

func newValue[V any]() V {
	if isPtr[V]() {
		return reflect.New(getVType[V]()).Interface().(V)
	} else {
		return reflect.New(getVType[V]()).Elem().Interface().(V)
	}
}

func WithAutoMigrate[K comparable, V any]() StoreOption[K, V] {
	return func(s *GormStore[K, V]) {
		s.autoMigrate = true
	}
}

// NewGormStore 创建一个新的 GORM 存储
func NewGormStore[K comparable, V any](db *gorm.DB, opts ...StoreOption[K, V]) (*GormStore[K, V], error) {
	store := &GormStore[K, V]{
		db:          db,
		keyConv:     DefaultKeyConverter[K]{}, // 默认使用直接转换
		isptr:       isPtr[V](),
		autoMigrate: false,
		vType:       getVType[V](),
	}

	// 应用选项
	for _, opt := range opts {
		opt(store)
	}

	// 自动迁移表结构
	if store.autoMigrate {
		var zero = store.newValue()
		model := store.convertToModel(zero)
		if err := db.AutoMigrate(model); err != nil {
			return nil, err
		}
	}

	return store, nil
}

// newValue 创建一个新值
func (s *GormStore[K, V]) newValueAt(p unsafe.Pointer) {
	reflect.NewAt(s.vType, p)
}

func (s *GormStore[K, V]) newValue() V {
	if s.isptr {
		return reflect.New(s.vType).Interface().(V)
	} else {
		return reflect.New(s.vType).Elem().Interface().(V)
	}
}

// 判断是否是指针类型
func isPtr[V any]() bool {
	var zero V
	return reflect.TypeOf(zero).Kind() == reflect.Ptr
}

func getVType[V any]() reflect.Type {
	var zero V
	if isPtr[V]() {
		return reflect.TypeOf(zero).Elem()
	} else {
		return reflect.TypeOf(zero)
	}
}

// DB 返回底层的 GORM DB 实例
func (s *GormStore[K, V]) DB() *gorm.DB {
	return s.db
}

// convertToModel 将实体转换为模型
func (s *GormStore[K, V]) convertToModel(v V) any {
	if s.entityToModel == nil {
		if s.isptr {
			return v
		} else {
			return &v
		}
	}

	return s.entityToModel(v)
}

// convertToEntity 将模型转换为实体
func (s *GormStore[K, V]) convertToEntity(m any) V {
	if s.modelToEntity == nil {
		if s.isptr {
			return m.(V)
		} else {
			return *m.(*V)
		}
	}

	return s.modelToEntity(m)
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
		query = scope(query, key, value)
	}

	model := s.convertToModel(value)
	err := query.Save(model).Error
	if err != nil {
		return err
	}
	value = s.convertToEntity(model)

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

	zero := s.newValue()
	model := s.convertToModel(zero)
	query := s.db.WithContext(ctx).Model(model)

	// 应用 get scopes
	for _, scope := range s.getScopes {
		query = scope(query, key)
	}
	// 应用查询选项
	for _, opt := range opts {
		query = opt(query)
	}

	err := query.First(model, s.keyConv.ToDBKey(key)).Error
	if err == gorm.ErrRecordNotFound {
		return value, store.ErrNotFound
	}
	if err != nil {
		return value, err
	}

	value = s.convertToEntity(model)

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
		query = scope(query, key)
	}

	var zero V
	model := s.convertToModel(zero)
	result := query.Delete(model, s.keyConv.ToDBKey(key))
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
	var zero V
	model := s.convertToModel(zero)
	query := s.db.WithContext(ctx).Model(model)

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

// GetSet 获取旧值并设置新值
func (s *GormStore[K, V]) GetSet(ctx context.Context, key K, value V) (oldValue V, err error) {
	// 开启事务
	tx := s.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return oldValue, tx.Error
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// 执行 Get 前置钩子
	for _, hook := range s.beforeGet {
		if err = hook(ctx, key); err != nil {
			return
		}
	}

	// 获取旧值，使用悲观锁
	var zero V
	oldModel := s.convertToModel(zero)

	// 应用 get scopes
	for _, scope := range s.getScopes {
		tx = scope(tx, key)
	}

	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		First(oldModel, s.keyConv.ToDBKey(key)).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return oldValue, err
	}

	if err != gorm.ErrRecordNotFound {
		oldValue = s.convertToEntity(oldModel)
		// 执行 Get 后置钩子
		for _, hook := range s.afterGet {
			if err = hook(ctx, key, oldValue); err != nil {
				return
			}
		}
	}

	// 执行 Set 前置钩子
	for _, hook := range s.beforeSet {
		if err = hook(ctx, key, value); err != nil {
			return
		}
	}

	// 保存新值
	model := s.convertToModel(value)
	// 应用 set scopes
	for _, scope := range s.setScopes {
		tx = scope(tx, key, value)
	}

	if err = tx.Save(model).Error; err != nil {
		return
	}

	// 执行 Set 后置钩子
	for _, hook := range s.afterSet {
		if err = hook(ctx, key, value); err != nil {
			return
		}
	}

	// 提交事务
	if err = tx.Commit().Error; err != nil {
		return
	}

	// 如果之前的值不存在，返回 ErrNotFound
	if err == gorm.ErrRecordNotFound {
		err = store.ErrNotFound
	}

	return
}
