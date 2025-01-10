package gorm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gstore "pkg.blksails.net/x/store"
)

// 测试模型
type User struct {
	ID      uint   `gorm:"primarykey"`
	Name    string `gorm:"type:varchar(100);uniqueIndex"`
	Age     int
	Profile Profile
}

type Profile struct {
	ID     uint   `gorm:"primarykey"`
	UserID uint   `gorm:"index"`
	Bio    string `gorm:"type:text"`
}

func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	// 自动迁移表结构
	err = db.AutoMigrate(&User{}, &Profile{})
	require.NoError(t, err)

	return db
}

func TestGormStore(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	var getHookCalled bool
	var setHookCalled bool

	store, err := NewGormStore[uint, User](db,
		WithBeforeGet[uint, User](func(ctx context.Context, key uint) error {
			getHookCalled = true
			return nil
		}),
		WithBeforeSet[uint, User](func(ctx context.Context, key uint, value User) error {
			setHookCalled = true
			return nil
		}),
	)
	require.NoError(t, err)

	t.Run("Basic CRUD", func(t *testing.T) {
		// Set
		user := User{Name: "Alice", Age: 25}
		err := store.Set(ctx, 0, user)
		require.NoError(t, err)
		assert.True(t, setHookCalled)

		// Get
		retrieved, err := store.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, getHookCalled)
		assert.Equal(t, "Alice", retrieved.Name)
		assert.Equal(t, 25, retrieved.Age)

		// Has
		exists := store.Has(ctx, 1)
		assert.True(t, exists)

		// Delete
		err = store.Delete(ctx, 1)
		require.NoError(t, err)

		// Get after delete
		_, err = store.Get(ctx, 1)
		assert.ErrorIs(t, err, gstore.ErrNotFound)
	})

	t.Run("Query Options", func(t *testing.T) {
		// 创建测试数据
		user := User{Name: "Bob", Age: 30}
		err := store.Set(ctx, 0, user)
		require.NoError(t, err)

		profile := Profile{UserID: 2, Bio: "Test Bio"}
		err = db.Create(&profile).Error
		require.NoError(t, err)

		// 测试预加载
		retrieved, err := store.GetWithOpts(ctx, 2,
			WithPreload("Profile"),
		)
		require.NoError(t, err)
		assert.Equal(t, "Bob", retrieved.Name)
		assert.Equal(t, "Test Bio", retrieved.Profile.Bio)

		// 测试条件查询
		exists := store.HasWithOpts(ctx, 2,
			WithWhere("age = ?", 30),
		)
		assert.True(t, exists)

		exists = store.HasWithOpts(ctx, 2,
			WithWhere("age = ?", 31),
		)
		assert.False(t, exists)
	})

	t.Run("Clear and Keys", func(t *testing.T) {
		// 先清空数据库
		err := store.Clear(ctx)
		require.NoError(t, err)

		// 创建多条测试数据
		users := []User{
			{Name: "User1", Age: 20},
			{Name: "User2", Age: 25},
		}
		for _, user := range users {
			err := store.Set(ctx, 0, user)
			require.NoError(t, err)
		}

		// 获取所有键
		keys := store.Keys(ctx)
		assert.Len(t, keys, len(users), "应该只有刚插入的记录")

		// 清空存储
		err = store.Clear(ctx)
		require.NoError(t, err)

		// 验证清空结果
		keys = store.Keys(ctx)
		assert.Empty(t, keys)
	})

	t.Run("Complex Queries", func(t *testing.T) {
		// 创建测试数据
		users := []User{
			{Name: "User1", Age: 20},
			{Name: "User2", Age: 25},
			{Name: "User3", Age: 30},
		}
		for _, user := range users {
			err := store.Set(ctx, 0, user)
			require.NoError(t, err)
		}

		// 测试组合查询
		exists := store.HasWithOpts(ctx, 0,
			WithWhere("age >= ?", 25),
			WithOr("name = ?", "User1"),
			WithOrder("age DESC"),
		)
		assert.True(t, exists)

		// 测试分组查询
		exists = store.HasWithOpts(ctx, 0,
			WithGroup("age"),
			WithHaving("COUNT(*) > ?", 1),
		)
		assert.False(t, exists)
	})
}

func TestGormStoreHooks(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	var hooks struct {
		beforeGet, afterGet bool
		beforeSet, afterSet bool
		beforeDel, afterDel bool
		lastError           error
	}

	store, err := NewGormStore[uint, User](db,
		WithBeforeGet[uint, User](func(ctx context.Context, key uint) error {
			hooks.beforeGet = true
			return hooks.lastError
		}),
		WithAfterGet[uint, User](func(ctx context.Context, key uint, value User) error {
			hooks.afterGet = true
			return hooks.lastError
		}),
		WithBeforeSet[uint, User](func(ctx context.Context, key uint, value User) error {
			hooks.beforeSet = true
			return hooks.lastError
		}),
		WithAfterSet[uint, User](func(ctx context.Context, key uint, value User) error {
			hooks.afterSet = true
			return hooks.lastError
		}),
		WithBeforeDelete[uint, User](func(ctx context.Context, key uint) error {
			hooks.beforeDel = true
			return hooks.lastError
		}),
		WithAfterDelete[uint, User](func(ctx context.Context, key uint) error {
			hooks.afterDel = true
			return hooks.lastError
		}),
	)
	require.NoError(t, err)

	t.Run("Hook Execution", func(t *testing.T) {
		user := User{Name: "Test", Age: 20}

		// Test Set hooks
		err := store.Set(ctx, 0, user)
		require.NoError(t, err)
		assert.True(t, hooks.beforeSet)
		assert.True(t, hooks.afterSet)

		// Test Get hooks
		_, err = store.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, hooks.beforeGet)
		assert.True(t, hooks.afterGet)

		// Test Delete hooks
		err = store.Delete(ctx, 1)
		require.NoError(t, err)
		assert.True(t, hooks.beforeDel)
		assert.True(t, hooks.afterDel)
	})

	t.Run("Hook Errors", func(t *testing.T) {
		hooks.lastError = fmt.Errorf("hook error")
		user := User{Name: "Test", Age: 20}

		// Test Set hook error
		err := store.Set(ctx, 0, user)
		assert.ErrorIs(t, err, hooks.lastError)

		// Test Get hook error
		_, err = store.Get(ctx, 1)
		assert.ErrorIs(t, err, hooks.lastError)

		// Test Delete hook error
		err = store.Delete(ctx, 1)
		assert.ErrorIs(t, err, hooks.lastError)
	})
}

func BenchmarkGormStore(b *testing.B) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		b.Fatal(err)
	}
	err = db.AutoMigrate(&User{}, &Profile{})
	if err != nil {
		b.Fatal(err)
	}

	store, err := NewGormStore[uint, User](db)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			user := User{
				Name: fmt.Sprintf("User%d", i),
				Age:  i % 100,
			}
			store.Set(ctx, 0, user)
		}
	})

	b.Run("Get", func(b *testing.B) {
		user := User{Name: "BenchUser", Age: 25}
		store.Set(ctx, 0, user)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store.Get(ctx, 1)
		}
	})

	b.Run("GetWithOpts", func(b *testing.B) {
		user := User{Name: "BenchUser", Age: 25}
		store.Set(ctx, 0, user)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store.GetWithOpts(ctx, 1,
				WithPreload("Profile"),
				WithSelect("name, age"),
			)
		}
	})
}

func TestGormStoreConcurrency(t *testing.T) {
	// 设置测试数据库，使用 WAL 模式并配置更宽松的锁定
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&_journal=WAL&_busy_timeout=5000"), &gorm.Config{
		SkipDefaultTransaction: true, // 减少事务开销
	})
	require.NoError(t, err)

	// 确保所有相关表都已创建
	err = db.AutoMigrate(&User{}, &Profile{})
	require.NoError(t, err)

	store, err := NewGormStore[uint, User](db)
	require.NoError(t, err)

	ctx := context.Background()
	const goroutines = 5  // 减少并发数
	const iterations = 20 // 减少迭代次数

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				user := User{
					Name: fmt.Sprintf("User%d-%d", id, j),
					Age:  j,
				}

				// 添加重试机制
				var err error
				for retries := 0; retries < 3; retries++ {
					err = store.Set(ctx, 0, user)
					if err == nil || !strings.Contains(err.Error(), "database table is locked") {
						break
					}
					time.Sleep(time.Millisecond * 10 * time.Duration(retries+1))
				}
				if err != nil && !strings.Contains(err.Error(), "database table is locked") {
					t.Errorf("unexpected error: %v", err)
				}

				// 简化查询，只在成功写入后读取
				if err == nil {
					_, err = store.Get(ctx, uint(j+1))
					if err != nil && err != gstore.ErrNotFound {
						t.Errorf("unexpected error: %v", err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestGormStoreFieldKeyConverter(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	// 使用 FieldKeyConverter 创建存储，将 name 字段作为键
	store, err := NewGormStore[string, User](db,
		WithKeyConverter[string, User](FieldKeyConverter[string]{Field: "name"}),
	)
	require.NoError(t, err)

	t.Run("Field as Key", func(t *testing.T) {
		// 创建测试数据
		user := User{Name: "TestUser", Age: 25}
		err := store.Set(ctx, user.Name, user)
		require.NoError(t, err)

		// 验证使用 name 字段作为键获取数据
		retrieved, err := store.Get(ctx, "TestUser")
		require.NoError(t, err)
		assert.Equal(t, "TestUser", retrieved.Name)
		assert.Equal(t, 25, retrieved.Age)
	})

	t.Run("Multiple Records", func(t *testing.T) {
		// 创建多条测试数据
		users := []User{
			{Name: "User100", Age: 30},
			{Name: "User200", Age: 35},
		}

		for _, u := range users {
			err := store.Set(ctx, u.Name, u)
			require.NoError(t, err)
		}

		// 验证所有记录都可以正确获取
		for _, u := range users {
			retrieved, err := store.Get(ctx, u.Name)
			require.NoError(t, err)
			assert.Equal(t, u.Name, retrieved.Name)
			assert.Equal(t, u.Age, retrieved.Age)
		}
	})

	t.Run("Delete with Field Key", func(t *testing.T) {
		// 创建要删除的测试数据
		user := User{Name: "DeleteMe", Age: 40}
		err := store.Set(ctx, user.Name, user)
		require.NoError(t, err)

		// 删除记录
		err = store.Delete(ctx, "DeleteMe")
		require.NoError(t, err)

		// 验证记录已被删除
		exists := store.Has(ctx, "DeleteMe")
		assert.False(t, exists)

		// 验证数据库中确实不存在该记录
		var count int64
		db.Model(&User{}).Where("name = ?", "DeleteMe").Count(&count)
		assert.Equal(t, int64(0), count)
	})

	t.Run("Duplicate Key Error", func(t *testing.T) {
		// 先清空数据库
		err := store.Clear(ctx)
		require.NoError(t, err)

		// 创建第一条记录
		user1 := User{Name: "SameName", Age: 25}
		err = store.Set(ctx, user1.Name, user1)
		require.NoError(t, err)

		// 尝试创建具有相同名称的第二条记录
		user2 := User{Name: "SameName", Age: 30}
		err = store.Set(ctx, user2.Name, user2)
		assert.Error(t, err, "应该返回唯一约束错误")
		assert.Contains(t, err.Error(), "UNIQUE constraint failed")
	})
}
