package gorm

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gstore "pkg.blksails.net/store"
)

// 测试模型
type User struct {
	ID      uint   `gorm:"primarykey"`
	Name    string `gorm:"type:varchar(100)"`
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
		assert.Len(t, keys, 2)

		// 清空存储
		err := store.Clear(ctx)
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
	db := setupTestDB(t)
	store, err := NewGormStore[uint, User](db)
	require.NoError(t, err)

	ctx := context.Background()
	const goroutines = 10
	const iterations = 100

	done := make(chan bool)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < iterations; j++ {
				user := User{
					Name: fmt.Sprintf("User%d-%d", id, j),
					Age:  j,
				}
				err := store.Set(ctx, 0, user)
				assert.NoError(t, err)

				_, err = store.GetWithOpts(ctx, uint(j+1),
					WithPreload("Profile"),
					WithLock(ForShare),
				)
				// 忽略未找到的错误
				if err != nil && err != gstore.ErrNotFound {
					assert.NoError(t, err)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < goroutines; i++ {
		<-done
	}
}
