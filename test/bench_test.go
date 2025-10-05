package test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type User struct {
	ID        uint `gorm:"primaryKey"`
	Name      string
	Email     string `gorm:"uniqueIndex"`
	CreatedAt time.Time
}

var (
	dbGorm *gorm.DB
	dbPgx  *pgxpool.Pool
)

const dsn = "postgres://postgres:postgrespassword@localhost:5432/benchdb?sslmode=disable"

func TestMain(m *testing.M) {
	var err error
	dbGorm, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:      logger.Default.LogMode(logger.Silent),
		PrepareStmt: true,
	})
	if err != nil {
		log.Fatalf("Failed to connect to GORM database: %v", err)
	}

	dbPgx, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Failed to create pgx connection pool: %v", err)
	}
	defer dbPgx.Close()

	if err := dbGorm.AutoMigrate(&User{}); err != nil {
		log.Fatalf("GORM auto migrate failed: %v", err)
	}
	dbGorm.Exec("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
	log.Println("Database setup complete.")

	code := m.Run()
	log.Println("Benchmark finished.")
	os.Exit(code)
}

// =================== 插入基准测试 ===================

func BenchmarkGormInsert(b *testing.B) {
	for i := 0; b.Loop(); i++ {
		user := User{Name: fmt.Sprintf("Gorm User %d", i), Email: fmt.Sprintf("gorm_user_%d@example.com", i)}
		if err := dbGorm.Create(&user).Error; err != nil {
			b.Fatalf("GORM insert failed: %v", err)
		}
	}
}

func BenchmarkPgxInsert(b *testing.B) {
	for i := 0; b.Loop(); i++ {
		name := fmt.Sprintf("Pgx User %d", i)
		email := fmt.Sprintf("pgx_user_%d@example.com", i)
		_, err := dbPgx.Exec(context.Background(), "INSERT INTO users (name, email, created_at) VALUES ($1, $2, NOW())", name, email)
		if err != nil {
			b.Fatalf("pgx insert failed: %v", err)
		}
	}
}

func BenchmarkPgxInsertCopy(b *testing.B) {
	const totalRows = 10000

	for b.Loop() {
		rows := make([][]any, 0, totalRows)
		for j := range totalRows {
			rows = append(rows, []any{
				fmt.Sprintf("Copy User %d", j),
				fmt.Sprintf("copy_user_%d@example.com", j),
			})
		}

		_, err := dbPgx.CopyFrom(
			context.Background(),
			pgx.Identifier{"users"},   // 表名
			[]string{"name", "email"}, // 列名
			pgx.CopyFromRows(rows),    // 数据源
		)
		if err != nil {
			b.Fatalf("pgx copy from failed: %v", err)
		}
	}
}

// =================== 查询基准测试 ===================

// 为查询测试准备一些数据
func prepareDataForQuery(count int) {
	dbGorm.Exec("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
	users := make([]User, count)
	for i := range count {
		users[i] = User{Name: fmt.Sprintf("Query User %d", i), Email: fmt.Sprintf("query_user_%d@example.com", i)}
	}
	dbGorm.Create(&users)
}

func BenchmarkGormQuery(b *testing.B) {
	const totalUsers = 1000
	prepareDataForQuery(totalUsers)

	for b.Loop() {
		randomID := rand.Intn(totalUsers) + 1
		var user User
		if err := dbGorm.First(&user, randomID).Error; err != nil {
			b.Fatalf("GORM query failed: %v", err)
		}
	}
}

func BenchmarkPgxQuery(b *testing.B) {
	const totalUsers = 1000
	prepareDataForQuery(totalUsers)

	for b.Loop() {
		randomID := rand.Intn(totalUsers) + 1
		var name, email string
		var createdAt time.Time
		err := dbPgx.QueryRow(context.Background(), "SELECT name, email, created_at FROM users WHERE id = $1", randomID).Scan(&name, &email, &createdAt)
		if err != nil {
			b.Fatalf("pgx query failed: %v", err)
		}
	}
}
