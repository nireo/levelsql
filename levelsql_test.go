package levelsql

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// Benchmarks

func setupDB(b *testing.B) *DB {
	dbPath := fmt.Sprintf("test_db_%d", b.N)
	db, err := OpenDB(dbPath)
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}

	b.Cleanup(func() {
		db.Close()
		os.RemoveAll(dbPath)
	})

	return db
}

func BenchmarkInsert(b *testing.B) {
	db := setupDB(b)
	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING)")
	if err != nil {
		b.Fatalf("failed to create table: %s", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i)
		_, err := db.Execute(query)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	db := setupDB(b)
	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 1000; i++ {
		query := fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d', %d)", i, i, 20+i%50)
		_, err := db.Execute(query)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Execute("SELECT id, name, age FROM users WHERE age < 30")
		if err != nil {
			b.Fatalf("Failed to select: %v", err)
		}
	}
}

func setupTestDB(t *testing.T) (*DB, func()) {
	dbPath := fmt.Sprintf("test_db_%d", rand.Int31())
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dbPath)
	}

	return db, cleanup
}

func TestCreateTable(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
}

func TestInsert(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}
}

func TestSelect(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	result, err := db.Execute("SELECT name, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(result.rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.rows))
	}

	if result.rows[0][0] != "Alice" || result.rows[0][1] != "30" {
		t.Fatalf("Unexpected result: %v", result.rows[0])
	}
}

func TestMultipleInsertAndSelect(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	_, err = db.Execute("INSERT INTO users VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	result, err := db.Execute("SELECT name FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(result.rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.rows))
	}

	if result.rows[0][0] != "Alice" {
		t.Fatalf("Unexpected result: %v", result.rows[0])
	}
}

func TestFunctionInSelect(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	result, err := db.Execute("SELECT CONCAT(name, '_suffix') FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select with function: %v", err)
	}

	if len(result.rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.rows))
	}

	if result.rows[0][0] != "Alice_suffix" {
		t.Fatalf("Unexpected result: %v", result.rows[0])
	}
}
