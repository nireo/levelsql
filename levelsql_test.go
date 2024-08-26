package levelsql

import (
	"fmt"
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
