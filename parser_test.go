package levelsql

import (
	"fmt"
	"testing"
)

func TestParser_Expression(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		shouldErr      bool
		expectedString string
	}{
		{"Basic equal", "hello = world", false, "hello = world"},
		{"Integer literal", "10", false, "10"},
		{"String literal", "'hello'", false, "hello"},
		{"Concat expression", "first || ' ' || last_name", false, "first   last"},
	}

	for i, tc := range tests {
		fmt.Println(i)
		l := lexer{
			content: tc.input,
			index:   0,
		}

		tokens := l.lex()
		p := parser{
			index:  0,
			tokens: tokens,
		}

		exp, err := p.expr()
		if tc.shouldErr && err == nil {
			t.Fatalf("should have error but don't\n")
		}

		if !tc.shouldErr && err != nil {
			t.Fatalf("got error even though shouldnt: %s\n", err)
		}

		if exp.String() != tc.expectedString {
			t.Fatalf("the resulting strings are not equal, got: %s | want: %s", exp.String(), tc.expectedString)
		}
	}
}

func TestParser_Select(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		shouldErr      bool
		expectedString string
	}{
		{"Basic select", "SELECT hello FROM world", false, "SELECT\n  hello\nFROM\n  world\n"},
		{"Multiple columns", "SELECT id, name, age FROM users", false, "SELECT\n  id,\n  name,\n  age\nFROM\n  users\n"},
	}

	for _, tc := range tests {
		l := lexer{
			content: tc.input,
			index:   0,
		}

		tokens := l.lex()
		p := parser{
			index:  0,
			tokens: tokens,
		}

		exp, err := p.pselect()
		if tc.shouldErr && err == nil {
			t.Fatalf("should have error but don't\n")
		}

		if !tc.shouldErr && err != nil {
			t.Fatalf("got error even though shouldnt: %s\n", err)
		}

		if exp.String() != tc.expectedString {
			t.Fatalf("the resulting strings are not equal, got: %s | want: %s", exp.String(), tc.expectedString)
		}
	}
}

func TestParser_CreateTable(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		shouldErr      bool
		expectedString string
	}{
		{"Basic CREATE TABLE", "CREATE TABLE users (id INTEGER, name TEXT)", false, "CREATE TABLE users (\nid INTEGER,\nname TEXT\n)\n"},
		{"Single column", "CREATE TABLE numbers (value INTEGER)", false, "CREATE TABLE numbers (\nvalue INTEGER\n)\n"},
		{"Multiple columns", "CREATE TABLE products (id INTEGER, name TEXT, price REAL, stock INTEGER)", false, "CREATE TABLE products (\nid INTEGER,\nname TEXT,\nprice REAL,\nstock INTEGER\n)\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := lexer{content: tc.input, index: 0}
			tokens := l.lex()
			p := parser{index: 0, tokens: tokens}

			result, err := p.createTable()

			if tc.shouldErr && err == nil {
				t.Fatalf("Expected error, but got none")
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if !tc.shouldErr && result.String() != tc.expectedString {
				t.Fatalf("Expected:\n%s\nBut got:\n%s", tc.expectedString, result.String())
			}
		})
	}
}

func TestParser_Insert(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		shouldErr      bool
		expectedString string
	}{
		{"Basic INSERT", "INSERT INTO users VALUES(1, 'John')", false, "INSERT INTO users VALUES(1,John)\n"},
		{"Multiple values", "INSERT INTO products VALUES(1, 'Laptop', 999, 50)", false, "INSERT INTO products VALUES(1,Laptop,999,50)\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := lexer{content: tc.input, index: 0}
			tokens := l.lex()
			p := parser{index: 0, tokens: tokens}

			result, err := p.insert()

			if tc.shouldErr && err == nil {
				t.Fatalf("Expected error, but got none")
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if !tc.shouldErr && result.String() != tc.expectedString {
				t.Fatalf("Expected:\n%s\nBut got:\n%s", tc.expectedString, result.String())
			}
		})
	}
}

func TestParser_Parse(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		shouldErr bool
	}{
		{"Valid SELECT", "SELECT name FROM users", false},
		{"Valid CREATE TABLE", "CREATE TABLE products (id INTEGER, name TEXT)", false},
		{"Valid INSERT", "INSERT INTO users VALUES(1, 'Alice')", false},
		{"Invalid statement", "DELETE FROM users", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := lexer{content: tc.input, index: 0}
			tokens := l.lex()
			p := parser{index: 0, tokens: tokens}

			_, err := p.parse()

			if tc.shouldErr && err == nil {
				t.Fatalf("Expected error, but got none")
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		})
	}
}
