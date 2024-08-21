package levelsql

import (
	"reflect"
	"testing"
)

func TestLexer_Whitespace(t *testing.T) {
	l := &lexer{content: "   abc"}
	l.whitespace()
	if l.index != 3 {
		t.Errorf("Expected index to be 3, got %d", l.index)
	}
}

func TestLexer_Keyword(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected token
		newIndex int
	}{
		{"SELECT keyword", "SELECT", token{tokType: selectToken}, 6},
		{"CREATE TABLE keyword", "CREATE TABLE", token{tokType: createTableToken}, 12},
		{"Invalid keyword", "INVALID", token{tokType: invalidToken}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lexer{content: tt.input}
			result := l.keyword()
			if result.tokType != tt.expected.tokType {
				t.Errorf("Expected token type %d, got %d", tt.expected.tokType, result.tokType)
			}
			if l.index != tt.newIndex {
				t.Errorf("Expected index to be %d, got %d", tt.newIndex, l.index)
			}
		})
	}
}

func TestLexer_Integer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected token
		newIndex int
	}{
		{"Valid integer", "123", token{tokType: integerToken, content: "123"}, 3},
		{"Invalid integer", "abc", token{tokType: invalidToken}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lexer{content: tt.input}
			result := l.integer()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected token %+v, got %+v", tt.expected, result)
			}
			if l.index != tt.newIndex {
				t.Errorf("Expected index to be %d, got %d", tt.newIndex, l.index)
			}
		})
	}
}

func TestLexer_String(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected token
		newIndex int
	}{
		{"Valid string", "'hello'", token{tokType: stringToken, content: "hello"}, 7},
		{"Invalid string", "hello", token{tokType: invalidToken}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lexer{content: tt.input}
			result := l.str()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected token %+v, got %+v", tt.expected, result)
			}
			if l.index != tt.newIndex {
				t.Errorf("Expected index to be %d, got %d", tt.newIndex, l.index)
			}
		})
	}
}

func TestLexer_Identifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected token
		newIndex int
	}{
		{"Valid identifier", "abc123", token{tokType: identifierToken, content: "abc"}, 3},
		{"Invalid identifier", "123abc", token{tokType: invalidToken}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lexer{content: tt.input}
			result := l.identifier()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected token %+v, got %+v", tt.expected, result)
			}
			if l.index != tt.newIndex {
				t.Errorf("Expected index to be %d, got %d", tt.newIndex, l.index)
			}
		})
	}
}

func TestLexer_Lex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []token
	}{
		{
			name:  "Simple SELECT query",
			input: "SELECT * FROM users WHERE id = 1",
			expected: []token{
				{tokType: selectToken},
				{tokType: identifierToken, content: "*"},
				{tokType: fromToken},
				{tokType: identifierToken, content: "users"},
				{tokType: whereToken},
				{tokType: identifierToken, content: "id"},
				{tokType: equalToken},
				{tokType: integerToken, content: "1"},
			},
		},
		{
			name:  "INSERT query",
			input: "INSERT INTO users VALUES ('John', 30)",
			expected: []token{
				{tokType: insertToken},
				{tokType: identifierToken, content: "users"},
				{tokType: valuesToken},
				{tokType: leftParenToken},
				{tokType: stringToken, content: "John"},
				{tokType: commaToken},
				{tokType: integerToken, content: "30"},
				{tokType: rightParenToken},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lexer{content: tt.input}
			result := l.lex()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected tokens %+v, got %+v", tt.expected, result)
			}
		})
	}
}
