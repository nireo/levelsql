package boltsql

import "testing"

func TestParser_Expression(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		shouldErr      bool
		expectedString string
	}{
		{"Basic equal", "hello = world", false, "hello = world"},
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
