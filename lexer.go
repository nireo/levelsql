package boltsql

import (
	"strings"
	"unicode"
)

const (
	selectToken int = iota
	createTableToken
	insertToken
	valuesToken
	fromToken
	whereToken
	plusToken
	equalToken
	ltToken
	concattoken
	leftParenToken
	rightParenToken
	commaToken
	identifierToken
	integerToken
	stringToken
	invalidToken
)

type token struct {
	tokType int
	content string // content only filled if it matters for keywords empty
}

func (t *token) ok() bool {
	return t.tokType != invalidToken
}

type builtin struct {
	name    string
	tokType int
}

var builtins = [...]builtin{
	{name: "CREATE TABLE", tokType: createTableToken},
	{name: "INSERT INTO", tokType: insertToken},
	{name: "SELECT", tokType: selectToken},
	{name: "VALUES", tokType: valuesToken},
	{name: "WHERE", tokType: whereToken},
	{name: "FROM", tokType: fromToken},
	{name: "||", tokType: concattoken},
	{name: "=", tokType: equalToken},
	{name: "+", tokType: plusToken},
	{name: "<", tokType: ltToken},
	{name: "(", tokType: leftParenToken},
	{name: ")", tokType: rightParenToken},
	{name: ",", tokType: commaToken},
}

type lexer struct {
	index   int
	content string
}

func (l *lexer) whitespace() {
	for i := l.index; i < len(l.content); i++ {
		if !unicode.IsSpace(rune(l.content[i])) {
			l.index = i
			break
		}
	}
}

func (l *lexer) keyword() token {
	longest := 0
	ty := selectToken

	for _, b := range builtins {
		if l.index+len(b.name) >= len(l.content) {
			continue
		}

		if strings.EqualFold(l.content[l.index:len(b.name)], b.name) {
			longest = len(b.name)
			ty = b.tokType
			break
		}
	}

	if longest == 0 {
		return token{tokType: invalidToken}
	}

	l.index += longest
	return token{tokType: ty}
}

func (l *lexer) integer() token {
	start := l.index
	end := l.index
	currIdx := l.index

	for l.content[currIdx] >= '0' && l.content[currIdx] <= '9' {
		end += 1
		currIdx += 1
	}

	if start == end {
		return token{tokType: invalidToken}
	}
	l.index = end

	return token{tokType: integerToken, content: l.content[start:end]}
}

func (l *lexer) str() token {
	idx := l.index
	if l.content[idx] != '\'' {
		return token{tokType: invalidToken}
	}

	idx += 1

	start := idx
	end := idx

	for l.content[idx] != '\'' {
		end += 1
		idx += 1
	}

	if l.content[idx] == '\'' {
		idx += 1
	}

	if start == end {
		return token{tokType: invalidToken}
	}
	l.index = end
	return token{tokType: stringToken, content: l.content[start:end]}
}

func (l *lexer) identifier() token {
	start := l.index
	end := l.index
	idx := l.index

	for (l.content[idx] >= 'a' && l.content[idx] <= 'z') ||
		(l.content[idx] >= 'A' && l.content[idx] <= 'Z') ||
		l.content[idx] == '*' {
		end += 1
		idx += 1
	}

	if start == end {
		return token{tokType: invalidToken}
	}
	l.index = end

	return token{tokType: identifierToken, content: l.content[start:end]}
}

func (l *lexer) lex() []token {
	l.index = 0 // ensure that at start
	var tokens []token

	lexFuncs := []func() token{
		l.identifier,
		l.str,
		l.keyword,
		l.integer,
	}

	for {
		l.whitespace()
		if l.index >= len(l.content) {
			break
		}

		for _, lexFn := range lexFuncs {
			tok := lexFn()
			if tok.ok() {
				tokens = append(tokens, tok)
				break
			}
		}
	}

	return tokens
}
