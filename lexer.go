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
	for l.index < len(l.content) && unicode.IsSpace(rune(l.content[l.index])) {
		l.index++
	}
}

func (l *lexer) keyword() token {
	for _, b := range builtins {
		if l.index+len(b.name) <= len(l.content) &&
			strings.EqualFold(l.content[l.index:l.index+len(b.name)], b.name) {
			l.index += len(b.name)
			return token{tokType: b.tokType}
		}
	}
	return token{tokType: invalidToken}
}

func (l *lexer) integer() token {
	start := l.index
	for l.index < len(l.content) && l.content[l.index] >= '0' && l.content[l.index] <= '9' {
		l.index++
	}
	if start == l.index {
		return token{tokType: invalidToken}
	}
	return token{tokType: integerToken, content: l.content[start:l.index]}
}

func (l *lexer) str() token {
	if l.index >= len(l.content) || l.content[l.index] != '\'' {
		return token{tokType: invalidToken}
	}
	start := l.index + 1
	l.index++
	for l.index < len(l.content) && l.content[l.index] != '\'' {
		l.index++
	}
	if l.index >= len(l.content) {
		return token{tokType: invalidToken}
	}
	l.index++ // consume closing quote
	return token{tokType: stringToken, content: l.content[start : l.index-1]}
}

func (l *lexer) identifier() token {
	start := l.index
	for l.index < len(l.content) &&
		((l.content[l.index] >= 'a' && l.content[l.index] <= 'z') ||
			(l.content[l.index] >= 'A' && l.content[l.index] <= 'Z') ||
			l.content[l.index] == '*') {
		l.index++
	}
	if start == l.index {
		return token{tokType: invalidToken}
	}
	return token{tokType: identifierToken, content: l.content[start:l.index]}
}

func (l *lexer) lex() []token {
	var tokens []token
	lexFuncs := []func() token{
		l.keyword,
		l.identifier,
		l.str,
		l.integer,
	}
	for l.index < len(l.content) {
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
