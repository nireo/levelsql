package boltsql

import "strings"

// this file contains a simple ast representation for sql. ast nodes have a debug string function

type node interface {
	String() string
}

type binopNode struct {
	left  node
	right node

	op token
}

func (b *binopNode) String() string {
	return b.left.String() + " " + b.op.content + " " + b.right.String()
}

type selectNode struct {
	columns []node
	from    token
	where   node // can be null
}

func (s *selectNode) String() string {
	var b strings.Builder

	b.WriteString("SELECT\n")
	for i, col := range s.columns {
		b.WriteString("  ")
		b.WriteString(col.String())
		if i < len(s.columns)-1 {
			b.WriteString(",")
		}
		b.WriteRune('\n')
	}

	b.WriteString("FROM\n")
	b.WriteString("  " + s.from.content)
	if s.where != nil {
		b.WriteString("\nWHERE\n")
		b.WriteString(s.where.String())
	}

	b.WriteRune('\n')
	return b.String()
}

type literalNode struct {
	lit token
}

func (l *literalNode) String() string {
	return l.lit.content
}
