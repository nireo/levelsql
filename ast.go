package levelsql

import (
	"fmt"
	"strings"
)

// this file contains a simple ast representation for sql. ast nodes have a debug string function
type node interface {
	String() string
}

type binopNode struct {
	left  node
	right node

	op token
}

type functionCallNode struct {
	args []node
	name token
}

func (f *functionCallNode) String() string {
	var b strings.Builder
	b.WriteString(f.name.content)
	b.WriteByte('(')
	for idx, arg := range f.args {
		b.WriteString(arg.String())
		if idx < len(f.args)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteByte(')')

	return b.String()
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

type createTableColumn struct {
	name token
	kind token
}

type createTableNode struct {
	table   token
	columns []createTableColumn
}

func (c *createTableNode) String() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", c.table.content))

	for i, col := range c.columns {
		b.WriteString(col.name.content + " " + col.kind.content)
		if i < len(c.columns)-1 {
			b.WriteRune(',')
		}
		b.WriteRune('\n')
	}
	b.WriteString(")\n")
	return b.String()
}

type insertNode struct {
	table  token
	values []node
}

func (i *insertNode) String() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("INSERT INTO %s VALUES(", i.table.content))
	for idx, val := range i.values {
		b.WriteString(val.String())
		if idx < len(i.values)-1 {
			b.WriteRune(',')
		}
	}

	b.WriteString(")\n")

	return b.String()
}
