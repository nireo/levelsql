package boltsql

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
	colums []node
	from   token
	where  node // can be null
}

func (s *selectNode) String() string {
	// TODO: actual debug print
	return ""
}

type literalNode struct {
	lit token
}

func (l *literalNode) String() string {
	return l.lit.content
}
