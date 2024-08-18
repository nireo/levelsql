package boltsql

import "errors"

type parser struct {
	tokens []token
	index  int
}

func (p *parser) expect(ty int) bool {
	if p.index >= len(p.tokens) {
		return false
	}

	return p.tokens[p.index].tokType == ty
}

func (p *parser) expr() (node, error) {
	var exp node
	if p.expect(integerToken) || p.expect(identifierToken) || p.expect(stringToken) {
		exp = &literalNode{lit: p.tokens[p.index]}
		p.index++
	} else {
		return nil, errors.New("no expression")
	}

	if p.expect(ltToken) || p.expect(equalToken) || p.expect(plusToken) || p.expect(concattoken) {
		binExp := &binopNode{
			left: exp,
			op:   p.tokens[p.index],
		}

		p.index++
		rhs, err := p.expr()
		if err != nil {
			return nil, err
		}

		binExp.right = rhs
		exp = binExp
	}

	return exp, nil
}
