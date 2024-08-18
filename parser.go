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

func (p *parser) pselect() (node, error) {
	if !p.expect(selectToken) {
		return nil, errors.New("expected select keyword")
	}
	p.index++

	sn := &selectNode{}
	for !p.expect(fromToken) {
		if len(sn.columns) > 0 {
			if !p.expect(commaToken) {
				return nil, errors.New("expected comma")
			}

			p.index++
		}

		colexpr, err := p.expr()
		if err != nil {
			return nil, err
		}

		sn.columns = append(sn.columns, colexpr)
	}

	if !p.expect(fromToken) {
		return nil, errors.New("expected FROM")
	}
	p.index++

	if !p.expect(identifierToken) {
		return nil, errors.New("expected FROM")
	}
	sn.from = p.tokens[p.index]
	p.index++

	if p.expect(whereToken) {
		p.index++
		whereexpr, err := p.expr()
		if err != nil {
			return nil, err
		}

		sn.where = whereexpr
	}

	if p.index < len(p.tokens) {
		return nil, errors.New("did not consume whole statement")
	}

	return sn, nil
}
