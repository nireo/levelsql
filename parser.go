package levelsql

import "errors"

type parser struct {
	tokens []token
	index  int
}

func (p *parser) reset(newTokens []token) {
	p.tokens = newTokens
	p.index = 0
}

func (p *parser) expect(ty int) bool {
	if p.index >= len(p.tokens) {
		return false
	}

	return p.tokens[p.index].tokType == ty
}

func (p *parser) consume(ty int) bool {
	if p.expect(ty) {
		p.index++
		return true
	}

	return false
}

func (p *parser) expr() (node, error) {
	var exp node
	canBeFunction := false
	callerToken := token{}
	if p.expect(integerToken) || p.expect(identifierToken) || p.expect(stringToken) {
		if p.tokens[p.index].tokType == identifierToken {
			canBeFunction = true
			callerToken = p.tokens[p.index]
		}

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
	} else if p.expect(leftParenToken) && canBeFunction {
		return p.parseFuncCall(callerToken)
	}

	return exp, nil
}

func (p *parser) parseFuncCall(callerToken token) (node, error) {
	callNode := &functionCallNode{
		name: callerToken,
	}

	if !p.consume(leftParenToken) {
		return nil, errors.New("need parenthesis before call arguments")
	}

	for !p.expect(rightParenToken) {
		if len(callNode.args) > 0 {
			if !p.consume(commaToken) {
				return nil, errors.New("expected comma")
			}
		}

		colexpr, err := p.expr()
		if err != nil {
			return nil, err
		}

		callNode.args = append(callNode.args, colexpr)
	}

	if !p.consume(rightParenToken) {
		return nil, errors.New("expected closing call")
	}

	return callNode, nil
}

func (p *parser) pselect() (node, error) {
	p.index = 0
	if !p.consume(selectToken) {
		return nil, errors.New("expected select keyword")
	}

	sn := &selectNode{}
	for !p.expect(fromToken) {
		if len(sn.columns) > 0 {
			if !p.consume(commaToken) {
				return nil, errors.New("expected comma")
			}
		}

		colexpr, err := p.expr()
		if err != nil {
			return nil, err
		}

		sn.columns = append(sn.columns, colexpr)
	}

	if !p.consume(fromToken) {
		return nil, errors.New("expected FROM")
	}

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

func (p *parser) createTable() (node, error) {
	p.index = 0

	if !p.consume(createTableToken) {
		return nil, errors.New("expected CREATE TABLE keyword")
	}

	if !p.expect(identifierToken) {
		return nil, errors.New("expected create table name")
	}

	var cols []createTableColumn
	cn := &createTableNode{
		table: p.tokens[p.index],
	}
	p.index++

	if !p.consume(leftParenToken) {
		return nil, errors.New("expected opening paren")
	}

	for !p.expect(rightParenToken) {
		if len(cols) > 0 {
			if !p.consume(commaToken) {
				return nil, errors.New("expected comma")
			}
		}

		col := createTableColumn{}
		if !p.expect(identifierToken) {
			return nil, errors.New("expected identifier")
		}
		col.name = p.tokens[p.index]
		p.index++

		if !p.expect(identifierToken) {
			return nil, errors.New("expected identifier")
		}
		col.kind = p.tokens[p.index]
		p.index++

		cols = append(cols, col)
	}

	p.index++
	if p.index < len(p.tokens) {
		return nil, errors.New("didn't read whole token stream")
	}

	cn.columns = cols
	return cn, nil
}

func (p *parser) insert() (node, error) {
	p.index = 0
	if !p.consume(insertToken) {
		return nil, errors.New("expected insert into keyword")
	}

	if !p.expect(identifierToken) {
		return nil, errors.New("expected identifier after insert into")
	}

	in := &insertNode{
		table: p.tokens[p.index],
	}
	p.index++

	if !p.consume(valuesToken) {
		return nil, errors.New("expected values token")
	}

	if !p.consume(leftParenToken) {
		return nil, errors.New("expected left paren")
	}

	var values []node
	for !p.expect(rightParenToken) {
		if len(values) > 0 {
			if !p.consume(commaToken) {
				return nil, errors.New("expected comma")
			}
		}

		v, err := p.expr()
		if err != nil {
			return nil, err
		}

		values = append(values, v)
	}
	p.index++

	if p.index < len(p.tokens) {
		return nil, errors.New("did not parse whole token stream")
	}
	in.values = values
	return in, nil
}

func (p *parser) parse() (node, error) {
	if p.expect(selectToken) {
		return p.pselect()
	}

	if p.expect(createTableToken) {
		return p.createTable()
	}

	if p.expect(insertToken) {
		return p.insert()
	}

	return nil, errors.New("unrecognized statement")
}
