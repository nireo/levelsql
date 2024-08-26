package levelsql

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	nullVal int = iota
	boolVal
	stringVal
	integerVal
)

type value struct {
	ty         int
	boolVal    bool
	stringVal  string
	integerVal int64
}

func (v value) asBool() bool {
	switch v.ty {
	case nullVal:
		return false
	case boolVal:
		return v.boolVal
	case stringVal:
		return len(v.stringVal) > 0
	case integerVal:
		return v.integerVal != 0
	default:
		return false
	}
}

func (v value) asStr() string {
	switch v.ty {
	case nullVal:
		return ""
	case boolVal:
		return strconv.FormatBool(v.boolVal)
	case stringVal:
		return v.stringVal
	case integerVal:
		return strconv.FormatInt(v.integerVal, 10)
	default:
		return ""
	}
}

func (v value) asInt() int64 {
	switch v.ty {
	case nullVal:
		return 0
	case boolVal:
		if v.boolVal {
			return 1
		}
		return 0
	case stringVal:
		i, _ := strconv.ParseInt(v.stringVal, 10, 64)
		return i
	case integerVal:
		return v.integerVal
	default:
		return 0
	}
}

func (v value) bytes() []byte {
	var buf []byte
	switch v.ty {
	case nullVal:
		return []byte{0}
	case boolVal:
		if v.boolVal {
			return []byte{1, 1}
		}
		return []byte{1, 0}
	case stringVal:
		buf = make([]byte, 1+len(v.stringVal))
		buf[0] = 2
		copy(buf[1:], v.stringVal)
	case integerVal:
		buf = make([]byte, 9)
		buf[0] = 3
		binary.BigEndian.PutUint64(buf[1:], uint64(v.integerVal))
	}

	return buf
}

func deserializeValue(b []byte) value {
	if len(b) == 0 {
		return value{ty: nullVal}
	}

	switch b[0] {
	case 0:
		return value{ty: nullVal}
	case 1:
		return value{ty: boolVal, boolVal: b[1] == 1}
	case 2:
		return value{ty: stringVal, stringVal: string(b[1:])}
	case 3:
		return value{ty: integerVal, integerVal: int64(binary.BigEndian.Uint64(b[1:]))}
	default:
		return value{ty: nullVal}
	}
}

type storageIterator interface {
	Next() (*row, bool)
	Close() error
}

type storage interface {
	getTable(name string) (*table, error)
	writeRow(table string, row *row) error
	writeTable(table *table) error
	getRowIterator(table string) (storageIterator, error)
	Close() error
}

type leveldbStorage struct {
	db *leveldb.DB
}

type leveldbRowIterator struct {
	iter   iterator.Iterator
	fields [][]byte
}

func NewStorage(dbPath string) (storage, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return &leveldbStorage{db: db}, nil
}

func (s *leveldbStorage) Close() error {
	return s.db.Close()
}

type row struct {
	Fields [][]byte
	Cells  []value
}

// rows are often allocated a lot so reduce the amount of allocations
var rowPool = sync.Pool{
	New: func() interface{} {
		return &row{
			Fields: make([][]byte, 0),
			Cells:  make([]value, 0),
		}
	},
}

func getRow() *row {
	return rowPool.Get().(*row)
}

func putRow(r *row) {
	r.Fields = r.Fields[:0]
	r.Cells = r.Cells[:0]
	rowPool.Put(r)
}

func newRow(fields [][]byte) *row {
	r := getRow()
	r.reset(fields)

	return r
}

func (r *row) Release() {
	putRow(r)
}

func (r *row) reset(fields [][]byte) {
	r.Fields = r.Fields[:0]
	r.Fields = append(r.Fields, fields...)
	r.Cells = r.Cells[:0]
}

func (r *row) Append(v value) {
	r.Cells = append(r.Cells, v)
}

func (r *row) Get(field []byte) value {
	for i, f := range r.Fields {
		if bytes.Equal(f, field) {
			return r.Cells[i]
		}
	}
	return value{ty: nullVal}
}

func (s *leveldbStorage) writeRow(table string, row *row) error {
	key := make([]byte, 16)
	rand.Read(key)
	keyPrefix := fmt.Sprintf("row_%s_", table)
	fullKey := append([]byte(keyPrefix), key...)

	var value []byte
	for _, cell := range row.Cells {
		cellBytes := cell.bytes()
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(len(cellBytes)))
		value = append(value, lenBytes...)
		value = append(value, cellBytes...)
	}

	return s.db.Put(fullKey, value, nil)
}

func (ri *leveldbRowIterator) Next() (*row, bool) {
	if !ri.iter.Next() {
		return nil, false
	}
	value := ri.iter.Value()
	row := newRow(ri.fields)

	offset := 0
	for offset < len(value) {
		cellLen := binary.BigEndian.Uint64(value[offset : offset+8])
		offset += 8
		cellData := value[offset : offset+int(cellLen)]
		row.Append(deserializeValue(cellData))
		offset += int(cellLen)
	}

	return row, true
}

func (ri *leveldbRowIterator) Close() error {
	ri.iter.Release()
	return nil
}

func (s *leveldbStorage) getRowIterator(table string) (storageIterator, error) {
	prefix := []byte(fmt.Sprintf("row_%s_", table))
	iter := s.db.NewIterator(util.BytesPrefix(prefix), nil)

	tableInfo, err := s.getTable(table)
	if err != nil {
		iter.Release()
		return nil, err
	}

	return &leveldbRowIterator{
		iter:   iter,
		fields: tableInfo.Columns,
	}, nil
}

type table struct {
	Name    string
	Columns [][]byte
	Types   []string
}

func (s *leveldbStorage) writeTable(table *table) error {
	key := []byte(fmt.Sprintf("tbl_%s_", table.Name))
	var value []byte
	for i, column := range table.Columns {
		colLenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(colLenBytes, uint64(len(column)))
		value = append(value, colLenBytes...)
		value = append(value, column...)

		typeBytes := []byte(table.Types[i])
		typeLenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(typeLenBytes, uint64(len(typeBytes)))
		value = append(value, typeLenBytes...)
		value = append(value, typeBytes...)
	}

	return s.db.Put(key, value, nil)
}

func (s *leveldbStorage) getTable(name string) (*table, error) {
	key := []byte(fmt.Sprintf("tbl_%s_", name))
	value, err := s.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("no such table")
	} else if err != nil {
		return nil, err
	}

	table := &table{
		Name:    name,
		Columns: make([][]byte, 0),
		Types:   make([]string, 0),
	}

	offset := 0
	for offset < len(value) {
		colLen := binary.BigEndian.Uint64(value[offset : offset+8])
		offset += 8
		column := value[offset : offset+int(colLen)]
		offset += int(colLen)
		table.Columns = append(table.Columns, column)

		typeLen := binary.BigEndian.Uint64(value[offset : offset+8])
		offset += 8
		colType := string(value[offset : offset+int(typeLen)])
		offset += int(typeLen)
		table.Types = append(table.Types, colType)
	}

	return table, nil
}

type exec struct {
	storage storage
}

type QueryResponse struct {
	fields []string
	rows   [][]string
	empty  bool
}

func (qr *QueryResponse) String() string {
	if qr.empty {
		return "ok"
	}

	var b strings.Builder
	b.WriteString("| ")

	for _, field := range qr.fields {
		b.WriteString(field)
		b.WriteString("\t\t|")
	}
	b.WriteByte('\n')

	b.WriteString("+ ")
	for _, field := range qr.fields {
		b.WriteString(strings.Repeat("=", len(field)) + "\t\t+")
	}
	b.WriteByte('\n')

	for _, row := range qr.rows {
		b.WriteString("| ")
		for _, cell := range row {
			b.WriteString(cell)
			b.WriteString("\t\t|")
		}
		b.WriteByte('\n')
	}

	return b.String()
}

func (e *exec) executeBinop(binop *binopNode, row *row) (value, error) {
	lhs, err := e.executeExpression(binop.left, row)
	if err != nil {
		return value{}, nil
	}

	rhs, err := e.executeExpression(binop.right, row)
	if err != nil {
		return value{}, nil
	}

	if binop.op.tokType == equalToken {
		// TODO: rhs and lhs different types
		if rhs.ty != lhs.ty {
			return value{}, errors.New("equaling for different types")
		}

		switch lhs.ty {
		case nullVal:
			return value{ty: boolVal, boolVal: true}, nil
		case boolVal:
			return value{ty: boolVal, boolVal: lhs.boolVal == rhs.boolVal}, nil
		case stringVal:
			return value{ty: boolVal, boolVal: lhs.stringVal == rhs.stringVal}, nil
		case integerVal:
			return value{ty: boolVal, boolVal: lhs.integerVal == rhs.integerVal}, nil
		}
	}

	if binop.op.tokType == ltToken {

	}

	return value{ty: nullVal}, nil
}

func (e *exec) executeExpression(expr node, row *row) (value, error) {
	switch parsedNode := expr.(type) {
	case *literalNode:
		return e.executeLiteral(parsedNode, row)
	case *binopNode:
		return e.executeBinop(parsedNode, row)
	case *functionCallNode:
		return e.executeFunctionCall(parsedNode, row)
	}

	return value{}, nil
}

func (e *exec) executeLiteral(l *literalNode, row *row) (value, error) {
	litToken := l.lit
	switch litToken.tokType {
	case integerToken:
		convertedNum, err := strconv.Atoi(litToken.content)
		if err != nil {
			return value{}, nil
		}
		return value{ty: integerVal, integerVal: int64(convertedNum)}, nil
	case stringToken:
		return value{ty: stringVal, stringVal: litToken.content}, nil
	case identifierToken:
		return row.Get([]byte(litToken.content)), nil
	default:
		return value{}, nil
	}
}

func (e *exec) executeFunctionCall(fcn *functionCallNode, row *row) (value, error) {
	fn, ok := builtinFuncs[strings.ToLower(fcn.name.content)]
	if !ok {
		return value{}, fmt.Errorf("function was not found: %s", fcn.name.content)
	}

	return fn(e, row, fcn.args)
}

func (e *exec) executeSelect(sn *selectNode) (*QueryResponse, error) {
	_, err := e.storage.getTable(sn.from.content)
	if err != nil {
		return nil, fmt.Errorf("cannot get table: %s", err)
	}

	requestedFields := make([]string, 0, len(sn.columns))
	for _, col := range sn.columns {
		lit, ok := col.(*literalNode)
		if !ok {
			continue
		}

		if lit.lit.tokType == identifierToken {
			requestedFields = append(requestedFields, lit.lit.content)
		}
	}

	resp := &QueryResponse{
		fields: requestedFields,
		empty:  false,
	}

	iter, err := e.storage.getRowIterator(sn.from.content)
	if err != nil {
		return nil, fmt.Errorf("couldn't get row iterator")
	}
	defer iter.Close()

	row, ok := iter.Next()
	for ok {
		add := false
		if sn.where != nil {
			val, err := e.executeExpression(sn.where, row)
			if err != nil {
				row.Release()
				return nil, fmt.Errorf("something went wrong when executing where: %s", err)
			}

			add = val.asBool()
		} else {
			add = true
		}

		if add {
			var rowRes []string
			for _, col := range sn.columns {
				val, err := e.executeExpression(col, row)
				if err != nil {
					row.Release()
					return nil, fmt.Errorf("error executing expression: %s", err)
				}

				rowRes = append(rowRes, val.asStr())
			}

			resp.rows = append(resp.rows, rowRes)
		}

		row.Release()
		row, ok = iter.Next()
	}

	return resp, nil
}

func (e *exec) executeCreateTable(cn *createTableNode) (*QueryResponse, error) {
	cols := make([][]byte, 0, len(cn.columns))
	types := make([]string, 0, len(cn.columns))

	for _, col := range cn.columns {
		cols = append(cols, []byte(col.name.content))
		types = append(types, col.kind.content)
	}

	table := &table{
		Name:    cn.table.content,
		Types:   types,
		Columns: cols,
	}

	err := e.storage.writeTable(table)
	if err != nil {
		return nil, err
	}

	return &QueryResponse{empty: true}, nil
}

func (e *exec) executeInsert(in *insertNode) (*QueryResponse, error) {
	emptyRow := &row{}
	resRow := &row{}
	for _, val := range in.values {
		expr, err := e.executeExpression(val, emptyRow)
		if err != nil {
			return nil, err
		}

		resRow.Append(expr)
	}

	err := e.storage.writeRow(in.table.content, resRow)
	if err != nil {
		return nil, err
	}

	return &QueryResponse{empty: true}, nil
}

func (e *exec) execute(n node) (*QueryResponse, error) {
	switch astNode := n.(type) {
	case *insertNode:
		return e.executeInsert(astNode)
	case *createTableNode:
		return e.executeCreateTable(astNode)
	case *selectNode:
		return e.executeSelect(astNode)
	default:
		return nil, errors.New("executing a non-parent node")
	}
}
