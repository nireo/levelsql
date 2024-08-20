package boltsql

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"

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

type Storage struct {
	db *leveldb.DB
}

func NewStorage(dbPath string) (*Storage, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

type row struct {
	Fields [][]byte
	Cells  []value
}

func newRow(fields [][]byte) *row {
	return &row{
		Fields: fields,
		Cells:  make([]value, 0, len(fields)),
	}
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

func (s *Storage) writeRow(table string, row *row) error {
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

type rowIterator struct {
	iter   iterator.Iterator
	fields [][]byte
}

func (ri *rowIterator) Next() (*row, bool) {
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

func (ri *rowIterator) Close() {
	ri.iter.Release()
}

func (s *Storage) GetrowIterator(table string) (*rowIterator, error) {
	prefix := []byte(fmt.Sprintf("row_%s_", table))
	iter := s.db.NewIterator(util.BytesPrefix(prefix), nil)

	tableInfo, err := s.getTable(table)
	if err != nil {
		iter.Release()
		return nil, err
	}

	return &rowIterator{
		iter:   iter,
		fields: tableInfo.Columns,
	}, nil
}

type table struct {
	Name    string
	Columns [][]byte
	Types   []string
}

func (s *Storage) writeTable(table *table) error {
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

func (s *Storage) getTable(name string) (*table, error) {
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
