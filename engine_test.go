package levelsql

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestStorage(t *testing.T) {
	// Create a temporary database for testing
	dbPath := "test.db"
	defer os.Remove(dbPath)

	// Initialize storage
	storage, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test table operations
	t.Run("Table Operations", func(t *testing.T) {
		tableName := "test_table"
		columns := [][]byte{[]byte("id"), []byte("name"), []byte("age")}
		types := []string{"integer", "string", "integer"}

		table := &table{
			Name:    tableName,
			Columns: columns,
			Types:   types,
		}

		err := storage.writeTable(table)
		if err != nil {
			t.Fatalf("Failed to write table: %v", err)
		}

		retrievedTable, err := storage.getTable(tableName)
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		if retrievedTable.Name != tableName {
			t.Errorf("Expected table name %s, got %s", tableName, retrievedTable.Name)
		}

		if len(retrievedTable.Columns) != len(columns) {
			t.Errorf("Expected %d columns, got %d", len(columns), len(retrievedTable.Columns))
		}

		for i, col := range retrievedTable.Columns {
			if !bytes.Equal(col, columns[i]) {
				t.Errorf("Expected column %s, got %s", columns[i], col)
			}
		}

		if len(retrievedTable.Types) != len(types) {
			t.Errorf("Expected %d types, got %d", len(types), len(retrievedTable.Types))
		}

		for i, typ := range retrievedTable.Types {
			if typ != types[i] {
				t.Errorf("Expected type %s, got %s", types[i], typ)
			}
		}
	})

	t.Run("Row Operations", func(t *testing.T) {
		tableName := "test_table"
		columns := [][]byte{[]byte("id"), []byte("name"), []byte("age")}

		row := newRow(columns)
		row.Append(value{ty: integerVal, integerVal: 1})
		row.Append(value{ty: stringVal, stringVal: "Alice"})
		row.Append(value{ty: integerVal, integerVal: 30})

		err := storage.writeRow(tableName, row)
		if err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}

		iter, err := storage.getRowIterator(tableName)
		if err != nil {
			t.Fatalf("Failed to get row iterator: %v", err)
		}
		defer iter.Close()

		retrievedRow, ok := iter.Next()
		if !ok {
			t.Fatal("Failed to retrieve row")
		}

		if len(retrievedRow.Cells) != 3 {
			t.Errorf("Expected 3 cells, got %d", len(retrievedRow.Cells))
		}

		if retrievedRow.Cells[0].ty != integerVal || retrievedRow.Cells[0].integerVal != 1 {
			t.Errorf("Expected id 1, got %d", retrievedRow.Cells[0].integerVal)
		}

		if retrievedRow.Cells[1].ty != stringVal || retrievedRow.Cells[1].stringVal != "Alice" {
			t.Errorf("Expected name Alice, got %s", retrievedRow.Cells[1].stringVal)
		}

		if retrievedRow.Cells[2].ty != integerVal || retrievedRow.Cells[2].integerVal != 30 {
			t.Errorf("Expected age 30, got %d", retrievedRow.Cells[2].integerVal)
		}

		idVal := retrievedRow.Get([]byte("id"))
		if idVal.ty != integerVal || idVal.integerVal != 1 {
			t.Errorf("Expected id 1, got %d", idVal.integerVal)
		}

		nameVal := retrievedRow.Get([]byte("name"))
		if nameVal.ty != stringVal || nameVal.stringVal != "Alice" {
			t.Errorf("Expected name Alice, got %s", nameVal.stringVal)
		}

		ageVal := retrievedRow.Get([]byte("age"))
		if ageVal.ty != integerVal || ageVal.integerVal != 30 {
			t.Errorf("Expected age 30, got %d", ageVal.integerVal)
		}
	})

	t.Run("Value Serialization", func(t *testing.T) {
		testCases := []struct {
			name  string
			input value
		}{
			{"Null", value{ty: nullVal}},
			{"True", value{ty: boolVal, boolVal: true}},
			{"False", value{ty: boolVal, boolVal: false}},
			{"String", value{ty: stringVal, stringVal: "test"}},
			{"Integer", value{ty: integerVal, integerVal: 12345}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				serialized := tc.input.bytes()
				deserialized := deserializeValue(serialized)

				if deserialized.ty != tc.input.ty {
					t.Errorf("Expected type %d, got %d", tc.input.ty, deserialized.ty)
				}

				switch tc.input.ty {
				case boolVal:
					if deserialized.boolVal != tc.input.boolVal {
						t.Errorf("Expected bool %v, got %v", tc.input.boolVal, deserialized.boolVal)
					}
				case stringVal:
					if deserialized.stringVal != tc.input.stringVal {
						t.Errorf("Expected string %s, got %s", tc.input.stringVal, deserialized.stringVal)
					}
				case integerVal:
					if deserialized.integerVal != tc.input.integerVal {
						t.Errorf("Expected integer %d, got %d", tc.input.integerVal, deserialized.integerVal)
					}
				}
			})
		}
	})
}

type mockStorage struct {
	tables map[string]*table
	rows   map[string][]*row
}

func (ms *mockStorage) getTable(name string) (*table, error) {
	table, ok := ms.tables[name]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}

	return table, nil
}

func (ms *mockStorage) getRowIterator(tableName string) (*RowIterator, error) {
	rows, ok := ms.rows[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}
	return &RowIterator{rows: rows, current: -1}, nil
}

type RowIterator struct {
	rows    []*row
	current int
}

func (ri *RowIterator) Next() (*row, bool) {
	ri.current++
	if ri.current < len(ri.rows) {
		return ri.rows[ri.current], true
	}
	return nil, false
}

func (ri *RowIterator) Close() {}

func setupTestData() *mockStorage {
	ms := &mockStorage{
		tables: make(map[string]*table),
		rows:   make(map[string][]*row),
	}

	ms.tables["users"] = &table{
		Name:    "users",
		Columns: [][]byte{[]byte("id"), []byte("name"), []byte("age")},
		Types:   []string{"integer", "string", "integer"},
	}

	ms.rows["users"] = []*row{
		{
			Fields: [][]byte{[]byte("id"), []byte("name"), []byte("age")},
			Cells: []value{
				{ty: integerVal, integerVal: 1},
				{ty: stringVal, stringVal: "Alice"},
				{ty: integerVal, integerVal: 30},
			},
		},
		{
			Fields: [][]byte{[]byte("id"), []byte("name"), []byte("age")},
			Cells: []value{
				{ty: integerVal, integerVal: 2},
				{ty: stringVal, stringVal: "Bob"},
				{ty: integerVal, integerVal: 25},
			},
		},
	}

	return ms
}

func TestExecuteExpression(t *testing.T) {
	e := &exec{}
	row := &row{
		Fields: [][]byte{[]byte("id"), []byte("name"), []byte("age")},
		Cells: []value{
			{ty: integerVal, integerVal: 1},
			{ty: stringVal, stringVal: "Alice"},
			{ty: integerVal, integerVal: 30},
		},
	}

	tests := []struct {
		name    string
		expr    node
		want    value
		wantErr bool
	}{
		{
			name:    "Literal integer",
			expr:    &literalNode{lit: token{tokType: integerToken, content: "42"}},
			want:    value{ty: integerVal, integerVal: 42},
			wantErr: false,
		},
		{
			name:    "Literal string",
			expr:    &literalNode{lit: token{tokType: stringToken, content: "hello"}},
			want:    value{ty: stringVal, stringVal: "hello"},
			wantErr: false,
		},
		{
			name:    "Identifier",
			expr:    &literalNode{lit: token{tokType: identifierToken, content: "name"}},
			want:    value{ty: stringVal, stringVal: "Alice"},
			wantErr: false,
		},
		{
			name: "Equal operation (true)",
			expr: &binopNode{
				left:  &literalNode{lit: token{tokType: identifierToken, content: "age"}},
				op:    token{tokType: equalToken},
				right: &literalNode{lit: token{tokType: integerToken, content: "30"}},
			},
			want:    value{ty: boolVal, boolVal: true},
			wantErr: false,
		},
		{
			name: "Equal operation (false)",
			expr: &binopNode{
				left:  &literalNode{lit: token{tokType: identifierToken, content: "age"}},
				op:    token{tokType: equalToken},
				right: &literalNode{lit: token{tokType: integerToken, content: "25"}},
			},
			want:    value{ty: boolVal, boolVal: false},
			wantErr: false,
		},
		{
			name: "Function call to lower",
			expr: &functionCallNode{
				name: token{tokType: identifierToken, content: "lower"},
				args: []node{
					&literalNode{
						lit: token{tokType: identifierToken, content: "name"},
					},
				},
			},
			want:    value{ty: stringVal, stringVal: "alice"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := e.executeExpression(tt.expr, row)
			if (err != nil) != tt.wantErr {
				t.Errorf("executeExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("executeExpression() got = %v, want %v", got, tt.want)
			}
		})
	}
}
