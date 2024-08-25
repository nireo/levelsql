package levelsql

// This package contains the usable interface that can be embedded into code.
type DB struct {
	executor *exec
	parser   *parser
}

// OpenDB opens up a database for a file path.
func OpenDB(dbpath string) (*DB, error) {
	leveldbStorage, err := NewStorage(dbpath)
	if err != nil {
		return nil, err
	}

	return &DB{
		executor: &exec{
			storage: leveldbStorage,
		},
		parser: &parser{},
	}, nil
}

func (d *DB) Close() error {
	return d.executor.storage.Close()
}

func (d *DB) Execute(query string) (*QueryResponse, error) {
	lexer := lexer{
		index:   0,
		content: query,
	}
	tokens := lexer.lex()

	d.parser.reset(tokens)
	root, err := d.parser.parse()
	if err != nil {
		return nil, err
	}

	return d.executor.execute(root)
}
