# levelsql: A SQL database built on top of leveldb

A very barebones implementation of a SQL database using leveldb as the storage engine. The way values are stored in leveldb takes inspiration from what CockroachDB does.

At a high level, queries go through a small SQL pipeline: the lexer tokenizes the input, the parser builds an AST, and the executor walks that tree against LevelDB-backed storage. `CREATE TABLE` writes table metadata, `INSERT` serializes a row into bytes and stores it under a generated key, and `SELECT` scans the matching row prefix, deserializes each row, evaluates expressions and `WHERE` clauses, and then formats the matching values into the response.

## Example REPL

```
$ go run cmd/repl/main.go

>> CREATE TABLE test (hello integer, world text)   
ok
>> INSERT INTO test VALUES (1, 'yes')
ok
>> INSERT INTO test VALUES (2, 'no')
ok

>> SELECT hello, world FROM test
| hello         |world          |
+ =====         +=====          +
| 1             |yes            |
| 2             |no             |
```

## Benchmarks

Benchmarks were last run with `go test -bench=. -benchmem ./...`.

```
goos: darwin
goarch: arm64
pkg: github.com/nireo/levelsql
cpu: Apple M5
BenchmarkInsert-10    	  382814	      2712 ns/op	    1160 B/op	      25 allocs/op
BenchmarkSelect-10    	   12570	     95202 ns/op	   50723 B/op	    1863 allocs/op
PASS
```

## Encoding

Each table definition is stored under a `tbl_<table>_` key, where the value is a sequence of length-prefixed column names and types. Rows are stored under `row_<table>_<random 16-byte id>` keys, and each row value is a sequence of length-prefixed cells. Every cell begins with a 1-byte type tag (`0` for null, `1` for bool, `2` for string, `3` for integer); booleans store one extra byte, strings store their raw bytes, and integers are written as big-endian `uint64` values after the tag.
