# levelsql: A SQL database built on top of leveldb

A very barebones implementation of a SQL database using leveldb as the storage engine. The way values are stored in leveldb takes inspiration from what CockroachDB does.

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

>> 
```

## Benchmarks

```
goos: linux
goarch: amd64
pkg: github.com/nireo/levelsql
cpu: AMD Ryzen 5 5600X 6-Core Processor             
BenchmarkInsert-12    	  245182	      5428 ns/op	    1329 B/op	      29 allocs/op
BenchmarkSelect-12    	    3804	    408066 ns/op	  187352 B/op	    3069 allocs/op
PASS
```