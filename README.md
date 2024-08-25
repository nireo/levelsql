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
