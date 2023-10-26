# ApertureKV

A high performance KV engine based on optimized bitcask model.


### Benchmark result

Tested on 14-inch Macbook Pro wich 8 cores M1 Pro CPU 

```
goos: darwin
goarch: arm64
pkg: github.com/minimAluminiumalism/ApertureKV/benchmark
Benchmark_Put-8            87481             12702 ns/op            4672 B/op         10 allocs/op
Benchmark_Get-8          2332399               516.1 ns/op           135 B/op          4 allocs/op
Benchmark_Delete-8       2303164               516.0 ns/op           135 B/op          4 allocs/op
```
### To-do list

 - [x] sorted datastructure as index(B tree & ART & B+ tree)
 - [x] memory-stored index to disk(B+ tree)
 - [x] Start-up speed optimization(Hint file)
 - [x] Compatible with redis protocol
 - [ ] Index granularity optimization(min-heap & index fragment)

### Redis data structure design(example)

**Hash**

**Raw command in redis**
```
HSET myset a 100

myset: `key`
field: `a`
value: `100`
```

**metadata**
```
key => `metadata`

`metadata`
+--------------------------------+
| type | expire | version | size |
+--------------------------------+

size: the num of key-value pairs in the db
```

**raw data**
```
			     +-------+
encode(key|version|field) => | value |
			     +-------+
```


### Reference
- [Bitcask intro paper](https://riak.com/assets/bitcask-intro.pdf)