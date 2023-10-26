# ApertureKV

A high performance KV engine based on optimized bitcask model.

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