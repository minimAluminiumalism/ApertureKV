package redis

import (
	"encoding/binary"
	"errors"
	"time"

	aperture "github.com/minimAluminiumalism/ApertureKV"
	"github.com/minimAluminiumalism/ApertureKV/utils"
)

var ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type redisDataType = byte

const (
	String	redisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDS struct {
	db		*aperture.DB
}

func NewRedisDS(options aperture.Options) (*RedisDS, error) {
	db, err := aperture.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDS{db: db}, nil
}

func (rds *RedisDS) Close() error {
	return rds.db.Close()
}

// String set
func (rds *RedisDS) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	/*
		encValue:
		+-----------------------+
		| type | ttl | rawValue |
		+-----------------------+
	*/
	buf := make([]byte, binary.MaxVarintLen64)
	buf[0] = String
	idx := 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	idx += binary.PutVarint(buf[idx:], expire)

	encValue := make([]byte, idx+len(value))
	copy(encValue[:idx], buf[idx:])
	copy(encValue[idx:], value)

	return rds.db.Put(key, encValue)
}

// String get 
func (rds *RedisDS) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	idx := 1
	expire, n := binary.Varint(encValue[idx:])
	idx += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[idx:], nil
}


func (rds *RedisDS) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	
	hk := &hashInternalKey{
		key: 		key,
		version:	meta.version,
		field: 		field,
	}
	encKey := hk.encode()

	exist := true
	if _, err := rds.db.Get(encKey); err == aperture.ErrKeyNotFound {
		exist = false
	}
	wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
	/*
		需要两步工作：更新元数据，更新数据
	*/
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDS) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key: 		key,
		version: 	meta.version,
		field: 		field,

	}
	return rds.db.Get(hk.encode())
}

func (rds *RedisDS) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key: 		key,
		version: 	meta.version,
		field: 		field,

	}
	encKey := hk.encode()
	exist := true
	if _, err := rds.db.Get(encKey); err == aperture.ErrKeyNotFound {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

func (rds *RedisDS) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == aperture.ErrKeyNotFound {
		// 不存在的话则更新
		wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDS) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != aperture.ErrKeyNotFound {
		return false, err
	}
	if err == aperture.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDS) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == aperture.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}


func (rds *RedisDS) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDS) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDS) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDS) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDS) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDS) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

func (rds *RedisDS) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != aperture.ErrKeyNotFound {
		return false, err
	}
	if err == aperture.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(aperture.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDS) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDS) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != aperture.ErrKeyNotFound {
		return nil, err
	}
	var meta *metadata
	exist := true
	if err == aperture.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: 	dataType,
			expire: 	0,
			version: 	time.Now().UnixNano(),
			size:		0,	
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
