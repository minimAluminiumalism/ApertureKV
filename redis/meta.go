package redis

import (
	"encoding/binary"
	"math"

	"github.com/minimAluminiumalism/ApertureKV/utils"
)

const (
	maxMetadataSize		= 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32	// dataType + expire + version + size
	extraListMetaSize	= binary.MaxVarintLen64*2	// head + tail
	initialListMark 	= math.MaxUint64 / 2
)

type metadata struct {
	dataType	byte
	expire		int64
	version		int64
	size		uint32
	head 		uint64
	tail		uint64
}

func (md *metadata) encode() []byte {
	size := maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize // head 和 tail 是 List 特有的
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	idx := 1
	idx += binary.PutVarint(buf[idx:], md.expire)
	idx += binary.PutVarint(buf[idx:], md.version)
	idx += binary.PutVarint(buf[idx:], int64(md.size))
	
	if md.dataType == List {
		idx += binary.PutUvarint(buf[idx:], md.head)
		idx += binary.PutUvarint(buf[idx:], md.tail)
	}
	return buf[:idx]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	
	idx := 1
	expire, n := binary.Varint(buf[idx:])
	idx += n
	version, n := binary.Varint(buf[idx:])
	idx += n
	size, n := binary.Varint(buf[idx:])
	idx += n

	var head, tail uint64 = 0, 0
	if dataType == List {
		head, n = binary.Uvarint(buf[idx:])
		idx += n
		tail, _ = binary.Uvarint(buf[idx:])
	}
	return &metadata{
		dataType:	dataType,
		expire: 	expire,
		version: 	version,
		size:		uint32(size),
		head:		head,
		tail:		tail,
	}
}


type hashInternalKey struct {
	key		[]byte
	version	int64
	field 	[]byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.field))
	idx := 0
	copy(buf[:idx+len(hk.key)], hk.key)
	idx += len(hk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(hk.version))
	idx += 8
	copy(buf[idx:], hk.field)
	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

