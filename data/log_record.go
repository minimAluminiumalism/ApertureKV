package data

import (
	"encoding/binary"
	"hash/crc32"
)


type LogRecordType = byte

const (
	LogRecordNormal	LogRecordType = iota // 正常的日志
	LogRecordDeleted
	LogRecordTxnFinished
)

/* A complete LogRecord consists of 6 parts: 
+------------------------------------+
|	|    |		 |		   |   |	 |
|crc|type|keySize|valueSize|key|value|
| 4 |  1 |	 5	 |	  5	   |   |	 |
+------------------------------------+
the max length of header is 15
*/
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 // 15

// 一个 LogRecord 磁盘上的一条数据
type LogRecord struct {
	Key		[]byte
	Value	[]byte
	Type	LogRecordType
}

// LogRecord 头部
type logRecordHeader struct {
	crc			uint32
	recordType	LogRecordType
	keySize		uint32
	valueSize	uint32
}

// LogRecordPos 数据内存索引，主要描述数据在磁盘上的位置（参考 bitcask 论文）
type LogRecordPos struct {
	Fid		uint32 	// 文件 id，数据存在了哪个文件上
	Offset	int64 	// 存储在一个文件上的具体位置
	Size	uint32 	// 标识数据在磁盘上的大小
}

// 暂存的事务相关的数据
type TransactionRecord struct {
	Record	*LogRecord
	Pos		*LogRecordPos
}

// 编码 LogRecord，返回字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type
	index := 5

	// PutVarint 存储变长的数据
	// crc + type + keySize + valueSize
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	size := index + len(logRecord.Key)+ len(logRecord.Value)

	encBytes := make([]byte, size)
	// header 部分拷贝过来
	copy(encBytes[:index], header[:index]) // header(crc + type + keySize + valueSize)
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 小端序
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// fmt.Printf("header length: %d, crc: %d\n", index, crc)
	return encBytes, int64(size)
}

func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	idx := 0
	idx += binary.PutVarint(buf[idx:], int64(pos.Fid))
	idx += binary.PutVarint(buf[idx:], pos.Offset)
	idx += binary.PutVarint(buf[idx:], int64(pos.Size))
	return buf[:idx]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	idx := 0
	fileId, n := binary.Varint(buf[idx:])
	idx += n
	offset, n := binary.Varint(buf[idx:])
	idx += n
	size, _ := binary.Varint(buf[idx:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset, Size: uint32(size)}
}

// 解码 Header 信息
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc: 		binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	index := 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	
	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	/*
	+----------------------------+
	| type | keySize | valueSize |
	+----------------------------+
	without field `crc`.
	*/
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
