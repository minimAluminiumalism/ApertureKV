package aperturekv

import "os"


type Options struct {
	DirPath				string		// 数据库数据目录
	DataFileSize		int64		// 数据文件的大小
	SyncWrites			bool		// 每次写数据是否持久化
	IndexType			IndexType	// 索引类型
	DataFileMergeRatio	float32
}

type IteratorOptions struct {
	Prefix		[]byte	// 遍历前缀为指定值的 key，默认为ks
	Reverse		bool	// 是否反向遍历
}

type WriteBatchOptions struct {
	MaxBatchNum	uint	// 一个批次中最大的数据量
	SyncWrites	bool	// 提交是是否 sync 持久化
}

type IndexType = int8

const (
	BTree	IndexType = iota + 1
	ART
	// B+ 树索引，将索引存储到本地磁盘
	BPlusTree
)

var DefaultOptions = Options {
	DirPath:			os.TempDir(),
	DataFileSize: 		256*1024*1024, // 256MB
	SyncWrites: 		false,
	IndexType: 			BTree,
	DataFileMergeRatio: 0.5, // 无效数据达到总数据的一半就 merge
}

var DefaultIteratorOptions = IteratorOptions {
	Prefix:		nil,
	Reverse: 	false,
}

var DefaultWriteBatchOptions = WriteBatchOptions {
	MaxBatchNum: 10000,
	SyncWrites: true,
}