package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/minimAluminiumalism/ApertureKV/data"
)


type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
		Delete(key []byte) (*data.LogRecordPos, bool)
	Iterator(reverse bool) Iterator	// 索引迭代器
	Size() int						// 索引中的数据量
}

type IndexType = int8

const (
	// Btree 索引
	Btree	IndexType = iota+1

	// ART 索引
	ART 

	// B+ 树索引
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type.")
	}
}

type Item struct {
	key	[]byte
	pos	*data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	// ai.key < bi.(*Item).key
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}


type Iterator interface {
	Rewind()					// 回到迭代器的起点
	Seek(key []byte)			//根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Next()						// 跳转到下一个 key
	Valid() bool				// 是否已经遍历完了所有的 key
	Key() []byte				// 当前遍历位置的 key 数据
	Value()	*data.LogRecordPos	// 当前遍历位置的 value 数据
	Close()						// 关闭迭代器
}

