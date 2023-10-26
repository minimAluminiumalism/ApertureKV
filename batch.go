package aperturekv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/minimAluminiumalism/ApertureKV/data"
)

const nonTransactionSeqNo uint64 = 0
var txnFinKey = []byte("txn-fin")

// 保证原子性
type WriteBatch struct {
	options			WriteBatchOptions
	mu				*sync.Mutex
	db				*DB
	pendingWrites	map[string]*data.LogRecord	// 暂存用户写入的数据
}


func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options: 		opts,
		mu: 			new(sync.Mutex),
		db:				db,
		pendingWrites: 	map[string]*data.LogRecord{},	
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	
	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {	// 索引（数据库）中不存在但是 wb 暂存中存在
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	

	// 暂存
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	
	// 数据库加锁保证串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 写数据到数据文件中
	postions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:	logRecordKeyWithSeq(record.Key, seqNo),
			Value: 	record.Value,
			Type: 	record.Type,
		})
		if err != nil {
			return err
		}
		postions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key: 	logRecordKeyWithSeq(txnFinKey, seqNo),
		Type:	data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	
	// 是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := postions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}


// key+seqNum 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}


func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}