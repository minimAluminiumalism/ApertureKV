package aperturekv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/minimAluminiumalism/ApertureKV/data"
	"github.com/minimAluminiumalism/ApertureKV/index"
	"github.com/minimAluminiumalism/ApertureKV/utils"
)

type DB struct {
	options		Options
	mu			*sync.RWMutex
	fileIds		[]int						// 文件 id，只能在加载索引的时候使用，不能在其他地方更新和使用
	activeFile	*data.DataFile 				// 当前活跃文件，可以用于写入
	olderFiles	map[uint32]*data.DataFile	// 旧的文件，只能用于读
	index		index.Indexer				// 内存索引
	seqNo		uint64						// 事务序列号，全局递增
	reclaimSize	int64						// 当前有多少数据需要被 merge 掉/是无效数据
	isMerging	bool						// 数据库正在 merge 中_
}

type Stat struct {
	KeyNum			uint
	DataFileNum		uint	// 数据文件的数量
	ReclaimableSize	int64	// 可以 merge 回收的数据量(Bytes)
	DiskSize		int64	// 数据目录所占磁盘大小
}


func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	db := &DB{
		options: 	options,
		mu:			new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:		index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
	}
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	
	return db, nil
}

func (db *DB) Close() error {
	if db.activeFile != nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile != nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to count dir size: %v", err))
	}
	return &Stat{
		KeyNum:				uint(db.index.Size()),
		DataFileNum: 		dataFiles,
		ReclaimableSize: 	db.reclaimSize,
		DiskSize: 			dirSize,
	}
}

// 写入 Key/Value 数据
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	log_record := &data.LogRecord{
		Key:	logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value:	value,
		Type: 	data.LogRecordNormal,
	}
	// 追加写入文件到当前活跃文件中
	pos, err := db.appendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引
	/* 这里返回的是上一个在 oldPos 索引位置的元素，如果原来没有元素那就是 nil，
		如果有那就说明在 Put 之后被覆盖了，这里是一个无效的数据需要更新 reclaimSize。
	*/
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}


func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	logRecord := &data.LogRecord{
		Key: logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)	// 	删除了一条**数据记录**，所以这条数据是无效的，后面需要 merge

	oldPos, ok := db.index.Delete(key)

	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	
	// 从内存数据中读出 Key 对应的索引信息
	logRecordPos := db.index.Get(key)

	// key 不在内存索引中，说明 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中读取 value
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	idx := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}


// 获取所有的数据库数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte)bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移获取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
} 

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入数据已经达到了活跃文件的阈值，关闭当前的活跃文件，打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化磁盘防止数据丢失
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}

	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}


// 设置当前活跃文件
// 调用前必须加锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("Database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("Database data size must be an interger larger than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio which is must between 0 ansd 1")
	}
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)
	db.fileIds = fileIds
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds) - 1 { // 最后一个文件，说明当前的文件是活跃的
			db.activeFile = dataFile
		} else { 
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历所有文件中的记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(oldPos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo


	// 遍历所有文件 id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		
		if db.activeFile.FileId == fileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 文件都读完了，正常跳出循环
				if err == io.EOF {
					break
				}
				// 其他错误返回
				return err
			}
			// 构建内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid: 	fileId,
				Offset: offset,
				Size:	uint32(size),
			}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo { // 非事务操作
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对于 seqNo 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos: 	logRecordPos,
					})
				}
			}
			
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	db.seqNo = currentSeqNo
	return nil
}

