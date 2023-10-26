package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/minimAluminiumalism/ApertureKV/fio"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix		= ".data"
	HintFileName			= "hint-index"
	MergeFinishedFileName	= "merge-finished"
)

type DataFile struct {
	FileId		uint32			// 文件 ID
	WriteOff	int64			// 文件写到了哪个位置(offset)
	IoManager	fio.IOManager	// io 读写接口
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId) + DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId: fileId,
		WriteOff: 0,
		IoManager: ioManager,
	}, nil
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	filName := filepath.Join(dirPath, HintFileName)
	return newDataFile(filName, 0, fio.StandardFIO)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:	key,
		Value:	EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}


func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	
	// 如果读取的最大 header 长度超过了文件的长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset + maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	} 
	// 按最大 header 长度读取，实际的 header 可能没有那么长
	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 读取到了文件末尾，直接返回 EOF 错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	// 读取用户实际存储的 kv 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验 crc
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize]) // crc32.Size = 4
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) ReadNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}