package fio

const DateFilePerm = 0644

type FileIOType = byte

const (
	// 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

type IOManager interface {
	// 读取文件
	Read([]byte, int64) (int, error)
	// 写入文件
	Write([]byte) (int, error)

	// 持久化
	Sync() error

	// 关闭文件
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
 }

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
