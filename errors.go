package aperturekv

import "errors"


var (
	ErrKeyIsEmpty 				= errors.New("the key is empty")
	ErrIndexUpdateFailed		= errors.New("failed to update index")
	ErrKeyNotFound				= errors.New("key not found in database")
	ErrDataFileNotFound			= errors.New("data file not found")
	ErrDataDirectoryCorrupted	= errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum		= errors.New("exceed the max batch num")
	ErrMergeInProgress			= errors.New("merge is in progress, try again later")
	ErrMergeRatioUnreached		= errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge	= errors.New("no enough disk space for merge")
)