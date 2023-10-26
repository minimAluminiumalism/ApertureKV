package redis

import "errors"

func (rds *RedisDS) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDS) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("the value is null")
	}
	return encValue[0], nil
}