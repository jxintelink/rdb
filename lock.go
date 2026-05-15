package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const unlockIfValueMatchesScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// Lock 简单的分布式锁，用于多节点防止冲突
func (r *RedisDB) Lock(ctx context.Context, key string, val interface{}, ttl time.Duration) (bool, error) {
	_, err := r.Client.SetArgs(ctx, key, val, redis.SetArgs{
		TTL:  ttl,
		Mode: "NX",
	}).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RedisDB) Unlock(ctx context.Context, key string, val interface{}) (bool, error) {
	res, err := r.Client.Eval(ctx, unlockIfValueMatchesScript, []string{key}, val).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// UnlockForce 强制解锁，不校验锁值（谨慎使用）
func (r *RedisDB) UnlockForce(ctx context.Context, key string) error {
	return r.Client.Del(ctx, key).Err()
}
