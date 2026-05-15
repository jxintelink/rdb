package rdb

import (
	"context"
	"time"
)

// SetKV 函数用于设置键值对
func (r *RedisDB) SetKV(ctx context.Context, key string, value any) error {
	return r.Client.Set(ctx, key, value, 0).Err()
}

// SetEx 设置带过期时间的 KV，中间件验证时非常有用
func (r *RedisDB) SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.Client.Set(ctx, key, value, expiration).Err()
}

func (r *RedisDB) GetKV(ctx context.Context, key string) (string, error) {
	return r.Client.Get(ctx, key).Result()
}

// Del 增加删除操作
func (r *RedisDB) Del(ctx context.Context, keys ...string) error {
	return r.Client.Del(ctx, keys...).Err()
}

// SetHash 存储用户信息结构
func (r *RedisDB) SetHash(ctx context.Context, key string, values interface{}) error {
	return r.Client.HSet(ctx, key, values).Err()
}

// GetHash 获取用户信息
func (r *RedisDB) GetHashAll(ctx context.Context, key string) (map[string]string, error) {
	return r.Client.HGetAll(ctx, key).Result()
}
