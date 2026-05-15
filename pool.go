package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Host         string `yaml:"Host"`
	User         string `yaml:"User"`
	Password     string `yaml:"Password"`
	DB           int    `yaml:"DB"`
	PoolSize     int    `yaml:"PoolSize"`
	PoolTimeout  int    `yaml:"PoolTimeout"`
	DialTimeout  int    `yaml:"DialTimeout"`
	MinIdleConns int    `yaml:"MinIdleConns"`
}

func NewRedisConfig() *RedisConfig {
	return &RedisConfig{}

}

type RedisDB struct {
	Client *redis.Client
}

func NewRedisDB(rc *RedisConfig) (*RedisDB, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         rc.Host,
		Username:     rc.User,
		Password:     rc.Password,
		DB:           rc.DB,
		PoolSize:     rc.PoolSize,
		PoolTimeout:  time.Duration(rc.PoolTimeout) * time.Second,
		DialTimeout:  time.Duration(rc.DialTimeout) * time.Second,
		MinIdleConns: rc.MinIdleConns,
	})

	// 连通性检查
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &RedisDB{Client: client}, nil
}

func (r *RedisDB) Close() error {
	return r.Client.Close()
}
