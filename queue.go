package rdb

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue 队列结构
type Queue struct {
	client        *redis.Client
	stream        string
	consumerGroup string
	readBlock     time.Duration
}

// NewQueue 改为接收 *redis.Client，这样它直接使用底层的连接池
func NewQueue(client *redis.Client, stream string, consumerGroup string) *Queue {
	return NewQueueWithBlock(client, stream, consumerGroup, 1*time.Second)
}

// NewQueueWithBlock 创建可配置阻塞读取时长的队列
func NewQueueWithBlock(client *redis.Client, stream string, consumerGroup string, readBlock time.Duration) *Queue {
	if readBlock < 0 {
		readBlock = 0
	}
	return &Queue{
		client:        client,
		stream:        stream,
		consumerGroup: consumerGroup,
		readBlock:     readBlock,
	}
}

// ProduceMsg 生产者推入单条消息
func (q *Queue) ProduceMsg(ctx context.Context, msg map[string]any) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: msg,
	}).Err()
}

// ProduceMsgs 批量推入消息（使用 Pipeline 提高性能）
func (q *Queue) ProduceMsgs(ctx context.Context, msgs []map[string]any) error {
	pipe := q.client.Pipeline()
	for _, msg := range msgs {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			Values: msg,
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ConsumeGroup 消费者组读取
func (q *Queue) ConsumeGroup(ctx context.Context, n int64, consumerName string) ([]redis.XMessage, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.consumerGroup,
		Consumer: consumerName,
		Streams:  []string{q.stream, ">"}, // ">" 表示只读从未交付给其他消费者的消息
		Count:    n,
		Block:    q.readBlock, // 默认短阻塞，避免空转消耗 CPU
	}).Result()

	if err != nil {
		if err == redis.Nil { // 队列为空
			return []redis.XMessage{}, nil
		}
		return nil, err
	}

	if len(streams) == 0 {
		return []redis.XMessage{}, nil
	}

	return streams[0].Messages, nil
}

// ConsumeGroup 消费者组读取，读取后立即确认并删除， 用于非可靠场景，如消息处理失败后需要重新处理
func (q *Queue) ConsumeWithAckDel(ctx context.Context, n int64, consumerName string) ([]redis.XMessage, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.consumerGroup,
		Consumer: consumerName,
		Streams:  []string{q.stream, ">"}, // ">" 表示只读从未交付给其他消费者的消息
		Count:    n,
		Block:    q.readBlock, // 默认短阻塞，避免空转消耗 CPU
	}).Result()

	if err != nil {
		if err == redis.Nil { // 队列为空
			return []redis.XMessage{}, nil
		}
		return nil, err
	}

	if len(streams) == 0 {
		return []redis.XMessage{}, nil
	}
	msgs := streams[0].Messages
	if len(msgs) == 0 {
		return []redis.XMessage{}, nil
	}

	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		ids = append(ids, msg.ID)
	}

	pipe := q.client.Pipeline()
	pipe.XAck(ctx, q.stream, q.consumerGroup, ids...)
	pipe.XDel(ctx, q.stream, ids...)
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, err
	}

	return msgs, nil
}

// AckMsg 确认消息（处理完必须调用，否则消息会进入 Pending 队列）
func (q *Queue) AckMsg(ctx context.Context, id string) error {
	return q.client.XAck(ctx, q.stream, q.consumerGroup, id).Err()
}

func (q *Queue) DelMsg(ctx context.Context, id string) error {
	return q.client.XDel(ctx, q.stream, id).Err()
}

// --- 以下是新增的实用功能 ---

// GetPendingMsgs 获取已读取但未确认的消息（用于处理崩溃重启后的补偿）
func (q *Queue) GetPendingMsgs(ctx context.Context, consumerName string, count int64) ([]redis.XMessage, error) {
	// "0" 表示从第一条未确认的消息开始读
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.consumerGroup,
		Consumer: consumerName,
		Streams:  []string{q.stream, "0"},
		Count:    count,
	}).Result()

	if err != nil && err != redis.Nil {
		return nil, err
	}
	if len(streams) == 0 {
		return []redis.XMessage{}, nil
	}
	return streams[0].Messages, nil
}

// InitializeConsumerGroup 初始化消费者组
func (q *Queue) InitializeConsumerGroup(ctx context.Context) error {
	// 使用 XGroupCreateMkStream，如果 Stream 不存在会自动创建
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.consumerGroup, "$").Err()
	if err != nil {
		// 忽略“组已存在”的错误
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return err
	}
	return nil
}

// IntializeConsumerGroup 兼容旧方法名（拼写保留）
func (q *Queue) IntializeConsumerGroup(ctx context.Context) error {
	return q.InitializeConsumerGroup(ctx)
}

// GetQueueLen 获取队列当前长度
func (q *Queue) GetQueueLen(ctx context.Context) (int64, error) {
	return q.client.XLen(ctx, q.stream).Result()
}

// Clear 清空队列
func (q *Queue) Clear(ctx context.Context) error {
	return q.client.XTrimMaxLen(ctx, q.stream, 0).Err()
}

// CheckQueueFull 检查队列是否已满
func (q *Queue) CheckQueueFull(ctx context.Context, length int) (bool, error) {
	len, err := q.GetQueueLen(ctx)
	if err != nil {
		return true, err
	}
	if len >= int64(length) {
		return true, nil
	}
	return false, nil
}
