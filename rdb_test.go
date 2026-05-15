package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(func() {
		mr.Close()
	})

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func TestLockUnlockByValue(t *testing.T) {
	ctx := context.Background()
	r := &RedisDB{Client: newTestClient(t)}

	ok, err := r.Lock(ctx, "lock:key", "owner-a", 10*time.Second)
	if err != nil {
		t.Fatalf("lock first time error: %v", err)
	}
	if !ok {
		t.Fatalf("lock first time should succeed")
	}

	ok, err = r.Lock(ctx, "lock:key", "owner-b", 10*time.Second)
	if err != nil {
		t.Fatalf("lock second time error: %v", err)
	}
	if ok {
		t.Fatalf("lock second time should fail")
	}

	unlocked, err := r.Unlock(ctx, "lock:key", "owner-b")
	if err != nil {
		t.Fatalf("unlock with wrong owner error: %v", err)
	}
	if unlocked {
		t.Fatalf("unlock with wrong owner should fail")
	}

	got, err := r.Client.Get(ctx, "lock:key").Result()
	if err != nil {
		t.Fatalf("get lock value error: %v", err)
	}
	if got != "owner-a" {
		t.Fatalf("unexpected lock owner: %s", got)
	}

	unlocked, err = r.Unlock(ctx, "lock:key", "owner-a")
	if err != nil {
		t.Fatalf("unlock with owner error: %v", err)
	}
	if !unlocked {
		t.Fatalf("unlock with owner should succeed")
	}
}

func TestUnlockForce(t *testing.T) {
	ctx := context.Background()
	r := &RedisDB{Client: newTestClient(t)}

	ok, err := r.Lock(ctx, "force:key", "owner-a", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("lock before force unlock failed: ok=%v err=%v", ok, err)
	}

	if err := r.UnlockForce(ctx, "force:key"); err != nil {
		t.Fatalf("unlock force error: %v", err)
	}

	if _, err := r.Client.Get(ctx, "force:key").Result(); err != redis.Nil {
		t.Fatalf("force:key should be deleted, got err=%v", err)
	}
}

func TestQueueInitAndConsume(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	q := NewQueueWithBlock(client, "events", "group-a", 50*time.Millisecond)

	if err := q.IntializeConsumerGroup(ctx); err != nil {
		t.Fatalf("init consumer group by compatibility method error: %v", err)
	}
	if err := q.InitializeConsumerGroup(ctx); err != nil {
		t.Fatalf("init consumer group by new method error: %v", err)
	}

	if err := q.ProduceMsg(ctx, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("produce msg error: %v", err)
	}

	msgs, err := q.ConsumeGroup(ctx, 1, "consumer-1")
	if err != nil {
		t.Fatalf("consume msg error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expect 1 msg, got %d", len(msgs))
	}

	if err := q.AckMsg(ctx, msgs[0].ID); err != nil {
		t.Fatalf("ack msg error: %v", err)
	}
}

func TestQueueReadBlockNormalize(t *testing.T) {
	client := newTestClient(t)
	q := NewQueueWithBlock(client, "s", "g", -1*time.Second)
	if q.readBlock != 0 {
		t.Fatalf("negative readBlock should normalize to 0, got %v", q.readBlock)
	}
}

func TestKVOperations(t *testing.T) {
	ctx := context.Background()
	r := &RedisDB{Client: newTestClient(t)}

	if err := r.SetKV(ctx, "k1", "v1"); err != nil {
		t.Fatalf("set kv error: %v", err)
	}
	got, err := r.GetKV(ctx, "k1")
	if err != nil {
		t.Fatalf("get kv error: %v", err)
	}
	if got != "v1" {
		t.Fatalf("unexpected kv value: %s", got)
	}

	if err := r.SetEx(ctx, "k2", "v2", 5*time.Second); err != nil {
		t.Fatalf("setex error: %v", err)
	}
	ttl, err := r.Client.TTL(ctx, "k2").Result()
	if err != nil {
		t.Fatalf("ttl error: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("ttl should be positive, got %v", ttl)
	}

	if err := r.SetHash(ctx, "h1", map[string]any{"a": "1", "b": "2"}); err != nil {
		t.Fatalf("set hash error: %v", err)
	}
	m, err := r.GetHashAll(ctx, "h1")
	if err != nil {
		t.Fatalf("get hash all error: %v", err)
	}
	if m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("unexpected hash values: %#v", m)
	}

	if err := r.Del(ctx, "k1", "k2"); err != nil {
		t.Fatalf("del error: %v", err)
	}
	if _, err := r.Client.Get(ctx, "k1").Result(); err != redis.Nil {
		t.Fatalf("k1 should be deleted, got err=%v", err)
	}
}

func TestPoolNewAndClose(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	addr := client.Options().Addr
	_ = client.Close()

	rc := &RedisConfig{
		Host:         addr,
		User:         "",
		Password:     "",
		DB:           0,
		PoolSize:     5,
		PoolTimeout:  1,
		DialTimeout:  1,
		MinIdleConns: 1,
	}
	r, err := NewRedisDB(rc)
	if err != nil {
		t.Fatalf("new redis db error: %v", err)
	}

	if err := r.Client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping error: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if err := r.Client.Ping(ctx).Err(); err == nil {
		t.Fatalf("ping should fail after close")
	}
}

func TestQueueExtraOperations(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	q := NewQueue(client, "stream-extra", "group-extra")

	if q.readBlock != 1*time.Second {
		t.Fatalf("new queue default read block mismatch, got %v", q.readBlock)
	}
	if err := q.InitializeConsumerGroup(ctx); err != nil {
		t.Fatalf("init consumer group error: %v", err)
	}

	if err := q.ProduceMsgs(ctx, []map[string]any{
		{"n": "1"},
		{"n": "2"},
	}); err != nil {
		t.Fatalf("produce msgs error: %v", err)
	}

	full, err := q.CheckQueueFull(ctx, 2)
	if err != nil {
		t.Fatalf("check queue full error: %v", err)
	}
	if !full {
		t.Fatalf("queue should be full for threshold 2")
	}

	msgs, err := q.ConsumeGroup(ctx, 2, "consumer-extra")
	if err != nil {
		t.Fatalf("consume error: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatalf("expect consumed messages")
	}

	pending, err := q.GetPendingMsgs(ctx, "consumer-extra", 10)
	if err != nil {
		t.Fatalf("get pending msgs error: %v", err)
	}
	if len(pending) == 0 {
		t.Fatalf("expect pending messages")
	}

	if err := q.DelMsg(ctx, msgs[0].ID); err != nil {
		t.Fatalf("del msg error: %v", err)
	}

	if err := q.Clear(ctx); err != nil {
		t.Fatalf("clear queue error: %v", err)
	}
	n, err := q.GetQueueLen(ctx)
	if err != nil {
		t.Fatalf("get queue len error: %v", err)
	}
	if n != 0 {
		t.Fatalf("queue len should be 0 after clear, got %d", n)
	}
}

func TestConsumeWithAckDel(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	q := NewQueueWithBlock(client, "stream-ackdel", "group-ackdel", 50*time.Millisecond)

	if err := q.InitializeConsumerGroup(ctx); err != nil {
		t.Fatalf("init consumer group error: %v", err)
	}
	if err := q.ProduceMsgs(ctx, []map[string]any{
		{"k": "1"},
		{"k": "2"},
	}); err != nil {
		t.Fatalf("produce msgs error: %v", err)
	}

	msgs, err := q.ConsumeWithAckDel(ctx, 2, "consumer-ackdel")
	if err != nil {
		t.Fatalf("consume with ackdel error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expect 2 messages, got %d", len(msgs))
	}

	pending, err := q.GetPendingMsgs(ctx, "consumer-ackdel", 10)
	if err != nil {
		t.Fatalf("get pending msgs error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending should be empty after ack+del, got %d", len(pending))
	}

	n, err := q.GetQueueLen(ctx)
	if err != nil {
		t.Fatalf("get queue len error: %v", err)
	}
	if n != 0 {
		t.Fatalf("queue len should be 0 after consume ack+del, got %d", n)
	}
}

func TestLockUnlockErrorBranches(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	r := &RedisDB{Client: client}

	_ = client.Close()

	if _, err := r.Lock(ctx, "err-lock", "v", time.Second); err == nil {
		t.Fatalf("lock should return error after client close")
	}
	if _, err := r.Unlock(ctx, "err-lock", "v"); err == nil {
		t.Fatalf("unlock should return error after client close")
	}
}

func TestQueueEmptyAndErrorBranches(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	q := NewQueueWithBlock(client, "stream-branch", "group-branch", 10*time.Millisecond)

	if err := q.InitializeConsumerGroup(ctx); err != nil {
		t.Fatalf("init consumer group error: %v", err)
	}

	// 空流读取应返回空切片而非错误
	msgs, err := q.ConsumeGroup(ctx, 1, "consumer-branch")
	if err != nil {
		t.Fatalf("consume empty stream error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("empty stream should return 0 messages, got %d", len(msgs))
	}

	pending, err := q.GetPendingMsgs(ctx, "consumer-branch", 10)
	if err != nil {
		t.Fatalf("get pending on empty stream error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("empty pending should return 0 messages, got %d", len(pending))
	}

	full, err := q.CheckQueueFull(ctx, 1)
	if err != nil {
		t.Fatalf("check queue full error: %v", err)
	}
	if full {
		t.Fatalf("empty queue should not be full")
	}

	_ = client.Close()
	if err := q.InitializeConsumerGroup(ctx); err == nil {
		t.Fatalf("initialize should return error after client close")
	}
}

func TestNewRedisDBErrorBranch(t *testing.T) {
	rc := &RedisConfig{
		Host:         "127.0.0.1:1",
		User:         "",
		Password:     "",
		DB:           0,
		PoolSize:     1,
		PoolTimeout:  1,
		DialTimeout:  1,
		MinIdleConns: 0,
	}
	if _, err := NewRedisDB(rc); err == nil {
		t.Fatalf("new redis db should fail on unreachable host")
	}
}
