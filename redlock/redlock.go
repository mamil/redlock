package redlock

import (
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

const (
	randomLen       = 16
	tolerance       = 500 // milliseconds
	millisPerSecond = 1000
	lockCommand     = `if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
    return "OK"
else
    return redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])
end`
	delCommand = `if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`
	compareCommand = `if redis.call("GET", KEYS[1]) == ARGV[1] then
	return 1
else
	return 0
end`
)

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = defaultLetters[rand.Intn(len(defaultLetters))]
	}

	return string(b)
}

// A RedisLock is a redis lock.
type RedisLock struct {
	store   *redis.Client
	seconds uint32
	key     string
	id      string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewRedisLock returns a RedisLock.
func NewRedisLock(store *redis.Client, key string) *RedisLock {
	return &RedisLock{
		store: store,
		key:   key,
		id:    randomString(randomLen),
	}
}

// Acquire acquires the lock.
func (rl *RedisLock) Acquire() (bool, error) {
	seconds := atomic.LoadUint32(&rl.seconds)
	resp, err := rl.store.Eval(lockCommand, []string{rl.key}, []string{
		rl.id, strconv.Itoa(int(seconds)*millisPerSecond + tolerance),
	}).Result()
	if err != nil {
		log.Errorf("Error on acquiring lock for %s, val:%s, err:%s", rl.key, rl.id, err.Error())
		return false, err
	} else if resp == nil {
		return false, nil
	}

	reply, ok := resp.(string)
	if ok && reply == "OK" {
		return true, nil
	}

	log.Errorf("Unknown reply when acquiring lock for %s: %v", rl.key, resp)
	return false, nil
}

// Release releases the lock.
func (rl *RedisLock) Release() (bool, error) {
	resp, err := rl.store.Eval(delCommand, []string{rl.key}, []string{rl.id}).Result()
	if err != nil {
		log.Errorf("Error on release lock for key:%s, val:%s, err:%s", rl.key, rl.id, err.Error())
		return false, err
	}

	reply, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return reply == 1, nil
}

// Compare if the lock is locked by myself
func (rl *RedisLock) Compare() (bool, error) {
	resp, err := rl.store.Eval(compareCommand, []string{rl.key}, []string{rl.id}).Result()
	if err != nil {
		log.Errorf("Error on Compare lock for key:%s, val:%s, err:%s", rl.key, rl.id, err.Error())
		return false, err
	}

	reply, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return reply == 1, nil
}

// SetExpire sets the expiration.
func (rl *RedisLock) SetExpire(seconds int) {
	atomic.StoreUint32(&rl.seconds, uint32(seconds))
}

func (rl *RedisLock) GetValue() string {
	return rl.id
}
