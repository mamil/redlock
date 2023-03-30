package redlock

import (
	"testing"

	"github.com/bmizerany/assert"
	"github.com/go-redis/redis"
)

var redisServers = []string{
	"127.0.0.1:6379",
}

func TestLock(t *testing.T) {
	redisClient := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: redisServers,
	})

	redlock := NewRedisLock(redisClient, "testlock")
	redlock.SetExpire(2)

	ok, err := redlock.Acquire()
	assert.Equal(t, ok, true)
	assert.Equal(t, err, nil)

	ok, err = redlock.Compare()
	assert.Equal(t, ok, true)
	assert.Equal(t, err, nil)

	ok, err = redlock.Release()
	assert.Equal(t, ok, true)
	assert.Equal(t, err, nil)
}
