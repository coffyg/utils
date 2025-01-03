package utils

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var redisClient *redis.Client
var ctx = context.Background()

// SetRedisClientForUtils sets the global Redis client to be used by all Redis maps.
func SetRedisClientForUtils(client *redis.Client) {
	redisClient = client
}

// RedisSafeMap is a Redis-backed implementation of the same API as SafeMap.
type RedisSafeMap[V any] struct {
	// If customClient is non-nil, we'll use it instead of the global redisClient.
	customClient *redis.Client
}

// NewRedisMap returns a new Redis-backed map that uses the global redisClient.
func NewRedisMap[V any]() *RedisSafeMap[V] {
	return &RedisSafeMap[V]{}
}

// NewRedisMapClient returns a new Redis-backed map using a *custom* client.
func NewRedisMapClient[V any](client *redis.Client) *RedisSafeMap[V] {
	return &RedisSafeMap[V]{
		customClient: client,
	}
}

// Helper methods to get the correct client.
func (m *RedisSafeMap[V]) getClient() *redis.Client {
	if m.customClient != nil {
		return m.customClient
	}
	return redisClient
}

// Helper functions for serialization.
func encodeValue[V any](v V) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeValue[V any](data string) (V, error) {
	var v V
	err := json.Unmarshal([]byte(data), &v)
	return v, err
}

// Get retrieves the value for the given key.
func (m *RedisSafeMap[V]) Get(key string) (V, bool) {
	var zero V
	data, err := m.getClient().Get(ctx, key).Result()
	if err == redis.Nil {
		return zero, false
	} else if err != nil {
		// On error, treat as not found
		return zero, false
	}
	val, err := decodeValue[V](data)
	if err != nil {
		// If decoding fails, treat as not found
		return zero, false
	}
	return val, true
}

// Exists checks if the key exists in the map.
func (m *RedisSafeMap[V]) Exists(key string) bool {
	count, err := m.getClient().Exists(ctx, key).Result()
	if err != nil || count == 0 {
		return false
	}
	return true
}

// Set inserts or updates the value for the given key without expiration.
func (m *RedisSafeMap[V]) Set(key string, value V) {
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	m.getClient().Set(ctx, key, data, 0) // no expiration
}

// SetWithExpireDuration inserts or updates the value with an expiration duration.
func (m *RedisSafeMap[V]) SetWithExpireDuration(key string, value V, expireDuration time.Duration) {
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	m.getClient().Set(ctx, key, data, expireDuration)
}

// Len returns the total number of entries (non-expired).
func (m *RedisSafeMap[V]) Len() int {
	keys, err := m.getClient().Keys(ctx, "*").Result()
	if err != nil {
		return 0
	}
	return len(keys)
}

// Delete removes the entry for the given key.
func (m *RedisSafeMap[V]) Delete(key string) {
	m.getClient().Del(ctx, key)
}

// Range iterates over all entries. If f returns false, iteration stops.
func (m *RedisSafeMap[V]) Range(f func(key string, value V) bool) {
	// NOTE: Using KEYS * for demonstration. For large datasets, consider SCAN.
	keys, err := m.getClient().Keys(ctx, "*").Result()
	if err != nil {
		return
	}
	for _, k := range keys {
		data, err := m.getClient().Get(ctx, k).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			continue
		}
		val, err := decodeValue[V](data)
		if err != nil {
			continue
		}
		if !f(k, val) {
			return
		}
	}
}

// Keys returns all the keys in the map.
func (m *RedisSafeMap[V]) Keys() []string {
	keys, err := m.getClient().Keys(ctx, "*").Result()
	if err != nil {
		return []string{}
	}
	return keys
}

// Clear removes all entries.
func (m *RedisSafeMap[V]) Clear() {
	// Clears the current database
	m.getClient().FlushDB(ctx)
}

// UpdateExpireTime updates the expiration time for a key.
func (m *RedisSafeMap[V]) UpdateExpireTime(key string, expireDuration time.Duration) bool {
	// Check if key exists
	if !m.Exists(key) {
		return false
	}
	// Set expiration
	m.getClient().Expire(ctx, key, expireDuration)
	return true
}

// DeleteAllKeysStartingWith deletes all keys with the given prefix.
func (m *RedisSafeMap[V]) DeleteAllKeysStartingWith(prefix string) {
	keys, err := m.getClient().Keys(ctx, prefix+"*").Result()
	if err != nil || len(keys) == 0 {
		return
	}
	m.getClient().Del(ctx, keys...)
}

// ExpiredAndGet retrieves the value if it hasn't expired.
// In Redis, if the key is expired, it won't be found, so this is just a Get.
func (m *RedisSafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return m.Get(key)
}
