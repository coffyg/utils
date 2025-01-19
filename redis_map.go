package utils

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var redisClient *redis.Client

// SetRedisClientForUtils sets the global Redis client to be used by all Redis maps.
func SetRedisClientForUtils(client *redis.Client) {
	redisClient = client
}

// RedisSafeMap is a Redis-backed implementation of the same API as SafeMap.
type RedisSafeMap[V any] struct {
	customClient *redis.Client
	prefix       string // optional prefix to prepend to keys
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

// NewRedisMapClientWithPrefix returns a new Redis-backed map using a *custom* client
// and a static prefix for all keys. This is optional but useful if you want to
// namespace or separate sets of keys in the same DB.
func NewRedisMapClientWithPrefix[V any](client *redis.Client, prefix string) *RedisSafeMap[V] {
	return &RedisSafeMap[V]{
		customClient: client,
		prefix:       prefix,
	}
}

// getClient returns the appropriate *redis.Client, preferring the customClient
// if present, otherwise falling back to the global one.
func (m *RedisSafeMap[V]) getClient() *redis.Client {
	if m.customClient != nil {
		return m.customClient
	}
	return redisClient
}

// prependPrefix is a small helper to unify how we add prefixes to keys.
func (m *RedisSafeMap[V]) prependPrefix(key string) string {
	if m.prefix == "" {
		return key
	}
	return m.prefix + ":" + key
}

// encodeValue and decodeValue handle (de)serialization of values to/from JSON.
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
	rkey := m.prependPrefix(key)

	data, err := m.getClient().Get(ctx, rkey).Result()
	if err == redis.Nil {
		return zero, false
	}
	if err != nil {
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
	rkey := m.prependPrefix(key)
	count, err := m.getClient().Exists(ctx, rkey).Result()
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
	rkey := m.prependPrefix(key)
	m.getClient().Set(ctx, rkey, data, 0)
}

// SetWithExpireDuration inserts or updates the value with an expiration duration.
func (m *RedisSafeMap[V]) SetWithExpireDuration(key string, value V, expireDuration time.Duration) {
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	rkey := m.prependPrefix(key)
	m.getClient().Set(ctx, rkey, data, expireDuration)
}

// Len returns the total number of entries (non-expired).
// For performance, we use a SCAN-based approach instead of KEYS *.
func (m *RedisSafeMap[V]) Len() int {
	count := 0
	// pattern: if prefix is set, scan only that prefix with wildcard
	pattern := "*"
	if m.prefix != "" {
		pattern = m.prefix + ":*"
	}

	client := m.getClient()
	var cursor uint64
	for {
		keys, cur, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return count
		}
		count += len(keys)
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return count
}

// Delete removes the entry for the given key.
func (m *RedisSafeMap[V]) Delete(key string) {
	rkey := m.prependPrefix(key)
	m.getClient().Del(ctx, rkey)
}

// Range iterates over all entries. If f returns false, iteration stops.
// Uses SCAN internally.
func (m *RedisSafeMap[V]) Range(f func(key string, value V) bool) {
	pattern := "*"
	if m.prefix != "" {
		pattern = m.prefix + ":*"
	}

	client := m.getClient()

	var cursor uint64
	for {
		keys, cur, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return
		}
		for _, k := range keys {
			data, err := client.Get(ctx, k).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				continue
			}
			val, err := decodeValue[V](data)
			if err != nil {
				continue
			}
			// We strip the prefix from `k` before passing to user callback
			userKey := k
			if m.prefix != "" {
				userKey = userKey[len(m.prefix)+1:] // remove "prefix:"
			}
			if !f(userKey, val) {
				return
			}
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
}

// Keys returns all the keys in the map (non-expired).
// Uses SCAN internally.
func (m *RedisSafeMap[V]) Keys() []string {
	pattern := "*"
	if m.prefix != "" {
		pattern = m.prefix + ":*"
	}

	client := m.getClient()
	var cursor uint64
	var allKeys []string

	for {
		keys, cur, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			break
		}
		for _, k := range keys {
			if m.prefix != "" {
				// strip prefix
				k = k[len(m.prefix)+1:]
			}
			allKeys = append(allKeys, k)
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return allKeys
}

// Clear removes all entries from the current DB for this prefix ONLY if prefix is set.
// If prefix is NOT set, it flushes the entire DB to remain consistent with older
// behavior. Adjust as needed if you prefer scanning for all keys in that case too.
func (m *RedisSafeMap[V]) Clear() {
	client := m.getClient()
	if m.prefix == "" {
		// Clears the current database
		client.FlushDB(ctx)
		return
	}
	// If a prefix is specified, only delete those keys
	pattern := m.prefix + ":*"
	var cursor uint64
	for {
		keys, cur, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
}

// UpdateExpireTime updates the expiration time for a key.
func (m *RedisSafeMap[V]) UpdateExpireTime(key string, expireDuration time.Duration) bool {
	rkey := m.prependPrefix(key)
	// Check if key exists
	count, err := m.getClient().Exists(ctx, rkey).Result()
	if err != nil || count == 0 {
		return false
	}
	// Set expiration
	m.getClient().Expire(ctx, rkey, expireDuration)
	return true
}

// DeleteAllKeysStartingWith deletes all keys with the given prefix (in addition to the static prefix, if set).
func (m *RedisSafeMap[V]) DeleteAllKeysStartingWith(prefix string) {
	client := m.getClient()
	// Combine the map prefix with the user prefix
	actualPattern := prefix
	if m.prefix != "" {
		actualPattern = m.prefix + ":" + prefix
	}
	// SCAN to find all matching
	var cursor uint64
	for {
		keys, cur, err := client.Scan(ctx, cursor, actualPattern+"*", 1000).Result()
		if err != nil || len(keys) == 0 {
			if err != nil {
				// If there's an error, break out
				break
			}
		} else {
			client.Del(ctx, keys...)
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
}

// ExpiredAndGet retrieves the value if it hasn't expired.
// In Redis, if the key is expired, it won't be found, so this is just a Get.
func (m *RedisSafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return m.Get(key)
}
