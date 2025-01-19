package utils

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Global client for backward-compatibility.
var redisClient *redis.Client

// SetRedisClientForUtils sets the global Redis client.
func SetRedisClientForUtils(client *redis.Client) {
	redisClient = client
}

// RedisSafeMap is a Redis-backed map with an optional prefix, plus
// a companion Redis Set that tracks our keys to allow fast Len/Keys/Range.
type RedisSafeMap[V any] struct {
	customClient *redis.Client
	prefix       string // optional prefix for keys in redis
	setKey       string // the Redis set that keeps track of our keys
}

// NewRedisMap uses the global redisClient with no prefix (API-compat).
func NewRedisMap[V any]() *RedisSafeMap[V] {
	return &RedisSafeMap[V]{}
}
func NewRedisMapPrefix[V any](prefix string) *RedisSafeMap[V] {
	return &RedisSafeMap[V]{prefix: prefix, setKey: prefix + ":__keys"}
}

// NewRedisMapClient uses a *custom* client with no prefix.
func NewRedisMapClient[V any](client *redis.Client) *RedisSafeMap[V] {
	return &RedisSafeMap[V]{
		customClient: client,
	}
}

// NewRedisMapClientWithPrefix uses a custom client and a prefix for keys.
func NewRedisMapClientWithPrefix[V any](client *redis.Client, prefix string) *RedisSafeMap[V] {
	// We'll also keep a companion set name: e.g. "myPrefix:__keys"
	setName := prefix + ":__keys"
	return &RedisSafeMap[V]{
		customClient: client,
		prefix:       prefix,
		setKey:       setName,
	}
}

// If user never called the prefix-based constructor, we generate a fallback set key.
func (m *RedisSafeMap[V]) getSetKey() string {
	// If user gave us a prefix, use prefix:__keys.
	if m.setKey != "" {
		return m.setKey
	}
	// Otherwise, store all keys in a global set so that Len/Keys/Range are limited
	// to the keys inserted by *this* map. We'll generate something stable per pointer:
	// (In practice, you might prefer a random or user-specified set name.)
	return "RedisSafeMap:" // or some unique ID
}

// getClient picks the custom or global client.
func (m *RedisSafeMap[V]) getClient() *redis.Client {
	if m.customClient != nil {
		return m.customClient
	}
	return redisClient
}

// Build the actual Redis key: prefix + ":" + userKey
func (m *RedisSafeMap[V]) buildKey(key string) string {
	if m.prefix == "" {
		return key
	}
	return m.prefix + ":" + key
}

// marshal & unmarshal helpers
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

// Get retrieves a value by key.
func (m *RedisSafeMap[V]) Get(key string) (V, bool) {
	var zero V
	rkey := m.buildKey(key)

	data, err := m.getClient().Get(ctx, rkey).Result()
	if err == redis.Nil {
		return zero, false
	}
	if err != nil {
		return zero, false
	}

	val, err := decodeValue[V](data)
	if err != nil {
		return zero, false
	}
	return val, true
}

// Exists checks if key is present (not expired).
func (m *RedisSafeMap[V]) Exists(key string) bool {
	rkey := m.buildKey(key)
	count, err := m.getClient().Exists(ctx, rkey).Result()
	return (err == nil && count == 1)
}

// Set inserts/updates a value with no expiration.
func (m *RedisSafeMap[V]) Set(key string, value V) {
	rkey := m.buildKey(key)
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	// We'll pipeline the SET + SADD for better concurrency
	c := m.getClient().Pipeline()
	c.Set(ctx, rkey, data, 0)
	c.SAdd(ctx, m.getSetKey(), key) // store the *unprefixed* key in the set
	_, _ = c.Exec(ctx)
}

// SetWithExpireDuration inserts/updates with TTL.
func (m *RedisSafeMap[V]) SetWithExpireDuration(key string, value V, expire time.Duration) {
	rkey := m.buildKey(key)
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	// pipeline
	c := m.getClient().Pipeline()
	c.Set(ctx, rkey, data, expire)
	c.SAdd(ctx, m.getSetKey(), key)
	_, _ = c.Exec(ctx)
}

// Delete removes a key from redis and from our set
func (m *RedisSafeMap[V]) Delete(key string) {
	rkey := m.buildKey(key)
	c := m.getClient().Pipeline()
	c.Del(ctx, rkey)
	c.SRem(ctx, m.getSetKey(), key)
	_, _ = c.Exec(ctx)
}

// UpdateExpireTime extends or sets TTL for an existing key. If key does not exist, returns false.
func (m *RedisSafeMap[V]) UpdateExpireTime(key string, expire time.Duration) bool {
	rkey := m.buildKey(key)
	// check if it exists
	exists, err := m.getClient().Exists(ctx, rkey).Result()
	if err != nil || exists == 0 {
		return false
	}
	m.getClient().Expire(ctx, rkey, expire)
	return true
}

// DeleteAllKeysStartingWith removes all keys whose *unprefixed* name starts with `prefixArg`.
// (We still store the unprefixed key in our set. So we can filter easily.)
func (m *RedisSafeMap[V]) DeleteAllKeysStartingWith(prefixArg string) {
	client := m.getClient()

	// We'll scan the set for matching keys, then delete them in a pipeline.
	var cursor uint64
	setKey := m.getSetKey()

	for {
		// sscan the *set* for keys that start with prefixArg
		// The pattern "prefixArg*" matches the *raw/unprefixed* keys in the set.
		keys, cur, err := client.SScan(ctx, setKey, cursor, prefixArg+"*", 300).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			pipe := client.Pipeline()
			for _, k := range keys {
				pipe.Del(ctx, m.buildKey(k))
				pipe.SRem(ctx, setKey, k)
			}
			_, _ = pipe.Exec(ctx)
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
}

// ExpiredAndGet is basically just Get. If the key is expired, it's not found.
func (m *RedisSafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return m.Get(key)
}

// Clear removes *all* keys belonging to this map’s set from Redis.
// If no prefix was set, it removes everything from our "global" set as well.
func (m *RedisSafeMap[V]) Clear() {
	client := m.getClient()
	setKey := m.getSetKey()

	// We'll just sscan all keys in the set, remove them, then empty the set.
	var cursor uint64
	for {
		keys, cur, err := client.SScan(ctx, setKey, cursor, "*", 300).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			pipe := client.Pipeline()
			for _, k := range keys {
				pipe.Del(ctx, m.buildKey(k))
			}
			// after we delete all, we can also SREM them from the set
			pipe.SRem(ctx, setKey, keys)
			_, _ = pipe.Exec(ctx)
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
}

// Len returns how many *unexpired* keys are in our map.
//
// We do this by iterating all members of the set. For each chunk, we pipeline
// GET or EXISTS calls. If a key doesn't exist (expired or manually deleted),
// we remove it from the set.
func (m *RedisSafeMap[V]) Len() int {
	client := m.getClient()
	setKey := m.getSetKey()

	total := 0
	var cursor uint64
	for {
		keys, cur, err := client.SScan(ctx, setKey, cursor, "*", 300).Result()
		if err != nil {
			break
		}
		if len(keys) == 0 {
			cursor = cur
			if cursor == 0 {
				break
			}
			continue
		}

		// Pipeline an EXISTS for each key
		pipe := client.Pipeline()
		existsCmds := make([]*redis.IntCmd, len(keys))
		for i, k := range keys {
			existsCmds[i] = pipe.Exists(ctx, m.buildKey(k))
		}
		_, _ = pipe.Exec(ctx)

		// Pipeline for removing stale
		removePipe := client.Pipeline()
		staleCount := 0
		for i, k := range keys {
			ex, _ := existsCmds[i].Result()
			if ex > 0 {
				// Key still exists
				total++
			} else {
				// Key no longer exists, remove from set
				removePipe.SRem(ctx, setKey, k)
				staleCount++
			}
		}
		if staleCount > 0 {
			_, _ = removePipe.Exec(ctx)
		}

		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return total
}

// Keys returns a slice of all *unexpired* keys.
func (m *RedisSafeMap[V]) Keys() []string {
	client := m.getClient()
	setKey := m.getSetKey()

	var out []string
	var cursor uint64
	for {
		keys, cur, err := client.SScan(ctx, setKey, cursor, "*", 300).Result()
		if err != nil {
			break
		}
		if len(keys) == 0 {
			cursor = cur
			if cursor == 0 {
				break
			}
			continue
		}

		// Pipeline an EXISTS for each
		pipe := client.Pipeline()
		existsCmds := make([]*redis.IntCmd, len(keys))
		for i, k := range keys {
			existsCmds[i] = pipe.Exists(ctx, m.buildKey(k))
		}
		_, _ = pipe.Exec(ctx)

		// Another pipeline to remove stales
		removePipe := client.Pipeline()
		for i, k := range keys {
			ex, _ := existsCmds[i].Result()
			if ex > 0 {
				// Still valid
				out = append(out, k)
			} else {
				// stale, remove from set
				removePipe.SRem(ctx, setKey, k)
			}
		}
		_, _ = removePipe.Exec(ctx)

		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return out
}

// Range iterates over each *unexpired* key-value pair. If f returns false, iteration stops.
//
// Implementation: we sscan the set in chunks, pipeline GET calls, remove expired keys from the set.
func (m *RedisSafeMap[V]) Range(f func(key string, value V) bool) {
	client := m.getClient()
	setKey := m.getSetKey()

	var cursor uint64
	for {
		keys, cur, err := client.SScan(ctx, setKey, cursor, "*", 300).Result()
		if err != nil {
			break
		}
		if len(keys) == 0 {
			cursor = cur
			if cursor == 0 {
				break
			}
			continue
		}

		// We'll pipeline GET calls for each key
		pipe := client.Pipeline()
		getCmds := make([]*redis.StringCmd, len(keys))
		for i, k := range keys {
			getCmds[i] = pipe.Get(ctx, m.buildKey(k))
		}
		_, _ = pipe.Exec(ctx)

		// Another pipeline to remove stales
		removePipe := client.Pipeline()

		// Process the results
		for i, k := range keys {
			data, err := getCmds[i].Result()
			if err == redis.Nil {
				// stale (expired or never existed)
				removePipe.SRem(ctx, setKey, k)
				continue
			}
			if err != nil {
				// some transient redis error => skip
				continue
			}

			val, err := decodeValue[V](data)
			if err != nil {
				// skip decoding error
				continue
			}
			// If f returns false, stop iteration
			if !f(k, val) {
				_, _ = removePipe.Exec(ctx)
				return
			}
		}
		_, _ = removePipe.Exec(ctx)

		cursor = cur
		if cursor == 0 {
			break
		}
	}
}
