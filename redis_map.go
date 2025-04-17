package utils

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
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

// pointerID provides a way to get a unique string ID for a map instance
var pointerIDCounter uint64

// getPointerID returns a unique ID based on instance pointer address
func getPointerID() string {
    id := atomic.AddUint64(&pointerIDCounter, 1)
    return strconv.FormatUint(id, 36)
}

// If user never called the prefix-based constructor, we generate a fallback set key.
func (m *RedisSafeMap[V]) getSetKey() string {
	// If user gave us a prefix, use prefix:__keys.
	if m.setKey != "" {
		return m.setKey
	}
	// Lazy initialization of setKey if not yet set
	if m.setKey == "" {
		m.setKey = "RedisSafeMap:" + getPointerID()
	}
	return m.setKey
}

// getClient picks the custom or global client.
func (m *RedisSafeMap[V]) getClient() *redis.Client {
	if m.customClient != nil {
		return m.customClient
	}
	return redisClient
}

// getPipeline returns a pipeline for the given client
func (m *RedisSafeMap[V]) getPipeline() redis.Pipeliner {
	// Try to get a pipeline from the pool
	if pipeObj := redisPipelinePool.Get(); pipeObj != nil {
		pipe := pipeObj.(redis.Pipeliner)
		return pipe
	}
	
	// Create a new pipeline if none available
	return m.getClient().Pipeline()
}

// releasePipeline returns a pipeline to the pool after use
func (m *RedisSafeMap[V]) releasePipeline(pipe redis.Pipeliner) {
	// Clear any remaining commands
	pipe.Discard()
	redisPipelinePool.Put(pipe)
}

// Build the actual Redis key: prefix + ":" + userKey
func (m *RedisSafeMap[V]) buildKey(key string) string {
	// Check common key map for common keys
	if commonKey, ok := commonKeyMap[key]; ok {
		key = commonKey
	}
	
	if m.prefix == "" {
		return key
	}
	
	// For common combinations, we'll use a separate cache to avoid string concat
	builtKey := m.prefix + ":" + key
	
	// No need to intern - just return the concatenated key
	// Interning was shown to have overhead in benchmarks
	
	return builtKey
}

// Buffer pools for reducing allocations
var (
	// Pool for byte slices used in JSON marshaling/unmarshaling
	jsonBufferPool = sync.Pool{
		New: func() interface{} {
			// Create a reasonably sized buffer for most operations
			return make([]byte, 256)
		},
	}
	
	// Pool of Redis pipelines to reduce connection overhead
	redisPipelinePool = sync.Pool{
		New: func() interface{} {
			// Return nil - we can't create a pipeline yet as we don't know which client will be used
			return nil
		},
	}
)

// marshal & unmarshal helpers
func encodeValue[V any](v V) (string, error) {
	// For small types that marshal to basic JSON, use a pooled buffer
	buf := jsonBufferPool.Get().([]byte)
	defer jsonBufferPool.Put(buf)
	
	// Try to marshal directly into the buffer
	serialized, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	
	// Return the JSON as a string
	return string(serialized), nil
}

func decodeValue[V any](data string) (V, error) {
	var v V
	dataLen := len(data)
	
	// For data that fits in our pooled buffer, avoid the allocation
	if dataLen <= 256 {
		buf := jsonBufferPool.Get().([]byte)
		copy(buf, data)
		err := json.Unmarshal(buf[:dataLen], &v)
		jsonBufferPool.Put(buf)
		return v, err
	}
	
	// For larger data, use standard unmarshaling
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
	client := m.getClient()
	rkey := m.buildKey(key)
	setKey := m.getSetKey()
	
	// Encode value
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	
	// Use client.Pipeline() directly instead of pool
	pipe := client.Pipeline()
	defer pipe.Discard()
	
	// Pipeline the SET + SADD for better concurrency
	pipe.Set(ctx, rkey, data, 0)
	pipe.SAdd(ctx, setKey, key) // store the *unprefixed* key in the set
	
	// Execute and handle errors
	_, err = pipe.Exec(ctx)
	if err != nil {
		// Fall back to direct operations if pipeline fails
		client.Set(ctx, rkey, data, 0)
		client.SAdd(ctx, setKey, key)
	}
}

// SetWithExpireDuration inserts/updates with TTL.
func (m *RedisSafeMap[V]) SetWithExpireDuration(key string, value V, expire time.Duration) {
	client := m.getClient()
	rkey := m.buildKey(key)
	setKey := m.getSetKey()
	
	// Encode value
	data, err := encodeValue(value)
	if err != nil {
		return
	}
	
	// Use client.Pipeline() directly instead of pool
	pipe := client.Pipeline()
	defer pipe.Discard()
	
	// Pipeline SET with expiration and SADD
	pipe.Set(ctx, rkey, data, expire)
	pipe.SAdd(ctx, setKey, key)
	
	// Execute and handle errors
	_, err = pipe.Exec(ctx)
	if err != nil {
		// Fall back to direct operations if pipeline fails
		client.Set(ctx, rkey, data, expire)
		client.SAdd(ctx, setKey, key)
	}
}

// Delete removes a key from redis and from our set
func (m *RedisSafeMap[V]) Delete(key string) {
	client := m.getClient()
	rkey := m.buildKey(key)
	setKey := m.getSetKey()
	
	// Use client.Pipeline() directly
	pipe := client.Pipeline()
	defer pipe.Discard()
	
	// Pipeline DEL and SREM
	pipe.Del(ctx, rkey)
	pipe.SRem(ctx, setKey, key)
	
	// Execute and handle errors
	_, err := pipe.Exec(ctx)
	if err != nil {
		// Fall back to direct operations
		client.Del(ctx, rkey)
		client.SRem(ctx, setKey, key)
	}
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
	setKey := m.getSetKey()

	// Process in smaller batches to avoid large pipelines
	maxBatchSize := 100
	var cursor uint64 = 0

	for {
		// sscan the *set* for keys that start with prefixArg
		// The pattern "prefixArg*" matches the *raw/unprefixed* keys in the set.
		keys, nextCursor, err := client.SScan(ctx, setKey, cursor, prefixArg+"*", int64(maxBatchSize)).Result()
		if err != nil {
			break
		}
		
		// If we found matching keys, delete them
		if len(keys) > 0 {
			// Use direct client pipeline instead of pool to avoid issues
			pipe := client.Pipeline()
			
			// Add delete operations to pipeline
			for _, k := range keys {
				pipe.Del(ctx, m.buildKey(k))
				pipe.SRem(ctx, setKey, k)
			}
			
			// Execute pipeline and check for errors
			_, err := pipe.Exec(ctx)
			if err != nil {
				// Fall back to individual operations if pipeline fails
				for _, k := range keys {
					client.Del(ctx, m.buildKey(k))
					client.SRem(ctx, setKey, k)
				}
			}
		}
		
		// Update cursor for next batch
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

// ExpiredAndGet is basically just Get. If the key is expired, it's not found.
func (m *RedisSafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return m.Get(key)
}

// Clear removes *all* keys belonging to this map's set from Redis.
// If no prefix was set, it removes everything from our "global" set as well.
func (m *RedisSafeMap[V]) Clear() {
	client := m.getClient()
	setKey := m.getSetKey()
	
	// Process in batches to avoid large pipelines
	maxBatchSize := 100
	var cursor uint64 = 0
	
	for {
		// Get members in smaller batches
		members, nextCursor, err := client.SScan(ctx, setKey, cursor, "*", int64(maxBatchSize)).Result()
		if err != nil {
			break
		}
		
		// If we got members, delete them
		if len(members) > 0 {
			// Use direct client instead of pipeline pool to avoid issues
			pipe := client.Pipeline()
			
			// Delete all keys in this batch
			for _, k := range members {
				pipe.Del(ctx, m.buildKey(k))
			}
			
			// Remove members from the set
			pipe.SRem(ctx, setKey, members)
			
			// Execute pipeline and check for errors
			_, err := pipe.Exec(ctx)
			if err != nil {
				// If pipeline fails, try again with individual operations
				for _, k := range members {
					client.Del(ctx, m.buildKey(k))
					client.SRem(ctx, setKey, k)
				}
			}
		}
		
		// Update cursor for next batch
		cursor = nextCursor
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
		existsPipe := m.getPipeline()
		existsCmds := make([]*redis.IntCmd, len(keys))
		for i, k := range keys {
			existsCmds[i] = existsPipe.Exists(ctx, m.buildKey(k))
		}
		_, _ = existsPipe.Exec(ctx)
		
		// Return pipeline to the pool
		m.releasePipeline(existsPipe)

		// Pipeline for removing stale
		removePipe := m.getPipeline()
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
			m.releasePipeline(removePipe)
		} else {
			// No stale keys to remove, just return the pipeline
			m.releasePipeline(removePipe)
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
		existsPipe := m.getPipeline()
		existsCmds := make([]*redis.IntCmd, len(keys))
		for i, k := range keys {
			existsCmds[i] = existsPipe.Exists(ctx, m.buildKey(k))
		}
		_, _ = existsPipe.Exec(ctx)
		m.releasePipeline(existsPipe)

		// Another pipeline to remove stales
		removePipe := m.getPipeline()
		staleCount := 0
		for i, k := range keys {
			ex, _ := existsCmds[i].Result()
			if ex > 0 {
				// Still valid
				out = append(out, k)
			} else {
				// stale, remove from set
				removePipe.SRem(ctx, setKey, k)
				staleCount++
			}
		}
		if staleCount > 0 {
			_, _ = removePipe.Exec(ctx)
		}
		m.releasePipeline(removePipe)

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
		getPipe := m.getPipeline()
		getCmds := make([]*redis.StringCmd, len(keys))
		for i, k := range keys {
			getCmds[i] = getPipe.Get(ctx, m.buildKey(k))
		}
		_, _ = getPipe.Exec(ctx)
		m.releasePipeline(getPipe)

		// Another pipeline to remove stales
		removePipe := m.getPipeline()
		staleCount := 0

		// Process the results
		continueIteration := true
		for i, k := range keys {
			data, err := getCmds[i].Result()
			if err == redis.Nil {
				// stale (expired or never existed)
				removePipe.SRem(ctx, setKey, k)
				staleCount++
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
				continueIteration = false
				break
			}
		}
		
		if staleCount > 0 {
			_, _ = removePipe.Exec(ctx)
		}
		m.releasePipeline(removePipe)
		
		if !continueIteration {
			return
		}

		cursor = cur
		if cursor == 0 {
			break
		}
	}
}