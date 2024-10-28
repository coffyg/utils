package utils

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type mapEntry[V any] struct {
	value      atomic.Value // Stores V
	expireTime int64        // Unix timestamp in nanoseconds (0 means no expiration)
}

type readOnlyMap[V any] struct {
	m       map[string]*mapEntry[V]
	amended bool
}

type mapShard[V any] struct {
	mu       sync.Mutex
	readOnly atomic.Value // Stores readOnlyMap[V]
	dirty    map[string]*mapEntry[V]
	misses   atomic.Int64
}

type SafeMap[V any] struct {
	shardCount int
	shards     []*mapShard[V]
}

func NewSafeMap[V any]() *SafeMap[V] {
	return NewOptimizedSafeMap[V]()
}
func NewOptimizedSafeMap[V any]() *SafeMap[V] {
	numCPU := runtime.NumCPU()
	shardCount := numCPU * 64 // Adjust multiplier based on experimentation
	return NewSafeMapWithShardCount[V](shardCount)
}
func NewSafeMapWithShardCount[V any](shardCount int) *SafeMap[V] {
	sm := &SafeMap[V]{
		shardCount: shardCount,
		shards:     make([]*mapShard[V], shardCount),
	}
	for i := 0; i < shardCount; i++ {
		shard := &mapShard[V]{}
		shard.readOnly.Store(readOnlyMap[V]{m: make(map[string]*mapEntry[V])})
		sm.shards[i] = shard
	}
	return sm
}

// fnv32 hashes a string to a uint32 using FNV-1a algorithm.
func fnv32(key string) uint32 {
	var hash uint32 = 2166136261
	const prime32 = 16777619
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

func (sm *SafeMap[V]) getShard(key string) *mapShard[V] {
	hash := fnv32(key)
	return sm.shards[hash%uint32(sm.shardCount)]
}

// Get retrieves the value for the given key.
func (sm *SafeMap[V]) Get(key string) (V, bool) {
	shard := sm.getShard(key)
	return shard.get(key)
}

func (s *mapShard[V]) get(key string) (V, bool) {
	now := time.Now().UnixNano() // Cache current time
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	entry, ok := readOnly.m[key]
	if ok {
		if !s.isExpired(entry, now) {
			value := entry.value.Load().(V)
			return value, true
		}
		s.deleteExpired(key)
		var zero V
		return zero, false
	}
	if readOnly.amended {
		s.mu.Lock()
		entry, ok = s.dirty[key]
		if ok {
			if !s.isExpired(entry, now) {
				value := entry.value.Load().(V)
				s.mu.Unlock()
				return value, true
			}
			s.deleteExpiredLocked(key)
			s.mu.Unlock()
			var zero V
			return zero, false
		}
		s.missLocked()
		s.mu.Unlock()
	} else {
		s.miss()
	}
	var zero V
	return zero, false
}

// Exists checks if the key exists in the map.
func (sm *SafeMap[V]) Exists(key string) bool {
	shard := sm.getShard(key)
	return shard.exists(key)
}

func (s *mapShard[V]) exists(key string) bool {
	now := time.Now().UnixNano() // Cache current time
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	entry, ok := readOnly.m[key]
	if ok {
		if !s.isExpired(entry, now) {
			return true
		}
		s.deleteExpired(key)
		return false
	}
	if readOnly.amended {
		s.mu.Lock()
		defer s.mu.Unlock()
		entry, ok = s.dirty[key]
		if ok {
			if !s.isExpired(entry, now) {
				return true
			}
			s.deleteExpiredLocked(key)
		}
	}
	return false
}

// Set inserts or updates the value for the given key.
func (sm *SafeMap[V]) Set(key string, value V) {
	shard := sm.getShard(key)
	shard.store(key, value, 0)
}

// SetWithExpireDuration inserts or updates the value with an expiration duration.
func (sm *SafeMap[V]) SetWithExpireDuration(key string, value V, expireDuration time.Duration) {
	expireTime := time.Now().Add(expireDuration).UnixNano()
	shard := sm.getShard(key)
	shard.store(key, value, expireTime)
}

func (s *mapShard[V]) store(key string, value V, expireTime int64) {
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	entry, ok := readOnly.m[key]
	if ok {
		entry.value.Store(value)
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return
	}
	s.mu.Lock()
	if s.dirty == nil {
		s.dirtyLocked()
		readOnly := s.readOnly.Load().(readOnlyMap[V])
		newReadOnly := readOnlyMap[V]{m: readOnly.m, amended: true}
		s.readOnly.Store(newReadOnly)
	}
	if entry, ok := s.dirty[key]; ok {
		entry.value.Store(value)
		atomic.StoreInt64(&entry.expireTime, expireTime)
	} else {
		newEntry := &mapEntry[V]{}
		newEntry.value.Store(value)
		atomic.StoreInt64(&newEntry.expireTime, expireTime)
		s.dirty[key] = newEntry
	}
	s.mu.Unlock()
}

func (s *mapShard[V]) miss() {
	s.misses.Add(1)
	if s.misses.Load() > int64(len(s.readOnly.Load().(readOnlyMap[V]).m)/2) {
		s.mu.Lock()
		s.promoteLocked()
		s.mu.Unlock()
	}
}

func (s *mapShard[V]) missLocked() {
	s.misses.Add(1)
	if s.misses.Load() > int64(len(s.readOnly.Load().(readOnlyMap[V]).m)/2) {
		s.promoteLocked()
	}
}

func (s *mapShard[V]) promoteLocked() {
	if s.dirty != nil {
		s.readOnly.Store(readOnlyMap[V]{m: s.dirty})
		s.dirty = nil
		s.misses.Store(0)
	}
}

func (s *mapShard[V]) dirtyLocked() {
	if s.dirty != nil {
		return
	}
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	s.dirty = make(map[string]*mapEntry[V], len(readOnly.m))
	for k, v := range readOnly.m {
		s.dirty[k] = v
	}
}

func (s *mapShard[V]) isExpired(e *mapEntry[V], now int64) bool {
	expireTime := atomic.LoadInt64(&e.expireTime)
	if expireTime == 0 {
		return false
	}
	return now > expireTime
}

func (s *mapShard[V]) deleteExpired(key string) {
	s.mu.Lock()
	s.deleteExpiredLocked(key)
	s.mu.Unlock()
}

func (s *mapShard[V]) deleteExpiredLocked(key string) {
	if s.dirty == nil {
		s.dirtyLocked()
		readOnly := s.readOnly.Load().(readOnlyMap[V])
		newReadOnly := readOnlyMap[V]{m: readOnly.m, amended: true}
		s.readOnly.Store(newReadOnly)
	}
	delete(s.dirty, key)
	// Force promotion
	s.misses.Store(int64(len(s.readOnly.Load().(readOnlyMap[V]).m) + 1))
	s.promoteLocked()
}

// Len returns the total number of unexpired entries.
func (sm *SafeMap[V]) Len() int {
	total := 0
	now := time.Now().UnixNano()
	for _, shard := range sm.shards {
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		for _, entry := range readOnly.m {
			if !shard.isExpired(entry, now) {
				total++
			}
		}
		if shard.dirty != nil {
			shard.mu.Lock()
			for _, entry := range shard.dirty {
				if !shard.isExpired(entry, now) {
					total++
				}
			}
			shard.mu.Unlock()
		}
	}
	return total
}

// Delete removes the entry for the given key.
func (sm *SafeMap[V]) Delete(key string) {
	shard := sm.getShard(key)
	shard.delete(key)
}

func (s *mapShard[V]) delete(key string) {
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	if _, ok := readOnly.m[key]; !ok {
		s.mu.Lock()
		if s.dirty != nil {
			delete(s.dirty, key)
		}
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	if s.dirty == nil {
		s.dirtyLocked()
		readOnly := s.readOnly.Load().(readOnlyMap[V])
		newReadOnly := readOnlyMap[V]{m: readOnly.m, amended: true}
		s.readOnly.Store(newReadOnly)
	}
	delete(s.dirty, key)
	s.mu.Unlock()
}

// Range iterates over all unexpired entries.
func (sm *SafeMap[V]) Range(f func(key string, value V) bool) {
	now := time.Now().UnixNano()
	for _, shard := range sm.shards {
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		for k, entry := range readOnly.m {
			if !shard.isExpired(entry, now) {
				value := entry.value.Load().(V)
				if !f(k, value) {
					return
				}
			}
		}
		shard.mu.Lock()
		if shard.dirty != nil {
			for k, entry := range shard.dirty {
				if !shard.isExpired(entry, now) {
					value := entry.value.Load().(V)
					if !f(k, value) {
						shard.mu.Unlock()
						return
					}
				}
			}
		}
		shard.mu.Unlock()
	}
}

// Keys returns all the keys in the map.
func (sm *SafeMap[V]) Keys() []string {
	keys := []string{}
	sm.Range(func(key string, _ V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Clear removes all entries.
func (sm *SafeMap[V]) Clear() {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		shard.readOnly.Store(readOnlyMap[V]{m: make(map[string]*mapEntry[V])})
		shard.dirty = nil
		shard.misses.Store(0)
		shard.mu.Unlock()
	}
}

// UpdateExpireTime updates the expiration time for a key.
func (sm *SafeMap[V]) UpdateExpireTime(key string, expireDuration time.Duration) bool {
	expireTime := time.Now().Add(expireDuration).UnixNano()
	shard := sm.getShard(key)
	now := time.Now().UnixNano()
	readOnly := shard.readOnly.Load().(readOnlyMap[V])
	if entry, ok := readOnly.m[key]; ok && !shard.isExpired(entry, now) {
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return true
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if entry, ok := shard.dirty[key]; ok && !shard.isExpired(entry, now) {
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return true
	}
	return false
}

// DeleteAllKeysStartingWith deletes all keys with the given prefix.
func (sm *SafeMap[V]) DeleteAllKeysStartingWith(prefix string) {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		if shard.dirty == nil {
			shard.dirtyLocked()
			newReadOnly := readOnlyMap[V]{m: readOnly.m, amended: true}
			shard.readOnly.Store(newReadOnly)
		}
		for k := range readOnly.m {
			if strings.HasPrefix(k, prefix) {
				delete(shard.dirty, k)
			}
		}
		for k := range shard.dirty {
			if strings.HasPrefix(k, prefix) {
				delete(shard.dirty, k)
			}
		}
		shard.misses.Store(int64(len(readOnly.m) + 1)) // Force promotion
		shard.promoteLocked()
		shard.mu.Unlock()
	}
}

// ExpiredAndGet retrieves the value if it hasn't expired.
func (sm *SafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return sm.Get(key)
}
