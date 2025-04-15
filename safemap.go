package utils

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// cacheLine is used to pad structs to prevent false sharing
type cacheLine [64]byte

// stringInternPool for reducing string allocations in maps
var stringInternPool sync.Map

// commonMapKeys contains frequently used keys for string interning
var commonMapKeys = []string{
	"id", "name", "title", "description", "type", "status", "value",
	"count", "total", "price", "cost", "date", "time", "timestamp",
	"created_at", "updated_at", "deleted_at", "user_id", "user",
	"cache", "token", "access", "refresh", "session", "auth", "key",
	"config", "settings", "options", "data", "result", "error", "success",
}

// internString returns an interned version of the string to reduce memory usage
func internString(s string) string {
	// Check if string is already interned
	if interned, ok := stringInternPool.Load(s); ok {
		return interned.(string)
	}
	
	// If the string is short (under 24 bytes), it's not worth interning
	// because the sync.Map overhead would be more than the saved memory
	if len(s) < 24 {
		return s
	}
	
	// Store the string in the pool for future use
	stringInternPool.Store(s, s)
	return s
}

func init() {
	// Pre-intern common map keys
	for _, key := range commonMapKeys {
		stringInternPool.Store(key, key)
	}
}

type mapEntry[V any] struct {
	value      atomic.Value // Stores V
	expireTime int64        // Unix timestamp in nanoseconds (0 means no expiration)
}

type readOnlyMap[V any] struct {
	m       map[string]*mapEntry[V]
	amended bool
}

type mapShard[V any] struct {
	mu        sync.Mutex
	readOnly  atomic.Value // Stores readOnlyMap[V]
	dirty     map[string]*mapEntry[V]
	misses    atomic.Int64
	_         cacheLine // Prevent false sharing between shards
}

type SafeMap[V any] struct {
	shardCount int
	shards     []*mapShard[V]
	// Precomputed modMask for power-of-two shardCount
	modMask uint32
}

func NewSafeMap[V any]() *SafeMap[V] {
	return NewOptimizedSafeMap[V]()
}

func NewOptimizedSafeMap[V any]() *SafeMap[V] {
	numCPU := runtime.NumCPU()
	// Make shardCount a power of two to optimize modulo operation
	shardCount := 1
	for shardCount < numCPU*64 {
		shardCount *= 2
	}
	return NewSafeMapWithShardCount[V](shardCount)
}

func NewSafeMapWithShardCount[V any](shardCount int) *SafeMap[V] {
	// Ensure shardCount is a power of 2 for modMask optimization
	powerOfTwo := 1
	for powerOfTwo < shardCount {
		powerOfTwo *= 2
	}
	shardCount = powerOfTwo

	sm := &SafeMap[V]{
		shardCount: shardCount,
		shards:     make([]*mapShard[V], shardCount),
		modMask:    uint32(shardCount - 1), // For fast modulo with bitwise AND
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
	
	// Process bytes in chunks of 8 for better performance
	length := len(key)
	i := 0
	
	// Process 8 bytes at a time
	for ; i+8 <= length; i += 8 {
		hash ^= uint32(key[i])
		hash *= prime32
		hash ^= uint32(key[i+1])
		hash *= prime32
		hash ^= uint32(key[i+2])
		hash *= prime32
		hash ^= uint32(key[i+3])
		hash *= prime32
		hash ^= uint32(key[i+4])
		hash *= prime32
		hash ^= uint32(key[i+5])
		hash *= prime32
		hash ^= uint32(key[i+6])
		hash *= prime32
		hash ^= uint32(key[i+7])
		hash *= prime32
	}
	
	// Process remaining bytes
	for ; i < length; i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	
	return hash
}

func (sm *SafeMap[V]) getShard(key string) *mapShard[V] {
	// Intern the key if it's a common pattern or long
	key = internString(key)
	hash := fnv32(key)
	// Use bitwise AND with modMask instead of modulo
	return sm.shards[hash&sm.modMask]
}

// isExpired checks if an entry is expired
func (s *mapShard[V]) isExpired(e *mapEntry[V], now int64) bool {
	expireTime := atomic.LoadInt64(&e.expireTime)
	if expireTime == 0 {
		return false
	}
	return now > expireTime
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
	
	if !readOnly.amended {
		var zero V
		return zero, false
	}
	
	// Need to check dirty map
	s.mu.Lock()
	// Double-check now that we have the lock
	readOnly = s.readOnly.Load().(readOnlyMap[V])
	if entry, ok = readOnly.m[key]; ok {
		s.mu.Unlock()
		if !s.isExpired(entry, now) {
			value := entry.value.Load().(V)
			return value, true
		}
		s.deleteExpired(key)
		var zero V
		return zero, false
	}
	
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
	
	if !readOnly.amended {
		return false
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Double-check now that we have the lock
	readOnly = s.readOnly.Load().(readOnlyMap[V])
	if entry, ok = readOnly.m[key]; ok {
		if !s.isExpired(entry, now) {
			return true
		}
		s.deleteExpiredLocked(key)
		return false
	}
	
	entry, ok = s.dirty[key]
	if ok {
		if !s.isExpired(entry, now) {
			return true
		}
		s.deleteExpiredLocked(key)
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
		// Fast path: update existing entry without locking
		entry.value.Store(value)
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return
	}
	
	s.mu.Lock()
	// Double-check now that we have the lock
	readOnly = s.readOnly.Load().(readOnlyMap[V])
	if entry, ok = readOnly.m[key]; ok {
		entry.value.Store(value)
		atomic.StoreInt64(&entry.expireTime, expireTime)
		s.mu.Unlock()
		return
	}
	
	if s.dirty == nil {
		s.dirtyLocked()
		readOnly = s.readOnly.Load().(readOnlyMap[V])
		s.readOnly.Store(readOnlyMap[V]{m: readOnly.m, amended: true})
	}
	
	if entry, ok = s.dirty[key]; ok {
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

// Adaptive threshold for promotion - reduces frequency for larger maps
func (s *mapShard[V]) thresholdForPromotion(readOnlySize int) int64 {
	// For small maps, use lower threshold for faster promotion
	if readOnlySize < 100 {
		return int64(max(1, readOnlySize/2))
	}
	// For medium maps
	if readOnlySize < 1000 {
		return int64(readOnlySize/3 + 10)
	}
	// For large maps, use higher threshold to avoid frequent promotions
	return int64(readOnlySize/4 + 50)
}

func (s *mapShard[V]) miss() {
	missCount := s.misses.Add(1)
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	threshold := s.thresholdForPromotion(len(readOnly.m))
	
	if missCount >= threshold {
		s.mu.Lock()
		s.promoteLocked()
		s.mu.Unlock()
	}
}

func (s *mapShard[V]) missLocked() {
	missCount := s.misses.Add(1)
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	threshold := s.thresholdForPromotion(len(readOnly.m))
	
	if missCount >= threshold {
		s.promoteLocked()
	}
}

func (s *mapShard[V]) promoteLocked() {
	if s.dirty != nil {
		// Create a new map with exact capacity requirement
		dirtyMap := s.dirty
		s.readOnly.Store(readOnlyMap[V]{m: dirtyMap})
		s.dirty = nil
		s.misses.Store(0)
	}
}

func (s *mapShard[V]) dirtyLocked() {
	if s.dirty != nil {
		return
	}
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	s.dirty = make(map[string]*mapEntry[V], len(readOnly.m)+1) // +1 for the new entry
	for k, v := range readOnly.m {
		s.dirty[k] = v
	}
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
		s.readOnly.Store(readOnlyMap[V]{m: readOnly.m, amended: true})
	}
	delete(s.dirty, key)
	
	// Only promote if we have accumulated enough misses
	readOnly := s.readOnly.Load().(readOnlyMap[V])
	threshold := s.thresholdForPromotion(len(readOnly.m))
	missCount := s.misses.Load()
	
	if missCount >= threshold {
		s.promoteLocked()
	}
}

// Len returns the total number of unexpired entries.
func (sm *SafeMap[V]) Len() int {
	total := 0
	now := time.Now().UnixNano()
	
	// Use a batch approach to reduce lock contention
	batch := make(map[*mapShard[V]]struct{}, sm.shardCount)
	for _, shard := range sm.shards {
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		// Check entries in read-only map without locking
		for _, entry := range readOnly.m {
			if !shard.isExpired(entry, now) {
				total++
			}
		}
		
		// Collect shards with dirty maps
		if readOnly.amended {
			batch[shard] = struct{}{}
		}
	}
	
	// Process dirty maps in a second pass
	for shard := range batch {
		shard.mu.Lock()
		if shard.dirty != nil {
			for _, entry := range shard.dirty {
				readOnly := shard.readOnly.Load().(readOnlyMap[V])
				// Only count entries that are not in the read-only map
				if _, ok := readOnly.m[""]; !ok {
					if !shard.isExpired(entry, now) {
						total++
					}
				}
			}
		}
		shard.mu.Unlock()
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
	if _, ok := readOnly.m[key]; !ok && !readOnly.amended {
		// Fast path: key doesn't exist in read-only map and no dirty map
		return
	}
	
	s.mu.Lock()
	// Double-check under lock
	readOnly = s.readOnly.Load().(readOnlyMap[V])
	if _, ok := readOnly.m[key]; !ok {
		if s.dirty != nil {
			delete(s.dirty, key)
		}
		s.mu.Unlock()
		return
	}
	
	if s.dirty == nil {
		s.dirtyLocked()
		readOnly = s.readOnly.Load().(readOnlyMap[V])
		s.readOnly.Store(readOnlyMap[V]{m: readOnly.m, amended: true})
	}
	delete(s.dirty, key)
	s.mu.Unlock()
}

// Range iterates over all unexpired entries.
func (sm *SafeMap[V]) Range(f func(key string, value V) bool) {
	now := time.Now().UnixNano()
	
	// Store keys and values temporarily to prevent holding locks during callback
	type keyValuePair struct {
		key   string
		value V
	}
	
	// Process each shard
	for _, shard := range sm.shards {
		var pairs []keyValuePair
		
		// First collect entries from read-only map without locking
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		for k, entry := range readOnly.m {
			if !shard.isExpired(entry, now) {
				value := entry.value.Load().(V)
				pairs = append(pairs, keyValuePair{k, value})
			}
		}
		
		// Process entries from dirty map if needed
		if readOnly.amended {
			shard.mu.Lock()
			if shard.dirty != nil {
				// To avoid duplicates, only add entries not in read-only map
				for k, entry := range shard.dirty {
					if _, exists := readOnly.m[k]; !exists {
						if !shard.isExpired(entry, now) {
							value := entry.value.Load().(V)
							pairs = append(pairs, keyValuePair{k, value})
						}
					}
				}
			}
			shard.mu.Unlock()
		}
		
		// Process all entries without holding locks
		for _, pair := range pairs {
			if !f(pair.key, pair.value) {
				return
			}
		}
	}
}

// Keys returns all the keys in the map.
func (sm *SafeMap[V]) Keys() []string {
	// Preallocate with an estimate
	estimatedSize := 0
	for _, shard := range sm.shards {
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		estimatedSize += len(readOnly.m)
		if readOnly.amended && shard.dirty != nil {
			shard.mu.Lock()
			estimatedSize += len(shard.dirty)
			shard.mu.Unlock()
		}
	}
	
	keys := make([]string, 0, estimatedSize)
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
	
	if !readOnly.amended {
		return false
	}
	
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	// Double-check under lock
	readOnly = shard.readOnly.Load().(readOnlyMap[V])
	if entry, ok := readOnly.m[key]; ok && !shard.isExpired(entry, now) {
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return true
	}
	
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
			readOnly = shard.readOnly.Load().(readOnlyMap[V])
			shard.readOnly.Store(readOnlyMap[V]{m: readOnly.m, amended: true})
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