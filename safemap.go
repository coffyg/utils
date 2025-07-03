package utils

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// cacheLine is used to pad structs to prevent false sharing
type cacheLine [64]byte

// Precomputed common strings to avoid allocations without the overhead of sync.Map
var commonKeyMap = make(map[string]string)

// commonMapKeys contains frequently used keys
var commonMapKeys = []string{
	"id", "name", "title", "description", "type", "status", "value",
	"count", "total", "price", "cost", "date", "time", "timestamp",
	"created_at", "updated_at", "deleted_at", "user_id", "user",
	"cache", "token", "access", "refresh", "session", "auth", "key",
	"config", "settings", "options", "data", "result", "error", "success",
}

func init() {
	// Pre-populate common key map for faster lookups without sync.Map overhead
	for _, key := range commonMapKeys {
		commonKeyMap[key] = key
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

// xxHash64 is a faster, high-quality hashing algorithm
func xxHash64(key string) uint32 {
	const (
		prime1 uint64 = 11400714785074694791
		prime2 uint64 = 14029467366897019727
		prime3 uint64 = 1609587929392839161
		prime4 uint64 = 9650029242287828579
		prime5 uint64 = 2870177450012600261
	)

	data := unsafe.Pointer(unsafe.StringData(key))
	length := len(key)
	var h64 uint64

	if length >= 4 {
		// Fast path for strings >= 4 bytes
		h64 = uint64(length) * prime5
		end := length & ^7 // Round down to multiple of 8
		
		for i := 0; i < end; i += 8 {
			k1 := *(*uint64)(unsafe.Pointer(uintptr(data) + uintptr(i)))
			k1 *= prime2
			k1 = (k1 << 31) | (k1 >> 33) // rotl31
			k1 *= prime1
			h64 ^= k1
			h64 = ((h64 << 27) | (h64 >> 37)) * prime1 + prime4
		}

		// Handle remaining bytes
		for i := end; i < length; i++ {
			h64 ^= uint64(*(*byte)(unsafe.Pointer(uintptr(data) + uintptr(i)))) * prime5
			h64 = ((h64 << 17) | (h64 >> 47)) * prime3
		}
	} else {
		// Short string optimization
		h64 = uint64(length) * prime5
		for i := 0; i < length; i++ {
			h64 ^= uint64(*(*byte)(unsafe.Pointer(uintptr(data) + uintptr(i)))) * prime5
			h64 = ((h64 << 17) | (h64 >> 47)) * prime3
		}
	}

	h64 ^= h64 >> 33
	h64 *= prime2
	h64 ^= h64 >> 29
	h64 *= prime3
	h64 ^= h64 >> 32

	return uint32(h64)
}

func (sm *SafeMap[V]) getShard(key string) *mapShard[V] {
	// Fast lookup for common keys without sync.Map overhead
	if internedKey, ok := commonKeyMap[key]; ok {
		key = internedKey
	}
	
	hash := xxHash64(key)
	// Use bitwise AND with modMask instead of modulo
	return sm.shards[hash&sm.modMask]
}

// isExpired is a non-locking helper that checks if an entry is expired
// Inlined for better performance
func isExpired[V any](e *mapEntry[V], now int64) bool {
	expireTime := atomic.LoadInt64(&e.expireTime)
	return expireTime != 0 && now > expireTime
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
		if !isExpired(entry, now) {
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
		if !isExpired(entry, now) {
			value := entry.value.Load().(V)
			return value, true
		}
		s.deleteExpired(key)
		var zero V
		return zero, false
	}
	
	entry, ok = s.dirty[key]
	if ok {
		if !isExpired(entry, now) {
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
		if !isExpired(entry, now) {
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
		if !isExpired(entry, now) {
			return true
		}
		s.deleteExpiredLocked(key)
		return false
	}
	
	entry, ok = s.dirty[key]
	if ok {
		if !isExpired(entry, now) {
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

// Fixed, simpler promotion threshold that doesn't scale too aggressively with map size
func (s *mapShard[V]) thresholdForPromotion(readOnlySize int) int64 {
	if readOnlySize == 0 {
		return 1
	}
	return int64(readOnlySize) / 2
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
	
	// Force promotion
	s.missLocked()
}

// Len returns the total number of unexpired entries.
func (sm *SafeMap[V]) Len() int {
	total := 0
	now := time.Now().UnixNano()
	
	for _, shard := range sm.shards {
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		// Check entries in read-only map without locking
		for _, entry := range readOnly.m {
			if !isExpired(entry, now) {
				total++
			}
		}
		
		// Check dirty map if needed
		if readOnly.amended {
			shard.mu.Lock()
			if shard.dirty != nil {
				for k, entry := range shard.dirty {
					if _, exists := readOnly.m[k]; !exists && !isExpired(entry, now) {
						total++
					}
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
	
	// Process each shard
	for _, shard := range sm.shards {
		// Create a function to process entries
		processEntry := func(k string, entry *mapEntry[V]) bool {
			if !isExpired(entry, now) {
				value := entry.value.Load().(V)
				return f(k, value)
			}
			return true
		}
		
		// First process entries from read-only map without locking
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		for k, entry := range readOnly.m {
			if !processEntry(k, entry) {
				return
			}
		}
		
		// Process entries from dirty map if needed
		if readOnly.amended {
			shard.mu.Lock()
			// Process dirty entries while holding lock
			if shard.dirty != nil {
				for k, entry := range shard.dirty {
					if _, exists := readOnly.m[k]; !exists {
						if !processEntry(k, entry) {
							shard.mu.Unlock()
							return
						}
					}
				}
			}
			shard.mu.Unlock()
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
	if entry, ok := readOnly.m[key]; ok && !isExpired(entry, now) {
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
	if entry, ok := readOnly.m[key]; ok && !isExpired(entry, now) {
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return true
	}
	
	if entry, ok := shard.dirty[key]; ok && !isExpired(entry, now) {
		atomic.StoreInt64(&entry.expireTime, expireTime)
		return true
	}
	
	return false
}

// DeleteAllKeysStartingWith deletes all keys with the given prefix.
func (sm *SafeMap[V]) DeleteAllKeysStartingWith(prefix string) {
	// Process each shard
	for _, shard := range sm.shards {
		// First, collect keys to delete from read-only map without holding lock
		readOnly := shard.readOnly.Load().(readOnlyMap[V])
		var keysToDelete []string
		
		// Identify keys to delete from readOnly map
		for k := range readOnly.m {
			if strings.HasPrefix(k, prefix) {
				keysToDelete = append(keysToDelete, k)
			}
		}
		
		// Only lock if we have keys to delete or if dirty map exists
		if len(keysToDelete) > 0 || readOnly.amended {
			shard.mu.Lock()
			
			// Delete the collected keys
			for _, k := range keysToDelete {
				// Ensure we have a dirty map
				if shard.dirty == nil {
					shard.dirtyLocked()
				}
				// Mark as deleted in dirty map
				delete(shard.dirty, k)
			}
			
			// Also check dirty map for additional keys
			if shard.dirty != nil {
				// Collect dirty keys to avoid modifying map during iteration
				var dirtyKeysToDelete []string
				for k := range shard.dirty {
					if strings.HasPrefix(k, prefix) {
						dirtyKeysToDelete = append(dirtyKeysToDelete, k)
					}
				}
				// Delete dirty keys
				for _, k := range dirtyKeysToDelete {
					delete(shard.dirty, k)
				}
			}
			
			shard.mu.Unlock()
		}
	}
}

// ExpiredAndGet retrieves the value if it hasn't expired.
func (sm *SafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	return sm.Get(key)
}