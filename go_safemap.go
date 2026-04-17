package utils

import (
	"strings"
	"sync"
	"time"
)

// SafeMapGo is a thread-safe map wrapper around Go's sync.Map
// that implements the same API as SafeMap
type SafeMapGo[V any] struct {
	m sync.Map
}

// NewSafeMapGo creates a new SafeMapGo instance
func NewSafeMapGo[V any]() *SafeMapGo[V] {
	return &SafeMapGo[V]{
		m: sync.Map{},
	}
}

// Get retrieves the value for the given key
func (sm *SafeMapGo[V]) Get(key string) (V, bool) {
	value, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	
	entry, ok := value.(*mapEntryGo[V])
	if !ok {
		var zero V
		return zero, false
	}
	
	// Check expiration
	if entry.expireTime != 0 && time.Now().UnixNano() > entry.expireTime {
		sm.m.Delete(key)
		var zero V
		return zero, false
	}
	
	return entry.value, true
}

// Set inserts or updates the value for the given key
func (sm *SafeMapGo[V]) Set(key string, value V) {
	entry := &mapEntryGo[V]{
		value:      value,
		expireTime: 0, // No expiration
	}
	sm.m.Store(key, entry)
}

// SetWithExpireDuration inserts or updates the value with an expiration duration
func (sm *SafeMapGo[V]) SetWithExpireDuration(key string, value V, expireDuration time.Duration) {
	expireTime := time.Now().Add(expireDuration).UnixNano()
	entry := &mapEntryGo[V]{
		value:      value,
		expireTime: expireTime,
	}
	sm.m.Store(key, entry)
}

// Delete removes the entry for the given key
func (sm *SafeMapGo[V]) Delete(key string) {
	sm.m.Delete(key)
}

// Exists checks if the key exists in the map
func (sm *SafeMapGo[V]) Exists(key string) bool {
	_, exists := sm.Get(key)
	return exists
}

// ExpiredAndGet retrieves the value if it hasn't expired
func (sm *SafeMapGo[V]) ExpiredAndGet(key string) (V, bool) {
	return sm.Get(key)
}

// UpdateExpireTime updates the expiration time for a key
func (sm *SafeMapGo[V]) UpdateExpireTime(key string, expireDuration time.Duration) bool {
	value, ok := sm.m.Load(key)
	if !ok {
		return false
	}
	
	entry, ok := value.(*mapEntryGo[V])
	if !ok {
		return false
	}
	
	// Check if entry has already expired
	if entry.expireTime != 0 && time.Now().UnixNano() > entry.expireTime {
		sm.m.Delete(key)
		return false
	}
	
	// Update expiration time
	newExpireTime := time.Now().Add(expireDuration).UnixNano()
	newEntry := &mapEntryGo[V]{
		value:      entry.value,
		expireTime: newExpireTime,
	}
	sm.m.Store(key, newEntry)
	return true
}

// Len returns the total number of unexpired entries
func (sm *SafeMapGo[V]) Len() int {
	count := 0
	now := time.Now().UnixNano()
	
	sm.m.Range(func(_, value interface{}) bool {
		entry, ok := value.(*mapEntryGo[V])
		if ok && (entry.expireTime == 0 || entry.expireTime > now) {
			count++
		}
		return true
	})
	
	return count
}

// Clear removes all entries
func (sm *SafeMapGo[V]) Clear() {
	// Create a new map (there's no clear method in sync.Map)
	sm.m = sync.Map{}
}

// Range iterates over all unexpired entries
func (sm *SafeMapGo[V]) Range(f func(key string, value V) bool) {
	now := time.Now().UnixNano()
	
	sm.m.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			return true
		}
		
		entry, ok := v.(*mapEntryGo[V])
		if !ok {
			return true
		}
		
		// Skip expired entries
		if entry.expireTime != 0 && entry.expireTime < now {
			sm.m.Delete(key)
			return true
		}
		
		// Call the user-provided function
		return f(key, entry.value)
	})
}

// Keys returns all the keys in the map
func (sm *SafeMapGo[V]) Keys() []string {
	keys := make([]string, 0)
	now := time.Now().UnixNano()
	
	sm.m.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			return true
		}
		
		entry, ok := v.(*mapEntryGo[V])
		if !ok {
			return true
		}
		
		// Skip expired entries
		if entry.expireTime != 0 && entry.expireTime < now {
			sm.m.Delete(key)
			return true
		}
		
		keys = append(keys, key)
		return true
	})
	
	return keys
}

// DeleteAllKeysStartingWith deletes all keys with the given prefix
func (sm *SafeMapGo[V]) DeleteAllKeysStartingWith(prefix string) {
	var keysToDelete []string
	
	// First pass: identify keys to delete
	sm.m.Range(func(k, _ interface{}) bool {
		key, ok := k.(string)
		if ok && strings.HasPrefix(key, prefix) {
			keysToDelete = append(keysToDelete, key)
		}
		return true
	})
	
	// Second pass: delete all identified keys
	for _, key := range keysToDelete {
		sm.m.Delete(key)
	}
}

// LoadOrStore atomically returns the existing value for the key if present,
// otherwise stores and returns the given value. loaded=true if the value was
// loaded, false if stored. Expired entries are treated as missing and
// overwritten.
// No expiration on stored value. Use LoadOrStoreWithExpireDuration for TTL.
func (sm *SafeMapGo[V]) LoadOrStore(key string, value V) (V, bool) {
	return sm.LoadOrStoreWithExpireDuration(key, value, 0)
}

// LoadOrStoreWithExpireDuration is LoadOrStore with a TTL applied when storing.
// If the value already exists (and is not expired), the existing value is
// returned and the TTL is NOT refreshed.
func (sm *SafeMapGo[V]) LoadOrStoreWithExpireDuration(key string, value V, expireDuration time.Duration) (V, bool) {
	var expireTime int64
	if expireDuration > 0 {
		expireTime = time.Now().Add(expireDuration).UnixNano()
	}
	newEntry := &mapEntryGo[V]{value: value, expireTime: expireTime}

	for {
		actual, loaded := sm.m.LoadOrStore(key, newEntry)
		if !loaded {
			return value, false
		}
		existing, ok := actual.(*mapEntryGo[V])
		if !ok {
			// Corrupt entry — try to overwrite.
			sm.m.Delete(key)
			continue
		}
		// Expired? Try to swap in the new entry atomically.
		if existing.expireTime != 0 && time.Now().UnixNano() > existing.expireTime {
			if sm.m.CompareAndSwap(key, actual, newEntry) {
				return value, false
			}
			// Another goroutine raced us — retry.
			continue
		}
		return existing.value, true
	}
}

// Update atomically reads the current value (zero value if missing or expired)
// and stores the result of updater(old, existed). The returned value is the
// stored result.
//
// Uses an optimistic CAS loop. The updater may be invoked MORE than once
// under contention (same semantics as Go's sync/atomic CAS loops) — keep it
// pure and cheap. Do not perform side effects inside updater.
// expireDuration=0 means no expiration.
func (sm *SafeMapGo[V]) Update(key string, expireDuration time.Duration, updater func(old V, existed bool) V) V {
	var expireTime int64
	if expireDuration > 0 {
		expireTime = time.Now().Add(expireDuration).UnixNano()
	}
	for {
		var old V
		var existed bool
		actual, loaded := sm.m.Load(key)
		if loaded {
			existing, ok := actual.(*mapEntryGo[V])
			if !ok {
				sm.m.Delete(key)
				continue
			}
			if existing.expireTime != 0 && time.Now().UnixNano() > existing.expireTime {
				// Expired — treat as missing.
				existed = false
			} else {
				old = existing.value
				existed = true
			}
		}
		newVal := updater(old, existed)
		newEntry := &mapEntryGo[V]{value: newVal, expireTime: expireTime}
		if !loaded {
			// Try to insert — only succeeds if nobody else inserted meanwhile.
			if _, inserted := sm.m.LoadOrStore(key, newEntry); !inserted {
				return newVal
			}
			// Someone else inserted. Retry.
			continue
		}
		// Existing entry — CAS to replace it.
		if sm.m.CompareAndSwap(key, actual, newEntry) {
			return newVal
		}
		// CAS lost — retry.
	}
}

// mapEntryGo is the internal structure for storing values with expiration
type mapEntryGo[V any] struct {
	value      V
	expireTime int64 // Unix timestamp in nanoseconds
}