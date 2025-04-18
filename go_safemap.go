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

// mapEntryGo is the internal structure for storing values with expiration
type mapEntryGo[V any] struct {
	value      V
	expireTime int64 // Unix timestamp in nanoseconds
}