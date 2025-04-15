package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

// Generate a set of keys with different patterns
func generateBenchKeys() []string {
	keys := make([]string, 0, 5000)
	
	// Short keys
	for i := 0; i < 1000; i++ {
		keys = append(keys, "key_"+strconv.Itoa(i))
	}
	
	// Medium keys
	for i := 0; i < 1000; i++ {
		keys = append(keys, "medium_prefix_key_"+strconv.Itoa(i))
	}
	
	// Long keys
	for i := 0; i < 1000; i++ {
		keys = append(keys, "very_long_key_with_lots_of_characters_for_testing_interning_"+strconv.Itoa(i))
	}
	
	// Common keys that should be interned
	for i := 0; i < 1000; i++ {
		idx := i % len(commonMapKeys)
		keys = append(keys, commonMapKeys[idx])
	}
	
	// Duplicate keys to simulate real-world usage
	for i := 0; i < 1000; i++ {
		idx := rand.Intn(3000)
		keys = append(keys, keys[idx])
	}
	
	return keys
}

// Benchmark for string interning using the internString function
func BenchmarkStringInterning(b *testing.B) {
	keys := generateBenchKeys()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_ = internString(key)
	}
}

// Benchmark for SafeMap Get with string interning
func BenchmarkSafeMapGetWithInterning(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateBenchKeys()
	
	// Prepopulate the map
	for _, key := range keys {
		sm.Set(key, 42)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(len(keys))]
			sm.Get(key)
		}
	})
}

// Benchmark for SafeMap without string interning
type SafeMapNoInterning[V any] struct {
	*SafeMap[V]
}

func NewSafeMapNoInterning[V any]() *SafeMapNoInterning[V] {
	return &SafeMapNoInterning[V]{
		SafeMap: NewOptimizedSafeMap[V](),
	}
}

func (sm *SafeMapNoInterning[V]) getShard(key string) *mapShard[V] {
	hash := fnv32(key)
	// Use bitwise AND with modMask instead of modulo
	return sm.shards[hash&sm.modMask]
}

func (sm *SafeMapNoInterning[V]) Get(key string) (V, bool) {
	shard := sm.getShard(key)
	return shard.get(key)
}

func (sm *SafeMapNoInterning[V]) Set(key string, value V) {
	shard := sm.getShard(key)
	shard.store(key, value, 0)
}

// Benchmark for SafeMap Get without string interning
func BenchmarkSafeMapGetNoInterning(b *testing.B) {
	sm := NewSafeMapNoInterning[int]()
	keys := generateBenchKeys()
	
	// Prepopulate the map
	for _, key := range keys {
		sm.Set(key, 42)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(len(keys))]
			sm.Get(key)
		}
	})
}

// Benchmark for hash function
func BenchmarkFnv32Hash(b *testing.B) {
	keys := generateBenchKeys()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_ = fnv32(key)
	}
}

// BenchmarkMapParallelAccess tests concurrent Get/Set operations to measure contention
func BenchmarkMapParallelAccess(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateBenchKeys()
	
	// Prepopulate the map
	for _, key := range keys[:1000] {
		sm.Set(key, 42)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localRand := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			// 10% writes, 90% reads
			operation := localRand.Intn(100)
			key := keys[localRand.Intn(len(keys))]
			
			if operation < 10 {
				// Write
				sm.Set(key, localRand.Intn(100))
			} else {
				// Read
				sm.Get(key)
			}
		}
	})
}

// Benchmark to compare SafeMap vs sync.Map in a real-world scenario
func BenchmarkRealWorldComparison(b *testing.B) {
	b.Run("SafeMap", func(b *testing.B) {
		sm := NewOptimizedSafeMap[string]()
		var wg sync.WaitGroup
		
		// Simulate a typical web server handling session data
		b.RunParallel(func(pb *testing.PB) {
			localRand := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				// 5% key creation, 15% updates, 80% reads
				operation := localRand.Intn(100)
				sessionID := "session_" + strconv.Itoa(localRand.Intn(1000))
				
				if operation < 5 {
					// Create new session
					userData := "user_" + strconv.Itoa(localRand.Intn(10000))
					sm.Set(sessionID, userData)
				} else if operation < 20 {
					// Update existing session
					_, exists := sm.Get(sessionID)
					if exists {
						userData := "user_" + strconv.Itoa(localRand.Intn(10000))
						sm.Set(sessionID, userData)
					}
				} else {
					// Read session
					sm.Get(sessionID)
				}
			}
		})
		
		wg.Wait()
	})
	
	b.Run("SyncMap", func(b *testing.B) {
		var sm sync.Map
		var wg sync.WaitGroup
		
		// Simulate a typical web server handling session data
		b.RunParallel(func(pb *testing.PB) {
			localRand := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				// 5% key creation, 15% updates, 80% reads
				operation := localRand.Intn(100)
				sessionID := "session_" + strconv.Itoa(localRand.Intn(1000))
				
				if operation < 5 {
					// Create new session
					userData := "user_" + strconv.Itoa(localRand.Intn(10000))
					sm.Store(sessionID, userData)
				} else if operation < 20 {
					// Update existing session
					_, exists := sm.Load(sessionID)
					if exists {
						userData := "user_" + strconv.Itoa(localRand.Intn(10000))
						sm.Store(sessionID, userData)
					}
				} else {
					// Read session
					sm.Load(sessionID)
				}
			}
		})
		
		wg.Wait()
	})
}