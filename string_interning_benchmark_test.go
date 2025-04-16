package utils

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

// Function for legacy internString support for benchmark compatibility
func internString(s string) string {
	// Check if it's in the common keys map first for speed
	if interned, ok := commonKeyMap[s]; ok {
		return interned
	}
	
	// If the string is short, it's not worth interning
	if len(s) < 24 {
		return s
	}
	
	// For benchmarking, we keep the same behavior
	return s
}

// Simple FNV-32 for benchmarks
func fnv32(s string) uint32 {
	var hash uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= 16777619
	}
	return hash
}

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

// Additional benchmarks for string operations

// Benchmark the memory impact of string interning by creating many duplicate strings
func BenchmarkStringInternMemoryUsage(b *testing.B) {
	// Create a pattern of strings with many duplicates
	keyTemplates := []string{
		"user_%d",
		"session_%d",
		"product_%d",
		"order_%d",
		"customer_%d",
	}
	
	keys := make([]string, 0, b.N)
	for i := 0; i < b.N; i++ {
		template := keyTemplates[i%len(keyTemplates)]
		id := i % 100 // Create many duplicates with same number
		keys = append(keys, fmt.Sprintf(template, id))
	}
	
	b.ResetTimer()
	
	// Run with interning
	b.Run("WithInterning", func(b *testing.B) {
		internedKeys := make([]string, 0, b.N)
		for i := 0; i < b.N; i++ {
			internedKeys = append(internedKeys, internString(keys[i%len(keys)]))
		}
		_ = internedKeys
	})
	
	// Run without interning
	b.Run("WithoutInterning", func(b *testing.B) {
		regularKeys := make([]string, 0, b.N)
		for i := 0; i < b.N; i++ {
			regularKeys = append(regularKeys, keys[i%len(keys)])
		}
		_ = regularKeys
	})
}

// Benchmark string comparison performance
func BenchmarkStringComparison(b *testing.B) {
	// Create some test data with duplicates
	keys := generateBenchKeys()
	internedKeys := make([]string, len(keys))
	
	// Create interned versions
	for i, key := range keys {
		internedKeys[i] = internString(key)
	}
	
	b.Run("RegularStringEquality", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx1 := i % len(keys)
			idx2 := (i + 1000) % len(keys) // Compare with a key 1000 positions away
			_ = keys[idx1] == keys[idx2]
		}
	})
	
	b.Run("InternedStringEquality", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx1 := i % len(internedKeys)
			idx2 := (i + 1000) % len(internedKeys)
			_ = internedKeys[idx1] == internedKeys[idx2]
		}
	})
}

// Test the internString function with different string sizes
func BenchmarkInternStringBySize(b *testing.B) {
	// Short strings that won't be interned
	b.Run("ShortStrings", func(b *testing.B) {
		keys := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			keys[i] = "key" + strconv.Itoa(i)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = internString(keys[i%1000])
		}
	})
	
	// Medium strings near the threshold
	b.Run("MediumStrings", func(b *testing.B) {
		keys := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			// Create a string just around the 24 byte threshold
			keys[i] = "medium_key_string_" + strconv.Itoa(i)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = internString(keys[i%1000])
		}
	})
	
	// Long strings that should be interned
	b.Run("LongStrings", func(b *testing.B) {
		keys := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			keys[i] = "this_is_a_very_long_string_that_should_definitely_be_interned_" + strconv.Itoa(i)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = internString(keys[i%1000])
		}
	})
}

// Benchmark for map shard selection with and without interning
func BenchmarkGetShardPerformance(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateBenchKeys()
	
	b.Run("WithInterning", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			_ = sm.getShard(key)
		}
	})
	
	b.Run("WithoutInterning", func(b *testing.B) {
		smNoIntern := NewSafeMapNoInterning[int]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			_ = smNoIntern.getShard(key)
		}
	})
}

// Real-world comparison between SafeMap and sync.Map with interning effects
func BenchmarkRealWorldComparisonWithInterning(b *testing.B) {
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