package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

// Benchmark parameters
const (
	numKeys        = 100000
	numGoroutines  = 100
	numOperations  = 1000000
	readPercentage = 90 // For mixed benchmarks
)

// Helper function to generate keys
func generateKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key_" + strconv.Itoa(i)
	}
	return keys
}

func BenchmarkSafeMapReads(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateKeys(numKeys)

	// Prepopulate the map
	for _, key := range keys {
		sm.Set(key, 42)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			sm.Get(key)
		}
	})
}

func BenchmarkSyncMapReads(b *testing.B) {
	var m sync.Map
	keys := generateKeys(numKeys)

	// Prepopulate the map
	for _, key := range keys {
		m.Store(key, 42)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			m.Load(key)
		}
	})
}

func BenchmarkSafeMapWrites(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateKeys(numKeys)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			sm.Set(key, rand.Intn(1000))
		}
	})
}

func BenchmarkSyncMapWrites(b *testing.B) {
	var m sync.Map
	keys := generateKeys(numKeys)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			m.Store(key, rand.Intn(1000))
		}
	})
}

func BenchmarkSafeMapMixed(b *testing.B) {
	sm := NewOptimizedSafeMap[int]()
	keys := generateKeys(numKeys)

	// Prepopulate the map
	for _, key := range keys {
		sm.Set(key, 42)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			operation := rand.Intn(100)
			key := keys[rand.Intn(numKeys)]
			if operation < readPercentage {
				sm.Get(key)
			} else {
				sm.Set(key, rand.Intn(1000))
			}
		}
	})
}

func BenchmarkSyncMapMixed(b *testing.B) {
	var m sync.Map
	keys := generateKeys(numKeys)

	// Prepopulate the map
	for _, key := range keys {
		m.Store(key, 42)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			operation := rand.Intn(100)
			key := keys[rand.Intn(numKeys)]
			if operation < readPercentage {
				m.Load(key)
			} else {
				m.Store(key, rand.Intn(1000))
			}
		}
	})
}
