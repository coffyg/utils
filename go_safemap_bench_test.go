package utils

import (
	"math/rand"
	"testing"
)

func BenchmarkSafeMapGoReads(b *testing.B) {
	sm := NewSafeMapGo[int]()
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

func BenchmarkSafeMapGoWrites(b *testing.B) {
	sm := NewSafeMapGo[int]()
	keys := generateKeys(numKeys)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			sm.Set(key, rand.Intn(1000))
		}
	})
}

func BenchmarkSafeMapGoMixed(b *testing.B) {
	sm := NewSafeMapGo[int]()
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