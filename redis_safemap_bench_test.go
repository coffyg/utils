// file: redis_safemap_bench_test.go

package utils

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkRedisMapSet(b *testing.B) {
	sm := NewSafeMap[int]()
	// optional: sm.Clear() if you want a fresh DB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Set(fmt.Sprintf("benchSetKey_%d", i), i)
	}
}

func BenchmarkRedisMapGet(b *testing.B) {
	sm := NewSafeMap[int]()

	// Prepopulate
	for i := 0; i < 10000; i++ {
		sm.Set(fmt.Sprintf("benchGetKey_%d", i), i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchGetKey_%d", i%10000)
		sm.Get(key)
	}
}

func BenchmarkRedisMapSetWithExpire(b *testing.B) {
	sm := NewSafeMap[int]()
	expiration := 5 * time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.SetWithExpireDuration(fmt.Sprintf("benchExpKey_%d", i), i, expiration)
	}
}

func BenchmarkRedisMapLen(b *testing.B) {
	sm := NewSafeMap[int]()
	// Prepopulate with 100k keys
	for i := 0; i < 100000; i++ {
		sm.Set(fmt.Sprintf("benchLenKey_%d", i), i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sm.Len()
	}
}

func BenchmarkRedisMapRange(b *testing.B) {
	sm := NewSafeMap[int]()
	// Prepopulate
	for i := 0; i < 50000; i++ {
		sm.Set(fmt.Sprintf("benchRangeKey_%d", i), i)
	}

	f := func(key string, value int) bool {
		_ = value // simulate some small processing
		return true
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Range(f)
	}
}
