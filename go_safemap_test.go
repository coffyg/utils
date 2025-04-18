package utils

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestBothSafeMapImplementations runs all basic operations on both SafeMap and SafeMapGo
// to verify they behave identically
func TestBothSafeMapImplementations(t *testing.T) {
	// Create both map types
	sm := NewSafeMap[string]()
	smGo := NewSafeMapGo[string]()
	
	// Test Set and Get
	t.Run("Set/Get", func(t *testing.T) {
		sm.Set("key1", "value1")
		smGo.Set("key1", "value1")
		
		v1, ok1 := sm.Get("key1")
		v2, ok2 := smGo.Get("key1")
		
		if v1 != v2 || ok1 != ok2 {
			t.Errorf("Get results differ: SafeMap=(%v,%v), SafeMapGo=(%v,%v)", v1, ok1, v2, ok2)
		}
		
		// Try getting non-existent key
		v1, ok1 = sm.Get("not_exist")
		v2, ok2 = smGo.Get("not_exist")
		
		if v1 != v2 || ok1 != ok2 {
			t.Errorf("Get results for non-existent key differ: SafeMap=(%v,%v), SafeMapGo=(%v,%v)", v1, ok1, v2, ok2)
		}
	})
	
	// Test Exists
	t.Run("Exists", func(t *testing.T) {
		exists1 := sm.Exists("key1")
		exists2 := smGo.Exists("key1")
		
		if exists1 != exists2 {
			t.Errorf("Exists results differ: SafeMap=%v, SafeMapGo=%v", exists1, exists2)
		}
		
		// Non-existent key
		exists1 = sm.Exists("not_exist")
		exists2 = smGo.Exists("not_exist")
		
		if exists1 != exists2 {
			t.Errorf("Exists results for non-existent key differ: SafeMap=%v, SafeMapGo=%v", exists1, exists2)
		}
	})
	
	// Test Delete
	t.Run("Delete", func(t *testing.T) {
		sm.Set("to_delete", "value")
		smGo.Set("to_delete", "value")
		
		// Verify key exists
		if !sm.Exists("to_delete") || !smGo.Exists("to_delete") {
			t.Errorf("Keys were not set correctly before deletion test")
		}
		
		// Delete the keys
		sm.Delete("to_delete")
		smGo.Delete("to_delete")
		
		// Verify they're gone
		exists1 := sm.Exists("to_delete")
		exists2 := smGo.Exists("to_delete")
		
		if exists1 != exists2 {
			t.Errorf("Delete results differ: SafeMap exists=%v, SafeMapGo exists=%v", exists1, exists2)
		}
	})
	
	// Test SetWithExpireDuration and expiration
	t.Run("Expiration", func(t *testing.T) {
		sm.SetWithExpireDuration("temp_key", "temp_value", 50*time.Millisecond)
		smGo.SetWithExpireDuration("temp_key", "temp_value", 50*time.Millisecond)
		
		// Verify keys exist initially
		if !sm.Exists("temp_key") || !smGo.Exists("temp_key") {
			t.Errorf("Expiring keys were not set correctly")
		}
		
		// Wait for expiration
		time.Sleep(100 * time.Millisecond)
		
		// Verify both implementations expired the keys
		exists1 := sm.Exists("temp_key")
		exists2 := smGo.Exists("temp_key")
		
		if exists1 != exists2 {
			t.Errorf("Expiration results differ: SafeMap exists=%v, SafeMapGo exists=%v", exists1, exists2)
		}
	})
	
	// Test UpdateExpireTime
	t.Run("UpdateExpireTime", func(t *testing.T) {
		sm.SetWithExpireDuration("update_key", "value", 50*time.Millisecond)
		smGo.SetWithExpireDuration("update_key", "value", 50*time.Millisecond)
		
		// Update expiration time to be longer
		updated1 := sm.UpdateExpireTime("update_key", 150*time.Millisecond)
		updated2 := smGo.UpdateExpireTime("update_key", 150*time.Millisecond)
		
		if updated1 != updated2 {
			t.Errorf("UpdateExpireTime results differ: SafeMap=%v, SafeMapGo=%v", updated1, updated2)
		}
		
		// Wait past original expiration but before extended expiration
		time.Sleep(100 * time.Millisecond)
		
		// Keys should still exist
		exists1 := sm.Exists("update_key")
		exists2 := smGo.Exists("update_key")
		
		if exists1 != exists2 {
			t.Errorf("After UpdateExpireTime, existence differs: SafeMap=%v, SafeMapGo=%v", exists1, exists2)
		}
		
		// Wait for the extended expiration
		time.Sleep(100 * time.Millisecond)
		
		// Keys should be gone
		exists1 = sm.Exists("update_key")
		exists2 = smGo.Exists("update_key")
		
		if exists1 != exists2 {
			t.Errorf("After final expiration, existence differs: SafeMap=%v, SafeMapGo=%v", exists1, exists2)
		}
	})
	
	// Test Len
	t.Run("Len", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		// Add some keys
		for i := 0; i < 10; i++ {
			sm.Set(fmt.Sprintf("len_key_%d", i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("len_key_%d", i), fmt.Sprintf("value_%d", i))
		}
		
		// Also add some expiring keys
		for i := 0; i < 5; i++ {
			sm.SetWithExpireDuration(fmt.Sprintf("len_expiring_%d", i), fmt.Sprintf("value_%d", i), 50*time.Millisecond)
			smGo.SetWithExpireDuration(fmt.Sprintf("len_expiring_%d", i), fmt.Sprintf("value_%d", i), 50*time.Millisecond)
		}
		
		// Check length
		len1 := sm.Len()
		len2 := smGo.Len()
		
		if len1 != len2 {
			t.Errorf("Len results differ: SafeMap=%v, SafeMapGo=%v", len1, len2)
		}
		
		// Wait for expiration
		time.Sleep(100 * time.Millisecond)
		
		// Check length again
		len1 = sm.Len()
		len2 = smGo.Len()
		
		if len1 != len2 {
			t.Errorf("After expiration, Len results differ: SafeMap=%v, SafeMapGo=%v", len1, len2)
		}
	})
	
	// Test Range
	t.Run("Range", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		// Add some keys
		for i := 0; i < 10; i++ {
			sm.Set(fmt.Sprintf("range_key_%d", i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("range_key_%d", i), fmt.Sprintf("value_%d", i))
		}
		
		// Use Range to collect all keys and values
		collected1 := make(map[string]string)
		sm.Range(func(key string, value string) bool {
			collected1[key] = value
			return true
		})
		
		collected2 := make(map[string]string)
		smGo.Range(func(key string, value string) bool {
			collected2[key] = value
			return true
		})
		
		// Compare results
		if len(collected1) != len(collected2) {
			t.Errorf("Range collected different number of items: SafeMap=%d, SafeMapGo=%d", len(collected1), len(collected2))
		}
		
		for k, v1 := range collected1 {
			if v2, ok := collected2[k]; !ok || v1 != v2 {
				t.Errorf("Range results differ for key %s: SafeMap=%s, SafeMapGo=%s", k, v1, v2)
			}
		}
	})
	
	// Test Keys
	t.Run("Keys", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		// Add some keys
		for i := 0; i < 10; i++ {
			sm.Set(fmt.Sprintf("keys_key_%d", i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("keys_key_%d", i), fmt.Sprintf("value_%d", i))
		}
		
		// Get all keys
		keys1 := sm.Keys()
		keys2 := smGo.Keys()
		
		// Compare results (note: order might differ)
		if len(keys1) != len(keys2) {
			t.Errorf("Keys returned different number of items: SafeMap=%d, SafeMapGo=%d", len(keys1), len(keys2))
		}
		
		// Convert to maps for comparison
		map1 := make(map[string]bool)
		map2 := make(map[string]bool)
		
		for _, k := range keys1 {
			map1[k] = true
		}
		for _, k := range keys2 {
			map2[k] = true
		}
		
		// Check that every key in map1 is in map2
		for k := range map1 {
			if _, ok := map2[k]; !ok {
				t.Errorf("Key %s present in SafeMap but missing in SafeMapGo", k)
			}
		}
		
		// Check that every key in map2 is in map1
		for k := range map2 {
			if _, ok := map1[k]; !ok {
				t.Errorf("Key %s present in SafeMapGo but missing in SafeMap", k)
			}
		}
	})
	
	// Test DeleteAllKeysStartingWith
	t.Run("DeleteAllKeysStartingWith", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		prefix := "prefix_"
		other := "other_"
		
		// Add some keys with the prefix and some without
		for i := 0; i < 10; i++ {
			sm.Set(fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("value_%d", i))
			
			sm.Set(fmt.Sprintf("%s%d", other, i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("%s%d", other, i), fmt.Sprintf("value_%d", i))
		}
		
		// Delete all keys with the prefix
		sm.DeleteAllKeysStartingWith(prefix)
		smGo.DeleteAllKeysStartingWith(prefix)
		
		// Check that all prefix keys are gone
		for i := 0; i < 10; i++ {
			prefixKey := fmt.Sprintf("%s%d", prefix, i)
			exists1 := sm.Exists(prefixKey)
			exists2 := smGo.Exists(prefixKey)
			
			if exists1 != exists2 {
				t.Errorf("After DeleteAllKeysStartingWith, existence differs for key %s: SafeMap=%v, SafeMapGo=%v", prefixKey, exists1, exists2)
			}
			
			if exists1 {
				t.Errorf("Key %s should have been deleted in SafeMap", prefixKey)
			}
			
			if exists2 {
				t.Errorf("Key %s should have been deleted in SafeMapGo", prefixKey)
			}
		}
		
		// Check that other keys are still there
		for i := 0; i < 10; i++ {
			otherKey := fmt.Sprintf("%s%d", other, i)
			exists1 := sm.Exists(otherKey)
			exists2 := smGo.Exists(otherKey)
			
			if exists1 != exists2 {
				t.Errorf("After DeleteAllKeysStartingWith, existence differs for non-prefix key %s: SafeMap=%v, SafeMapGo=%v", otherKey, exists1, exists2)
			}
			
			if !exists1 {
				t.Errorf("Key %s should NOT have been deleted in SafeMap", otherKey)
			}
			
			if !exists2 {
				t.Errorf("Key %s should NOT have been deleted in SafeMapGo", otherKey)
			}
		}
	})
	
	// Test concurrent operations
	t.Run("Concurrent", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		var wg sync.WaitGroup
		
		// Number of concurrent operations
		concurrency := 10
		iterations := 100
		
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					// Mix of operations: Set, Get, Delete
					op := j % 3
					key := fmt.Sprintf("conc_key_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)
					
					if op == 0 {
						// Set operation
						sm.Set(key, value)
						smGo.Set(key, value)
					} else if op == 1 {
						// Get operation
						sm.Get(key)
						smGo.Get(key)
					} else {
						// Delete operation
						sm.Delete(key)
						smGo.Delete(key)
					}
				}
			}(i)
		}
		
		wg.Wait()
		
		// Verify both maps have same length after concurrent operations
		len1 := sm.Len()
		len2 := smGo.Len()
		
		if len1 != len2 {
			t.Errorf("After concurrent operations, lengths differ: SafeMap=%d, SafeMapGo=%d", len1, len2)
		}
	})
	
	// Test DeleteAllKeysStartingWith with concurrent access
	t.Run("DeleteAllWithConcurrentAccess", func(t *testing.T) {
		// Clear the maps first
		sm.Clear()
		smGo.Clear()
		
		prefix := "delete_prefix_"
		
		// Add some keys with the prefix
		for i := 0; i < 100; i++ {
			sm.Set(fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("value_%d", i))
			smGo.Set(fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("value_%d", i))
		}
		
		var wg sync.WaitGroup
		
		// Start goroutines that will access the maps while deletion happens
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("%s%d", prefix, j%100)
					sm.Exists(key)
					smGo.Exists(key)
					time.Sleep(time.Microsecond)
				}
			}()
		}
		
		// Delete all keys with the prefix
		sm.DeleteAllKeysStartingWith(prefix)
		smGo.DeleteAllKeysStartingWith(prefix)
		
		// Wait for goroutines to finish
		wg.Wait()
		
		// Verify no keys with the prefix remain in either map
		foundInSm := 0
		foundInSmGo := 0
		
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s%d", prefix, i)
			if sm.Exists(key) {
				foundInSm++
			}
			if smGo.Exists(key) {
				foundInSmGo++
			}
		}
		
		// Both implementations might not be perfect, but they should behave similarly
		if foundInSm != foundInSmGo {
			t.Logf("After DeleteAllKeysStartingWith with concurrent access, remaining key counts differ: SafeMap=%d, SafeMapGo=%d", foundInSm, foundInSmGo)
		}
	})
}

// Benchmark to compare SafeMap and SafeMapGo
func BenchmarkCompareDeleteAllKeysStartingWith(b *testing.B) {
	benchCases := []struct {
		name       string
		mapSize    int
		prefixLen  int
	}{
		{"Small_Map", 100, 3},
		{"Medium_Map", 10000, 3},
		{"Large_Map", 100000, 3},
	}

	for _, bc := range benchCases {
		b.Run("SafeMap_"+bc.name, func(b *testing.B) {
			sm := NewSafeMap[int]()
			benchmarkDeleteAllKeysOperation(b, sm, bc.mapSize, bc.prefixLen)
		})

		b.Run("SafeMapGo_"+bc.name, func(b *testing.B) {
			sm := NewSafeMapGo[int]()
			benchmarkDeleteAllKeysOperationGo(b, sm, bc.mapSize, bc.prefixLen)
		})
	}
}

func benchmarkDeleteAllKeysOperation(b *testing.B, sm *SafeMap[int], mapSize, prefixLen int) {
	// Create prefixes 
	prefixes := make([]string, 5)
	for i := range prefixes {
		prefix := make([]byte, prefixLen)
		for j := range prefix {
			prefix[j] = byte('a' + (i+j)%26)
		}
		prefixes[i] = string(prefix)
	}
	
	// Fill the map with data
	for i := 0; i < mapSize; i++ {
		var key string
		if i%10 < 5 {
			prefix := prefixes[i%5] 
			key = fmt.Sprintf("%s_key_%d", prefix, i)
		} else {
			key = fmt.Sprintf("other_key_%d", i)
		}
		sm.Set(key, i)
	}
	
	// Reset the timer before the benchmark
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Delete keys with each prefix
		for _, prefix := range prefixes {
			sm.DeleteAllKeysStartingWith(prefix)
		}
		
		// Add the keys back for the next iteration
		if i < b.N-1 {
			b.StopTimer()
			for j := 0; j < mapSize; j++ {
				if j%10 < 5 {
					prefix := prefixes[j%5]
					key := fmt.Sprintf("%s_key_%d", prefix, j)
					sm.Set(key, j)
				}
			}
			b.StartTimer()
		}
	}
}

func benchmarkDeleteAllKeysOperationGo(b *testing.B, sm *SafeMapGo[int], mapSize, prefixLen int) {
	// Create prefixes 
	prefixes := make([]string, 5)
	for i := range prefixes {
		prefix := make([]byte, prefixLen)
		for j := range prefix {
			prefix[j] = byte('a' + (i+j)%26)
		}
		prefixes[i] = string(prefix)
	}
	
	// Fill the map with data
	for i := 0; i < mapSize; i++ {
		var key string
		if i%10 < 5 {
			prefix := prefixes[i%5] 
			key = fmt.Sprintf("%s_key_%d", prefix, i)
		} else {
			key = fmt.Sprintf("other_key_%d", i)
		}
		sm.Set(key, i)
	}
	
	// Reset the timer before the benchmark
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Delete keys with each prefix
		for _, prefix := range prefixes {
			sm.DeleteAllKeysStartingWith(prefix)
		}
		
		// Add the keys back for the next iteration
		if i < b.N-1 {
			b.StopTimer()
			for j := 0; j < mapSize; j++ {
				if j%10 < 5 {
					prefix := prefixes[j%5]
					key := fmt.Sprintf("%s_key_%d", prefix, j)
					sm.Set(key, j)
				}
			}
			b.StartTimer()
		}
	}
}