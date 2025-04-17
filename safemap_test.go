package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSafeMapBasicOperations(t *testing.T) {
	sm := NewSafeMap[int]()

	// Test Set and Get
	sm.Set("key1", 1)
	sm.Set("key2", 2)

	value, exists := sm.Get("key1")
	if !exists || value != 1 {
		t.Errorf("Expected key1 to have value 1, got %v", value)
	}

	value, exists = sm.Get("key2")
	if !exists || value != 2 {
		t.Errorf("Expected key2 to have value 2, got %v", value)
	}

	// Test Exists
	if !sm.Exists("key1") {
		t.Errorf("Expected key1 to exist")
	}

	if sm.Exists("key3") {
		t.Errorf("Expected key3 to not exist")
	}

	// Test Delete
	sm.Delete("key1")
	if sm.Exists("key1") {
		t.Errorf("Expected key1 to be deleted")
	}

	// Test Len
	if sm.Len() != 1 {
		t.Errorf("Expected length to be 1, got %d", sm.Len())
	}

	// Test Keys
	keys := sm.Keys()
	if len(keys) != 1 || keys[0] != "key2" {
		t.Errorf("Expected keys to be [\"key2\"], got %v", keys)
	}

	// Test Clear
	sm.Clear()
	if sm.Len() != 0 {
		t.Errorf("Expected length to be 0 after Clear, got %d", sm.Len())
	}
}

func TestSafeMapExpiration(t *testing.T) {
	sm := NewSafeMap[string]()

	// Set a key with expiration
	sm.SetWithExpireDuration("tempKey", "tempValue", 100*time.Millisecond)

	// Immediately check if key exists
	value, exists := sm.Get("tempKey")
	if !exists || value != "tempValue" {
		t.Errorf("Expected tempKey to exist with value 'tempValue', got %v", value)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Check if key has expired
	value, exists = sm.Get("tempKey")
	if exists {
		t.Errorf("Expected tempKey to have expired")
	}

	// Test ExpiredAndGet
	sm.SetWithExpireDuration("tempKey2", "tempValue2", 100*time.Millisecond)

	value, exists = sm.ExpiredAndGet("tempKey2")
	if !exists || value != "tempValue2" {
		t.Errorf("Expected tempKey2 to exist with value 'tempValue2', got %v", value)
	}

	time.Sleep(150 * time.Millisecond)

	value, exists = sm.ExpiredAndGet("tempKey2")
	if exists {
		t.Errorf("Expected tempKey2 to have expired")
	}
}

func TestSafeMapUpdateExpireTime(t *testing.T) {
	sm := NewSafeMap[int]()

	// Set a key with expiration
	sm.SetWithExpireDuration("key", 1, 100*time.Millisecond)

	// Update the expiration time
	time.Sleep(50 * time.Millisecond)
	updated := sm.UpdateExpireTime("key", 200*time.Millisecond)
	if !updated {
		t.Errorf("Expected to update expiration time for key")
	}

	// Wait and check if key still exists
	time.Sleep(100 * time.Millisecond)
	value, exists := sm.Get("key")
	if !exists || value != 1 {
		t.Errorf("Expected key to still exist after updating expiration time")
	}

	// Wait until after the updated expiration
	time.Sleep(150 * time.Millisecond)
	value, exists = sm.Get("key")
	if exists {
		t.Errorf("Expected key to have expired after updated expiration time")
	}
}

func TestSafeMapDeleteAllKeysStartingWith(t *testing.T) {
	sm := NewSafeMap[int]()

	sm.Set("prefix_key1", 1)
	sm.Set("prefix_key2", 2)
	sm.Set("other_key", 3)

	sm.DeleteAllKeysStartingWith("prefix_")

	if sm.Exists("prefix_key1") || sm.Exists("prefix_key2") {
		t.Errorf("Expected keys starting with 'prefix_' to be deleted")
	}

	if !sm.Exists("other_key") {
		t.Errorf("Expected 'other_key' to still exist")
	}
}

func TestSafeMapConcurrentAccess(t *testing.T) {
	sm := NewSafeMap[int]()
	var wg sync.WaitGroup

	numGoroutines := 100
	numOperations := 1000

	// Writer goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "key_" + strconv.Itoa(rand.Intn(100))
				value := rand.Intn(1000)
				sm.Set(key, value)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "key_" + strconv.Itoa(rand.Intn(100))
				_, _ = sm.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Just ensure that the map has some entries
	if sm.Len() == 0 {
		t.Errorf("Expected SafeMap to have some entries after concurrent access")
	}
}

func TestSafeMapSetWithExpireDurationConcurrent(t *testing.T) {
	sm := NewSafeMap[int]()
	var wg sync.WaitGroup

	numGoroutines := 50
	numOperations := 200

	// Set keys with expiration concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "tempKey_" + strconv.Itoa(rand.Intn(100))
				value := rand.Intn(1000)
				expire := time.Duration(rand.Intn(100)) * time.Millisecond
				sm.SetWithExpireDuration(key, value, expire)
			}
		}(i)
	}

	wg.Wait()

	// Wait for all possible expirations to occur
	time.Sleep(200 * time.Millisecond)

	// Ensure that expired keys are removed
	keys := sm.Keys()
	for _, key := range keys {
		_, exists := sm.Get(key)
		if !exists {
			t.Errorf("Expected key %s to exist", key)
		}
	}
}

func TestSafeMapExpiredAndGetConcurrent(t *testing.T) {
	sm := NewSafeMap[int]()
	var wg sync.WaitGroup

	numGoroutines := 50
	numOperations := 200

	// Set keys with expiration
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			key := "key_" + strconv.Itoa(i*numOperations+j)
			value := rand.Intn(1000)
			expire := time.Duration(rand.Intn(100)+50) * time.Millisecond
			sm.SetWithExpireDuration(key, value, expire)
		}
	}

	// Concurrently call ExpiredAndGet
	for i := 0; i < numGoroutines*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "key_" + strconv.Itoa(rand.Intn(numGoroutines*numOperations))
				_, _ = sm.ExpiredAndGet(key)
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// Wait for all possible expirations to occur
	time.Sleep(200 * time.Millisecond)

	// Ensure that expired keys are removed
	if sm.Len() != 0 {
		t.Errorf("Expected all keys to have expired")
	}
}
func TestSafeMapRange(t *testing.T) {
	sm := NewSafeMap[int]()

	// Set some key-value pairs
	sm.Set("key1", 1)
	sm.Set("key2", 2)
	sm.Set("key3", 3)

	// Use Range to iterate over the map and collect the keys and values
	collected := make(map[string]int)
	sm.Range(func(key string, value int) bool {
		collected[key] = value
		return true // Continue iteration
	})

	// Verify that all entries are collected
	if len(collected) != 3 {
		t.Errorf("Expected to collect 3 entries, got %d", len(collected))
	}
	for i := 1; i <= 3; i++ {
		key := "key" + strconv.Itoa(i)
		value, exists := collected[key]
		if !exists || value != i {
			t.Errorf("Expected collected[%s] to be %d, got %v", key, i, value)
		}
	}
}

func TestSafeMapClearConcurrent(t *testing.T) {
	sm := NewSafeMap[int]()
	var wg sync.WaitGroup
	
	// Fill the map with some data
	for i := 0; i < 1000; i++ {
		sm.Set("key_"+strconv.Itoa(i), i)
	}
	
	// Ensure the data was added properly
	if sm.Len() != 1000 {
		t.Errorf("Expected to have 1000 entries before clearing, got %d", sm.Len())
	}
	
	// Number of goroutines to run
	numGoroutines := 10
	
	// Start goroutines that use the map while clearing
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id == 0 {
				// One goroutine clears the map
				sm.Clear()
			} else if id % 2 == 0 {
				// Half of the others read from the map
				for j := 0; j < 100; j++ {
					key := "key_" + strconv.Itoa(rand.Intn(1000))
					_, _ = sm.Get(key)
					time.Sleep(time.Microsecond)
				}
			} else {
				// The other half write to the map
				for j := 0; j < 100; j++ {
					key := "new_key_" + strconv.Itoa(id) + "_" + strconv.Itoa(j)
					sm.Set(key, j)
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// Ensure the map still works after concurrent clearing and operations
	// Add some data again
	for i := 0; i < 10; i++ {
		sm.Set("post_clear_key_"+strconv.Itoa(i), i)
	}
	
	// Verify we can read the data
	for i := 0; i < 10; i++ {
		key := "post_clear_key_" + strconv.Itoa(i)
		val, exists := sm.Get(key)
		if !exists || val != i {
			t.Errorf("Expected %s to exist with value %d after Clear and new additions", key, i)
		}
	}
}

func TestSafeMapClearAfterExpiration(t *testing.T) {
	sm := NewSafeMap[int]()
	
	// Add some items with expiration
	for i := 0; i < 10; i++ {
		sm.SetWithExpireDuration("expire_key_"+strconv.Itoa(i), i, 50*time.Millisecond)
	}
	
	// Add some items without expiration
	for i := 0; i < 10; i++ {
		sm.Set("permanent_key_"+strconv.Itoa(i), i)
	}
	
	// Wait for expiration
	time.Sleep(100 * time.Millisecond)
	
	// Verify expired items are gone
	for i := 0; i < 10; i++ {
		_, exists := sm.Get("expire_key_" + strconv.Itoa(i))
		if exists {
			t.Errorf("Key expire_key_%d should have expired", i)
		}
	}
	
	// Now clear the map
	sm.Clear()
	
	// Verify all items are gone
	if sm.Len() != 0 {
		t.Errorf("Expected length to be 0 after Clear, got %d", sm.Len())
	}
	
	// Add new items and verify they work correctly
	sm.Set("new_key", 100)
	val, exists := sm.Get("new_key")
	if !exists || val != 100 {
		t.Errorf("Expected new key to exist with value 100 after Clear")
	}
}

func TestSafeMapClearAndDelete(t *testing.T) {
	sm := NewSafeMap[int]()
	
	// Add some items
	for i := 0; i < 100; i++ {
		sm.Set("key_"+strconv.Itoa(i), i)
	}
	
	// Clear the map
	sm.Clear()
	
	// Verify it's empty
	if sm.Len() != 0 {
		t.Errorf("Expected length to be 0 after Clear, got %d", sm.Len())
	}
	
	// Try to delete keys that don't exist anymore
	for i := 0; i < 10; i++ {
		sm.Delete("key_" + strconv.Itoa(i))
	}
	
	// Verify the length is still 0
	if sm.Len() != 0 {
		t.Errorf("Expected length to be 0 after deleting cleared keys, got %d", sm.Len())
	}
	
	// Add new items
	for i := 0; i < 10; i++ {
		sm.Set("new_key_"+strconv.Itoa(i), i)
	}
	
	// Verify the length is correct
	if sm.Len() != 10 {
		t.Errorf("Expected length to be 10 after adding new keys, got %d", sm.Len())
	}
}

func TestSafeMapDeleteWithComplexPrefixes(t *testing.T) {
	sm := NewSafeMap[string]()
	
	// Add keys with complex prefixes
	userUUID := "550e8400-e29b-41d4-a716-446655440000"
	personaUUID := "f47ac10b-58cc-4372-a567-0e02b2c3d479"
	slugName := "john-doe"
	userSlug := "user-123"
	
	// Add various types of keys
	sm.Set("/Persona/"+slugName+"/details", "persona details")
	sm.Set("/Persona/"+slugName+"/settings", "persona settings")
	sm.Set("/Persona/"+slugName+"/posts/recent", "recent posts")
	sm.Set("/User/_Personas/"+userSlug+"/list", "user personas list")
	sm.Set("/User/_Personas/"+userSlug+"/count", "user personas count")
	sm.Set("persona:"+personaUUID+":info", "persona info by UUID")
	sm.Set("persona:"+personaUUID+":stats", "persona stats by UUID")
	sm.Set("user:"+userUUID+":session", "user session")
	sm.Set("user:"+userUUID+":prefs", "user preferences")
	sm.Set("thread:user:"+userUUID+":posts", "user posts")
	sm.Set("unrelated:key", "unrelated value")
	
	// Initial count should be 11
	if sm.Len() != 11 {
		t.Errorf("Expected 11 items, got %d", sm.Len())
	}
	
	// Test deleting with the Persona slug prefix
	sm.DeleteAllKeysStartingWith("/Persona/" + slugName)
	
	// Should have deleted 3 keys
	if sm.Len() != 8 {
		t.Errorf("Expected 8 remaining items after deleting Persona slug prefix, got %d", sm.Len())
	}
	
	// Verify specific keys are gone
	if sm.Exists("/Persona/"+slugName+"/details") {
		t.Errorf("Expected /Persona/%s/details to be deleted", slugName)
	}
	
	// Test deleting with the User personas prefix
	sm.DeleteAllKeysStartingWith("/User/_Personas/" + userSlug)
	
	// Should have deleted 2 more keys
	if sm.Len() != 6 {
		t.Errorf("Expected 6 remaining items after deleting User personas prefix, got %d", sm.Len())
	}
	
	// Test deleting with the persona UUID prefix
	sm.DeleteAllKeysStartingWith("persona:" + personaUUID)
	
	// Should have deleted 2 more keys
	if sm.Len() != 4 {
		t.Errorf("Expected 4 remaining items after deleting persona UUID prefix, got %d", sm.Len())
	}
	
	// Test deleting with the user UUID prefix (includes thread:user:)
	sm.DeleteAllKeysStartingWith("user:" + userUUID)
	
	// Should have deleted 2 more keys (but not thread:user:)
	if sm.Len() != 2 {
		t.Errorf("Expected 2 remaining items after deleting user UUID prefix, got %d", sm.Len())
	}
	
	// Verify thread:user: key still exists
	if !sm.Exists("thread:user:"+userUUID+":posts") {
		t.Errorf("Expected thread:user:%s:posts to still exist", userUUID)
	}
	
	// Now test exact match prefix for thread:user:
	sm.DeleteAllKeysStartingWith("thread:user:" + userUUID)
	
	// Should have deleted 1 more key
	if sm.Len() != 1 {
		t.Errorf("Expected 1 remaining item after deleting thread:user: prefix, got %d", sm.Len())
	}
	
	// Only unrelated:key should remain
	if !sm.Exists("unrelated:key") {
		t.Errorf("Expected unrelated:key to still exist")
	}
}
