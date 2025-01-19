package utils

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	// Set up the Redis client before running tests
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := client.Ping(ctx).Err(); err != nil {
		panic("Failed to connect to Redis: " + err.Error())
	}

	SetRedisClientForUtils(client)

	// Clear the database before tests
	client.FlushDB(ctx)

	code := m.Run()

	// Clean up after tests if needed
	client.FlushDB(ctx)
	os.Exit(code)
}

func TestSafeMapRedisBasicOperations(t *testing.T) {
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

func TestSafeMapRedisExpiration(t *testing.T) {
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

func TestSafeMapRedisUpdateExpireTime(t *testing.T) {
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

func TestSafeMapRedisDeleteAllKeysStartingWith(t *testing.T) {
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

func TestSafeMapRedisConcurrentAccess(t *testing.T) {
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

func TestSafeMapRedisSetWithExpireDurationConcurrent(t *testing.T) {
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

func TestSafeMapRedisExpiredAndGetConcurrent(t *testing.T) {
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

func TestSafeMapRedisRange(t *testing.T) {
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

func TestRedisMapClientDifferentDB(t *testing.T) {
	// Create a new Redis client pointing to a different DB (e.g., DB = 1)
	customClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1, // Use DB #1 instead of the default DB #0
	})

	// Verify that the custom client is reachable
	if err := customClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis DB=1: %v", err)
	}

	// Clear DB=1 before tests
	if err := customClient.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush DB=1: %v", err)
	}

	// Create a new Redis map with the *custom* client
	customMap := NewRedisMapClient[int](customClient)

	// Insert data into the custom map (DB=1)
	customMap.Set("key_db1", 42)

	// Verify the data is present in DB=1
	val, exists := customMap.Get("key_db1")
	if !exists || val != 42 {
		t.Errorf("Expected key_db1 in DB=1 to have value 42, got %v", val)
	}

	// Now check the *global* map (which by default points to DB=0)
	globalMap := NewRedisMap[int]() // uses the global redisClient (DB=0)
	valGlobal, existsGlobal := globalMap.Get("key_db1")
	if existsGlobal {
		t.Errorf("Did not expect to find key_db1 in the global map (DB=0), but got value %v", valGlobal)
	}

	// Clean up DB=1
	if err := customClient.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush DB=1 after test: %v", err)
	}
}
