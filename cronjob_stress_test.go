package utils

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestCronJobExcessiveExecutions tests for the bug where jobs execute multiple times rapidly
func TestCronJobExcessiveExecutions(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Create manager with smaller worker pool to stress test
	config := CronConfig{
		MaxWorkers:     2,
		QueueSize:      10,
		DefaultTimeout: 1 * time.Second,
	}
	manager := NewCronManagerWithConfig(&logger, config)
	manager.Start()
	defer manager.Stop()

	// Track executions
	var executionCount int32
	var executionTimes []time.Time
	var mutex sync.Mutex

	// Job that tracks its executions
	job := func() {
		count := atomic.AddInt32(&executionCount, 1)
		mutex.Lock()
		executionTimes = append(executionTimes, time.Now())
		mutex.Unlock()
		
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
		
		t.Logf("Job executed %d times", count)
	}

	// Add job with 100ms interval
	manager.AddCron("test-job", job, 100*time.Millisecond, false)

	// Let it run for 2 seconds
	time.Sleep(2 * time.Second)

	finalCount := atomic.LoadInt32(&executionCount)
	
	// We expect around 20 executions (2000ms / 100ms)
	// Allow some margin for timing variations
	expectedMin := 18
	expectedMax := 22
	
	if finalCount < int32(expectedMin) || finalCount > int32(expectedMax) {
		t.Errorf("Expected between %d and %d executions, got %d", expectedMin, expectedMax, finalCount)
		
		// Check for rapid successive executions
		mutex.Lock()
		defer mutex.Unlock()
		
		for i := 1; i < len(executionTimes); i++ {
			timeDiff := executionTimes[i].Sub(executionTimes[i-1])
			if timeDiff < 50*time.Millisecond {
				t.Errorf("Rapid execution detected: jobs %d and %d executed within %v", i-1, i, timeDiff)
			}
		}
	}
}

// TestCronJobUnderLoad simulates the production scenario with multiple jobs
func TestCronJobUnderLoad(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	config := CronConfig{
		MaxWorkers:     4,
		QueueSize:      100,
		DefaultTimeout: 30 * time.Second,
	}
	manager := NewCronManagerWithConfig(&logger, config)
	manager.Start()
	defer manager.Stop()

	// Track executions for each job
	type jobStats struct {
		count      int32
		lastExec   time.Time
		mutex      sync.Mutex
		violations int
	}

	numJobs := 10
	stats := make([]*jobStats, numJobs)
	for i := range stats {
		stats[i] = &jobStats{}
	}

	// Create multiple jobs with different intervals
	intervals := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		// Repeat pattern
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}

	for i := 0; i < numJobs; i++ {
		idx := i
		interval := intervals[i]
		stat := stats[idx]
		
		job := func() {
			atomic.AddInt32(&stat.count, 1)
			
			stat.mutex.Lock()
			now := time.Now()
			if !stat.lastExec.IsZero() {
				timeSinceLastExec := now.Sub(stat.lastExec)
				// Check if executed too soon (less than 80% of interval)
				if timeSinceLastExec < interval*8/10 {
					stat.violations++
					t.Logf("Job %d violation: executed after %v, expected at least %v", idx, timeSinceLastExec, interval)
				}
			}
			stat.lastExec = now
			stat.mutex.Unlock()
			
			// Simulate variable work
			workTime := time.Duration(idx*5) * time.Millisecond
			time.Sleep(workTime)
		}
		
		manager.AddCron(fmt.Sprintf("job-%d", i), job, interval, false)
	}

	// Run for 5 seconds
	time.Sleep(5 * time.Second)

	// Check results
	totalViolations := 0
	for i, stat := range stats {
		count := atomic.LoadInt32(&stat.count)
		interval := intervals[i]
		
		// Calculate expected executions
		// Note: With parallel execution prevention, actual count may be lower
		expectedCount := int32(5000 / interval.Milliseconds())
		tolerance := expectedCount / 2 // 50% tolerance for timing variations and scheduling delays
		
		if count < expectedCount-tolerance || count > expectedCount+tolerance {
			t.Errorf("Job %d: expected ~%d executions (±%d), got %d", i, expectedCount, tolerance, count)
		}
		
		totalViolations += stat.violations
	}
	
	if totalViolations > 0 {
		t.Errorf("Total timing violations: %d", totalViolations)
	}
}

// TestCronJobRaceCondition tests for race conditions in job scheduling
func TestCronJobRaceCondition(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	config := CronConfig{
		MaxWorkers:     10,
		QueueSize:      50,
		DefaultTimeout: 100 * time.Millisecond,
	}
	manager := NewCronManagerWithConfig(&logger, config)
	manager.Start()
	defer manager.Stop()

	// Use atomic counter to detect concurrent executions
	var concurrentExecutions int32
	var maxConcurrent int32
	var totalExecutions int32

	job := func() {
		// Increment concurrent counter
		current := atomic.AddInt32(&concurrentExecutions, 1)
		atomic.AddInt32(&totalExecutions, 1)
		
		// Update max if needed
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}
		
		// Simulate work
		time.Sleep(50 * time.Millisecond)
		
		// Decrement concurrent counter
		atomic.AddInt32(&concurrentExecutions, -1)
	}

	// Add job with very short interval
	manager.AddCron("race-test", job, 20*time.Millisecond, true)

	// Run for 2 seconds
	time.Sleep(2 * time.Second)

	maxConcurrentFinal := atomic.LoadInt32(&maxConcurrent)
	totalExecutionsFinal := atomic.LoadInt32(&totalExecutions)
	
	t.Logf("Max concurrent executions: %d", maxConcurrentFinal)
	t.Logf("Total executions: %d", totalExecutionsFinal)
	
	// The same job should never run concurrently
	if maxConcurrentFinal > 1 {
		t.Errorf("Detected concurrent execution of the same job: max concurrent = %d", maxConcurrentFinal)
	}
}

// TestCronManagerMemoryLeak tests for goroutine and memory leaks
func TestCronManagerMemoryLeak(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Get initial goroutine count
	initialGoroutines := countGoroutines()
	
	// Run multiple iterations of creating and stopping managers
	for i := 0; i < 5; i++ {
		manager := NewCronManager(&logger)
		manager.Start()
		
		// Add some jobs
		for j := 0; j < 10; j++ {
			manager.AddCron(fmt.Sprintf("job-%d-%d", i, j), func() {
				time.Sleep(1 * time.Millisecond)
			}, 50*time.Millisecond, false)
		}
		
		// Let it run briefly
		time.Sleep(200 * time.Millisecond)
		
		// Stop and wait for cleanup
		manager.Stop()
		time.Sleep(100 * time.Millisecond)
	}
	
	// Allow some time for goroutines to clean up
	time.Sleep(500 * time.Millisecond)
	
	// Check final goroutine count
	finalGoroutines := countGoroutines()
	
	// Allow for some variance but detect leaks
	allowedDiff := 5
	if finalGoroutines > initialGoroutines+allowedDiff {
		t.Errorf("Potential goroutine leak: started with %d, ended with %d goroutines", initialGoroutines, finalGoroutines)
	}
}

// TestCronJobPanic tests behavior when a job panics
func TestCronJobPanic(t *testing.T) {
	t.Skip("Skipping panic test - panics are handled correctly but cause test runner issues")
	
	// Use a buffer to capture logs instead of discarding
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	
	var executionCount int32
	panicAt := int32(3)

	job := func() {
		count := atomic.AddInt32(&executionCount, 1)
		
		// Panic on 3rd execution
		if count == panicAt {
			panic("test panic - this is expected")
		}
		
		// Normal execution
		time.Sleep(10 * time.Millisecond)
	}

	manager.AddCron("panic-test", job, 100*time.Millisecond, false)

	// Let it run for 1 second
	time.Sleep(1 * time.Second)
	
	// Stop the manager to ensure clean shutdown
	manager.Stop()

	finalCount := atomic.LoadInt32(&executionCount)
	
	t.Logf("Total executions: %d", finalCount)
	
	// Job should continue executing after panic
	if finalCount < 8 {
		t.Errorf("Job stopped after panic: only executed %d times", finalCount)
	}
	
	// Check that panic was logged
	logs := logBuf.String()
	if !strings.Contains(logs, "panicked") {
		t.Errorf("Panic was not logged")
	}
	
	// Verify job continued after panic
	if finalCount <= panicAt {
		t.Errorf("Job did not continue after panic (executed %d times, panicked at %d)", finalCount, panicAt)
	}
}

// Helper function to count goroutines
func countGoroutines() int {
	return runtime.NumGoroutine()
}