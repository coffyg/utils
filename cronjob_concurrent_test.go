package utils

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestCronDifferentJobsConcurrent verifies that different jobs can run in parallel
// while the same job cannot run in parallel with itself
func TestCronDifferentJobsConcurrent(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	type jobStats struct {
		name              string
		executions        int32
		currentlyRunning  int32
		maxConcurrent     int32
		concurrentWith    map[string]bool
		concurrentMutex   sync.Mutex
	}

	// Create multiple job types like in production
	jobs := []*jobStats{
		{name: "Preview_Personas", concurrentWith: make(map[string]bool)},
		{name: "Persona_Recent_XP", concurrentWith: make(map[string]bool)},
		{name: "Spont_Message", concurrentWith: make(map[string]bool)},
		{name: "NFWS_Videos", concurrentWith: make(map[string]bool)},
		{name: "Delete_MSGS_Failed", concurrentWith: make(map[string]bool)},
	}

	// Track what's currently running globally
	var runningJobs sync.Map

	// Create job functions
	for _, stats := range jobs {
		s := stats // Capture loop variable
		
		jobFunc := func() {
			// Increment execution count
			execNum := atomic.AddInt32(&s.executions, 1)
			
			// Check if already running (should never happen)
			running := atomic.AddInt32(&s.currentlyRunning, 1)
			
			// Update max concurrent for this specific job
			for {
				max := atomic.LoadInt32(&s.maxConcurrent)
				if running <= max || atomic.CompareAndSwapInt32(&s.maxConcurrent, max, running) {
					break
				}
			}
			
			if running > 1 {
				t.Errorf("%s: VIOLATION - Multiple instances running concurrently! (count: %d)", s.name, running)
			}
			
			// Mark this job as running globally
			runningJobs.Store(s.name, true)
			
			// Check what other jobs are running
			s.concurrentMutex.Lock()
			runningJobs.Range(func(key, value interface{}) bool {
				otherJob := key.(string)
				if otherJob != s.name && value.(bool) {
					s.concurrentWith[otherJob] = true
				}
				return true
			})
			s.concurrentMutex.Unlock()
			
			// Simulate work (variable duration)
			workTime := time.Duration(50+execNum%50) * time.Millisecond
			time.Sleep(workTime)
			
			// Mark as not running
			atomic.AddInt32(&s.currentlyRunning, -1)
			runningJobs.Store(s.name, false)
		}
		
		// Add cron with different intervals
		switch s.name {
		case "Preview_Personas":
			manager.AddCron(s.name, jobFunc, 200*time.Millisecond, true)
		case "Persona_Recent_XP":
			manager.AddCron(s.name, jobFunc, 150*time.Millisecond, false)
		case "Spont_Message":
			manager.AddCron(s.name, jobFunc, 180*time.Millisecond, false)
		case "NFWS_Videos":
			manager.AddCron(s.name, jobFunc, 100*time.Millisecond, true)
		case "Delete_MSGS_Failed":
			manager.AddCron(s.name, jobFunc, 250*time.Millisecond, false)
		}
	}

	// Run for 3 seconds
	time.Sleep(3 * time.Second)

	// Analyze results
	t.Log("=== Job Execution Summary ===")
	
	allConcurrentPairs := make(map[string]bool)
	
	for _, stats := range jobs {
		t.Logf("%s: Executions=%d, MaxConcurrent=%d", 
			stats.name, 
			atomic.LoadInt32(&stats.executions),
			atomic.LoadInt32(&stats.maxConcurrent))
		
		// Check that job never ran in parallel with itself
		if atomic.LoadInt32(&stats.maxConcurrent) > 1 {
			t.Errorf("%s: FAILED - Ran in parallel with itself (max concurrent: %d)", 
				stats.name, atomic.LoadInt32(&stats.maxConcurrent))
		}
		
		// Record concurrent executions
		stats.concurrentMutex.Lock()
		for other := range stats.concurrentWith {
			pair := fmt.Sprintf("%s+%s", stats.name, other)
			reversePair := fmt.Sprintf("%s+%s", other, stats.name)
			if !allConcurrentPairs[reversePair] {
				allConcurrentPairs[pair] = true
			}
		}
		stats.concurrentMutex.Unlock()
	}
	
	// Verify different jobs ran concurrently
	t.Log("\n=== Concurrent Execution Pairs ===")
	concurrentCount := 0
	for pair := range allConcurrentPairs {
		t.Logf("✓ %s ran concurrently", pair)
		concurrentCount++
	}
	
	if concurrentCount == 0 {
		t.Error("FAILED: No jobs ran concurrently with each other")
	} else {
		t.Logf("\nSUCCESS: Found %d different job pairs that ran concurrently", concurrentCount)
	}
}

// TestCronSameJobNeverParallel specifically tests that the same job never runs in parallel
func TestCronSameJobNeverParallel(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	var (
		currentlyRunning int32
		maxConcurrent    int32
		violations       []string
		violationsMutex  sync.Mutex
		executionCount   int32
		startTimes       []time.Time
		timeMutex        sync.Mutex
	)

	// Job that takes longer than its interval
	longJob := func() {
		execNum := atomic.AddInt32(&executionCount, 1)
		running := atomic.AddInt32(&currentlyRunning, 1)
		
		// Record start time
		now := time.Now()
		timeMutex.Lock()
		startTimes = append(startTimes, now)
		timeMutex.Unlock()
		
		// Update max concurrent
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if running <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, running) {
				break
			}
		}
		
		// Check for violation
		if running > 1 {
			violationsMutex.Lock()
			violations = append(violations, fmt.Sprintf(
				"Execution #%d at %s: %d instances running",
				execNum, now.Format("15:04:05.000"), running))
			violationsMutex.Unlock()
		}
		
		// Simulate long-running work (300ms, but interval is 100ms)
		time.Sleep(300 * time.Millisecond)
		
		atomic.AddInt32(&currentlyRunning, -1)
	}

	// Add job with short interval
	manager.AddCron("LongRunningJob", longJob, 100*time.Millisecond, true)

	// Run for 2 seconds
	time.Sleep(2 * time.Second)

	// Check results
	finalCount := atomic.LoadInt32(&executionCount)
	finalMax := atomic.LoadInt32(&maxConcurrent)
	
	t.Logf("Total executions: %d", finalCount)
	t.Logf("Max concurrent: %d", finalMax)
	
	// With 300ms execution time and 100ms interval:
	// We should see about 6-7 executions (not 20)
	if finalCount > 10 {
		t.Errorf("Too many executions (%d), suggesting overlapping runs", finalCount)
	}
	
	// Should never run in parallel
	if finalMax > 1 {
		t.Errorf("Job ran in parallel! Max concurrent: %d", finalMax)
		violationsMutex.Lock()
		for _, v := range violations {
			t.Error(v)
		}
		violationsMutex.Unlock()
	}
	
	// Check timing between executions
	timeMutex.Lock()
	defer timeMutex.Unlock()
	
	for i := 1; i < len(startTimes); i++ {
		gap := startTimes[i].Sub(startTimes[i-1])
		if gap < 250*time.Millisecond { // Should be at least 300ms apart
			t.Errorf("Executions %d and %d started too close: %v apart", i-1, i, gap)
		}
	}
}

// TestCronLongTermStability runs jobs for extended time to check for leaks or degradation
func TestCronLongTermStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-term stability test in short mode")
	}
	
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	// Track metrics
	var totalExecutions int64
	var totalErrors int64
	initialGoroutines := countGoroutines()
	
	// Add various jobs with different characteristics
	jobs := []struct {
		name     string
		interval time.Duration
		work     time.Duration
	}{
		{"FastJob", 50 * time.Millisecond, 10 * time.Millisecond},
		{"MediumJob", 200 * time.Millisecond, 50 * time.Millisecond},
		{"SlowJob", 500 * time.Millisecond, 200 * time.Millisecond},
		{"VariableJob", 100 * time.Millisecond, 0}, // Variable work time
	}
	
	for _, job := range jobs {
		j := job // Capture loop variable
		
		manager.AddCron(j.name, func() {
			atomic.AddInt64(&totalExecutions, 1)
			
			// Variable work time for some jobs
			workTime := j.work
			if workTime == 0 {
				workTime = time.Duration(time.Now().UnixNano()%100) * time.Millisecond
			}
			
			// Occasionally simulate errors
			if atomic.LoadInt64(&totalExecutions)%100 == 0 {
				atomic.AddInt64(&totalErrors, 1)
				// Don't panic, just log
			}
			
			time.Sleep(workTime)
		}, j.interval, false)
	}
	
	// Run for 30 seconds
	testDuration := 30 * time.Second
	checkInterval := 5 * time.Second
	checks := int(testDuration / checkInterval)
	
	for i := 0; i < checks; i++ {
		time.Sleep(checkInterval)
		
		// Check goroutine count
		currentGoroutines := countGoroutines()
		if currentGoroutines > initialGoroutines+20 {
			t.Errorf("Potential goroutine leak at %ds: started with %d, now have %d",
				(i+1)*5, initialGoroutines, currentGoroutines)
		}
		
		// Check metrics
		metrics := manager.GetMetrics()
		t.Logf("At %ds: Executions=%d, Scheduled=%d, Completed=%d, Dropped=%d, Goroutines=%d",
			(i+1)*5,
			atomic.LoadInt64(&totalExecutions),
			metrics["jobs_scheduled"],
			metrics["jobs_completed"],
			metrics["jobs_dropped"],
			currentGoroutines)
		
		// Ensure jobs are still being executed
		execBefore := atomic.LoadInt64(&totalExecutions)
		time.Sleep(1 * time.Second)
		execAfter := atomic.LoadInt64(&totalExecutions)
		
		if execAfter <= execBefore {
			t.Errorf("Jobs stopped executing at %ds", (i+1)*5)
		}
	}
	
	// Final checks
	finalGoroutines := countGoroutines()
	finalExecutions := atomic.LoadInt64(&totalExecutions)
	
	t.Logf("Final: Executions=%d, Errors=%d, Goroutines=%d (started with %d)",
		finalExecutions, atomic.LoadInt64(&totalErrors), finalGoroutines, initialGoroutines)
	
	// Should have executed many times
	if finalExecutions < 1000 {
		t.Errorf("Too few executions over %v: %d", testDuration, finalExecutions)
	}
	
	// Goroutines should not have grown significantly
	if finalGoroutines > initialGoroutines+10 {
		t.Errorf("Goroutine leak: started with %d, ended with %d", initialGoroutines, finalGoroutines)
	}
}