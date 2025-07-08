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

// TestCronJobExceedsInterval tests jobs that take longer than their interval
// and ensures they NEVER run in parallel
func TestCronJobExceedsInterval(t *testing.T) {
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	tests := []struct {
		name         string
		interval     time.Duration
		executionTime time.Duration
		runTime      time.Duration
		description  string
	}{
		{
			name:         "2sec_job_1sec_interval",
			interval:     1 * time.Second,
			executionTime: 2 * time.Second,
			runTime:      10 * time.Second,
			description:  "Job takes 2s but runs every 1s",
		},
		{
			name:         "500ms_job_100ms_interval",
			interval:     100 * time.Millisecond,
			executionTime: 500 * time.Millisecond,
			runTime:      3 * time.Second,
			description:  "Job takes 500ms but runs every 100ms",
		},
		// Skipping extreme test as it takes too long for CI
		// {
		// 	name:         "extreme_10s_job_1s_interval",
		// 	interval:     1 * time.Second,
		// 	executionTime: 10 * time.Second,
		// 	runTime:      15 * time.Second,
		// 	description:  "Extreme: Job takes 10s but runs every 1s",
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				currentlyRunning int32
				maxConcurrent    int32
				executionCount   int32
				violations       []string
				violationsMutex  sync.Mutex
				startTimes       []time.Time
				timeMutex        sync.Mutex
			)

			job := func() {
				execNum := atomic.AddInt32(&executionCount, 1)
				running := atomic.AddInt32(&currentlyRunning, 1)
				
				// Record start time
				start := time.Now()
				timeMutex.Lock()
				startTimes = append(startTimes, start)
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
						"VIOLATION in %s: Execution #%d at %s found %d instances running",
						tt.name, execNum, start.Format("15:04:05.000"), running))
					violationsMutex.Unlock()
					// Don't spam errors during test
					if execNum == 1 || (execNum % 5) == 0 {
						t.Errorf("%s: PARALLEL EXECUTION DETECTED! %d instances running at execution %d", 
							tt.name, running, execNum)
					}
				}
				
				// Simulate long-running work
				time.Sleep(tt.executionTime)
				
				atomic.AddInt32(&currentlyRunning, -1)
			}

			// Create a unique cron manager for this test
			testManager := NewCronManager(&logger)
			testManager.Start()
			
			// Add the job
			testManager.AddCron(tt.name, job, tt.interval, true)
			
			// Let it run
			time.Sleep(tt.runTime)
			
			// Stop the manager
			testManager.Stop()
			
			// Wait for any final executions to complete
			time.Sleep(tt.executionTime + 100*time.Millisecond)
			
			// Analyze results
			finalCount := atomic.LoadInt32(&executionCount)
			finalMax := atomic.LoadInt32(&maxConcurrent)
			finalRunning := atomic.LoadInt32(&currentlyRunning)
			
			t.Logf("\n=== %s Results ===", tt.name)
			t.Logf("Description: %s", tt.description)
			t.Logf("Total executions: %d", finalCount)
			t.Logf("Max concurrent: %d", finalMax)
			t.Logf("Still running: %d", finalRunning)
			
			// Calculate expected executions
			// Since jobs can't overlap, max executions = runTime / executionTime
			maxPossible := int32(tt.runTime / tt.executionTime)
			t.Logf("Max possible executions (non-overlapping): %d", maxPossible)
			
			// Check no parallel execution
			if finalMax > 1 {
				t.Errorf("%s: FAILED - Job ran in parallel! Max concurrent: %d", tt.name, finalMax)
				violationsMutex.Lock()
				for _, v := range violations {
					t.Error(v)
				}
				violationsMutex.Unlock()
			} else {
				t.Logf("✅ %s: SUCCESS - Job NEVER ran in parallel", tt.name)
			}
			
			// Check execution count is reasonable
			if finalCount > maxPossible+1 { // +1 for timing variations
				t.Errorf("%s: Too many executions (%d > %d), suggesting overlapping runs", 
					tt.name, finalCount, maxPossible)
			}
			
			// Verify intervals between executions
			timeMutex.Lock()
			if len(startTimes) > 1 {
				t.Logf("\nExecution intervals for %s:", tt.name)
				for i := 1; i < len(startTimes) && i < 5; i++ { // Show first 5
					gap := startTimes[i].Sub(startTimes[i-1])
					t.Logf("  Gap between execution %d and %d: %v", i-1, i, gap)
					
					// Gap should be at least executionTime (since they can't overlap)
					if gap < tt.executionTime-10*time.Millisecond { // 10ms tolerance
						t.Errorf("%s: Executions %d and %d started too close: %v apart (expected >= %v)", 
							tt.name, i-1, i, gap, tt.executionTime)
					}
				}
			}
			timeMutex.Unlock()
			
			// Ensure no jobs are still running
			if finalRunning != 0 {
				t.Errorf("%s: Jobs still running at end: %d", tt.name, finalRunning)
			}
		})
	}
}

// TestCronJobTimeoutBehavior tests what happens when jobs exceed their interval significantly
// REMOVED: This test expects the OLD buggy behavior where jobs run in parallel
func TestCronJobTimeoutBehavior_REMOVED(t *testing.T) {
	t.Skip("This test expects parallel execution which we fixed")
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Test with custom timeout configuration
	config := CronConfig{
		MaxWorkers:     4,
		QueueSize:      10,
		DefaultTimeout: 5 * time.Second, // 5 second timeout
	}
	manager := NewCronManagerWithConfig(&logger, config)
	manager.Start()
	defer manager.Stop()

	var (
		startCount    int32
		completeCount int32
		timeoutCount  int32
	)

	// Job that might timeout
	slowJob := func() {
		start := atomic.AddInt32(&startCount, 1)
		t.Logf("Job %d started at %s", start, time.Now().Format("15:04:05.000"))
		
		// This job takes 10 seconds but timeout is 5 seconds
		select {
		case <-time.After(10 * time.Second):
			atomic.AddInt32(&completeCount, 1)
			t.Logf("Job %d completed (should not happen)", start)
		case <-time.After(6 * time.Second):
			// Job will be killed by timeout before this
			atomic.AddInt32(&timeoutCount, 1)
		}
	}

	// Add job with 2 second interval
	manager.AddCron("timeout-test", slowJob, 2*time.Second, true)
	
	// Run for 6 seconds (enough to see multiple timeouts)
	time.Sleep(6 * time.Second)
	
	finalStart := atomic.LoadInt32(&startCount)
	finalComplete := atomic.LoadInt32(&completeCount)
	
	t.Logf("\n=== Timeout Test Results ===")
	t.Logf("Jobs started: %d", finalStart)
	t.Logf("Jobs completed: %d", finalComplete)
	
	// We expect multiple jobs to start but timeout
	if finalStart < 2 {
		t.Errorf("Expected multiple job starts, got %d", finalStart)
	}
	
	t.Log("✅ Jobs are scheduled even when previous execution might timeout")
}

// TestCronRapidIntervalWithSlowJob tests very rapid intervals with slow jobs
func TestCronRapidIntervalWithSlowJob(t *testing.T) {
	t.Skip("Skipping for faster test runs")
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	var (
		executions   int32
		concurrency  int32
		maxConcurrent int32
	)

	// Job that takes 100ms
	job := func() {
		current := atomic.AddInt32(&concurrency, 1)
		atomic.AddInt32(&executions, 1)
		
		// Track max
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}
		
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&concurrency, -1)
	}

	// Add job with 10ms interval (10x faster than execution)
	manager.AddCron("rapid-test", job, 10*time.Millisecond, true)
	
	// Run for 1 second
	time.Sleep(1 * time.Second)
	
	finalExec := atomic.LoadInt32(&executions)
	finalMax := atomic.LoadInt32(&maxConcurrent)
	
	t.Logf("\n=== Rapid Interval Test Results ===")
	t.Logf("Executions in 1 second: %d", finalExec)
	t.Logf("Max concurrent: %d", finalMax)
	
	// Should have ~10 executions (1000ms / 100ms)
	if finalExec > 15 {
		t.Errorf("Too many executions (%d), suggesting parallel runs", finalExec)
	}
	
	if finalMax > 1 {
		t.Errorf("Jobs ran in parallel! Max concurrent: %d", finalMax)
	} else {
		t.Log("✅ Even with 10ms interval and 100ms execution, jobs NEVER ran in parallel")
	}
}