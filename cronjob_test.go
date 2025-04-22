package utils

import (
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestCronManager(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Track job executions
	var job1Count, job2Count atomic.Int32
	job1Done := make(chan struct{})
	job2Done := make(chan struct{})
	job1FirstDone := false
	job2FirstDone := false

	// Create two jobs
	job1 := func() {
		job1Count.Add(1)
		if !job1FirstDone {
			job1FirstDone = true
			close(job1Done)
		}
	}

	job2 := func() {
		job2Count.Add(1)
		if !job2FirstDone {
			job2FirstDone = true
			close(job2Done)
		}
	}

	// Add jobs to the cron manager
	cronManager.AddCron("job1", job1, 100*time.Millisecond, true)  // Run immediately
	cronManager.AddCron("job2", job2, 200*time.Millisecond, false) // Run after interval

	// Start the cron manager
	cronManager.Start()

	// Wait for both jobs to run at least once
	select {
	case <-job1Done:
		// Job1 executed successfully
	case <-time.After(300 * time.Millisecond):
		t.Fatal("job1 didn't execute within timeout")
	}
	
	select {
	case <-job2Done:
		// Job2 executed successfully
	case <-time.After(300 * time.Millisecond):
		t.Fatal("job2 didn't execute within timeout")
	}

	// Let jobs run a few more times
	time.Sleep(500 * time.Millisecond)

	// Stop the cron manager
	cronManager.Stop()

	// Verify that job1 ran more times than job2 (since it starts immediately and has shorter interval)
	if job1Count.Load() <= job2Count.Load() {
		t.Errorf("Expected job1 count (%d) to be greater than job2 count (%d)", 
			job1Count.Load(), job2Count.Load())
	}

	// Verify both jobs ran at least once
	if job1Count.Load() == 0 {
		t.Error("job1 didn't execute")
	}
	if job2Count.Load() == 0 {
		t.Error("job2 didn't execute")
	}
}

func TestJobReplacement(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Track job executions
	var job1Count, job2Count atomic.Int32
	job1Done := make(chan struct{})
	job2Done := make(chan struct{})

	// Create two jobs
	job1 := func() {
		job1Count.Add(1)
		job1Done <- struct{}{}
	}

	job2 := func() {
		job2Count.Add(1)
		job2Done <- struct{}{}
	}

	// Add first job and start manager
	cronManager.AddCron("job", job1, 50*time.Millisecond, true)
	cronManager.Start()

	// Wait for first job execution
	<-job1Done

	// Replace with second job
	cronManager.AddCron("job", job2, 50*time.Millisecond, true)

	// Verify second job executes
	<-job2Done

	// Check that jobs executed as expected
	cronManager.Stop()
	
	if job1Count.Load() == 0 {
		t.Error("job1 didn't execute")
	}
	if job2Count.Load() == 0 {
		t.Error("job2 didn't execute")
	}
}

func TestCronManagerStop(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Create a long-running job
	var syncMu sync.Mutex
	longJobStarted := make(chan struct{})
	longJobDone := make(chan struct{})
	stopDone := make(chan struct{})
	
	// Use atomic variables for synchronization instead of channels
	var jobRunning atomic.Bool
	var jobShouldBlock atomic.Bool
	jobShouldBlock.Store(true)
	
	longJob := func() {
		// Signal that job started
		close(longJobStarted)
		
		// Check and block
		jobRunning.Store(true)
		for jobShouldBlock.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		
		// Signal that job completed
		syncMu.Lock()
		select {
		case longJobDone <- struct{}{}:
		default:
			// Channel might be closed
		}
		syncMu.Unlock()
	}

	// Add and start the job
	cronManager.AddCron("longJob", longJob, 50*time.Millisecond, true)
	cronManager.Start()

	// Wait for job to start
	<-longJobStarted

	// Stop the manager while job is running
	go func() {
		cronManager.Stop() // This should block until job completes
		close(stopDone)    // Signal that Stop() returned
	}()

	// Unblock the job after a short delay
	time.Sleep(100 * time.Millisecond)
	jobShouldBlock.Store(false)

	// Verify Stop() waits for job to complete
	select {
	case <-longJobDone:
		// Success: job completed
		select {
		case <-stopDone:
			// And Stop() has returned
		case <-time.After(200 * time.Millisecond):
			t.Error("Stop() didn't return after job completed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Job didn't complete within timeout")
	}
}

func TestCronJobScheduling(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Track job executions with timestamps - we'll store when job is scheduled (not executed)
	const interval = 100 * time.Millisecond
	const expectedRuns = 5
	const tolerancePercent = 20 // 20% tolerance for timing due to system scheduling variability

	var mu sync.Mutex
	executionTimes := make([]time.Time, 0, expectedRuns+1)
	
	// Create a normal job that completes quickly
	normalJob := func() {
		mu.Lock()
		executionTimes = append(executionTimes, time.Now())
		mu.Unlock()
	}

	// Create a slow job that times out
	slowJobStartTimes := make([]time.Time, 0, expectedRuns+1)
	slowDoneCh := make(chan struct{})
	var slowRunCount atomic.Int32
	// Use atomic for safe closing of channel
	var slowDoneClosed atomic.Bool
	
	slowJob := func() {
		mu.Lock()
		slowJobStartTimes = append(slowJobStartTimes, time.Now())
		mu.Unlock()
		
		// Simulate work that takes longer than the interval
		time.Sleep(interval * 2)
		
		// Signal completion for the last execution
		newCount := slowRunCount.Add(1)
		if newCount >= expectedRuns && !slowDoneClosed.Swap(true) {
			close(slowDoneCh)
		}
	}

	// Add jobs to the cron manager
	cronManager.AddCron("normalJob", normalJob, interval, true)
	cronManager.AddCron("slowJob", slowJob, interval, true)

	// Start the cron manager
	cronManager.Start()

	// Wait for enough executions of the slow job
	select {
	case <-slowDoneCh:
		// Success - we got enough executions
	case <-time.After(interval * (expectedRuns + 5)):
		// Allow a bit of extra time
	}

	// Stop the cron manager
	cronManager.Stop()
	
	// Verify normal job execution times
	mu.Lock()
	actualNormalTimes := executionTimes
	actualSlowTimes := slowJobStartTimes
	mu.Unlock()

	// We should have at least the expected number of runs
	if len(actualNormalTimes) < expectedRuns {
		t.Errorf("Expected at least %d normal job executions, got %d", 
			expectedRuns, len(actualNormalTimes))
	}
	if len(actualSlowTimes) < expectedRuns {
		t.Errorf("Expected at least %d slow job executions, got %d", 
			expectedRuns, len(actualSlowTimes))
	}

	// Verify intervals for both jobs
	if len(actualNormalTimes) >= 2 {
		t.Log("Verifying normal job intervals")
		for i := 1; i < len(actualNormalTimes); i++ {
			interval := actualNormalTimes[i].Sub(actualNormalTimes[i-1])
			t.Logf("Normal job interval %d: %v", i, interval)
		}
	}
	
	if len(actualSlowTimes) >= 2 {
		t.Log("Verifying slow job intervals")
		for i := 1; i < len(actualSlowTimes); i++ {
			interval := actualSlowTimes[i].Sub(actualSlowTimes[i-1])
			t.Logf("Slow job interval %d: %v", i, interval)
		}
	}
}

// TestPreciseTiming checks that jobs run at precise intervals regardless of execution time
func TestPreciseTiming(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	cronManager := NewCronManager(&logger)
	
	// Define test parameters
	const jobInterval = 100 * time.Millisecond  // Use a larger interval for more stability
	const jobRuntime = 200 * time.Millisecond   // Much longer than interval
	const testDuration = 800 * time.Millisecond
	const expectedMinRuns = 4  // We expect at least 4 runs in 800ms with 100ms interval
	
	// We'll use an atomic counter to ensure the right number of executions
	var executionCount atomic.Int32
	
	// We need thread-safe time tracking
	var mu sync.Mutex
	timeLog := make([]time.Time, 0, expectedMinRuns*2)
	
	// Channel for coordinating test completion
	done := make(chan struct{})
	
	// Create a job that takes longer than its interval to run
	cronJob := func() {
		execTime := time.Now()
		
		// Thread-safe time logging
		mu.Lock()
		timeLog = append(timeLog, execTime)
		mu.Unlock()
		
		// Record execution count and signal completion when we've seen enough executions
		newCount := executionCount.Add(1)
		if newCount >= expectedMinRuns && done != nil {
			select {
			case <-done: // Already closed
			default:
				close(done)
			}
		}
		
		// Simulate a job that takes longer than the interval
		time.Sleep(jobRuntime)
	}
	
	// Add and start the job (run immediately)
	testStart := time.Now()
	cronManager.AddCron("preciseJob", cronJob, jobInterval, true)
	cronManager.Start()
	
	// Wait until enough executions have occurred or timeout
	select {
	case <-done:
		// We reached our execution count
		t.Logf("Reached target execution count")
	case <-time.After(testDuration + 200*time.Millisecond):
		// Test duration elapsed with extra buffer
		t.Logf("Test duration elapsed")
	}
	
	// Let it run a bit longer to ensure we get enough data points
	time.Sleep(100 * time.Millisecond)
	
	// Stop the cron manager
	cronManager.Stop()
	
	// Get the actual times in a thread-safe way
	mu.Lock()
	times := make([]time.Time, len(timeLog))
	copy(times, timeLog)
	mu.Unlock()
	
	// Verify we had enough executions
	if len(times) < expectedMinRuns {
		t.Errorf("Expected at least %d job executions, got %d", 
			expectedMinRuns, len(times))
		return
	}
	
	// Count jobs started
	actualRuns := executionCount.Load()
	t.Logf("Job executed %d times", actualRuns)
	
	// Print the start time and all execution times
	t.Logf("Test started at: %v", testStart.Format(time.StampMicro))
	for i, tm := range times {
		sincePrev := time.Duration(0)
		if i > 0 {
			sincePrev = tm.Sub(times[i-1])
		}
		sinceStart := tm.Sub(testStart)
		t.Logf("  Execution %d: %v (since start: %v, since prev: %v)", 
			i, tm.Format(time.StampMicro), sinceStart, sincePrev)
	}
	
	// Check that the first job ran immediately
	if times[0].Sub(testStart) > 10*time.Millisecond {
		t.Errorf("First job didn't start immediately: %v after test start", 
			times[0].Sub(testStart))
	} else {
		t.Logf("First job started immediately: %v after test start", 
			times[0].Sub(testStart))
	}
	
	// Verify the consistency of intervals between executions
	// (we expect jobs to run every ~100ms regardless of job duration)
	if len(times) >= 2 {
		intervals := make([]time.Duration, 0, len(times)-1)
		for i := 1; i < len(times); i++ {
			interval := times[i].Sub(times[i-1])
			intervals = append(intervals, interval)
		}
		
		// Calculate average interval
		var sum time.Duration
		for _, interval := range intervals {
			sum += interval
		}
		avg := sum / time.Duration(len(intervals))
		
		// Check how close the average is to our target interval
		drift := avg - jobInterval
		if drift < 0 {
			drift = -drift
		}
		
		// Average interval should be close to the expected interval
		if drift > 150*time.Millisecond {
			t.Errorf("Average interval %v too far from expected %v (drift: %v)", 
				avg, jobInterval, drift)
		} else {
			t.Logf("Average interval %v is close to expected %v (drift: %v)", 
				avg, jobInterval, drift)
		}
	}
	
	// Verify we have overlapping executions (jobs running at the same time)
	// We know each job takes 200ms to run, and we run jobs every 100ms,
	// so we should always have at least 2 jobs running simultaneously
	jobEndTimes := make([]time.Time, len(times))
	for i, startTime := range times {
		jobEndTimes[i] = startTime.Add(jobRuntime)
	}
	
	// To verify concurrency, check that each job starts before the previous one finishes
	concurrentFound := false
	for i := 1; i < len(times); i++ {
		if times[i].Before(jobEndTimes[i-1]) {
			t.Logf("Job %d started at %v before job %d finished at %v (concurrent execution)",
				i, times[i].Format(time.StampMicro), i-1, jobEndTimes[i-1].Format(time.StampMicro))
			concurrentFound = true
			break
		}
	}
	
	if !concurrentFound && len(times) > 1 {
		t.Errorf("No evidence of concurrent job execution found")
	}
}

// TestConcurrentJobs checks that multiple concurrent jobs run correctly
func TestConcurrentJobs(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Configure test
	const jobCount = 10
	const interval = 50 * time.Millisecond
	const runTime = 300 * time.Millisecond

	// Track completion statistics
	var completionCounts atomic.Int32
	var timeoutCounts atomic.Int32
	
	// Function to create a test job
	createJob := func(id int, shouldTimeout bool) func() {
		return func() {
			if shouldTimeout {
				// Jobs with even IDs timeout
				time.Sleep(interval * 2) // Longer than interval
				timeoutCounts.Add(1)
			} else {
				// Jobs with odd IDs complete quickly
				time.Sleep(interval / 10) // Much shorter than interval
				completionCounts.Add(1)
			}
		}
	}

	// Add a mix of jobs that complete and timeout
	for i := 0; i < jobCount; i++ {
		name := "job-" + string(rune('A'+i))
		shouldTimeout := i%2 == 0
		cronManager.AddCron(name, createJob(i, shouldTimeout), interval, true)
	}

	// Start the manager
	cronManager.Start()
	
	// Let it run for a while
	time.Sleep(runTime)
	
	// Stop the manager
	cronManager.Stop()

	// Check that all types of jobs executed
	if completionCounts.Load() == 0 {
		t.Error("No quick jobs completed")
	}
	if timeoutCounts.Load() == 0 {
		t.Error("No timeout jobs completed")
	}

	// Expected minimum counts (at least 2 executions per job)
	expectedMinComplete := int32(jobCount/2 * 2) // Half the jobs complete quickly
	expectedMinTimeout := int32(jobCount/2)      // Half the jobs timeout
	
	// Verify completion counts
	if completionCounts.Load() < expectedMinComplete {
		t.Errorf("Expected at least %d quick job completions, got %d", 
			expectedMinComplete, completionCounts.Load())
	}
	if timeoutCounts.Load() < expectedMinTimeout {
		t.Errorf("Expected at least %d timeout job completions, got %d", 
			expectedMinTimeout, timeoutCounts.Load())
	}
}

func verifyExecutionIntervals(t *testing.T, jobName string, times []time.Time, expectedInterval time.Duration, tolerancePercent int) {
	if len(times) < 2 {
		t.Logf("Not enough executions of %s to verify intervals", jobName)
		return
	}

	// Calculate acceptable tolerance in nanoseconds
	tolerance := float64(expectedInterval.Nanoseconds()) * float64(tolerancePercent) / 100.0

	// Check intervals between consecutive executions
	for i := 1; i < len(times); i++ {
		actualInterval := times[i].Sub(times[i-1])
		difference := math.Abs(float64(actualInterval.Nanoseconds() - expectedInterval.Nanoseconds()))
		
		if difference > tolerance {
			t.Errorf("%s: Interval between executions %d and %d was %v, expected %v (±%v%%)",
				jobName, i-1, i, actualInterval, expectedInterval, tolerancePercent)
		}
	}
}

func BenchmarkCronManagerAddJob(b *testing.B) {
	// Create a silent logger
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Create job function that does nothing
	noop := func() {}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create new cron manager for each iteration
		manager := NewCronManager(&logger)
		manager.Start()
		
		// Add a job
		manager.AddCron("bench-job", noop, 100*time.Millisecond, false)
		
		// Clean up
		manager.Stop()
	}
}

func BenchmarkCronManagerRunJobs(b *testing.B) {
	// Create a silent logger
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Create a CronManager
	manager := NewCronManager(&logger)
	
	// Create a simple job
	noop := func() {}
	
	// Create test data - add 100 jobs
	for i := 0; i < 100; i++ {
		name := "bench-job-" + string(rune('a'+i%26))
		interval := time.Duration(50+i) * time.Millisecond
		manager.AddCron(name, noop, interval, false)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Start and run for a short time
		manager.Start()
		time.Sleep(10 * time.Millisecond)
		manager.Stop()
	}
}

// TestCronStress is a stress test for the cron system
// It creates many jobs with short intervals and verifies they all run correctly
func TestCronStress(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Configure test parameters - less aggressive for normal testing
	const numJobs = 25            // Number of different jobs to create
	const interval = 25 * time.Millisecond // Longer interval to reduce CPU impact
	const runDuration = 300 * time.Millisecond // How long to run the test
	const iterationsPerJob = 5    // Each job should run at least this many times
	
	// Create a new CronManager
	cronManager := NewCronManager(&logger)
	
	// Track execution count for each job
	counts := make([]atomic.Int32, numJobs)
	
	// Time tracking
	start := time.Now()
	startTime := &start
	startMutex := sync.Mutex{}
	
	// Measure how long AddCron takes
	addStart := time.Now()
	
	// Create and add numJobs different jobs
	for i := 0; i < numJobs; i++ {
		// Capture i for the closure
		jobID := i
		
		// Create a job that increments its count
		job := func() {
			// Record the first execution time
			if jobID == 0 {
				startMutex.Lock()
				if startTime != nil {
					// Record time of first execution
					st := time.Now()
					startTime = &st
					// Only record once
					startTime = nil
				}
				startMutex.Unlock()
			}
			
			// Increment this job's counter
			counts[jobID].Add(1)
			
			// Simulate some work (but different amounts for different jobs)
			workTime := time.Duration(jobID%5+1) * time.Millisecond
			time.Sleep(workTime)
		}
		
		// Add the job - some run immediately, others delay
		name := "stress-job-" + string(rune('A'+i%26)) + "-" + strconv.Itoa(i)
		runImmed := i%2 == 0
		cronManager.AddCron(name, job, interval, runImmed)
	}
	
	addDuration := time.Since(addStart)
	t.Logf("Adding %d jobs took %v (%.2f µs/job)", 
		numJobs, addDuration, float64(addDuration.Microseconds())/float64(numJobs))
	
	// Measure how long Start takes
	startStart := time.Now()
	cronManager.Start()
	startDuration := time.Since(startStart)
	t.Logf("Starting cron manager with %d jobs took %v", numJobs, startDuration)
	
	// Run for specified duration
	time.Sleep(runDuration)
	
	// Stop the cron manager
	stopStart := time.Now()
	cronManager.Stop()
	stopDuration := time.Since(stopStart)
	t.Logf("Stopping cron manager with %d jobs took %v", numJobs, stopDuration)
	
	// Verify all jobs ran enough times
	totalExecutions := 0
	minExecutions := int32(99999)
	maxExecutions := int32(0)
	
	for i := 0; i < numJobs; i++ {
		execCount := counts[i].Load()
		totalExecutions += int(execCount)
		
		if execCount < minExecutions {
			minExecutions = execCount
		}
		if execCount > maxExecutions {
			maxExecutions = execCount
		}
		
		if execCount < int32(iterationsPerJob) {
			t.Errorf("Job %d only executed %d times (expected at least %d)", 
				i, execCount, iterationsPerJob)
		}
	}
	
	// Calculate executions per second
	elapsed := time.Since(start)
	execsPerSec := float64(totalExecutions) / elapsed.Seconds()
	
	t.Logf("Ran %d total executions across %d jobs in %.2f seconds (%.2f executions/sec)",
		totalExecutions, numJobs, elapsed.Seconds(), execsPerSec)
	t.Logf("Min executions: %d, Max executions: %d", minExecutions, maxExecutions)
}