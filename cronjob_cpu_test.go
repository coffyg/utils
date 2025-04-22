package utils

import (
	"fmt"
	"io"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestCronCPUUsage is a special test that analyzes CPU usage patterns in the cron system
// to identify potential performance issues
func TestCronCPUUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping CPU usage test in short mode")
	}

	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Configure test parameters for CPU profiling
	const numJobs = 50
	const interval = 20 * time.Millisecond 
	const runDuration = 2 * time.Second // Longer duration to get better profile data
	
	// Report test configuration
	fmt.Printf("=== CPU Usage Test Configuration ===\n")
	fmt.Printf("Number of CPUs: %d\n", runtime.NumCPU())
	fmt.Printf("Number of jobs: %d\n", numJobs)
	fmt.Printf("Job interval: %v\n", interval)
	fmt.Printf("Test duration: %v\n", runDuration)
	fmt.Printf("===================================\n\n")

	// Track total executions to avoid optimizer eliminating work
	var totalExecs atomic.Int64
	var timeoutsCount atomic.Int64
	
	// Create new manager
	cronManager := NewCronManager(&logger)
	
	// Add CPU-heavy job
	for i := 0; i < numJobs; i++ {
		// Capture i in the closure
		jobID := i 
		
		// Create a job that simulates realistic load
		job := func() {
			// Simulate real work with CPU and I/O mix
			_ = time.Now() // Time tracking removed but kept as placeholder
			
			// Busy work mixed with occasional Sleep to simulate real workload
			var total int64
			workload := jobID % 5 // Vary the work per job
			
			if workload == 0 {
				// CPU bound job
				for j := 0; j < 10000; j++ {
					total += int64(j)
				}
			} else if workload == 1 {
				// Short computation + sleep (I/O simulation)
				for j := 0; j < 1000; j++ {
					total += int64(j)
				}
				time.Sleep(interval / 5)
			} else if workload == 2 {
				// Timeout-prone job
				dur := interval * 3
				time.Sleep(dur) 
				timeoutsCount.Add(1)
			} else {
				// Normal job with mixed compute/wait
				for j := 0; j < 5000; j++ {
					total += int64(j)
				}
				time.Sleep(interval / 10)
			}
			
			// Record execution
			totalExecs.Add(1)
			_ = total // Prevent optimization
		}
		
		// Add job - some immediate, some delayed
		name := "cpu-job-" + strconv.Itoa(i)
		cronManager.AddCron(name, job, interval, i%2 == 0)
	}
	
	// Start a goroutine to report execution rate
	stopReport := make(chan struct{})
	go func() {
		lastCount := int64(0)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				current := totalExecs.Load()
				execRate := float64(current - lastCount) * 2 // per second
				lastCount = current
				fmt.Printf("Execution rate: %.1f jobs/sec, Total: %d, Timeouts: %d\n", 
					execRate, current, timeoutsCount.Load())
			case <-stopReport:
				return
			}
		}
	}()
	
	// Start CPU profiling
	fmt.Println("Starting profiling...")
	// We'll skip actual profiling in the test, but would use this in a local diagnostic:
	// f, _ := os.Create("cpu_profile.pprof")
	// pprof.StartCPUProfile(f)
	
	// Start the manager
	startTime := time.Now()
	cronManager.Start()
	
	// Run for test duration
	time.Sleep(runDuration)
	
	// Stop the manager
	cronManager.Stop()
	
	// Stop profiling and reporting
	// pprof.StopCPUProfile()
	close(stopReport)
	
	// Report execution statistics
	totalRun := totalExecs.Load()
	elapsed := time.Since(startTime)
	execPerSec := float64(totalRun) / elapsed.Seconds()
	
	fmt.Printf("\n=== CPU Usage Test Results ===\n")
	fmt.Printf("Total time: %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("Total executions: %d\n", totalRun)
	fmt.Printf("Execution rate: %.2f jobs/sec\n", execPerSec)
	fmt.Printf("Timeouts: %d\n", timeoutsCount.Load())
	
	// Analyze common issues
	goroutines := runtime.NumGoroutine()
	fmt.Printf("Goroutines at end: %d\n", goroutines)
	
	// Analyze memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory usage: %.2f MB\n", float64(m.Alloc)/1024/1024)
	
	// Check for signs of CPU thrashing
	if goroutines > numJobs*2 {
		fmt.Printf("WARNING: Unusually high goroutine count - possible goroutine leak\n")
	}
	
	// For test reporting, find main CPU users through pprof
	pprof.Lookup("goroutine").WriteTo(io.Discard, 1)
}

// This test checks if the problem might be with tight spin loops consuming CPU
func TestCronCPUSpinCheck(t *testing.T) {
	// Skip in normal test runs
	if testing.Short() {
		t.Skip("Skipping CPU spin check in short mode")
	}
	
	// Create a logger
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()
	
	// Configure test for spin detection
	const numJobs = 10
	const spinInterval = 5 * time.Millisecond // Very aggressive interval
	
	// Create a manager
	cronManager := NewCronManager(&logger)
	
	// Add some jobs that should create a CPU bottleneck
	var count atomic.Int64
	
	for i := 0; i < numJobs; i++ {
		job := func() {
			// Do virtually nothing in the job
			count.Add(1)
		}
		
		name := "spin-job-" + strconv.Itoa(i)
		cronManager.AddCron(name, job, spinInterval, true)
	}
	
	// Start the manager
	fmt.Println("Running spin test for 1 second...")
	cronManager.Start()
	
	// Monitor CPU seconds
	startTime := time.Now()
	time.Sleep(time.Second)
	cronManager.Stop()
	elapsed := time.Since(startTime)
	
	// Report findings
	fmt.Printf("Spin test: %d job executions in %.2f seconds\n", count.Load(), elapsed.Seconds())
	fmt.Printf("Jobs were scheduled every %v with empty work\n", spinInterval)
	fmt.Printf("If CPU usage was high during this test, there may be a spin loop issue in the cron system\n")
}