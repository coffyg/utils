package utils

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestCronManager(t *testing.T) {
	// Create a logger that doesn't output to stdout for testing
	logger := zerolog.New(io.Discard).With().Timestamp().Logger()

	// Create a new CronManager
	cronManager := NewCronManager(&logger)

	// Start the cron manager
	cronManager.Start()
	// We will call cronManager.Stop() explicitly later

	// Use WaitGroup to wait for the first execution of each job
	var wg sync.WaitGroup
	wg.Add(2)

	// Create a channel to signal that the test is over
	doneChan := make(chan struct{})

	// Create a channel to capture job execution order
	executedJobs := make(chan string, 10)

	// WaitGroup to track in-flight jobs
	var jobsWG sync.WaitGroup

	// Define jobs with sync.Once to ensure wg.Done() is called only once per job
	var job1Once sync.Once
	job1 := func() {
		jobsWG.Add(1)
		defer jobsWG.Done()

		select {
		case <-doneChan:
			// Test is over, do not send
		case executedJobs <- "job1":
			// Sent successfully
		}
		job1Once.Do(func() {
			wg.Done()
		})
	}

	var job2Once sync.Once
	job2 := func() {
		jobsWG.Add(1)
		defer jobsWG.Done()

		select {
		case <-doneChan:
			// Test is over, do not send
		case executedJobs <- "job2":
			// Sent successfully
		}
		job2Once.Do(func() {
			wg.Done()
		})
	}

	// Add jobs to the cron manager
	cronManager.AddCron("job1", job1, 100*time.Millisecond, true)
	cronManager.AddCron("job2", job2, 200*time.Millisecond, false)

	// Wait for jobs to finish, with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Jobs have executed
	case <-time.After(1 * time.Second):
		t.Fatal("Jobs did not execute within 1 second")
	}

	// Stop the cron manager and wait for dispatcher to finish
	cronManager.Stop()

	// Signal to jobs to stop sending to executedJobs
	close(doneChan)

	// Wait for any in-flight job executions to complete
	jobsWG.Wait()

	// Now it's safe to close executedJobs
	close(executedJobs)

	// Collect the executed job names into a map
	jobNamesMap := make(map[string]bool)
	for jobName := range executedJobs {
		jobNamesMap[jobName] = true
	}

	// Verify that both jobs were executed
	if len(jobNamesMap) != 2 {
		t.Errorf("Expected 2 jobs to be executed, but got %d", len(jobNamesMap))
	}

	if !jobNamesMap["job1"] {
		t.Errorf("Job1 did not execute")
	}
	if !jobNamesMap["job2"] {
		t.Errorf("Job2 did not execute")
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
