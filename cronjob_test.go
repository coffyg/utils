package utils

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestBasicScheduling checks that two jobs run with different intervals
// and do not block each other, verifying counts.
func TestBasicScheduling(t *testing.T) {
	logger := zerolog.New(io.Discard)
	cron := NewCronManager(&logger)

	var countA, countB int32

	jobA := func() {
		atomic.AddInt32(&countA, 1)
	}
	jobB := func() {
		atomic.AddInt32(&countB, 1)
	}

	// 1-second and 2-second intervals, but forced to minimum 5 seconds
	cron.AddCron("A", jobA, 1*time.Second, true) // <= will become 5 seconds
	cron.AddCron("B", jobB, 2*time.Second, false)

	cron.Start()
	defer cron.Stop()

	// We expect at least 1 run of each within ~6-7 seconds
	time.Sleep(7 * time.Second)

	a := atomic.LoadInt32(&countA)
	b := atomic.LoadInt32(&countB)

	if a < 1 {
		t.Errorf("Job A did not run at least once, got %d", a)
	}
	if b < 1 {
		t.Errorf("Job B did not run at least once, got %d", b)
	}
	t.Logf("A ran %d times, B ran %d times", a, b)
}

// TestReplacement ensures that if we replace a job with the same name,
// the old one is gone, and the new one is scheduled properly.
func TestReplacement(t *testing.T) {
	logger := zerolog.New(io.Discard)
	cron := NewCronManager(&logger)

	var count1, count2 int32
	job1 := func() { atomic.AddInt32(&count1, 1) }
	job2 := func() { atomic.AddInt32(&count2, 1) }

	cron.AddCron("test", job1, 5*time.Second, true)
	cron.Start()

	// Wait a bit for job1 to run
	time.Sleep(2 * time.Second)

	// Replace with job2
	cron.AddCron("test", job2, 5*time.Second, true)

	// Wait a bit more
	time.Sleep(2 * time.Second)
	cron.Stop()

	c1 := atomic.LoadInt32(&count1)
	c2 := atomic.LoadInt32(&count2)

	// Should have at least 1 run of job1 and 1 run of job2
	if c1 < 1 {
		t.Errorf("Expected replaced job to run at least once, got %d", c1)
	}
	if c2 < 1 {
		t.Errorf("Expected replacement job to run at least once, got %d", c2)
	}
	t.Logf("Job1 ran %d times, job2 ran %d times", c1, c2)
}

// TestStop waits for a job that is still running.
// In this simpler code, we only check that Stop() waits for the job goroutines to finish.
func TestStop(t *testing.T) {
	logger := zerolog.New(io.Discard)
	cron := NewCronManager(&logger)

	var running atomic.Bool
	longJob := func() {
		running.Store(true)
		time.Sleep(2 * time.Second)
		running.Store(false)
	}
	cron.AddCron("long", longJob, 5*time.Second, true)

	startTime := time.Now()
	cron.Start()

	// Wait for the job to start
	time.Sleep(200 * time.Millisecond)
	if !running.Load() {
		t.Fatal("Long job never started")
	}

	// Stop should block until the job finishes
	cron.Stop()

	elapsed := time.Since(startTime)
	// The job runs for 2s, so Stop should take at least that long
	if elapsed < 2*time.Second {
		t.Errorf("Stop returned too quickly (in %v); expected it to wait ~2s", elapsed)
	}
	t.Logf("Stop took %v, as expected, waiting for job to finish", elapsed)
}

// TestNoJobs ensures Start/Stop do nothing weird if no jobs are present.
func TestNoJobs(t *testing.T) {
	logger := zerolog.New(io.Discard)
	cron := NewCronManager(&logger)
	cron.Start()
	// Should not block or do anything unusual
	cron.Stop()
	t.Log("No jobs test passed (didn't crash).")
}

// TestConcurrentAdd tests that adding jobs while running doesn't cause issues.
func TestConcurrentAdd(t *testing.T) {
	logger := zerolog.New(io.Discard)
	cron := NewCronManager(&logger)

	// Job that increments a counter
	var count int32
	baseJob := func() { atomic.AddInt32(&count, 1) }

	cron.Start()
	defer cron.Stop()

	// We do concurrency: in one goroutine, we add jobs. In another, we wait.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			name := "job-" + time.Now().Format("150405.000000")
			cron.AddCron(name, baseJob, 5*time.Second, true)
			time.Sleep(100 * time.Millisecond)
		}
		close(done)
	}()

	// Meanwhile, we wait for a bit
	time.Sleep(2 * time.Second)
	<-done // ensure the add loop is finished

	// count should be > 0 because we added jobs that run immediately
	if c := atomic.LoadInt32(&count); c < 1 {
		t.Errorf("Expected some runs, got %d", c)
	} else {
		t.Logf("Got %d runs while concurrently adding jobs", c)
	}
}
