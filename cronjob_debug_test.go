package utils

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestCronJobDebugExceedInterval debugs the parallel execution issue
func TestCronJobDebugExceedInterval(t *testing.T) {
	// Use a logger that outputs to console for debugging
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	
	manager := NewCronManager(&logger)
	manager.Start()
	defer manager.Stop()

	var (
		currentlyRunning int32
		executionCount   int32
	)

	job := func() {
		execNum := atomic.AddInt32(&executionCount, 1)
		running := atomic.AddInt32(&currentlyRunning, 1)
		
		logger.Info().Msgf("Job execution #%d started, %d running", execNum, running)
		
		if running > 1 {
			t.Errorf("PARALLEL EXECUTION! %d instances running at execution %d", running, execNum)
		}
		
		// Simulate 500ms work
		time.Sleep(500 * time.Millisecond)
		
		atomic.AddInt32(&currentlyRunning, -1)
		logger.Info().Msgf("Job execution #%d completed", execNum)
	}

	// Add job with 100ms interval
	manager.AddCron("debug-test", job, 100*time.Millisecond, true)
	
	// Let it run for just 2 seconds
	time.Sleep(2 * time.Second)
	
	finalCount := atomic.LoadInt32(&executionCount)
	t.Logf("Total executions: %d", finalCount)
}