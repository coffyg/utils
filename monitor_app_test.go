package utils

import (
	"testing"
	"time"
)

func TestMonitor_StartAndStop(t *testing.T) {
	metricsCollected := false
	collectionCount := 0

	onMetrics := func(metrics Metrics) {
		metricsCollected = true
		collectionCount++
		// Optionally, you can log or process the metrics here
		// For testing, we'll just check that values are reasonable
		if metrics.OpenFileDescriptors <= 0 {
			t.Errorf("Expected positive number of open file descriptors, got %d", metrics.OpenFileDescriptors)
		}
		if metrics.NumGoroutines <= 0 {
			t.Errorf("Expected positive number of goroutines, got %d", metrics.NumGoroutines)
		}
		if metrics.Timestamp.IsZero() {
			t.Errorf("Expected non-zero timestamp")
		}
	}

	monitor := NewMonitor(200*time.Millisecond, onMetrics)
	monitor.Start()

	// Let the monitor collect metrics for a few intervals
	time.Sleep(1 * time.Second)
	monitor.Stop()

	if !metricsCollected {
		t.Error("Metrics were not collected")
	}
	if collectionCount < 4 {
		t.Errorf("Expected at least 4 metric collections, got %d", collectionCount)
	}
}

func TestMonitor_StopBeforeStart(t *testing.T) {
	monitor := NewMonitor(200*time.Millisecond, nil)
	// Stopping before starting should not cause a panic
	monitor.Stop()
}

func TestCountOpenFileDescriptors(t *testing.T) {
	numFDs := countOpenFileDescriptors()
	if numFDs <= 0 && numFDs != -1 {
		t.Errorf("Expected positive number of open file descriptors or -1 on error, got %d", numFDs)
	}
}

func BenchmarkMonitor_CollectMetrics(b *testing.B) {
	monitor := &Monitor{}
	for i := 0; i < b.N; i++ {
		_ = monitor.collectMetrics()
	}
}
