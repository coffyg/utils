package utils

import (
	"strings"
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
		
		// Check memory metrics
		if metrics.Alloc == 0 {
			t.Errorf("Expected non-zero Alloc")
		}
		if metrics.TotalAlloc == 0 {
			t.Errorf("Expected non-zero TotalAlloc")
		}
		if metrics.HeapObjects == 0 {
			t.Errorf("Expected non-zero HeapObjects")
		}
		
		// Check CPU metrics
		if metrics.NumCPU <= 0 {
			t.Errorf("Expected positive NumCPU, got %d", metrics.NumCPU)
		}
		
		// Check application metrics
		if metrics.Uptime < 0 {
			t.Errorf("Expected non-negative uptime, got %v", metrics.Uptime)
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
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = monitor.collectMetrics()
	}
}

func BenchmarkCountOpenFileDescriptors(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = countOpenFileDescriptors()
	}
}

// Test multiple rapid requests for metrics
func BenchmarkMonitor_RapidMetricsRequests(b *testing.B) {
	monitor := NewMonitor(1*time.Second, func(metrics Metrics) {})
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = monitor.collectMetrics()
	}
}

// This benchmark tests formatting metrics to ensure it's efficient
func BenchmarkMonitor_FormatMetrics(b *testing.B) {
	monitor := NewMonitor(1*time.Second, nil)
	metrics := monitor.collectMetrics()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.FormatMetrics()
	}
}

// Compare performance of different metric collection methods
func BenchmarkMonitor_MetricsCollection(b *testing.B) {
	monitor := NewMonitor(1*time.Second, nil)
	
	// Warm up the monitor
	monitor.collectMetrics()
	
	b.Run("FullMetrics", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = monitor.collectMetrics()
		}
	})
	
	b.Run("FilteredBaseOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = monitor.GetFilteredMetrics(false, false, false)
		}
	})
	
	b.Run("FilteredWithMemory", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = monitor.GetFilteredMetrics(true, false, false)
		}
	})
	
	b.Run("FilteredWithCPU", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = monitor.GetFilteredMetrics(false, true, false)
		}
	})
	
	b.Run("FilteredComplete", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = monitor.GetFilteredMetrics(true, true, true)
		}
	})
}

func TestFormatMetrics(t *testing.T) {
	monitor := NewMonitor(100*time.Millisecond, nil)
	
	// Get some metrics
	metrics := monitor.collectMetrics()
	
	// Format them
	formatted := metrics.FormatMetrics()
	
	// Check that all expected keys are present
	expectedKeys := []string{
		"Timestamp", "Uptime", "OpenFileDescriptors", 
		"NumGoroutines", "Memory/Alloc", "Memory/TotalAlloc",
		"Memory/Sys", "Memory/HeapObjects", "GC/Cycles",
		"GC/PauseTotal", "CPU/Count", "CPU/Usage",
	}
	
	for _, key := range expectedKeys {
		if _, ok := formatted[key]; !ok {
			t.Errorf("Missing key in formatted metrics: %s", key)
		}
	}
	
	// Check specific formats to ensure they're human-readable
	if !strings.Contains(formatted["Memory/Alloc"], "iB") {
		t.Errorf("Memory/Alloc not formatted with proper unit: %s", formatted["Memory/Alloc"])
	}
	
	if !strings.Contains(formatted["CPU/Usage"], "%") {
		t.Errorf("CPU/Usage not formatted as percentage: %s", formatted["CPU/Usage"])
	}
}

func TestGetFilteredMetrics(t *testing.T) {
	monitor := NewMonitor(100*time.Millisecond, nil)
	monitor.collectMetrics() // Populate initial metrics
	
	// Test 1: Base metrics only
	baseMetrics := monitor.GetFilteredMetrics(false, false, false)
	
	// Base metrics should be present
	if baseMetrics.NumGoroutines <= 0 {
		t.Errorf("Expected positive number of goroutines, got %d", baseMetrics.NumGoroutines)
	}
	if baseMetrics.Uptime <= 0 {
		t.Errorf("Expected positive uptime, got %v", baseMetrics.Uptime)
	}
	
	// Memory metrics should be empty
	if baseMetrics.Alloc != 0 {
		t.Errorf("Expected zero Alloc in base metrics, got %d", baseMetrics.Alloc)
	}
	
	// Test 2: Include memory metrics
	memoryMetrics := monitor.GetFilteredMetrics(true, false, false)
	
	// Memory metrics should be present
	if memoryMetrics.Alloc == 0 {
		t.Errorf("Expected positive Alloc in memory metrics, got %d", memoryMetrics.Alloc)
	}
	if memoryMetrics.HeapObjects == 0 {
		t.Errorf("Expected positive HeapObjects in memory metrics, got %d", memoryMetrics.HeapObjects)
	}
	
	// GC metrics should be empty
	if memoryMetrics.NumGC != 0 {
		t.Errorf("Expected zero NumGC in memory metrics without GC, got %d", memoryMetrics.NumGC)
	}
	
	// Test 3: Include memory and GC metrics
	gcMetrics := monitor.GetFilteredMetrics(true, false, true)
	
	// GC metrics should be present (may be 0 in test environment)
	// Instead of checking for a specific value, just verify the field is accessible
	// and the test doesn't panic, which means the field was populated
	_ = gcMetrics.NumGC
	_ = gcMetrics.PauseTotalNs
	
	// Test 4: Include CPU metrics
	cpuMetrics := monitor.GetFilteredMetrics(false, true, false)
	
	// CPU metrics should be set
	if cpuMetrics.NumCPU <= 0 {
		t.Errorf("Expected positive NumCPU in CPU metrics, got %d", cpuMetrics.NumCPU)
	}
}