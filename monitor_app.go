package utils

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

type Monitor struct {
	interval      time.Duration
	stopChan      chan struct{}
	stoppedChan   chan struct{}
	onMetricsFunc func(Metrics)
	startOnce     sync.Once
	stopOnce      sync.Once
	// Cached values for better performance
	lastMetrics   Metrics
	metricsLock   sync.RWMutex
	
	// Application tracking
	startTime     time.Time
	lastCPUTime   uint64     // Store as uint64 to match runtime.MemStats.PauseTotalNs
	lastCPUCheck  time.Time
}

// Metrics contains system and application performance metrics
type Metrics struct {
	// System metrics
	OpenFileDescriptors int
	NumGoroutines       int
	Timestamp           time.Time
	
	// Memory metrics
	Alloc        uint64    // Bytes allocated and not yet freed
	TotalAlloc   uint64    // Bytes allocated (even if freed)
	Sys          uint64    // Bytes obtained from system
	HeapObjects  uint64    // Total number of allocated heap objects
	NumGC        uint32    // Number of completed GC cycles
	PauseTotalNs uint64    // Total GC pause time in nanoseconds
	
	// CPU metrics
	NumCPU       int       // Number of logical CPUs
	CPUUsage     float64   // Process CPU usage (0.0-1.0 per core)
	
	// Application metrics
	Uptime       time.Duration // Application uptime
}

func NewMonitor(interval time.Duration, onMetrics func(Metrics)) *Monitor {
	return &Monitor{
		interval:      interval,
		stopChan:      make(chan struct{}),
		onMetricsFunc: onMetrics,
		startTime:     time.Now(),
		lastCPUCheck:  time.Now(),
	}
}

// Start begins the monitoring process.
func (m *Monitor) Start() {
	m.startOnce.Do(func() {
		m.stoppedChan = make(chan struct{}) // Initialize here

		go func() {
			ticker := time.NewTicker(m.interval)
			defer ticker.Stop()
			defer close(m.stoppedChan)

			for {
				select {
				case <-ticker.C:
					metrics := m.collectMetrics()
					if m.onMetricsFunc != nil {
						m.onMetricsFunc(metrics)
					}
				case <-m.stopChan:
					return
				}
			}
		}()
	})
}

func (m *Monitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		if m.stoppedChan != nil {
			<-m.stoppedChan
		}
	})
}

// CleanupFDCache closes the file descriptor directory cache.
// Should be called during program shutdown.
func CleanupFDCache() {
	if fdDirCache.dir != nil {
		fdDirCache.dir.Close()
		fdDirCache.dir = nil
	}
}

// getCPUUsage returns the approximate CPU usage
// This is a lightweight approximate method that won't impact performance
func (m *Monitor) getCPUUsage() float64 {
	// If it's the first call, just return 0
	if m.lastCPUTime == 0 {
		return 0
	}
	
	// Only update CPU metrics every 500ms to avoid performance impact
	now := time.Now()
	if now.Sub(m.lastCPUCheck) < 500*time.Millisecond {
		return m.lastMetrics.CPUUsage
	}
	
	// Simple approximation based on goroutines and GC activity
	// This is not precise but provides a reasonable indicator without performance impact
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	
	// Calculate CPU usage based on GC activity and time elapsed
	// This is a very lightweight approximation
	cpuUsage := float64(rtm.PauseTotalNs-m.lastCPUTime) / float64(now.Sub(m.lastCPUCheck).Nanoseconds())
	cpuUsage = cpuUsage * float64(runtime.NumCPU()) * 0.1 // Scale factor
	
	// Cap at a reasonable range
	if cpuUsage > float64(runtime.NumCPU()) {
		cpuUsage = float64(runtime.NumCPU())
	}
	if cpuUsage < 0 {
		cpuUsage = 0
	}
	
	// Update tracking values
	m.lastCPUTime = rtm.PauseTotalNs
	m.lastCPUCheck = now
	
	return cpuUsage
}

func (m *Monitor) collectMetrics() Metrics {
	now := time.Now()
	
	// Check if we've collected metrics less than 100ms ago
	m.metricsLock.RLock()
	lastMetrics := m.lastMetrics
	m.metricsLock.RUnlock()
	
	if !lastMetrics.Timestamp.IsZero() && now.Sub(lastMetrics.Timestamp) < 100*time.Millisecond {
		// Use cached values but update timestamp and uptime
		lastMetrics.Timestamp = now
		lastMetrics.Uptime = now.Sub(m.startTime)
		return lastMetrics
	}
	
	// Collect memory statistics
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	
	// Collect other metrics
	numFDs := countOpenFileDescriptors()
	numGoroutines := runtime.NumGoroutine()
	cpuUsage := m.getCPUUsage()
	
	metrics := Metrics{
		// System metrics
		OpenFileDescriptors: numFDs,
		NumGoroutines:       numGoroutines,
		Timestamp:           now,
		
		// Memory metrics
		Alloc:        rtm.Alloc,
		TotalAlloc:   rtm.TotalAlloc,
		Sys:          rtm.Sys,
		HeapObjects:  rtm.HeapObjects,
		NumGC:        rtm.NumGC,
		PauseTotalNs: rtm.PauseTotalNs,
		
		// CPU metrics
		NumCPU:       runtime.NumCPU(),
		CPUUsage:     cpuUsage,
		
		// Application metrics
		Uptime:       now.Sub(m.startTime),
	}
	
	// Cache the metrics for future requests
	m.metricsLock.Lock()
	m.lastMetrics = metrics
	m.metricsLock.Unlock()
	
	return metrics
}

// GetFilteredMetrics returns a subset of metrics based on filter flags
// Useful for getting only the metrics you need without processing everything
func (m *Monitor) GetFilteredMetrics(includeMemory, includeCPU, includeGC bool) Metrics {
	now := time.Now()
	
	// Always include base metrics
	metrics := Metrics{
		OpenFileDescriptors: m.lastMetrics.OpenFileDescriptors,
		NumGoroutines:       runtime.NumGoroutine(),
		Timestamp:           now,
		Uptime:              now.Sub(m.startTime),
		NumCPU:              runtime.NumCPU(),
	}
	
	// Include memory metrics if requested
	if includeMemory {
		var rtm runtime.MemStats
		runtime.ReadMemStats(&rtm)
		
		metrics.Alloc = rtm.Alloc
		metrics.TotalAlloc = rtm.TotalAlloc
		metrics.Sys = rtm.Sys
		metrics.HeapObjects = rtm.HeapObjects
		
		// Include GC metrics only if both memory and GC are requested
		if includeGC {
			metrics.NumGC = rtm.NumGC
			metrics.PauseTotalNs = rtm.PauseTotalNs
		}
	}
	
	// Include CPU metrics if requested
	if includeCPU {
		metrics.CPUUsage = m.getCPUUsage()
	}
	
	return metrics
}

// FormatMetrics returns a human-readable representation of metrics
func (m Metrics) FormatMetrics() map[string]string {
	// Format memory values to human-readable format
	formatSize := func(bytes uint64) string {
		const unit = 1024
		if bytes < unit {
			return fmt.Sprintf("%d B", bytes)
		}
		div, exp := uint64(unit), 0
		for n := bytes / unit; n >= unit; n /= unit {
			div *= unit
			exp++
		}
		return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
	}
	
	formatted := map[string]string{
		"Timestamp":           m.Timestamp.Format(time.RFC3339),
		"Uptime":              m.Uptime.Round(time.Second).String(),
		"OpenFileDescriptors": fmt.Sprintf("%d", m.OpenFileDescriptors),
		"NumGoroutines":       fmt.Sprintf("%d", m.NumGoroutines),
		"Memory/Alloc":        formatSize(m.Alloc),
		"Memory/TotalAlloc":   formatSize(m.TotalAlloc),
		"Memory/Sys":          formatSize(m.Sys),
		"Memory/HeapObjects":  fmt.Sprintf("%d", m.HeapObjects),
		"GC/Cycles":           fmt.Sprintf("%d", m.NumGC),
		"GC/PauseTotal":       fmt.Sprintf("%.2fms", float64(m.PauseTotalNs)/float64(time.Millisecond)),
		"CPU/Count":           fmt.Sprintf("%d", m.NumCPU),
		"CPU/Usage":           fmt.Sprintf("%.2f%%", m.CPUUsage*100/float64(m.NumCPU)),
	}
	
	return formatted
}

// fdDirCache holds the open fd directory to avoid reopening it repeatedly
var fdDirCache struct {
	dir     *os.File
	once    sync.Once
	dirErr  error
	readErr error
}

func countOpenFileDescriptors() int {
	// Use a cached directory handle for better performance
	fdDirCache.once.Do(func() {
		fdDirCache.dir, fdDirCache.dirErr = os.Open("/proc/self/fd")
		if fdDirCache.dirErr != nil {
			return
		}
	})

	if fdDirCache.dirErr != nil {
		return -1 // Return -1 to indicate an error
	}

	// Rewind directory to beginning
	_, err := fdDirCache.dir.Seek(0, 0)
	if err != nil {
		return -1
	}

	// Read all directory entries
	names, err := fdDirCache.dir.Readdirnames(-1)
	if err != nil {
		return -1
	}
	
	// Return count minus 1 for the directory itself
	return len(names) - 1
}
