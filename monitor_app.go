package utils

import (
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
}

type Metrics struct {
	OpenFileDescriptors int
	NumGoroutines       int
	Timestamp           time.Time
}

func NewMonitor(interval time.Duration, onMetrics func(Metrics)) *Monitor {
	return &Monitor{
		interval:      interval,
		stopChan:      make(chan struct{}),
		onMetricsFunc: onMetrics,
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

func (m *Monitor) collectMetrics() Metrics {
	now := time.Now()
	
	// Check if we've collected metrics less than 100ms ago
	m.metricsLock.RLock()
	lastMetrics := m.lastMetrics
	m.metricsLock.RUnlock()
	
	if !lastMetrics.Timestamp.IsZero() && now.Sub(lastMetrics.Timestamp) < 100*time.Millisecond {
		// Use cached values but update timestamp
		return Metrics{
			OpenFileDescriptors: lastMetrics.OpenFileDescriptors,
			NumGoroutines:       lastMetrics.NumGoroutines,
			Timestamp:           now,
		}
	}
	
	// Collect fresh metrics
	numFDs := countOpenFileDescriptors()
	numGoroutines := runtime.NumGoroutine()
	
	metrics := Metrics{
		OpenFileDescriptors: numFDs,
		NumGoroutines:       numGoroutines,
		Timestamp:           now,
	}
	
	// Cache the metrics for future requests
	m.metricsLock.Lock()
	m.lastMetrics = metrics
	m.metricsLock.Unlock()
	
	return metrics
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
