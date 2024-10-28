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

func (m *Monitor) collectMetrics() Metrics {
	numFDs := countOpenFileDescriptors()
	numGoroutines := runtime.NumGoroutine()
	return Metrics{
		OpenFileDescriptors: numFDs,
		NumGoroutines:       numGoroutines,
		Timestamp:           time.Now(),
	}
}

func countOpenFileDescriptors() int {
	dir, err := os.Open("/proc/self/fd")
	if err != nil {
		return -1 // Return -1 to indicate an error
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return -1
	}
	return len(names)
}
