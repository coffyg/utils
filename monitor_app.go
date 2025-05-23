package utils

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MonitorConfig holds configuration for the monitor
type MonitorConfig struct {
	MetricsCacheInterval time.Duration // How often to refresh expensive metrics
	FDCacheInterval      time.Duration // How often to refresh FD count
	CPUCacheInterval     time.Duration // How often to refresh CPU metrics
	MaxCallbackRate      time.Duration // Minimum time between callback invocations
	EnableCPUMonitoring  bool          // Whether to enable CPU monitoring
	EnableFDMonitoring   bool          // Whether to enable FD monitoring
}

// DefaultMonitorConfig returns sensible defaults
func DefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		MetricsCacheInterval: 250 * time.Millisecond, // Cache expensive metrics for 250ms
		FDCacheInterval:      1 * time.Second,        // Cache FD count for 1 second
		CPUCacheInterval:     500 * time.Millisecond, // Cache CPU metrics for 500ms
		MaxCallbackRate:      50 * time.Millisecond,  // Max 20 callbacks per second
		EnableCPUMonitoring:  true,
		EnableFDMonitoring:   true,
	}
}

type Monitor struct {
	interval      time.Duration
	stopChan      chan struct{}
	stoppedChan   chan struct{}
	onMetricsFunc func(Metrics)
	startOnce     sync.Once
	stopOnce      sync.Once
	config        MonitorConfig

	// Cached values with timestamps for intelligent refresh
	cachedMetrics    Metrics
	metricsTimestamp int64 // atomic timestamp in nanoseconds
	metricsLock      sync.RWMutex

	// FD monitoring cache
	cachedFDCount int32 // atomic
	fdTimestamp   int64 // atomic timestamp in nanoseconds

	// CPU monitoring
	cpuStats     CPUStats
	cpuTimestamp int64 // atomic timestamp in nanoseconds
	cpuLock      sync.RWMutex

	// Application tracking
	startTime time.Time

	// Rate limiting for callbacks
	lastCallbackTime int64 // atomic timestamp in nanoseconds

	// Metrics for monitoring the monitor itself
	metricsCollected int64 // atomic counter
	callbacksInvoked int64 // atomic counter
	cacheHits        int64 // atomic counter
}

// Metrics contains system and application performance metrics
type Metrics struct {
	// System metrics
	OpenFileDescriptors int
	NumGoroutines       int
	Timestamp           time.Time

	// Memory metrics
	Alloc        uint64 // Bytes allocated and not yet freed
	TotalAlloc   uint64 // Bytes allocated (even if freed)
	Sys          uint64 // Bytes obtained from system
	HeapObjects  uint64 // Total number of allocated heap objects
	NumGC        uint32 // Number of completed GC cycles
	PauseTotalNs uint64 // Total GC pause time in nanoseconds

	// CPU metrics
	NumCPU   int     // Number of logical CPUs
	CPUUsage float64 // Process CPU usage (0.0-1.0 per core)

	// Application metrics
	Uptime time.Duration // Application uptime
}

// CPUStats holds CPU usage calculation data
type CPUStats struct {
	lastUserTime   uint64
	lastSystemTime uint64
	lastIdleTime   uint64
	lastCheckTime  time.Time
	usage          float64
}

// formatStringPool reuses string builders to reduce allocations
var formatStringPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// API-compatible constructors
func NewMonitor(interval time.Duration, onMetrics func(Metrics)) *Monitor {
	return NewMonitorWithConfig(interval, onMetrics, DefaultMonitorConfig())
}

func NewMonitorWithConfig(interval time.Duration, onMetrics func(Metrics), config MonitorConfig) *Monitor {
	return &Monitor{
		interval:      interval,
		stopChan:      make(chan struct{}),
		onMetricsFunc: onMetrics,
		config:        config,
		startTime:     time.Now(),
		cpuStats: CPUStats{
			lastCheckTime: time.Now(),
		},
	}
}

// Start begins the monitoring process.
// API-compatible method
func (m *Monitor) Start() {
	m.startOnce.Do(func() {
		m.stoppedChan = make(chan struct{})

		go func() {
			ticker := time.NewTicker(m.interval)
			defer ticker.Stop()
			defer close(m.stoppedChan)

			for {
				select {
				case <-ticker.C:
					m.handleTick()
				case <-m.stopChan:
					return
				}
			}
		}()
	})
}

func (m *Monitor) handleTick() {
	now := time.Now()

	// Rate limit callback invocations
	lastCallback := atomic.LoadInt64(&m.lastCallbackTime)
	if now.UnixNano()-lastCallback < m.config.MaxCallbackRate.Nanoseconds() {
		return
	}

	metrics := m.collectMetrics()
	if m.onMetricsFunc != nil {
		atomic.StoreInt64(&m.lastCallbackTime, now.UnixNano())
		atomic.AddInt64(&m.callbacksInvoked, 1)

		// Run callback in separate goroutine to prevent blocking
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// Callback panicked, but don't crash the monitor
				}
			}()
			m.onMetricsFunc(metrics)
		}()
	}
}

// Stop signals the monitoring to stop.
// API-compatible method
func (m *Monitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		if m.stoppedChan != nil {
			<-m.stoppedChan
		}
	})
}

// CleanupFDCache closes the file descriptor directory cache.
// API-compatible method
func CleanupFDCache() {
	cleanupOptimizedFDCache()
}

func (m *Monitor) collectMetrics() Metrics {
	now := time.Now()
	nowNano := now.UnixNano()

	// Check if we can use cached metrics
	lastTimestamp := atomic.LoadInt64(&m.metricsTimestamp)
	if nowNano-lastTimestamp < m.config.MetricsCacheInterval.Nanoseconds() {
		m.metricsLock.RLock()
		cached := m.cachedMetrics
		m.metricsLock.RUnlock()

		// Update only the cheap fields
		cached.Timestamp = now
		cached.Uptime = now.Sub(m.startTime)
		cached.NumGoroutines = runtime.NumGoroutine()

		atomic.AddInt64(&m.cacheHits, 1)
		return cached
	}

	atomic.AddInt64(&m.metricsCollected, 1)

	// Collect memory statistics (expensive operation)
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)

	// Collect other metrics
	var numFDs int
	if m.config.EnableFDMonitoring {
		numFDs = m.getFileDescriptorCount()
	}

	var cpuUsage float64
	if m.config.EnableCPUMonitoring {
		cpuUsage = m.getCPUUsage()
	}

	metrics := Metrics{
		// System metrics
		OpenFileDescriptors: numFDs,
		NumGoroutines:       runtime.NumGoroutine(),
		Timestamp:           now,

		// Memory metrics
		Alloc:        rtm.Alloc,
		TotalAlloc:   rtm.TotalAlloc,
		Sys:          rtm.Sys,
		HeapObjects:  rtm.HeapObjects,
		NumGC:        rtm.NumGC,
		PauseTotalNs: rtm.PauseTotalNs,

		// CPU metrics
		NumCPU:   runtime.NumCPU(),
		CPUUsage: cpuUsage,

		// Application metrics
		Uptime: now.Sub(m.startTime),
	}

	// Cache the metrics
	m.metricsLock.Lock()
	m.cachedMetrics = metrics
	m.metricsLock.Unlock()
	atomic.StoreInt64(&m.metricsTimestamp, nowNano)

	return metrics
}

func (m *Monitor) getFileDescriptorCount() int {
	nowNano := time.Now().UnixNano()
	lastTimestamp := atomic.LoadInt64(&m.fdTimestamp)

	// Use cached FD count if recent enough
	if nowNano-lastTimestamp < m.config.FDCacheInterval.Nanoseconds() {
		return int(atomic.LoadInt32(&m.cachedFDCount))
	}

	count := countOpenFileDescriptorsOptimized()
	atomic.StoreInt32(&m.cachedFDCount, int32(count))
	atomic.StoreInt64(&m.fdTimestamp, nowNano)

	return count
}

func (m *Monitor) getCPUUsage() float64 {
	nowNano := time.Now().UnixNano()
	lastTimestamp := atomic.LoadInt64(&m.cpuTimestamp)

	// Use cached CPU usage if recent enough
	if nowNano-lastTimestamp < m.config.CPUCacheInterval.Nanoseconds() {
		m.cpuLock.RLock()
		usage := m.cpuStats.usage
		m.cpuLock.RUnlock()
		return usage
	}

	usage := m.calculateCPUUsage()
	atomic.StoreInt64(&m.cpuTimestamp, nowNano)

	return usage
}

func (m *Monitor) calculateCPUUsage() float64 {
	now := time.Now()

	m.cpuLock.Lock()
	defer m.cpuLock.Unlock()

	// Read CPU stats from /proc/stat for more accurate measurement
	userTime, systemTime, idleTime := readProcStat()

	// Calculate time differences
	timeDiff := now.Sub(m.cpuStats.lastCheckTime).Seconds()
	if timeDiff < 0.1 || m.cpuStats.lastCheckTime.IsZero() {
		// Too little time has passed or first measurement
		m.cpuStats.lastUserTime = userTime
		m.cpuStats.lastSystemTime = systemTime
		m.cpuStats.lastIdleTime = idleTime
		m.cpuStats.lastCheckTime = now
		return m.cpuStats.usage
	}

	// Calculate CPU time differences (in USER_HZ, typically 100Hz)
	userDiff := userTime - m.cpuStats.lastUserTime
	systemDiff := systemTime - m.cpuStats.lastSystemTime
	idleDiff := idleTime - m.cpuStats.lastIdleTime

	totalDiff := userDiff + systemDiff + idleDiff
	if totalDiff == 0 {
		return m.cpuStats.usage
	}

	// Calculate CPU usage percentage
	activeDiff := userDiff + systemDiff
	usage := float64(activeDiff) / float64(totalDiff)

	// Update stored values
	m.cpuStats.lastUserTime = userTime
	m.cpuStats.lastSystemTime = systemTime
	m.cpuStats.lastIdleTime = idleTime
	m.cpuStats.lastCheckTime = now
	m.cpuStats.usage = usage

	return usage
}

// readProcStat reads CPU times from /proc/stat
func readProcStat() (user, system, idle uint64) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) >= 4 {
				user, _ = strconv.ParseUint(fields[1], 10, 64)
				system, _ = strconv.ParseUint(fields[3], 10, 64)
				idle, _ = strconv.ParseUint(fields[4], 10, 64)
			}
		}
	}

	return user, system, idle
}

// GetFilteredMetrics returns a subset of metrics based on filter flags
// API-compatible method with performance improvements
func (m *Monitor) GetFilteredMetrics(includeMemory, includeCPU, includeGC bool) Metrics {
	now := time.Now()

	// Always include base metrics (cheap to collect)
	metrics := Metrics{
		NumGoroutines: runtime.NumGoroutine(),
		Timestamp:     now,
		Uptime:        now.Sub(m.startTime),
		NumCPU:        runtime.NumCPU(),
	}

	// Include FD count from cache if enabled
	if m.config.EnableFDMonitoring {
		metrics.OpenFileDescriptors = m.getFileDescriptorCount()
	}

	// Include memory metrics if requested
	if includeMemory {
		// Try to use cached memory stats if recent enough
		nowNano := now.UnixNano()
		lastTimestamp := atomic.LoadInt64(&m.metricsTimestamp)

		if nowNano-lastTimestamp < m.config.MetricsCacheInterval.Nanoseconds() {
			m.metricsLock.RLock()
			cached := m.cachedMetrics
			m.metricsLock.RUnlock()

			metrics.Alloc = cached.Alloc
			metrics.TotalAlloc = cached.TotalAlloc
			metrics.Sys = cached.Sys
			metrics.HeapObjects = cached.HeapObjects

			if includeGC {
				metrics.NumGC = cached.NumGC
				metrics.PauseTotalNs = cached.PauseTotalNs
			}
		} else {
			// Need fresh memory stats
			var rtm runtime.MemStats
			runtime.ReadMemStats(&rtm)

			metrics.Alloc = rtm.Alloc
			metrics.TotalAlloc = rtm.TotalAlloc
			metrics.Sys = rtm.Sys
			metrics.HeapObjects = rtm.HeapObjects

			if includeGC {
				metrics.NumGC = rtm.NumGC
				metrics.PauseTotalNs = rtm.PauseTotalNs
			}
		}
	}

	// Include CPU metrics if requested
	if includeCPU && m.config.EnableCPUMonitoring {
		metrics.CPUUsage = m.getCPUUsage()
	}

	return metrics
}

// FormatMetrics returns a human-readable representation of metrics
// API-compatible method with performance improvements
func (m Metrics) FormatMetrics() map[string]string {
	// Use pooled string builder for efficiency
	builder := formatStringPool.Get().(*strings.Builder)
	builder.Reset()
	defer formatStringPool.Put(builder)

	// Pre-allocate map with expected size
	formatted := make(map[string]string, 12)

	// Format memory values to human-readable format
	formatSize := func(bytes uint64) string {
		const unit = 1024
		if bytes < unit {
			builder.Reset()
			builder.WriteString(strconv.FormatUint(bytes, 10))
			builder.WriteString(" B")
			return builder.String()
		}
		div, exp := uint64(unit), 0
		for n := bytes / unit; n >= unit; n /= unit {
			div *= unit
			exp++
		}
		builder.Reset()
		builder.WriteString(fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp]))
		return builder.String()
	}

	formatted["Timestamp"] = m.Timestamp.Format(time.RFC3339)
	formatted["Uptime"] = m.Uptime.Round(time.Second).String()
	formatted["OpenFileDescriptors"] = strconv.Itoa(m.OpenFileDescriptors)
	formatted["NumGoroutines"] = strconv.Itoa(m.NumGoroutines)
	formatted["Memory/Alloc"] = formatSize(m.Alloc)
	formatted["Memory/TotalAlloc"] = formatSize(m.TotalAlloc)
	formatted["Memory/Sys"] = formatSize(m.Sys)
	formatted["Memory/HeapObjects"] = strconv.FormatUint(m.HeapObjects, 10)
	formatted["GC/Cycles"] = strconv.FormatUint(uint64(m.NumGC), 10)
	formatted["GC/PauseTotal"] = fmt.Sprintf("%.2fms", float64(m.PauseTotalNs)/float64(time.Millisecond))
	formatted["CPU/Count"] = strconv.Itoa(m.NumCPU)
	formatted["CPU/Usage"] = fmt.Sprintf("%.2f%%", m.CPUUsage*100)

	return formatted
}

// GetMonitorStats returns statistics about the monitor itself
func (m *Monitor) GetMonitorStats() map[string]int64 {
	return map[string]int64{
		"metrics_collected": atomic.LoadInt64(&m.metricsCollected),
		"callbacks_invoked": atomic.LoadInt64(&m.callbacksInvoked),
		"cache_hits":        atomic.LoadInt64(&m.cacheHits),
	}
}

// Optimized FD cache management
var optimizedFDCache struct {
	dir      *os.File
	once     sync.Once
	dirErr   error
	lastStat os.FileInfo
	count    int
	mutex    sync.RWMutex
}

func countOpenFileDescriptorsOptimized() int {
	// Initialize cache once
	optimizedFDCache.once.Do(func() {
		optimizedFDCache.dir, optimizedFDCache.dirErr = os.Open("/proc/self/fd")
	})

	if optimizedFDCache.dirErr != nil {
		return -1
	}

	optimizedFDCache.mutex.Lock()
	defer optimizedFDCache.mutex.Unlock()

	// Check if directory has changed by comparing stat info
	stat, err := optimizedFDCache.dir.Stat()
	if err != nil {
		return -1
	}

	// If modification time hasn't changed, return cached count
	if optimizedFDCache.lastStat != nil &&
		stat.ModTime().Equal(optimizedFDCache.lastStat.ModTime()) {
		return optimizedFDCache.count
	}

	// Rewind and read directory
	_, err = optimizedFDCache.dir.Seek(0, 0)
	if err != nil {
		return -1
	}

	names, err := optimizedFDCache.dir.Readdirnames(-1)
	if err != nil {
		return -1
	}

	// Update cache
	optimizedFDCache.count = len(names) - 1 // Minus 1 for the directory itself
	optimizedFDCache.lastStat = stat

	return optimizedFDCache.count
}

func cleanupOptimizedFDCache() {
	optimizedFDCache.mutex.Lock()
	defer optimizedFDCache.mutex.Unlock()

	if optimizedFDCache.dir != nil {
		optimizedFDCache.dir.Close()
		optimizedFDCache.dir = nil
	}
}
