package utils

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Job func()

type CronJob struct {
	Action   Job
	Interval time.Duration
	RunImmed bool
	nextRun  time.Time
	Name     string
	index    int // The index of the item in the heap.

	// Added fields:
	isRunning int32 // atomic: 1 if this job is currently running, 0 otherwise
	skipNext  int32 // atomic: 1 if we should skip exactly one cycle
}

type JobHeap []*CronJob

func (h JobHeap) Len() int { return len(h) }
func (h JobHeap) Less(i, j int) bool {
	return h[i].nextRun.Before(h[j].nextRun)
}
func (h JobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *JobHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*CronJob)
	item.index = n
	*h = append(*h, item)
}
func (h *JobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1 // For safety.
	*h = old[0 : n-1]
	return item
}

// WorkerPool manages a fixed number of worker goroutines
type WorkerPool struct {
	jobQueue    chan *jobExecution
	workers     []*worker
	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zerolog.Logger
}

type jobExecution struct {
	job     *CronJob
	manager *CronManager
	timeout time.Duration
}

type worker struct {
	id       int
	jobQueue chan *jobExecution
	ctx      context.Context
	logger   *zerolog.Logger
}

func newWorkerPool(workerCount int, queueSize int, logger *zerolog.Logger) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		jobQueue:    make(chan *jobExecution, queueSize),
		workers:     make([]*worker, workerCount),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
	}

	// Start workers
	for i := 0; i < workerCount; i++ {
		w := &worker{
			id:       i,
			jobQueue: pool.jobQueue,
			ctx:      ctx,
			logger:   logger,
		}
		pool.workers[i] = w
		pool.wg.Add(1)
		go w.start(&pool.wg)
	}

	return pool
}

func (w *worker) start(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case jobExec, ok := <-w.jobQueue:
			if !ok {
				return
			}
			w.executeJob(jobExec)
		}
	}
}

func (w *worker) executeJob(jobExec *jobExecution) {
	job := jobExec.job
	manager := jobExec.manager
	timeout := jobExec.timeout

	// Mark job as running using atomic operation
	if !atomic.CompareAndSwapInt32(&job.isRunning, 0, 1) {
		// Job is already running, reschedule
		manager.rescheduleJob(job, false)
		return
	}

	defer func() {
		// Mark job as not running
		atomic.StoreInt32(&job.isRunning, 0)

		if r := recover(); r != nil {
			w.logger.Error().Msgf("[cron|%s|%s] worker-%d panicked: %v", job.Name, job.Interval, w.id, r)
			// Skip next cycle after panic
			atomic.StoreInt32(&job.skipNext, 1)
		}

		// Always reschedule the job
		manager.rescheduleJob(job, true)
	}()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()

	// Execute job with timeout
	done := make(chan struct{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// This panic will be caught by the outer defer
				panic(r)
			}
		}()
		job.Action()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Debug().Msgf("[cron|%s|%s] worker-%d completed", job.Name, job.Interval, w.id)
	case <-ctx.Done():
		w.logger.Error().Msgf("[cron|%s|%s] worker-%d timed out", job.Name, job.Interval, w.id)
		// Skip next cycle after timeout
		atomic.StoreInt32(&job.skipNext, 1)
	}
}

func (p *WorkerPool) submit(jobExec *jobExecution) bool {
	select {
	case p.jobQueue <- jobExec:
		return true
	default:
		// Queue is full, drop the job and log warning
		p.logger.Warn().Msgf("[cron|%s|%s] job queue full, dropping execution",
			jobExec.job.Name, jobExec.job.Interval)
		return false
	}
}

func (p *WorkerPool) stop() {
	p.cancel()
	close(p.jobQueue)
	p.wg.Wait()
}

// Improved timer pool with better resource management
type timerPool struct {
	pool sync.Pool
	size int64 // atomic counter for pool size
	max  int64 // maximum pool size
}

var globalTimerPool = &timerPool{
	pool: sync.Pool{
		New: func() interface{} {
			return time.NewTimer(time.Hour)
		},
	},
	max: int64(runtime.NumCPU() * 100), // Reasonable limit based on CPU cores
}

func (tp *timerPool) get(d time.Duration) *time.Timer {
	timer := tp.pool.Get().(*time.Timer)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
	return timer
}

func (tp *timerPool) put(t *time.Timer) {
	if t == nil {
		return
	}

	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}

	// Only put back if pool isn't too large
	if atomic.LoadInt64(&tp.size) < tp.max {
		atomic.AddInt64(&tp.size, 1)
		tp.pool.Put(t)
	}
}

type CronManager struct {
	jobHeap    JobHeap
	lock       sync.RWMutex
	wakeUpChan chan struct{}
	stopChan   chan struct{}
	running    bool
	logger     *zerolog.Logger
	workerPool *WorkerPool

	// Metrics
	jobsScheduled int64 // atomic
	jobsCompleted int64 // atomic
	jobsDropped   int64 // atomic

	wg sync.WaitGroup // WaitGroup for the dispatcher
}

// Configuration for the cron manager
type CronConfig struct {
	MaxWorkers     int
	QueueSize      int
	DefaultTimeout time.Duration
}

func NewCronManager(logger *zerolog.Logger) *CronManager {
	return NewCronManagerWithConfig(logger, CronConfig{
		MaxWorkers:     runtime.NumCPU() * 2, // 2x CPU cores
		QueueSize:      1000,                 // Reasonable queue size
		DefaultTimeout: 5 * time.Minute,      // 5 minute default timeout
	})
}

func NewCronManagerWithConfig(logger *zerolog.Logger, config CronConfig) *CronManager {
	return &CronManager{
		jobHeap:    make(JobHeap, 0),
		wakeUpChan: make(chan struct{}, 1),
		stopChan:   make(chan struct{}),
		logger:     logger,
		workerPool: newWorkerPool(config.MaxWorkers, config.QueueSize, logger),
	}
}

// AddCron adds a new cron job. If 'runImmed' is true, the job is scheduled to run immediately.
// API-compatible method
func (m *CronManager) AddCron(name string, job Job, interval time.Duration, runImmed bool) {
	cronJob := &CronJob{
		Action:   job,
		Interval: interval,
		RunImmed: runImmed,
		Name:     name,
	}

	m.lock.Lock()
	if runImmed {
		cronJob.nextRun = time.Now()
	} else {
		cronJob.nextRun = time.Now().Add(interval)
	}
	heap.Push(&m.jobHeap, cronJob)
	m.lock.Unlock()

	atomic.AddInt64(&m.jobsScheduled, 1)
	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)

	// Wake dispatcher if running
	if m.isRunning() {
		m.wakeDispatcher()
	}
}

func (m *CronManager) isRunning() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.running
}

func (m *CronManager) wakeDispatcher() {
	select {
	case m.wakeUpChan <- struct{}{}:
	default:
	}
}

func (m *CronManager) rescheduleJob(job *CronJob, normalCompletion bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.running {
		return // Don't reschedule if manager is stopped
	}

	if normalCompletion {
		atomic.AddInt64(&m.jobsCompleted, 1)
	}

	job.nextRun = time.Now().Add(job.Interval)
	heap.Push(&m.jobHeap, job)

	// Wake dispatcher for potential earlier scheduling
	m.wakeDispatcher()
}

func (m *CronManager) dispatcher() {
	defer m.wg.Done()

	var sleepTimer *time.Timer
	defer func() {
		if sleepTimer != nil {
			globalTimerPool.put(sleepTimer)
		}
	}()

	for {
		m.lock.Lock()

		// Check if we should stop
		if !m.running {
			m.lock.Unlock()
			return
		}

		// If no jobs, wait for one or stop signal
		if len(m.jobHeap) == 0 {
			m.lock.Unlock()
			select {
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				continue
			}
		}

		// Get next job
		nextJob := m.jobHeap[0]
		now := time.Now()
		delay := nextJob.nextRun.Sub(now)

		m.lock.Unlock()

		// If job is ready to run
		if delay <= 0 {
			m.lock.Lock()
			if !m.running {
				m.lock.Unlock()
				return
			}

			// Remove job from heap
			heap.Pop(&m.jobHeap)

			// Check if job should be skipped
			if atomic.LoadInt32(&nextJob.isRunning) == 1 {
				// Job is already running, reschedule
				m.rescheduleJobLocked(nextJob)
				m.lock.Unlock()
				continue
			}

			if atomic.CompareAndSwapInt32(&nextJob.skipNext, 1, 0) {
				// Skip this cycle
				m.rescheduleJobLocked(nextJob)
				m.lock.Unlock()
				continue
			}

			m.lock.Unlock()

			// Submit job to worker pool
			jobExec := &jobExecution{
				job:     nextJob,
				manager: m,
				timeout: nextJob.Interval, // Use interval as timeout
			}

			if !m.workerPool.submit(jobExec) {
				// Job was dropped, reschedule it
				atomic.AddInt64(&m.jobsDropped, 1)
				m.rescheduleJob(nextJob, false)
			}

		} else {
			// Wait for job time or wake signal
			if sleepTimer == nil {
				sleepTimer = globalTimerPool.get(delay)
			} else {
				sleepTimer.Reset(delay)
			}

			select {
			case <-sleepTimer.C:
				// Timer expired, check jobs again
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				// New job added or other event
			}
		}
	}
}

func (m *CronManager) rescheduleJobLocked(job *CronJob) {
	job.nextRun = time.Now().Add(job.Interval)
	heap.Push(&m.jobHeap, job)
}

// Start spins up the dispatcher goroutine if not already running.
// API-compatible method
func (m *CronManager) Start() {
	m.lock.Lock()
	if m.running {
		m.lock.Unlock()
		return
	}

	m.running = true
	m.wakeUpChan = make(chan struct{}, 1)
	m.stopChan = make(chan struct{})

	m.wg.Add(1)
	m.lock.Unlock()

	go m.dispatcher()

	m.logger.Info().Msg("CronManager started")
}

// Stop signals the dispatcher to exit and waits for cleanup.
// API-compatible method
func (m *CronManager) Stop() {
	m.lock.Lock()
	if !m.running {
		m.lock.Unlock()
		return
	}

	m.running = false
	close(m.stopChan)
	m.lock.Unlock()

	// Wait for dispatcher to exit
	m.wg.Wait()

	// Stop worker pool
	m.workerPool.stop()

	m.logger.Info().
		Int64("scheduled", atomic.LoadInt64(&m.jobsScheduled)).
		Int64("completed", atomic.LoadInt64(&m.jobsCompleted)).
		Int64("dropped", atomic.LoadInt64(&m.jobsDropped)).
		Msg("CronManager stopped")
}

// GetMetrics returns current metrics (bonus feature, doesn't break API)
func (m *CronManager) GetMetrics() map[string]int64 {
	return map[string]int64{
		"jobs_scheduled": atomic.LoadInt64(&m.jobsScheduled),
		"jobs_completed": atomic.LoadInt64(&m.jobsCompleted),
		"jobs_dropped":   atomic.LoadInt64(&m.jobsDropped),
	}
}
