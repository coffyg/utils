package utils

import (
	"container/heap"
	"sync"
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
	isRunning bool // true if this job is currently running
	skipNext  bool // true if we should skip exactly one cycle (e.g., after a timeout)
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

type CronManager struct {
	jobHeap    JobHeap
	lock       sync.Mutex
	wakeUpChan chan struct{}
	stopChan   chan struct{}
	running    bool
	logger     *zerolog.Logger

	wg    sync.WaitGroup // WaitGroup for the dispatcher itself
	jobWg sync.WaitGroup // WaitGroup for any running jobs
}

func NewCronManager(logger *zerolog.Logger) *CronManager {
	return &CronManager{
		jobHeap:    make(JobHeap, 0),
		wakeUpChan: make(chan struct{}, 1), // Buffered channel
		stopChan:   make(chan struct{}),
		logger:     logger,
	}
}

// AddCron adds a new cron job. If 'runImmed' is true, the job is scheduled to run immediately.
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

	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)

	// If manager is already running, wake the dispatcher in case this job is earlier than everything else.
	if m.running {
		select {
		case m.wakeUpChan <- struct{}{}:
		default:
		}
	}
}

// runJob executes the cron job asynchronously so the dispatcher does not block.
// The same job will never run in parallel because the dispatcher checks 'isRunning' before calling runJob.
func (m *CronManager) runJob(cronJob *CronJob) {
	m.jobWg.Add(1)

	go func(job *CronJob) {
		defer m.jobWg.Done()

		// Mark the job as running
		job.isRunning = true
		defer func() {
			job.isRunning = false
		}()

		// Recover from panic to avoid crashing the entire dispatcher
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
			}
		}()

		// Actually run the job's Action, but watch for timeout.
		done := make(chan struct{})
		go func() {
			job.Action()
			close(done)
		}()

		// Use a pooled timer instead of time.After to avoid allocations.
		timer := getTimer(job.Interval)
		defer releaseTimer(timer)

		select {
		case <-done:
			// Job completed before the timer fired
			m.logger.Debug().Msgf("[cron|%s|%s] completed", job.Name, job.Interval)
		case <-timer.C:
			// Timed out
			m.logger.Error().Msgf("[cron|%s|%s] timed out", job.Name, job.Interval)
			// Skip exactly one cycle
			job.skipNext = true
		}

		// Once job is done (or timed out), schedule its next run.
		// If we haven't been stopped, push it back with new nextRun.
		m.lock.Lock()
		if m.running { // Only reschedule if we're still running
			job.nextRun = time.Now().Add(job.Interval)
			heap.Push(&m.jobHeap, job)
			// Wake the dispatcher in case this job is now the next one
			select {
			case m.wakeUpChan <- struct{}{}:
			default:
			}
		}
		m.lock.Unlock()
	}(cronJob)
}

// timerPool reuses timer objects to reduce GC pressure.
var timerPool = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(time.Hour)
	},
}

// getTimer returns a pooled timer reset to 'd'.
func getTimer(d time.Duration) *time.Timer {
	timer := timerPool.Get().(*time.Timer)
	// Stop the timer if it's running
	if !timer.Stop() {
		// Drain channel if needed
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
	return timer
}

// releaseTimer returns a timer to the pool.
func releaseTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

// dispatcher runs in its own goroutine, scheduling jobs as they come due.
func (m *CronManager) dispatcher() {
	var sleepTimer *time.Timer
	defer func() {
		if sleepTimer != nil {
			releaseTimer(sleepTimer)
		}
	}()

	for {
		m.lock.Lock()

		// If no jobs, just wait until one arrives or we stop
		if len(m.jobHeap) == 0 {
			m.lock.Unlock()
			select {
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				continue
			}
		}

		// Peek at the next job
		nextJob := m.jobHeap[0]
		now := time.Now()
		delay := nextJob.nextRun.Sub(now)

		m.lock.Unlock()

		// If it's time (or overdue), try to run it
		if delay <= 0 {
			m.lock.Lock()
			if !m.running {
				// If we are no longer running, exit
				m.lock.Unlock()
				return
			}
			// Pop the job off the heap
			heap.Pop(&m.jobHeap)

			// If it’s already running, or flagged to skip once, skip this cycle
			if nextJob.isRunning {
				nextJob.nextRun = time.Now().Add(nextJob.Interval)
				heap.Push(&m.jobHeap, nextJob)
				m.lock.Unlock()
				continue
			}
			if nextJob.skipNext {
				nextJob.skipNext = false
				nextJob.nextRun = time.Now().Add(nextJob.Interval)
				heap.Push(&m.jobHeap, nextJob)
				m.lock.Unlock()
				continue
			}

			m.lock.Unlock()

			// Run the job asynchronously
			m.runJob(nextJob)

		} else {
			// Otherwise, wait for the earliest job to come due, or a new job to appear, or stop
			if sleepTimer == nil {
				sleepTimer = getTimer(delay)
			} else {
				sleepTimer.Reset(delay)
			}

			select {
			case <-sleepTimer.C:
				// Timer fired, loop around and see if job is ready now
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				// Possibly a new job was added or something changed
			}
		}
	}
}

// Start spins up the dispatcher goroutine if not already running.
func (m *CronManager) Start() {
	m.lock.Lock()
	if m.running {
		m.lock.Unlock()
		return
	}
	m.running = true
	// Recreate channels in case we stopped previously
	m.wakeUpChan = make(chan struct{}, 1)
	m.stopChan = make(chan struct{})

	m.wg.Add(1)
	m.lock.Unlock()

	go func() {
		m.dispatcher()
		m.wg.Done() // Signal that dispatcher has exited
	}()
}

// Stop signals the dispatcher to exit and waits for all running jobs to finish.
func (m *CronManager) Stop() {
	m.lock.Lock()
	if !m.running {
		m.lock.Unlock()
		return
	}
	m.running = false
	close(m.stopChan)
	m.lock.Unlock()

	// Wait for the dispatcher goroutine to exit
	m.wg.Wait()
	// Wait for any in-flight jobs to complete
	m.jobWg.Wait()
}
