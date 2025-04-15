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
	wg         sync.WaitGroup // For dispatcher
	jobWg      sync.WaitGroup // For jobs
}

func NewCronManager(logger *zerolog.Logger) *CronManager {
	return &CronManager{
		jobHeap:    make(JobHeap, 0),
		wakeUpChan: make(chan struct{}, 1), // Buffered channel
		stopChan:   make(chan struct{}),
		logger:     logger,
	}
}

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

	// Signal to the dispatcher that a new job is available.
	if m.running {
		select {
		case m.wakeUpChan <- struct{}{}:
		default:
		}
	}
}

func (m *CronManager) runJob(cronJob *CronJob) {
	m.jobWg.Add(1)
	defer m.jobWg.Done()
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", cronJob.Name, cronJob.Interval, r)
		}
	}()

	// Create a new channel each time - pooling channels is risky because of the close operation
	done := make(chan struct{})
	
	go func() {
		cronJob.Action()
		close(done)
	}()

	// Use a pooled timer instead of time.After to avoid allocations
	timer := getTimer(cronJob.Interval)
	defer releaseTimer(timer)
	
	select {
	case <-done:
		m.logger.Debug().Msgf("[cron|%s|%s] completed", cronJob.Name, cronJob.Interval)
	case <-timer.C:
		m.logger.Error().Msgf("[cron|%s|%s] timed out", cronJob.Name, cronJob.Interval)
	}
}

// timerPool reuses timer objects to reduce GC pressure
var timerPool = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(time.Hour)
	},
}

// getTimer returns a timer from the pool or creates a new one
func getTimer(d time.Duration) *time.Timer {
	timer := timerPool.Get().(*time.Timer)
	
	// Stop the timer if it's running
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	
	// Reset to the requested duration
	timer.Reset(d)
	return timer
}

// releaseTimer returns a timer to the pool
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

func (m *CronManager) dispatcher() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			releaseTimer(timer)
		}
	}()
	
	for {
		m.lock.Lock()
		if len(m.jobHeap) == 0 {
			m.lock.Unlock()
			select {
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				// New job added, continue
			}
			continue
		}

		nextJob := m.jobHeap[0]
		now := time.Now()
		delay := nextJob.nextRun.Sub(now)
		m.lock.Unlock()

		if delay <= 0 {
			m.lock.Lock()
			if !m.running {
				m.lock.Unlock()
				return
			}
			heap.Pop(&m.jobHeap)
			m.lock.Unlock()

			m.runJob(nextJob)

			// Reschedule the job with more accurate timing
			nextJob.nextRun = time.Now().Add(nextJob.Interval)

			m.lock.Lock()
			heap.Push(&m.jobHeap, nextJob)
			m.lock.Unlock()
		} else {
			// Reuse timer to avoid allocations
			if timer == nil {
				timer = getTimer(delay)
			} else {
				timer.Reset(delay)
			}
			
			select {
			case <-timer.C:
				// Time to check the jobs again.
			case <-m.stopChan:
				return
			case <-m.wakeUpChan:
				// New job added, continue
			}
		}
	}
}

func (m *CronManager) Start() {
	m.lock.Lock()
	if m.running {
		m.lock.Unlock()
		return
	}
	m.running = true
	m.wakeUpChan = make(chan struct{}, 1) // Buffered channel
	m.stopChan = make(chan struct{})
	m.wg.Add(1) // For dispatcher
	m.lock.Unlock()
	go func() {
		m.dispatcher()
		m.wg.Done() // Signal that dispatcher has exited
	}()
}

func (m *CronManager) Stop() {
	m.lock.Lock()
	if !m.running {
		m.lock.Unlock()
		return
	}
	m.running = false
	close(m.stopChan)
	m.lock.Unlock()
	m.wg.Wait()    // Wait for dispatcher to finish
	m.jobWg.Wait() // Wait for running jobs to finish
}
