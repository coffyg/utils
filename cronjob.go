package utils

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Job func()

// CronJob holds the user job, interval, and scheduling metadata.
type CronJob struct {
	Name     string
	Action   Job
	Interval time.Duration
	RunImmed bool

	nextRun time.Time // protected by CronManager.mu
}

// CronManager is the main scheduler.
// Public methods: NewCronManager, AddCron, Start, Stop.
type CronManager struct {
	logger    *zerolog.Logger
	mu        sync.Mutex
	jobs      map[string]*CronJob
	ctx       context.Context
	cancelCtx context.CancelFunc
	running   bool

	wg sync.WaitGroup // tracks running jobs
	// cond is used to signal the scheduler goroutine that something changed (job added, removed, etc.).
	cond *sync.Cond
}

// The enforced minimum interval (5 seconds).
const minInterval = 5 * time.Second

// NewCronManager returns a new CronManager instance.
func NewCronManager(logger *zerolog.Logger) *CronManager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &CronManager{
		logger:    logger,
		jobs:      make(map[string]*CronJob),
		ctx:       ctx,
		cancelCtx: cancel,
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// AddCron registers (or replaces) a job.
// If interval < minInterval, we force it to minInterval.
// If runImmed is true, nextRun is "now" (meaning it will start soon after Start is called).
// Otherwise, nextRun is "now+interval".
func (m *CronManager) AddCron(name string, job Job, interval time.Duration, runImmed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Enforce minimal interval
	if interval < minInterval {
		interval = minInterval
	}

	now := time.Now()
	next := now
	if !runImmed {
		next = now.Add(interval)
	}

	m.jobs[name] = &CronJob{
		Name:     name,
		Action:   job,
		Interval: interval,
		RunImmed: runImmed,
		nextRun:  next,
	}

	m.logger.Debug().Msgf("[cron|%s|%s] added/replaced job (runImmed=%v)", name, interval, runImmed)
	// Signal the scheduler goroutine to recalc earliest job
	if m.running {
		m.cond.Signal()
	}
}

// Start launches the scheduler goroutine, if not already started.
func (m *CronManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		// Already running
		return
	}

	if m.ctx.Err() != nil {
		// If previously canceled, recreate context
		m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	}
	m.running = true

	// Launch the scheduling goroutine
	go m.runScheduler()
}

// Stop cancels all scheduling, waits for any running job goroutines to finish.
func (m *CronManager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	m.cancelCtx()
	m.cond.Broadcast() // wake the scheduler if it’s waiting
	m.mu.Unlock()

	// Wait for running jobs to finish
	m.wg.Wait()
	m.logger.Debug().Msg("[cron] stopped")
}

// The core scheduler loop. Only one instance of runScheduler() at a time.
func (m *CronManager) runScheduler() {
	for {
		m.mu.Lock()
		// If we were told to stop, exit
		if !m.running || m.ctx.Err() != nil {
			m.mu.Unlock()
			return
		}

		var nextJob *CronJob
		var earliest time.Time
		// Find the job with the earliest nextRun
		for _, j := range m.jobs {
			if nextJob == nil || j.nextRun.Before(earliest) {
				nextJob = j
				earliest = j.nextRun
			}
		}

		if nextJob == nil {
			// No jobs -> wait until something is added or we’re stopped
			m.cond.Wait()
			m.mu.Unlock()
			continue
		}

		// Calculate how long until earliest job
		now := time.Now()
		delay := earliest.Sub(now)
		if delay < 0 {
			delay = 0
		}

		// We’ll wait up to delay for a "wake-up" event.
		// But we can spuriously wake earlier if a new job is added with an earlier nextRun.
		timer := time.NewTimer(delay)
		m.mu.Unlock()

		select {
		case <-m.ctx.Done():
			// We’re stopping
			timer.Stop()
			return
		case <-timer.C:
			// Time to run earliest job
		}

		// Double-check we’re not stopped
		m.mu.Lock()
		if !m.running || m.ctx.Err() != nil {
			m.mu.Unlock()
			timer.Stop()
			return
		}
		// The job might have changed or been removed.
		// We need to ensure nextJob is still valid and nextRun matches what we found.
		foundJob, stillExists := m.jobs[nextJob.Name]
		if !stillExists || foundJob != nextJob {
			// job was changed or removed, skip
			m.mu.Unlock()
			continue
		}

		// Because we waited 'delay', now is roughly the time we want to run
		now = time.Now()

		// Spawn the job in a new goroutine
		m.wg.Add(1)
		go m.runSingleJob(nextJob)

		// Reschedule this job’s next run
		nextJob.nextRun = now.Add(nextJob.Interval)
		// we’re done in terms of scheduling
		m.mu.Unlock()
	}
}

// runSingleJob executes the job’s Action in a goroutine.
func (m *CronManager) runSingleJob(j *CronJob) {
	defer m.wg.Done()

	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Msgf("[cron|%s] panic recovered: %v", j.Name, r)
		}
	}()

	j.Action()
}
