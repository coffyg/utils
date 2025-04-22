package utils

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Job func()

type CronJob struct {
	Name     string
	Action   Job
	Interval time.Duration
	RunImmed bool

	running atomic.Bool

	// nextRun is updated before scheduling each new timer
	nextRun atomic.Pointer[time.Time]

	// We no longer use a “startupTime” to compute “catch‐up intervals.”
	// Instead, each run sets nextRun = now + interval

	// Protected under CronManager.mu
	timer  *time.Timer
	cancel context.CancelFunc

	// For high frequency jobs: minimal spacing
	minWaitTime time.Duration
}

type CronManager struct {
	jobs      map[string]*CronJob
	mu        sync.RWMutex
	logger    *zerolog.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancelCtx context.CancelFunc
}

// NewCronManager returns a new CronManager instance.
func NewCronManager(logger *zerolog.Logger) *CronManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &CronManager{
		jobs:      make(map[string]*CronJob),
		logger:    logger,
		ctx:       ctx,
		cancelCtx: cancel,
	}
}

// AddCron registers a new cron job. If one with the same name exists, it is replaced.
func (m *CronManager) AddCron(name string, job Job, interval time.Duration, runImmed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If a job with the same name exists, stop its timer/cancel first
	if existing, exists := m.jobs[name]; exists {
		if existing.timer != nil {
			existing.timer.Stop()
		}
		if existing.cancel != nil {
			existing.cancel()
		}
		m.logger.Debug().Msgf("[cron|%s|%s] Replaced existing job", name, interval)
	}

	jobCtx, cancel := context.WithCancel(m.ctx)

	// Choose a minWaitTime to avoid extremely tight loops for high-frequency jobs
	minWaitTime := 10 * time.Millisecond
	if interval < 10*time.Millisecond {
		// Higher minimum for extremely frequent jobs
		minWaitTime = 20 * time.Millisecond
	} else if interval < 100*time.Millisecond {
		// Slightly lower minimum for moderately frequent jobs
		minWaitTime = 15 * time.Millisecond
	}

	now := time.Now()
	cronJob := &CronJob{
		Name:        name,
		Action:      job,
		Interval:    interval,
		RunImmed:    runImmed,
		cancel:      cancel,
		minWaitTime: minWaitTime,
	}

	// Set initial next run
	if runImmed {
		cronJob.nextRun.Store(&now)
	} else {
		t := now.Add(interval)
		cronJob.nextRun.Store(&t)
	}

	m.jobs[name] = cronJob

	// Only schedule if the CronManager hasn't been stopped
	if m.ctx.Err() == nil {
		m.scheduleJobLocked(cronJob, jobCtx)
	}

	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)
}

// scheduleJobLocked calculates the delay and sets a time.AfterFunc for the job.
// Caller must hold m.mu.
func (m *CronManager) scheduleJobLocked(job *CronJob, ctx context.Context) {
	now := time.Now()
	nextRun := job.nextRun.Load()
	if nextRun == nil {
		// fallback if missing
		n := now.Add(job.Interval)
		nextRun = &n
		job.nextRun.Store(nextRun)
	}

	delay := nextRun.Sub(now)

	// Enforce a minimal wait to avoid thrashing
	if delay < job.minWaitTime && job.minWaitTime > 0 {
		delay = job.minWaitTime
	}
	if delay < 5*time.Millisecond {
		delay = 5 * time.Millisecond
	}

	job.timer = time.AfterFunc(delay, func() {
		m.executeJobAndReschedule(job, ctx)
	})
}

// executeJobAndReschedule runs the cron job and schedules its next run.
func (m *CronManager) executeJobAndReschedule(job *CronJob, ctx context.Context) {
	// If job was canceled (manager stopped), do nothing
	if ctx.Err() != nil {
		return
	}

	now := time.Now()

	// For the next run, simply do: now + job.Interval
	// This avoids "catching up" from an old startup time.
	nextRunTime := now.Add(job.Interval)
	job.nextRun.Store(&nextRunTime)

	// Lock to schedule the next run
	m.mu.Lock()
	if m.ctx.Err() == nil && ctx.Err() == nil {
		delay := nextRunTime.Sub(time.Now())

		// Respect minWaitTime for high-frequency or behind-schedule
		if delay < job.minWaitTime && job.minWaitTime > 0 {
			delay = job.minWaitTime
		}
		if delay < 5*time.Millisecond {
			delay = 5 * time.Millisecond
		}

		job.timer = time.AfterFunc(delay, func() {
			m.executeJobAndReschedule(job, ctx)
		})
	}
	m.mu.Unlock()

	// Avoid overlapping runs
	if job.running.Swap(true) {
		// If it was already true, we skip. Possibly log if interval is large.
		if job.Interval > 200*time.Millisecond {
			m.logger.Debug().Msgf("[cron|%s|%s] still running, skipping overlap", job.Name, job.Interval)
		}
		return
	}
	m.wg.Add(1)

	// Actually run the job
	// For extremely frequent jobs, run synchronously
	if job.Interval < 10*time.Millisecond {
		func() {
			defer m.wg.Done()
			defer job.running.Store(false)
			defer func() {
				if r := recover(); r != nil {
					m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
				}
			}()
			job.Action()
		}()
		return
	}

	// Otherwise, run asynchronously
	go func() {
		defer m.wg.Done()
		defer job.running.Store(false)

		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
			}
		}()

		// For jobs with intervals > 100ms, do a simple time-based timeout
		if job.Interval > 100*time.Millisecond {
			timer := time.NewTimer(job.Interval)
			jobComplete := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
					}
					jobComplete <- struct{}{}
				}()
				job.Action()
			}()

			select {
			case <-jobComplete:
				timer.Stop()
			case <-timer.C:
				m.logger.Error().Msgf("[cron|%s|%s] timed out", job.Name, job.Interval)
			case <-ctx.Done():
				timer.Stop()
			}
			return
		}

		// Medium frequency: run job directly without timeout overhead
		job.Action()
	}()
}

// Start restarts scheduling if it was stopped. We simply re-init each job’s timer.
func (m *CronManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx.Err() != nil {
		m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	}

	// Re-schedule each existing job
	for _, job := range m.jobs {
		jobCtx, cancel := context.WithCancel(m.ctx)
		job.cancel = cancel

		now := time.Now()
		if job.RunImmed {
			job.nextRun.Store(&now)
		} else {
			n := now.Add(job.Interval)
			job.nextRun.Store(&n)
		}

		if job.timer != nil {
			job.timer.Stop()
		}

		m.scheduleJobLocked(job, jobCtx)
	}
}

// Stop cancels all jobs and waits for any running job to complete.
func (m *CronManager) Stop() {
	// Acquire the lock before calling m.cancelCtx()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cancelCtx() // now the "write" to context is protected

	// Stop all timers, cancel job contexts, etc.
	for _, job := range m.jobs {
		if job.cancel != nil {
			job.cancel()
		}
		if job.timer != nil {
			job.timer.Stop()
		}
	}

	// Wait for running jobs
	m.wg.Wait()
}
