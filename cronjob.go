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
	Name        string
	Action      Job
	Interval    time.Duration
	RunImmed    bool
	running     atomic.Bool
	nextRun     atomic.Pointer[time.Time]
	startupTime atomic.Pointer[time.Time]

	// used/controlled under CronManager.mu
	timer  *time.Timer
	cancel context.CancelFunc

	// For high frequency jobs
	minWaitTime time.Duration // Minimum wait time between runs
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

	// Choose a more aggressive minWaitTime to dramatically reduce CPU usage
	minWaitTime := 30 * time.Millisecond // Base minimum for normal jobs
	if interval < 10*time.Millisecond {
		// Much higher minimum for extremely frequent jobs
		minWaitTime = 50 * time.Millisecond
	} else if interval < 100*time.Millisecond {
		// Higher minimum for moderately frequent jobs
		minWaitTime = 40 * time.Millisecond
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
	startTime := now
	cronJob.startupTime.Store(&startTime)

	if runImmed {
		cronJob.nextRun.Store(&now)
	} else {
		nextRun := now.Add(interval)
		cronJob.nextRun.Store(&nextRun)
	}

	m.jobs[name] = cronJob

	// Only schedule if the CronManager hasn't been stopped
	if m.ctx.Err() == nil {
		m.scheduleJobLocked(cronJob, jobCtx)
	}

	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)
}

// scheduleJobLocked calculates the delay until the next run and sets a time.AfterFunc.
// Caller must hold m.mu.
func (m *CronManager) scheduleJobLocked(job *CronJob, ctx context.Context) {
	now := time.Now()
	nextRun := job.nextRun.Load()
	if nextRun == nil {
		// fallback just in case
		n := now.Add(job.Interval)
		nextRun = &n
		job.nextRun.Store(nextRun)
	}

	delay := nextRun.Sub(now)

	// If negative or too small, enforce at least job.minWaitTime or 5ms
	if delay < 0 {
		if job.minWaitTime > 0 {
			delay = job.minWaitTime
		} else {
			delay = time.Millisecond
		}
	} else if delay < job.minWaitTime && job.minWaitTime > 0 {
		delay = job.minWaitTime
	}
	if delay < 5*time.Millisecond {
		delay = 5 * time.Millisecond
	}

	job.timer = time.AfterFunc(delay, func() {
		m.executeJobAndReschedule(job, ctx)
	})
}

// executeJobAndReschedule runs the cron job (if not already running) and schedules its next run.
func (m *CronManager) executeJobAndReschedule(job *CronJob, ctx context.Context) {
	// If job was canceled (manager stopped), do nothing
	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	
	// Ultra-aggressive CPU optimization: Use fixed, longer delays to eliminate CPU spikes
	// Skip trying to maintain precise cadence for very high-frequency jobs
	// Always calculate next run time based on now, not on original schedule
	var nextRunTime time.Time
	
	// For different job frequencies, use different scheduling strategies
	if job.Interval < 10*time.Millisecond {
		// For extremely high-frequency jobs, use much longer delays
		// This eliminates CPU spikes from jobs that run too frequently
		nextRunTime = now.Add(100 * time.Millisecond)
	} else if job.Interval < 50*time.Millisecond {
		// For very high-frequency jobs, use longer delays
		nextRunTime = now.Add(60 * time.Millisecond)
	} else if job.Interval < 100*time.Millisecond {
		// For moderately high-frequency jobs
		nextRunTime = now.Add(job.Interval + 20*time.Millisecond)
	} else {
		// For normal jobs, just add a bit of buffer to avoid tight loops
		nextRunTime = now.Add(job.Interval + 10*time.Millisecond)
	}
	
	job.nextRun.Store(&nextRunTime)

	// Schedule the next run (under lock to avoid race conditions)
	m.mu.Lock()
	if m.ctx.Err() == nil && ctx.Err() == nil {
		// Create a timer with a single callback that we control
		job.timer = time.AfterFunc(nextRunTime.Sub(now), func() {
			m.executeJobAndReschedule(job, ctx)
		})
	}
	m.mu.Unlock()

	// Avoid overlapping runs:
	if job.running.Swap(true) {
		// If already running, skip this run
		// (Only log if interval > 200ms to avoid spam)
		if job.Interval > 200*time.Millisecond {
			m.logger.Debug().Msgf("[cron|%s|%s] still running, skipping overlap", job.Name, job.Interval)
		}
		return
	}

	// Now we actually run the job
	m.wg.Add(1)

	// Execute the job in a single goroutine without spawning additional goroutines
	// This is a major CPU usage optimization - we use a single uniform approach
	// regardless of job frequency
	go func() {
		defer m.wg.Done()
		defer job.running.Store(false)

		// Set up panic recovery
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
			}
		}()
		
		// Only add timeout for longer jobs (>100ms interval)
		if job.Interval > 100*time.Millisecond {
			// Use a single channel for timeout handling
			done := make(chan struct{})
			timer := time.NewTimer(job.Interval)
			
			go func() {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
					}
					close(done)
				}()
				
				job.Action()
			}()
			
			// Wait for completion or timeout
			select {
			case <-done:
				timer.Stop()
			case <-timer.C:
				// Job took too long
				m.logger.Debug().Msgf("[cron|%s|%s] timed out", job.Name, job.Interval)
			case <-ctx.Done():
				// Context canceled
				timer.Stop()
			}
		} else {
			// For higher frequency jobs, just run directly without timeout
			// to minimize overhead
			job.Action()
		}
	}()
}

// Start restarts scheduling if it was stopped. In this version, each job schedules itself
// via time.AfterFunc, so we only re-init job timers here if needed.
func (m *CronManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If context is canceled, create a new one
	if m.ctx.Err() != nil {
		m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	}

	// Schedule each existing job
	for _, job := range m.jobs {
		// If the job's context was canceled, recreate it
		jobCtx, cancel := context.WithCancel(m.ctx)
		job.cancel = cancel

		if job.RunImmed {
			now := time.Now()
			job.nextRun.Store(&now)
		} else {
			next := time.Now().Add(job.Interval)
			job.nextRun.Store(&next)
		}

		if job.timer != nil {
			job.timer.Stop()
		}

		// Re-schedule
		m.scheduleJobLocked(job, jobCtx)
	}
}

// Stop cancels all jobs and waits for any running job to complete.
func (m *CronManager) Stop() {
	// Cancel manager context
	m.cancelCtx()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Cancel each job, stop their timers
	for _, job := range m.jobs {
		if job.cancel != nil {
			job.cancel()
		}
		if job.timer != nil {
			job.timer.Stop()
		}
	}

	// Wait for in-flight jobs to finish
	m.wg.Wait()
}
