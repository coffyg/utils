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
	
	// protected by CronManager.mu
	timer  *time.Timer
	cancel context.CancelFunc
}

type CronManager struct {
	jobs      map[string]*CronJob
	mu        sync.RWMutex
	logger    *zerolog.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancelCtx context.CancelFunc
}

func NewCronManager(logger *zerolog.Logger) *CronManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &CronManager{
		jobs:      make(map[string]*CronJob),
		logger:    logger,
		ctx:       ctx,
		cancelCtx: cancel,
	}
}

func (m *CronManager) AddCron(name string, job Job, interval time.Duration, runImmed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	cronJob := &CronJob{
		Name:     name,
		Action:   job,
		Interval: interval,
		RunImmed: runImmed,
		cancel:   cancel,
	}
	
	now := time.Now()
	if runImmed {
		cronJob.nextRun.Store(&now)
	} else {
		nextRun := now.Add(interval)
		cronJob.nextRun.Store(&nextRun)
	}
	
	startTime := now
	cronJob.startupTime.Store(&startTime)
	
	m.jobs[name] = cronJob

	if m.ctx.Err() == nil { // Only schedule if manager is running
		m.scheduleJobLocked(cronJob, jobCtx)
	}

	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)
}

// scheduleJobLocked schedules a job to run at its next execution time
// Caller must hold m.mu lock
func (m *CronManager) scheduleJobLocked(job *CronJob, ctx context.Context) {
	// Calculate time until the next run
	now := time.Now()
	nextRun := job.nextRun.Load()
	delay := nextRun.Sub(now)
	
	// If delay is negative (e.g., for immediate execution), set to zero
	if delay < 0 {
		delay = 0
	}

	// Timer callback must acquire lock before modifying shared state
	job.timer = time.AfterFunc(delay, func() {
		m.executeJobAndReschedule(job, ctx)
	})
}

func (m *CronManager) executeJobAndReschedule(job *CronJob, ctx context.Context) {
	if ctx.Err() != nil {
		return // Context canceled
	}

	// Calculate the next run time based on the scheduled interval cadence
	startupTime := job.startupTime.Load()
	now := time.Now()
	
	// Calculate how many intervals have passed since startup
	elapsedSinceStart := now.Sub(*startupTime)
	intervals := elapsedSinceStart / job.Interval
	
	// Next run is at the next multiple of interval from the startup time
	nextRunTime := startupTime.Add(job.Interval * (intervals + 1))
	job.nextRun.Store(&nextRunTime)
	
	// Schedule next execution with mutex protection
	m.mu.Lock()
	// Double-check context hasn't been canceled while waiting for lock
	if m.ctx.Err() == nil && ctx.Err() == nil {
		nextDelay := nextRunTime.Sub(now)
		job.timer = time.AfterFunc(nextDelay, func() {
			m.executeJobAndReschedule(job, ctx)
		})
	}
	m.mu.Unlock()
	
	// Only run the job if it's not already running (prevents overlap)
	if !job.running.Swap(true) {
		// Execute the job asynchronously
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer job.running.Store(false)
			
			jobCtx, cancel := context.WithTimeout(ctx, job.Interval)
			defer cancel()
			
			done := make(chan struct{}, 1)
			
			go func() {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
					}
					done <- struct{}{}
				}()
				
				job.Action()
			}()
			
			select {
			case <-done:
				m.logger.Debug().Msgf("[cron|%s|%s] completed", job.Name, job.Interval)
			case <-jobCtx.Done():
				m.logger.Error().Msgf("[cron|%s|%s] timed out or canceled", job.Name, job.Interval)
			}
		}()
	} else {
		// Job is still running from previous interval, log and continue
		m.logger.Warn().Msgf("[cron|%s|%s] still running from previous interval, skipping", job.Name, job.Interval)
	}
}

func (m *CronManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Create new context if previously stopped
	if m.ctx.Err() != nil {
		m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	}
	
	// Start all jobs
	for name, job := range m.jobs {
		jobCtx, cancel := context.WithCancel(m.ctx)
		job.cancel = cancel
		m.logger.Debug().Msgf("[cron|%s|%s] Starting job", name, job.Interval)
		m.scheduleJobLocked(job, jobCtx)
	}
}

func (m *CronManager) Stop() {
	m.mu.Lock()
	
	// Cancel the context to stop all jobs
	m.cancelCtx()
	
	// Stop all timers
	for _, job := range m.jobs {
		if job.timer != nil {
			job.timer.Stop()
		}
		if job.cancel != nil {
			job.cancel()
		}
	}
	
	m.mu.Unlock()
	
	// Wait for all running jobs to complete
	m.wg.Wait()
}