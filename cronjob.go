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
	
	// Set more aggressive minimum wait times to reduce CPU usage
	// This will reduce precision but significantly decrease CPU load
	minWaitTime := 10 * time.Millisecond // Base minimum for all jobs
	if interval < 10*time.Millisecond {
		minWaitTime = 20 * time.Millisecond // Higher minimum for very high-frequency jobs
	} else if interval < 100*time.Millisecond {
		minWaitTime = 15 * time.Millisecond // Moderate minimum for high-frequency jobs
	}
	
	cronJob := &CronJob{
		Name:        name,
		Action:      job,
		Interval:    interval,
		RunImmed:    runImmed,
		cancel:      cancel,
		minWaitTime: minWaitTime,
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
	
	// If delay is negative (e.g., for immediate execution), set to minimum wait time
	if delay < 0 {
		if job.minWaitTime > 0 {
			delay = job.minWaitTime
		} else {
			delay = time.Millisecond // Minimum 1ms delay to prevent CPU thrashing
		}
	} else if delay < job.minWaitTime && job.minWaitTime > 0 {
		// Ensure we respect minimum wait time for high-frequency jobs
		delay = job.minWaitTime
	}

	// Ensure we never use a delay less than 5ms to prevent CPU thrashing
	// This will reduce precision but significantly decrease CPU load
	if delay < 5*time.Millisecond {
		delay = 5*time.Millisecond
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
		
		// Apply more aggressive minimum delay rules to reduce CPU usage
		if nextDelay < 0 {
			// We're already behind schedule
			if job.minWaitTime > 0 {
				nextDelay = job.minWaitTime
			} else {
				nextDelay = 10*time.Millisecond // Increased minimum delay
			}
		} else if nextDelay < job.minWaitTime && job.minWaitTime > 0 {
			// Respect minimum wait time for high-frequency jobs
			nextDelay = job.minWaitTime
		} else if nextDelay < 5*time.Millisecond {
			// Higher absolute minimum delay to reduce CPU usage
			nextDelay = 5*time.Millisecond
		}
		
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
			
			// For very high-frequency jobs, use a timeout proportional to the interval
			// but with a reasonable minimum to prevent resource exhaustion
			timeoutDuration := job.Interval
			if timeoutDuration < 10*time.Millisecond {
				timeoutDuration = 30 * time.Millisecond // More aggressive timeout for CPU reduction
			} else if timeoutDuration < 100*time.Millisecond {
				timeoutDuration = 20 * time.Millisecond // Moderate timeout for CPU reduction
			}
			
			// Set a timeout that prevents long-running jobs from consuming resources indefinitely
			jobCtx, cancel := context.WithTimeout(ctx, timeoutDuration)
			defer cancel()
			
			done := make(chan struct{}, 1)
			
			// Run the actual job in yet another goroutine to allow for timeout handling
			go func() {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", job.Name, job.Interval, r)
					}
					select {
					case done <- struct{}{}:
						// Successfully sent completion signal
					default:
						// Channel might be full or closed, which is fine
					}
				}()
				
				job.Action()
			}()
			
			select {
			case <-done:
				// Only log completion at debug level to avoid excessive logging
				// with many frequently-running jobs
				m.logger.Debug().Msgf("[cron|%s|%s] completed", job.Name, job.Interval)
			case <-jobCtx.Done():
				// This handles both timeouts and cancellations - only log at Error level for custom timeouts
				if job.Interval >= 10*time.Millisecond {
					m.logger.Error().Msgf("[cron|%s|%s] timed out or canceled", job.Name, job.Interval)
				} else {
					// Use debug level for very frequent jobs to reduce log spam
					m.logger.Debug().Msgf("[cron|%s|%s] timed out or canceled", job.Name, job.Interval)
				}
			}
		}()
	} else {
		// Job is still running from previous interval, log and continue
		// Use Debug instead of Warn for high-frequency jobs to avoid log spam
		if job.Interval > 100*time.Millisecond {
			// Only log skipped jobs with larger intervals to avoid excessive logging
			m.logger.Debug().Msgf("[cron|%s|%s] still running from previous interval, skipping", job.Name, job.Interval)
		}
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
	// First acquire the lock to stop new jobs from starting
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
	
	// Release the lock so jobs can complete
	m.mu.Unlock()
	
	// Wait for all running jobs to complete (outside of lock)
	m.wg.Wait()
}