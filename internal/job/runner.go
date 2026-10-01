package job

import (
	"context"
	"time"
)

// DefaultMinSweepGap bounds how often event bursts (35 pods of one
// Deployment all changing at once) can trigger sweeps.
const DefaultMinSweepGap = 500 * time.Millisecond

// Runner drives one Job. A sweep happens (a) at startup, (b) when Kick is
// called by an informer event handler, (c) when the earliest pending item the
// previous sweep reported reaches its deadline, and (d) after Interval, as a
// safety net against a missed event.
type Runner struct {
	Job      Job
	Interval time.Duration
	// MinSweepGap defaults to DefaultMinSweepGap.
	MinSweepGap time.Duration

	kick chan struct{}
}

func NewRunner(j Job, interval time.Duration) *Runner {
	return &Runner{Job: j, Interval: interval, kick: make(chan struct{}, 1)}
}

// Kick requests a sweep as soon as possible. Safe to call from informer
// event handlers: it never blocks, and kicks arriving while one is already
// pending coalesce into it.
func (r *Runner) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	gap := r.MinSweepGap
	if gap == 0 {
		gap = DefaultMinSweepGap
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-r.kick:
		}
		wake := r.Job.Sweep(ctx)
		next := r.Interval
		if wake > 0 && wake < next {
			next = wake
		}
		timer.Reset(next)

		select {
		case <-ctx.Done():
			return
		case <-time.After(gap):
		}
	}
}
