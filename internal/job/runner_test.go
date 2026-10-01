package job

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type fakeJob struct {
	sweeps atomic.Int64
	// wake is what each Sweep reports as "come back in".
	wake atomic.Int64
}

func (f *fakeJob) Name() string       { return "fake" }
func (f *fakeJob) Triggers() Triggers { return Triggers{} }
func (f *fakeJob) Sweep(context.Context) time.Duration {
	f.sweeps.Add(1)
	return time.Duration(f.wake.Load())
}

func run(t *testing.T, j Job, interval time.Duration) (*Runner, func()) {
	t.Helper()
	r := NewRunner(j, interval)
	r.MinSweepGap = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	return r, func() { cancel(); <-done }
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSweepsAtStartupWithoutWaitingForTheInterval(t *testing.T) {
	f := &fakeJob{}
	_, stop := run(t, f, time.Hour)
	defer stop()
	waitFor(t, time.Second, "the startup sweep", func() bool { return f.sweeps.Load() >= 1 })
}

func TestKickWakesTheJobEarly(t *testing.T) {
	f := &fakeJob{}
	r, stop := run(t, f, time.Hour)
	defer stop()
	waitFor(t, time.Second, "the startup sweep", func() bool { return f.sweeps.Load() == 1 })

	time.Sleep(100 * time.Millisecond)
	if f.sweeps.Load() != 1 {
		t.Fatalf("swept without a trigger: %d", f.sweeps.Load())
	}
	r.Kick()
	waitFor(t, time.Second, "a sweep after Kick", func() bool { return f.sweeps.Load() == 2 })
}

func TestSleepsExactlyUntilTheDeadlineTheJobReported(t *testing.T) {
	f := &fakeJob{}
	f.wake.Store(int64(150 * time.Millisecond))
	_, stop := run(t, f, time.Hour)
	defer stop()
	// Interval is an hour and nobody kicks: only the reported deadline can
	// produce the repeated sweeps.
	waitFor(t, 2*time.Second, "repeated sweeps driven by the reported wake time", func() bool { return f.sweeps.Load() >= 3 })
}

func TestZeroWakeFallsBackToTheSafetyNetInterval(t *testing.T) {
	f := &fakeJob{} // reports nothing pending
	_, stop := run(t, f, 120*time.Millisecond)
	defer stop()
	waitFor(t, 2*time.Second, "interval-driven sweeps", func() bool { return f.sweeps.Load() >= 3 })
}

func TestABurstOfKicksCoalesces(t *testing.T) {
	f := &fakeJob{}
	r, stop := run(t, f, time.Hour)
	defer stop()
	waitFor(t, time.Second, "the startup sweep", func() bool { return f.sweeps.Load() == 1 })
	for i := 0; i < 500; i++ {
		r.Kick()
	}
	time.Sleep(300 * time.Millisecond)
	if n := f.sweeps.Load(); n > 1+3 {
		t.Fatalf("500 kicks caused %d sweeps; they should coalesce", n-1)
	}
	if f.sweeps.Load() < 2 {
		t.Fatal("kicks were lost entirely")
	}
}

func TestRunReturnsWhenTheContextIsCancelled(t *testing.T) {
	_, stop := run(t, &fakeJob{}, time.Hour)
	stop() // blocks until Run returns; the test hangs (and fails) if it doesn't
}
