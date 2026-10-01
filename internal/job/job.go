// Package job is the small contract every shepherd job implements, plus the
// Runner that drives a job from watch events and its own deadlines.
//
// A job is a pure "look at the cluster cache, act on what is due" function
// (Sweep). It does not own informers, timers or leader election: the shepherd
// package wires those once and shares them, so adding a job means writing one
// package that implements Job and registering it in internal/shepherd.
package job

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Job is one unit of janitorial work.
type Job interface {
	// Name is the stable identifier used for --exclude-jobs and in logs.
	Name() string
	// Triggers says which watch events should wake the job early.
	Triggers() Triggers
	// Sweep does one pass over the informer caches and returns how long
	// until something it is waiting on becomes due (0 = nothing pending).
	// Run uses that to sleep exactly until then rather than polling.
	Sweep(ctx context.Context) time.Duration
}

// Triggers are the live events a job cares about. Nil funcs / false flags
// mean "don't wake me for this". Every job is also swept at startup and on
// the Runner's safety-net interval, so a trigger is a latency optimisation,
// never the only thing correctness depends on.
type Triggers struct {
	// Pod is called for pod add (old == nil) and update events.
	Pod func(old, cur *corev1.Pod) bool
	// Node is called for node update events.
	Node func(old, cur *corev1.Node) bool
	// NodeDeleted wakes the job when a Node object is deleted.
	NodeDeleted bool
}
