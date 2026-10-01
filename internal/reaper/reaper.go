// Package reaper is the stuck-Terminating-pod reaper: it looks at every
// terminating pod each sweep, asks internal/policy what to do, and applies
// the safety rails that only make sense across many pods (per-sweep delete
// cap, circuit breaker, dry-run).
package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/policy"
)

type Config struct {
	Policy   policy.Config
	Interval time.Duration
	// DryRun logs and counts what would be deleted but never calls Delete.
	DryRun bool
	// MaxDeletesPerSweep bounds the blast radius of one bad sweep. Pods over
	// the cap are picked up next sweep.
	MaxDeletesPerSweep int
	// BreakerFraction: when more than this fraction of nodes are NotReady
	// (and there are at least BreakerMinNodes), something bigger than a dead
	// node is happening (network partition, control-plane trouble, bad
	// rollout of a node image). Force-deleting pods then risks running
	// duplicates on nodes that are merely unreachable, so we stop acting on
	// dead-node decisions and just alert. 0 disables.
	BreakerFraction float64
	BreakerMinNodes int
}

type Reaper struct {
	cfg      Config
	client   kubernetes.Interface
	pods     listersv1.PodLister
	nodes    listersv1.NodeLister
	recorder record.EventRecorder
	log      *slog.Logger
	now      func() time.Time
	m        *Metrics
	kick     chan struct{}

	// alerted remembers what we already told humans about each stuck pod, so
	// a pod that stays stuck is reported once (and again after alertRepeat
	// as a reminder), not on every sweep. Only touched from Sweep, which
	// never runs concurrently with itself.
	alerted        map[types.UID]alertState
	breakerWasOpen bool
}

type alertState struct {
	reason string
	at     time.Time
}

// alertRepeat is how long a still-stuck pod stays quiet before being
// reported again.
const alertRepeat = time.Hour

func New(cfg Config, client kubernetes.Interface, pods listersv1.PodLister, nodes listersv1.NodeLister,
	recorder record.EventRecorder, log *slog.Logger, m *Metrics) *Reaper {
	return &Reaper{cfg: cfg, client: client, pods: pods, nodes: nodes, recorder: recorder, log: log, now: time.Now, m: m, kick: make(chan struct{}, 1),
		alerted: map[types.UID]alertState{}}
}

// minSweepGap bounds how often event bursts (35 pods of one Deployment all
// flipping to Terminating at once) can trigger sweeps.
const minSweepGap = 500 * time.Millisecond

// Kick requests a sweep as soon as possible. Safe to call from informer
// event handlers: it never blocks, and kicks arriving while one is already
// pending coalesce into it.
func (r *Reaper) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run sweeps until ctx is cancelled. A sweep happens when (a) an event
// handler calls Kick, (b) the earliest pending pod reaches its force-delete
// deadline, or (c) cfg.Interval elapses, as a safety net against a missed
// event.
func (r *Reaper) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-r.kick:
		}
		wake := r.Sweep(ctx)
		next := r.cfg.Interval
		if wake > 0 && wake < next {
			next = wake
		}
		timer.Reset(next)

		select {
		case <-ctx.Done():
			return
		case <-time.After(minSweepGap):
		}
	}
}

// Sweep does one pass over the informer caches.
// It returns how long until the soonest pod it is waiting on becomes
// eligible (0 if none), so Run can sleep until exactly then.
func (r *Reaper) Sweep(ctx context.Context) time.Duration {
	start := r.now()
	pods, err := r.pods.List(everything)
	if err != nil {
		r.log.Error("list pods", "err", err)
		return 0
	}
	nodeList, err := r.nodes.List(everything)
	if err != nil {
		r.log.Error("list nodes", "err", err)
		return 0
	}
	nodes := make(map[string]*corev1.Node, len(nodeList))
	notReady := 0
	for _, n := range nodeList {
		nodes[n.Name] = n
		if isNotReady(n) {
			notReady++
		}
	}

	breaker := r.breakerOpen(len(nodeList), notReady)
	r.m.breakerOpen.Set(boolToFloat(breaker))
	// Log transitions only: a breaker that stays open for an hour must not
	// print a line per sweep.
	if breaker && !r.breakerWasOpen {
		r.log.Warn("circuit breaker open: too many NotReady nodes, not force-deleting pods on dead nodes",
			"notReady", notReady, "nodes", len(nodeList))
	} else if !breaker && r.breakerWasOpen {
		r.log.Info("circuit breaker closed: NotReady nodes back below threshold",
			"notReady", notReady, "nodes", len(nodeList))
	}
	r.breakerWasOpen = breaker

	// Oldest-stuck first so the per-sweep cap can't starve a long-stuck pod.
	var terminating []*corev1.Pod
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			terminating = append(terminating, p)
		}
	}
	sort.Slice(terminating, func(i, j int) bool {
		return terminating[i].DeletionTimestamp.Before(terminating[j].DeletionTimestamp)
	})

	r.m.stuck.Reset()
	stillAlerting := map[types.UID]struct{}{}
	deleted := 0
	var nextWake time.Duration
	for _, p := range terminating {
		d := policy.Decide(p, nodes[p.Spec.NodeName], start, r.cfg.Policy)

		if d.Action == policy.ForceDelete && breaker && d.NodeState != "ready" {
			d.Action, d.Reason = policy.Alert, "circuit-open"
		}
		if d.Action == policy.ForceDelete && deleted >= r.cfg.MaxDeletesPerSweep {
			r.log.Warn("per-sweep delete cap reached, deferring", "pod", key(p), "cap", r.cfg.MaxDeletesPerSweep)
			d.Action, d.Reason, d.RequeueAfter = policy.Wait, "rate-limited", time.Second
		}

		switch d.Action {
		case policy.Skip:
			// not ours; keep quiet
		case policy.Wait:
			r.m.stuck.WithLabelValues(d.Action.String(), d.Reason).Inc()
			if d.RequeueAfter > 0 && (nextWake == 0 || d.RequeueAfter < nextWake) {
				nextWake = d.RequeueAfter
			}
			// DEBUG only: answers "why hasn't it deleted that pod yet?"
			// without a line per waiting pod per sweep at the default level.
			r.log.Debug("pod terminating, waiting for its deadline",
				"pod", key(p), "node", p.Spec.NodeName, "nodeState", d.NodeState, "mode", string(d.Mode),
				"reason", d.Reason, "remaining", d.RequeueAfter.Round(time.Second).String())
		case policy.Alert:
			r.m.stuck.WithLabelValues(d.Action.String(), d.Reason).Inc()
			stillAlerting[p.UID] = struct{}{}
			prev, seen := r.alerted[p.UID]
			if seen && prev.reason == d.Reason && start.Sub(prev.at) < alertRepeat {
				break // already told humans; stay quiet until it changes or the reminder is due
			}
			r.alerted[p.UID] = alertState{reason: d.Reason, at: start}
			r.log.Warn("pod stuck terminating, not force-deleting",
				"pod", key(p), "node", p.Spec.NodeName, "nodeState", d.NodeState, "reason", d.Reason,
				"terminatingFor", start.Sub(p.DeletionTimestamp.Time).Round(time.Second).String())
			r.recorder.Eventf(p, corev1.EventTypeWarning, "ShepherdStuckTerminating",
				"Pod is stuck terminating (%s); shepherd will not force-delete it", d.Reason)
		case policy.ForceDelete:
			r.m.stuck.WithLabelValues(d.Action.String(), d.Reason).Inc()
			if r.forceDelete(ctx, p, d) {
				deleted++
			}
		}
	}
	// Forget pods that are gone or no longer alerting, so the map can't
	// grow forever and a pod that gets stuck again later is reported again.
	for uid := range r.alerted {
		if _, ok := stillAlerting[uid]; !ok {
			delete(r.alerted, uid)
		}
	}
	r.m.sweepSeconds.Observe(time.Since(start).Seconds())
	return nextWake
}

// forceDelete returns true if it counted against the sweep cap.
func (r *Reaper) forceDelete(ctx context.Context, p *corev1.Pod, d policy.Decision) bool {
	attrs := []any{
		"pod", key(p), "node", p.Spec.NodeName, "nodeState", d.NodeState,
		"mode", string(d.Mode), "reason", d.Reason, "dryRun", r.cfg.DryRun,
	}
	if r.cfg.DryRun {
		r.log.Info("would force-delete pod", attrs...)
		r.m.forceDeleted.WithLabelValues(d.Reason, string(d.Mode), "true").Inc()
		return true
	}

	uid := p.UID
	zero := int64(0)
	err := r.client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		// Never delete a *new* pod that reused the name (StatefulSet, or a
		// controller recreating quickly) between our read and our write.
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	switch {
	case err == nil:
		r.log.Info("force-deleted pod", attrs...)
		r.m.forceDeleted.WithLabelValues(d.Reason, string(d.Mode), "false").Inc()
		r.recorder.Eventf(p, corev1.EventTypeWarning, "ShepherdForceDeleted",
			"Force-deleted pod stuck terminating on node %s (%s)", p.Spec.NodeName, d.Reason)
		return true
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		// Gone already, or replaced by a different UID: exactly what the
		// precondition is for.
		r.log.Info("pod already gone or replaced", append(attrs, "err", err.Error())...)
		return false
	default:
		r.log.Error("force delete failed", append(attrs, "err", err)...)
		r.m.errors.Inc()
		return false
	}
}

func (r *Reaper) breakerOpen(total, notReady int) bool {
	if r.cfg.BreakerFraction <= 0 || total < r.cfg.BreakerMinNodes || total == 0 {
		return false
	}
	return float64(notReady)/float64(total) > r.cfg.BreakerFraction
}

func isNotReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status != corev1.ConditionTrue
		}
	}
	return true
}

func key(p *corev1.Pod) string { return fmt.Sprintf("%s/%s", p.Namespace, p.Name) }

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
