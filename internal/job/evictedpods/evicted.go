package evictedpods

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	"github.com/magicorntech/shepherd/internal/job"
)

// JobName is the identifier used for --exclude-jobs and in logs.
const JobName = "evicted-pods"

type Config struct {
	// TTL is how long an evicted pod is kept before deletion.
	TTL time.Duration
	// DryRun logs what would be deleted and deletes nothing.
	DryRun bool
	// MaxDeletesPerSweep bounds API load after a big eviction storm (there
	// can be tens of thousands of Failed pods). The rest are picked up on
	// the next sweep, a second later.
	MaxDeletesPerSweep int
}

type Job struct {
	cfg    Config
	client kubernetes.Interface
	pods   listersv1.PodLister
	log    *slog.Logger
	now    func() time.Time
	m      *Metrics
}

func New(cfg Config, client kubernetes.Interface, pods listersv1.PodLister, log *slog.Logger, m *Metrics) *Job {
	return &Job{cfg: cfg, client: client, pods: pods, log: log, now: time.Now, m: m}
}

func (j *Job) Name() string { return JobName }

// Triggers implements job.Job: only an evicted pod appearing or changing is
// interesting; the Runner sleeps until the oldest-waiting pod's TTL is up.
func (j *Job) Triggers() job.Triggers {
	return job.Triggers{
		Pod: func(_, cur *corev1.Pod) bool { return IsEvicted(cur) && cur.DeletionTimestamp == nil },
	}
}

var _ job.Job = (*Job)(nil)

// retryDelay is how soon to come back after hitting the per-sweep cap.
const retryDelay = time.Second

// Sweep implements job.Job.
func (j *Job) Sweep(ctx context.Context) time.Duration {
	now := j.now()
	all, err := j.pods.List(everything)
	if err != nil {
		j.log.Error("list pods", "err", err)
		return 0
	}

	var evicted []*corev1.Pod
	for _, p := range all {
		if IsEvicted(p) {
			evicted = append(evicted, p)
		}
	}
	// Oldest first, so the per-sweep cap can't starve the longest-waiting.
	sort.Slice(evicted, func(a, b int) bool { return EvictedAt(evicted[a]).Before(EvictedAt(evicted[b])) })

	var waiting, due, optedOut int
	var nextWake time.Duration
	deleted := 0
	for _, p := range evicted {
		d := Decide(p, now, j.cfg.TTL)
		switch d.Action {
		case Skip:
			if d.Reason == ReasonOptedOut {
				optedOut++
			}
		case Wait:
			waiting++
			if nextWake == 0 || d.RequeueAfter < nextWake {
				nextWake = d.RequeueAfter
			}
			j.log.Debug("evicted pod waiting for its TTL",
				"pod", key(p), "age", d.Age.Round(time.Second).String(), "remaining", d.RequeueAfter.Round(time.Second).String())
		case Delete:
			due++
			if deleted >= j.cfg.MaxDeletesPerSweep {
				if nextWake == 0 || retryDelay < nextWake {
					nextWake = retryDelay
				}
				continue
			}
			if j.delete(ctx, p, d) {
				deleted++
			}
		}
	}
	j.m.pods.Reset()
	j.m.pods.WithLabelValues("waiting").Set(float64(waiting))
	j.m.pods.WithLabelValues("due").Set(float64(due - deleted))
	j.m.pods.WithLabelValues("opted_out").Set(float64(optedOut))
	return nextWake
}

// delete returns true if it counted against the sweep cap.
func (j *Job) delete(ctx context.Context, p *corev1.Pod, d Decision) bool {
	attrs := []any{
		"pod", key(p), "node", p.Spec.NodeName, "age", d.Age.Round(time.Second).String(),
		// The pod is the only record of why it was evicted, and it is about
		// to disappear: keep the reason in the log.
		"evictionMessage", truncate(p.Status.Message, 300), "dryRun", j.cfg.DryRun,
	}
	if j.cfg.DryRun {
		j.log.Info("would delete evicted pod", attrs...)
		j.m.deleted.WithLabelValues("true").Inc()
		return true
	}
	uid := p.UID
	// A normal delete: the pod is already terminal, so the API server removes
	// it at once. The UID precondition keeps us from touching a pod that
	// reused the name since we looked.
	err := j.client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	switch {
	case err == nil:
		j.log.Info("deleted evicted pod", attrs...)
		j.m.deleted.WithLabelValues("false").Inc()
		return true
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		j.log.Info("evicted pod already gone or replaced", append(attrs, "err", err.Error())...)
		return false
	default:
		j.log.Error("delete evicted pod failed", append(attrs, "err", err)...)
		j.m.errors.Inc()
		return false
	}
}

func key(p *corev1.Pod) string { return fmt.Sprintf("%s/%s", p.Namespace, p.Name) }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
