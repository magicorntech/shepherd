package evictedpods

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type harness struct {
	j       *Job
	mu      sync.Mutex
	deleted []string
	opts    []metav1.DeleteOptions
	logs    bytes.Buffer
}

func newHarness(t *testing.T, cfg Config, pods ...*corev1.Pod) *harness {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	var objs []runtime.Object
	for _, p := range pods {
		_ = idx.Add(p)
		objs = append(objs, p)
	}
	h := &harness{}
	client := fake.NewSimpleClientset(objs...)
	client.PrependReactor("delete", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		d := a.(k8stesting.DeleteActionImpl)
		h.mu.Lock()
		h.deleted = append(h.deleted, d.Name)
		h.opts = append(h.opts, d.DeleteOptions)
		h.mu.Unlock()
		return false, nil, nil
	})
	if cfg.MaxDeletesPerSweep == 0 {
		cfg.MaxDeletesPerSweep = 1000
	}
	h.j = New(cfg, client, listersv1.NewPodLister(idx),
		slog.New(slog.NewJSONHandler(&h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		NewMetrics(prometheus.NewRegistry()))
	h.j.now = func() time.Time { return t0 }
	return h
}

func named(p *corev1.Pod, name string) *corev1.Pod {
	p.Name, p.UID = name, types.UID("uid-"+name)
	return p
}

func TestDeletesOnlyExpiredEvictedPods(t *testing.T) {
	failedOther := named(evicted(48*time.Hour), "failed-other")
	failedOther.Status.Reason = "NodeAffinity"
	running := named(evicted(48*time.Hour), "running")
	running.Status.Phase, running.Status.Reason = corev1.PodRunning, ""

	h := newHarness(t, Config{TTL: 24 * time.Hour},
		named(evicted(25*time.Hour), "old"), named(evicted(time.Hour), "young"), failedOther, running)
	h.j.Sweep(context.Background())

	if len(h.deleted) != 1 || h.deleted[0] != "old" {
		t.Fatalf("deleted %v, want only [old]", h.deleted)
	}
}

func TestDeleteIsGracefulNotForcedAndCarriesTheUID(t *testing.T) {
	h := newHarness(t, Config{TTL: time.Hour}, named(evicted(2*time.Hour), "old"))
	h.j.Sweep(context.Background())

	o := h.opts[0]
	if o.GracePeriodSeconds != nil {
		t.Errorf("grace period = %d; an evicted pod is already terminal, no need to force", *o.GracePeriodSeconds)
	}
	if o.Preconditions == nil || o.Preconditions.UID == nil || *o.Preconditions.UID != "uid-old" {
		t.Errorf("missing UID precondition: %+v", o.Preconditions)
	}
}

func TestSweepReportsTheSoonestTTLExpiry(t *testing.T) {
	// TTL 24h: one pod has 6h left, another 1h left.
	h := newHarness(t, Config{TTL: 24 * time.Hour},
		named(evicted(18*time.Hour), "a"), named(evicted(23*time.Hour), "b"))
	if got := h.j.Sweep(context.Background()); got != time.Hour {
		t.Fatalf("next wake = %s, want 1h", got)
	}
	if len(h.deleted) != 0 {
		t.Fatal("deleted before the TTL")
	}
}

func TestDryRunNeverDeletes(t *testing.T) {
	h := newHarness(t, Config{TTL: time.Hour, DryRun: true}, named(evicted(2*time.Hour), "old"))
	h.j.Sweep(context.Background())
	if len(h.deleted) != 0 {
		t.Fatalf("dry run issued deletes: %v", h.deleted)
	}
	if !strings.Contains(h.logs.String(), `"msg":"would delete evicted pod"`) {
		t.Fatalf("dry run did not say what it would do:\n%s", h.logs.String())
	}
}

func TestOptedOutPodIsKept(t *testing.T) {
	keep := named(evicted(48*time.Hour), "keep")
	keep.Labels = map[string]string{OptOutLabel: OptOutValue}
	h := newHarness(t, Config{TTL: time.Hour}, keep)
	h.j.Sweep(context.Background())
	if len(h.deleted) != 0 {
		t.Fatalf("deleted an opted-out pod: %v", h.deleted)
	}
}

func TestPerSweepCapDeletesOldestFirstAndComesBackSoon(t *testing.T) {
	h := newHarness(t, Config{TTL: time.Hour, MaxDeletesPerSweep: 2},
		named(evicted(2*time.Hour), "newest"), named(evicted(9*time.Hour), "oldest"), named(evicted(5*time.Hour), "middle"))
	wake := h.j.Sweep(context.Background())

	if len(h.deleted) != 2 || h.deleted[0] != "oldest" || h.deleted[1] != "middle" {
		t.Fatalf("deleted %v, want [oldest middle]", h.deleted)
	}
	if wake != retryDelay {
		t.Fatalf("wake = %s, want %s so the remainder is picked up promptly", wake, retryDelay)
	}
}

func TestTheEvictionMessageSurvivesInTheLog(t *testing.T) {
	p := named(evicted(2*time.Hour), "old")
	p.Status.Message = "The node was low on resource: ephemeral-storage."
	h := newHarness(t, Config{TTL: time.Hour}, p)
	h.j.Sweep(context.Background())
	if !strings.Contains(h.logs.String(), "ephemeral-storage") || !strings.Contains(h.logs.String(), `"msg":"deleted evicted pod"`) {
		t.Fatalf("the reason for the eviction was not logged before the pod vanished:\n%s", h.logs.String())
	}
}

func TestTriggersOnlyForLiveEvictedPods(t *testing.T) {
	trig := (&Job{}).Triggers().Pod
	if !trig(nil, evicted(0)) {
		t.Error("an evicted pod should wake the job")
	}
	running := evicted(0)
	running.Status.Phase, running.Status.Reason = corev1.PodRunning, ""
	if trig(nil, running) {
		t.Error("a running pod should not wake the job")
	}
	deleting := evicted(0)
	ts := metav1.NewTime(t0)
	deleting.DeletionTimestamp = &ts
	if trig(nil, deleting) {
		t.Error("a pod already being deleted should not wake the job")
	}
}
