package stuckpods

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
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/job"
	"github.com/magicorntech/shepherd/internal/job/stuckpods/policy"
)

var now = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func mkNode(name string, ready bool) *corev1.Node {
	st := corev1.ConditionTrue
	if !ready {
		st = corev1.ConditionFalse
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: st}}},
	}
}

// stuckPod is terminating on node, 10 minutes past its deadline.
func stuckPod(name, node string, age time.Duration) *corev1.Pod {
	ts := metav1.NewTime(now.Add(-age))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", UID: types.UID("uid-" + name),
			DeletionTimestamp: &ts,
			Labels:            map[string]string{policy.ModeLabel: "dead-node"},
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

type harness struct {
	r       *Reaper
	client  *fake.Clientset
	mu      sync.Mutex
	logs    bytes.Buffer // JSON log lines, debug level
	podIdx  cache.Indexer
	rec     *record.FakeRecorder
	clock   time.Time
	deletes []metav1.DeleteOptions
	names   []string
}

func (h *harness) deletesSnapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.names...)
}

func newHarness(t *testing.T, cfg Config, nodes []*corev1.Node, pods []*corev1.Pod) *harness {
	t.Helper()
	var objs []runtime.Object
	podIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	nodeIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, n := range nodes {
		_ = nodeIdx.Add(n)
	}
	for _, p := range pods {
		_ = podIdx.Add(p)
		objs = append(objs, p)
	}
	h := &harness{client: fake.NewSimpleClientset(objs...), podIdx: podIdx, clock: now}
	h.client.PrependReactor("delete", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		d := a.(k8stesting.DeleteActionImpl)
		h.mu.Lock()
		h.deletes = append(h.deletes, d.DeleteOptions)
		h.names = append(h.names, d.Name)
		h.mu.Unlock()
		return false, nil, nil
	})
	cfg.Policy.DeadNodeBuffer = 30 * time.Second
	cfg.Policy.HealthyNodeBuffer = 5 * time.Minute
	if cfg.MaxDeletesPerSweep == 0 {
		cfg.MaxDeletesPerSweep = 50
	}
	h.rec = record.NewFakeRecorder(100)
	h.r = New(cfg, h.client, listersv1.NewPodLister(podIdx), listersv1.NewNodeLister(nodeIdx),
		h.rec, slog.New(slog.NewJSONHandler(&h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		NewMetrics(prometheus.NewRegistry()))
	h.r.now = func() time.Time { return h.clock }
	return h
}

func TestDeadNodePodIsForceDeletedWithUIDPrecondition(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stuckPod("a", "dead", 10*time.Minute)})
	h.r.Sweep(context.Background())

	if len(h.deletes) != 1 {
		t.Fatalf("deletes = %d, want 1", len(h.deletes))
	}
	d := h.deletes[0]
	if d.GracePeriodSeconds == nil || *d.GracePeriodSeconds != 0 {
		t.Errorf("grace period = %v, want 0", d.GracePeriodSeconds)
	}
	if d.Preconditions == nil || d.Preconditions.UID == nil || *d.Preconditions.UID != "uid-a" {
		t.Errorf("missing UID precondition: %+v", d.Preconditions)
	}
}

func TestDryRunNeverDeletes(t *testing.T) {
	h := newHarness(t, Config{DryRun: true}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stuckPod("a", "dead", 10*time.Minute)})
	h.r.Sweep(context.Background())
	if len(h.deletes) != 0 {
		t.Fatalf("dry run issued %d deletes", len(h.deletes))
	}
}

func TestHealthyNodeInDeadNodeModeIsNotTouched(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("ok", true)},
		[]*corev1.Pod{stuckPod("a", "ok", 30*time.Minute)})
	h.r.Sweep(context.Background())
	if len(h.deletes) != 0 {
		t.Fatalf("deleted a pod on a healthy node in dead-node mode")
	}
}

func TestPerSweepCapDefersOldestFirst(t *testing.T) {
	h := newHarness(t, Config{MaxDeletesPerSweep: 2}, []*corev1.Node{mkNode("dead", false)}, []*corev1.Pod{
		stuckPod("newest", "dead", 5*time.Minute),
		stuckPod("oldest", "dead", 20*time.Minute),
		stuckPod("middle", "dead", 10*time.Minute),
	})
	h.r.Sweep(context.Background())
	if len(h.names) != 2 || h.names[0] != "oldest" || h.names[1] != "middle" {
		t.Fatalf("deleted %v, want [oldest middle]", h.names)
	}
}

func TestCircuitBreakerSuspendsDeadNodeDeletes(t *testing.T) {
	var nodes []*corev1.Node
	for _, n := range []string{"d1", "d2", "d3"} {
		nodes = append(nodes, mkNode(n, false))
	}
	nodes = append(nodes, mkNode("ok1", true), mkNode("ok2", true))
	cfg := Config{BreakerFraction: 0.3, BreakerMinNodes: 5} // 3/5 = 60% NotReady
	h := newHarness(t, cfg, nodes, []*corev1.Pod{stuckPod("a", "d1", 10*time.Minute)})
	h.r.Sweep(context.Background())
	if len(h.deletes) != 0 {
		t.Fatal("breaker open but a dead-node pod was force-deleted")
	}
}

func TestBreakerIgnoredInTinyClusters(t *testing.T) {
	// 1 of 2 nodes down is 50%, but below BreakerMinNodes: a 2-node cluster
	// losing a node is the normal case this tool exists for.
	cfg := Config{BreakerFraction: 0.3, BreakerMinNodes: 5}
	h := newHarness(t, cfg, []*corev1.Node{mkNode("dead", false), mkNode("ok", true)},
		[]*corev1.Pod{stuckPod("a", "dead", 10*time.Minute)})
	h.r.Sweep(context.Background())
	if len(h.deletes) != 1 {
		t.Fatalf("deletes = %d, want 1", len(h.deletes))
	}
}

func TestMissingNodeObjectCountsAsDead(t *testing.T) {
	h := newHarness(t, Config{}, nil, []*corev1.Pod{stuckPod("a", "gone", 10*time.Minute)})
	h.r.Sweep(context.Background())
	if len(h.deletes) != 1 {
		t.Fatalf("deletes = %d, want 1", len(h.deletes))
	}
}

func TestSweepReturnsTimeUntilEarliestDeadline(t *testing.T) {
	// Dead node: deadline = deletionTimestamp + 30s. Pod a is 10s in (20s
	// left), pod b is 25s in (5s left): the sweep must say 5s.
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)}, []*corev1.Pod{
		stuckPod("a", "dead", 10*time.Second),
		stuckPod("b", "dead", 25*time.Second),
	})
	if got := h.r.Sweep(context.Background()); got != 5*time.Second {
		t.Fatalf("next wake = %s, want 5s", got)
	}
	if len(h.deletes) != 0 {
		t.Fatal("deleted before the deadline")
	}
}

func TestRunnerSweepsAtStartupWithoutWaitingForInterval(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stuckPod("a", "dead", 10*time.Minute)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { job.NewRunner(h.r, time.Hour).Run(ctx); close(done) }()

	// Interval is 1h, so a delete within 2s proves Run sweeps immediately
	deadline := time.Now().Add(2 * time.Second)
	for len(h.deletesSnapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(h.deletesSnapshot()) != 1 {
		t.Fatal("startup sweep did not run")
	}
	cancel()
	<-done
}

// --- log hygiene ---

func (h *harness) logCount(msg string) int {
	return strings.Count(h.logs.String(), `"msg":"`+msg+`"`)
}

func stsPod(name, node string, age time.Duration) *corev1.Pod {
	p := stuckPod(name, node, age)
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db"}}
	return p
}

func TestAlertIsLoggedOnceNotEverySweep(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stsPod("db-0", "dead", 10*time.Minute)})
	for i := 0; i < 5; i++ {
		h.r.Sweep(context.Background())
	}
	if n := h.logCount("pod stuck terminating, not force-deleting"); n != 1 {
		t.Fatalf("alert logged %d times over 5 sweeps, want 1", n)
	}
	if n := len(h.rec.Events); n != 1 {
		t.Fatalf("%d Events emitted over 5 sweeps, want 1", n)
	}
}

func TestAlertIsRepeatedAsAReminderAfterAnHour(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stsPod("db-0", "dead", 10*time.Minute)})
	h.r.Sweep(context.Background())
	h.clock = h.clock.Add(alertRepeat - time.Minute)
	h.r.Sweep(context.Background())
	if n := h.logCount("pod stuck terminating, not force-deleting"); n != 1 {
		t.Fatalf("reminder came early: %d logs", n)
	}
	h.clock = h.clock.Add(2 * time.Minute)
	h.r.Sweep(context.Background())
	if n := h.logCount("pod stuck terminating, not force-deleting"); n != 2 {
		t.Fatalf("reminder missing after an hour: %d logs", n)
	}
}

func TestAlertIsLoggedAgainWhenTheReasonChanges(t *testing.T) {
	p := stsPod("db-0", "dead", 10*time.Minute)
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)}, []*corev1.Pod{p})
	h.r.Sweep(context.Background())
	// Same pod, but now it is stuck for a different reason (finalizer).
	changed := p.DeepCopy()
	changed.OwnerReferences = nil
	changed.Finalizers = []string{"example.com/cleanup"}
	_ = h.podIdx.Update(changed)
	h.r.Sweep(context.Background())
	if n := h.logCount("pod stuck terminating, not force-deleting"); n != 2 {
		t.Fatalf("alert logged %d times, want 2 (statefulset, then finalizers)", n)
	}
}

func TestAlertStateIsForgottenWhenThePodGoes(t *testing.T) {
	p := stsPod("db-0", "dead", 10*time.Minute)
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)}, []*corev1.Pod{p})
	h.r.Sweep(context.Background())
	if len(h.r.alerted) != 1 {
		t.Fatalf("alerted = %d, want 1", len(h.r.alerted))
	}
	_ = h.podIdx.Delete(p)
	h.r.Sweep(context.Background())
	if len(h.r.alerted) != 0 {
		t.Fatalf("alerted map leaked %d entries", len(h.r.alerted))
	}
}

func TestDurationsAreHumanReadableNotNanoseconds(t *testing.T) {
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stsPod("db-0", "dead", 10*time.Minute)})
	h.r.Sweep(context.Background())
	if !strings.Contains(h.logs.String(), `"terminatingFor":"10m0s"`) {
		t.Fatalf("terminatingFor is not a duration string:\n%s", h.logs.String())
	}
}

func TestWaitingPodsAreLoggedAtDebugOnly(t *testing.T) {
	// Dead node, 10s into its 30s buffer: 20s left.
	h := newHarness(t, Config{}, []*corev1.Node{mkNode("dead", false)},
		[]*corev1.Pod{stuckPod("a", "dead", 10*time.Second)})
	h.r.Sweep(context.Background())
	if !strings.Contains(h.logs.String(), `"msg":"pod terminating, waiting for its deadline"`) ||
		!strings.Contains(h.logs.String(), `"remaining":"20s"`) {
		t.Fatalf("no debug line for the waiting pod:\n%s", h.logs.String())
	}

	// The same sweep at the default (info) level prints nothing for it.
	var info bytes.Buffer
	h.r.log = slog.New(slog.NewJSONHandler(&info, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h.r.Sweep(context.Background())
	if info.Len() != 0 {
		t.Fatalf("info level logged for a merely-waiting pod:\n%s", info.String())
	}
}

func TestBreakerIsLoggedOnTransitionsOnly(t *testing.T) {
	nodes := []*corev1.Node{mkNode("d1", false), mkNode("d2", false), mkNode("d3", false), mkNode("ok1", true), mkNode("ok2", true)}
	cfg := Config{BreakerFraction: 0.3, BreakerMinNodes: 5}
	h := newHarness(t, cfg, nodes, nil)
	for i := 0; i < 4; i++ {
		h.r.Sweep(context.Background())
	}
	if n := h.logCount("circuit breaker open: too many NotReady nodes, not force-deleting pods on dead nodes"); n != 1 {
		t.Fatalf("breaker-open logged %d times over 4 sweeps, want 1", n)
	}
}
