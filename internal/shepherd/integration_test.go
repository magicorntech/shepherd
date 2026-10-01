package shepherd

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	"github.com/magicorntech/shepherd/internal/policy"
	"github.com/magicorntech/shepherd/internal/reaper"
	"github.com/magicorntech/shepherd/internal/testenv"
)

// These tests run the real thing (shepherd.Run, real informers, real
// reaper) against a real kube-apiserver. See internal/testenv for why that
// environment reproduces the production failure: scheduled pods that are
// deleted gracefully stay Terminating forever, because there is no kubelet.

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Stop()
	os.Exit(code)
}

// ---------- helpers ----------

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

var seq atomic.Int64

func uniq(prefix string) string { return fmt.Sprintf("%s-%d", prefix, seq.Add(1)) }

func ptr[T any](v T) *T { return &v }

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

func newNamespace(t *testing.T, c kubernetes.Interface) string {
	t.Helper()
	ns := uniq("ns")
	if _, err := c.CoreV1().Namespaces().Create(context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return ns
}

func newNode(t *testing.T, c kubernetes.Interface, ready bool) string {
	t.Helper()
	name := uniq("node")
	if _, err := c.CoreV1().Nodes().Create(context.Background(),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	setReady(t, c, name, ready)
	return name
}

func setReady(t *testing.T, c kubernetes.Interface, name string, ready bool) {
	t.Helper()
	st := corev1.ConditionFalse
	if ready {
		st = corev1.ConditionTrue
	}
	n, err := c.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: st, LastHeartbeatTime: metav1.Now()}}
	if _, err := c.CoreV1().Nodes().UpdateStatus(context.Background(), n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// podOn creates a pod already bound to node (so no scheduler is needed).
func podOn(t *testing.T, c kubernetes.Interface, ns, node string, mutate ...func(*corev1.Pod)) *corev1.Pod {
	t.Helper()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: uniq("pod"), Namespace: ns},
		Spec:       corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "c", Image: "example/c:1"}}},
	}
	for _, m := range mutate {
		m(p)
	}
	out, err := c.CoreV1().Pods(ns).Create(context.Background(), p, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// terminate deletes the pod gracefully (1s). Because it is bound to a node
// and nothing runs a kubelet, the API server keeps it, Terminating, forever.
func terminate(t *testing.T, c kubernetes.Interface, p *corev1.Pod) {
	t.Helper()
	if err := c.CoreV1().Pods(p.Namespace).Delete(context.Background(), p.Name,
		metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(1))}); err != nil {
		t.Fatal(err)
	}
	got, err := c.CoreV1().Pods(p.Namespace).Get(context.Background(), p.Name, metav1.GetOptions{})
	if err != nil || got.DeletionTimestamp == nil {
		t.Fatalf("test premise broken: pod should be Terminating, got %v / %v", got, err)
	}
}

func exists(c kubernetes.Interface, ns, name string) bool {
	_, err := c.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
	return !apierrors.IsNotFound(err)
}

func waitGone(t *testing.T, c kubernetes.Interface, p *corev1.Pod, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "pod "+p.Name+" to be deleted", func() bool { return !exists(c, p.Namespace, p.Name) })
}

func hasEvent(c kubernetes.Interface, ns, podName, reason string) bool {
	evs, err := c.CoreV1().Events(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return false
	}
	for _, e := range evs.Items {
		if e.InvolvedObject.Name == podName && e.Reason == reason {
			return true
		}
	}
	return false
}

type instance struct {
	cancel context.CancelFunc
	done   chan struct{}
	logs   *syncBuf
	reg    *prometheus.Registry
}

func (i *instance) stop() { i.cancel(); <-i.done }

// start runs shepherd.Run in the background, restricted to ns, with short
// buffers, and returns once its caches have synced.
func start(t *testing.T, cfg *rest.Config, ns string, tweak ...func(*Options)) *instance {
	t.Helper()
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	synced := make(chan struct{})
	opts := Options{
		Version:   "test",
		Namespace: ns,
		Reaper: reaper.Config{
			Policy: policy.Config{
				DefaultMode:       policy.ModeDeadNode,
				DeadNodeBuffer:    time.Second,
				HealthyNodeBuffer: 2 * time.Second,
			},
			// An hour: anything that happens sooner was driven by a watch
			// event or a deadline timer, not by the safety-net poll.
			Interval:           time.Hour,
			MaxDeletesPerSweep: 50,
		},
		Metrics:       reaper.NewMetrics(reg),
		LeaseName:     "shepherd-test",
		LeaseDuration: 4 * time.Second,
		RenewDeadline: 3 * time.Second,
		RetryPeriod:   500 * time.Millisecond,
		OnSynced:      func() { close(synced) },
	}
	for _, f := range tweak {
		f(&opts)
	}
	logs := &syncBuf{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, cancel := context.WithCancel(context.Background())
	inst := &instance{cancel: cancel, done: make(chan struct{}), logs: logs, reg: reg}
	go func() {
		defer close(inst.done)
		if err := Run(ctx, client, opts, log); err != nil {
			log.Error("run returned", "err", err.Error())
		}
	}()
	t.Cleanup(inst.stop)
	select {
	case <-synced:
	case <-time.After(30 * time.Second):
		t.Fatalf("shepherd did not sync its caches in 30s; logs:\n%s", logs.String())
	}
	return inst
}

// ---------- tests ----------

func TestStuckPodOnDeadNodeIsForceDeleted(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, false)
	start(t, testenv.Config(t), ns)

	p := podOn(t, c, ns, node)
	terminate(t, c, p)

	waitGone(t, c, p, 20*time.Second)
	waitFor(t, 5*time.Second, "a ShepherdForceDeleted Event", func() bool {
		return hasEvent(c, ns, p.Name, "ShepherdForceDeleted")
	})
}

func TestPodOnHealthyNodeIsReportedNotDeleted(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, true)
	start(t, testenv.Config(t), ns)

	p := podOn(t, c, ns, node)
	terminate(t, c, p)

	// dead-node mode on a Ready node: wait out grace + healthy buffer, then
	// alert (Event) but never delete.
	waitFor(t, 20*time.Second, "a ShepherdStuckTerminating Event", func() bool {
		return hasEvent(c, ns, p.Name, "ShepherdStuckTerminating")
	})
	if !exists(c, ns, p.Name) {
		t.Fatal("pod on a healthy node was force-deleted in dead-node mode")
	}
}

func TestPodWithFinalizerIsNeverForceDeleted(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, false)
	start(t, testenv.Config(t), ns)

	p := podOn(t, c, ns, node, func(p *corev1.Pod) { p.Finalizers = []string{"example.com/cleanup"} })
	terminate(t, c, p)

	waitFor(t, 20*time.Second, "a ShepherdStuckTerminating Event", func() bool {
		return hasEvent(c, ns, p.Name, "ShepherdStuckTerminating")
	})
	if !exists(c, ns, p.Name) {
		t.Fatal("pod with a finalizer was deleted")
	}
}

// The safety-net interval is an hour, and the pod is still inside its
// healthy-node buffer (an hour too) when the node dies. The only thing that
// can make shepherd act within seconds is the node watch event.
func TestNodeGoingNotReadyTriggersALiveSweep(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, true)
	start(t, testenv.Config(t), ns, func(o *Options) { o.Reaper.Policy.HealthyNodeBuffer = time.Hour })

	p := podOn(t, c, ns, node)
	terminate(t, c, p)

	time.Sleep(3 * time.Second) // well past grace + dead-node buffer
	if !exists(c, ns, p.Name) {
		t.Fatal("pod deleted while its node was still Ready")
	}

	setReady(t, c, node, false) // the node dies
	waitGone(t, c, p, 15*time.Second)
}

// Shepherd is already running, idle, with an hour-long safety net, when the
// pod starts terminating. Only the pod watch event can wake it.
func TestPodEnteringTerminatingTriggersALiveSweep(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, false)
	start(t, testenv.Config(t), ns)

	p := podOn(t, c, ns, node)
	time.Sleep(time.Second)
	terminate(t, c, p)

	waitGone(t, c, p, 15*time.Second)
}

func TestOnlyTheLeaderActsAndAStandbyTakesOver(t *testing.T) {
	c := testenv.Client(t)
	ns, node := newNamespace(t, c), newNode(t, c, false)
	leaderElect := func(id string) func(*Options) {
		return func(o *Options) { o.LeaderElect, o.LeaseNamespace, o.Identity = true, ns, id }
	}
	a := start(t, testenv.Config(t), ns, leaderElect("a"))
	b := start(t, testenv.Config(t), ns, leaderElect("b"))

	became := func(i *instance) bool { return strings.Contains(i.logs.String(), `"became leader"`) }
	waitFor(t, 15*time.Second, "one replica to become leader", func() bool { return became(a) || became(b) })
	time.Sleep(time.Second)
	if became(a) && became(b) {
		t.Fatal("both replicas became leader")
	}
	leader, standby := a, b
	if became(b) {
		leader, standby = b, a
	}

	p1 := podOn(t, c, ns, node)
	terminate(t, c, p1)
	waitGone(t, c, p1, 20*time.Second)
	if strings.Contains(standby.logs.String(), `"msg":"force-deleted pod"`) {
		t.Fatal("the standby acted while not leader")
	}

	leader.stop() // releases the Lease on the way out
	waitFor(t, 20*time.Second, "the standby to take over", func() bool { return became(standby) })

	p2 := podOn(t, c, ns, node)
	terminate(t, c, p2)
	waitGone(t, c, p2, 20*time.Second)
}

// Runs the whole flow as a user that has ONLY the rules from
// deploy/values.yaml. Proves the shipped RBAC is sufficient (informers sync,
// delete works, Events and the Lease can be written) and that it is not
// broader than necessary.
func TestWorksUnderExactlyTheShippedRBAC(t *testing.T) {
	admin := testenv.Client(t)
	ns, node := newNamespace(t, admin), newNode(t, admin, false)

	raw, err := os.ReadFile("../../deploy/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Global struct {
			Security struct {
				ServiceAccount struct {
					Rules []struct {
						APIGroups []string `json:"apiGroups"`
						Resources []string `json:"resources"`
						Verbs     []string `json:"verbs"`
					} `json:"rules"`
				} `json:"serviceAccount"`
			} `json:"security"`
		} `json:"global"`
	}
	if err := yaml.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	var rules []rbacv1.PolicyRule
	for _, r := range v.Global.Security.ServiceAccount.Rules {
		rules = append(rules, rbacv1.PolicyRule{APIGroups: r.APIGroups, Resources: r.Resources, Verbs: r.Verbs})
	}
	if len(rules) == 0 {
		t.Fatal("no rules found in deploy/values.yaml")
	}

	user := uniq("shepherd-rbac")
	ctx := context.Background()
	if _, err := admin.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: user}, Rules: rules}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: user},
		Subjects:   []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "User", Name: user}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	cfg := testenv.UserConfig(t, user)
	restricted, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	allowed := func(verb, group, resource string) bool {
		r, err := admin.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authv1.SubjectAccessReview{
			Spec: authv1.SubjectAccessReviewSpec{User: user, ResourceAttributes: &authv1.ResourceAttributes{
				Verb: verb, Group: group, Resource: resource, Namespace: ns}},
		}, metav1.CreateOptions{})
		return err == nil && r.Status.Allowed
	}
	waitFor(t, 10*time.Second, "RBAC to propagate", func() bool { return allowed("list", "", "pods") })

	// Not broader than needed.
	for _, c := range []struct{ verb, group, resource string }{
		{"delete", "", "nodes"}, {"get", "", "secrets"}, {"delete", "apps", "deployments"}, {"update", "", "pods"},
	} {
		if allowed(c.verb, c.group, c.resource) {
			t.Errorf("shipped RBAC unexpectedly allows %s %s/%s", c.verb, c.group, c.resource)
		}
	}

	inst := start(t, cfg, ns, func(o *Options) { o.LeaderElect, o.LeaseNamespace, o.Identity = true, ns, "rbac" })
	p := podOn(t, admin, ns, node)
	terminate(t, admin, p)

	waitGone(t, admin, p, 30*time.Second)
	waitFor(t, 5*time.Second, "an Event written under the restricted identity", func() bool {
		return hasEvent(admin, ns, p.Name, "ShepherdForceDeleted")
	})
	if _, err := restricted.CoordinationV1().Leases(ns).Get(ctx, "shepherd-test", metav1.GetOptions{}); err != nil {
		t.Errorf("the Lease was not created under the shipped RBAC: %v\nlogs:\n%s", err, inst.logs.String())
	}
}
