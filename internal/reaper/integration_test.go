package reaper

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/policy"
	"github.com/magicorntech/shepherd/internal/testenv"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Stop()
	os.Exit(code)
}

// The UID precondition is what stops shepherd from killing a *new* pod that
// reused a name between its read and its delete (same name is routine for
// StatefulSets, and for any controller that recreates quickly). The fake
// clientset ignores preconditions entirely, so only a real API server can
// show that it works.
func TestUIDPreconditionProtectsAReplacementPodWithTheSameName(t *testing.T) {
	c := testenv.Client(t)
	ctx := context.Background()

	if _, err := c.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "uid-test"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	newPod := func() *corev1.Pod {
		p, err := c.CoreV1().Pods("uid-test").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "uid-test"},
			Spec:       corev1.PodSpec{NodeName: "gone", Containers: []corev1.Container{{Name: "c", Image: "example/c:1"}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	// 1. The pod shepherd has decided about: Terminating, stuck.
	old := newPod()
	if err := c.CoreV1().Pods("uid-test").Delete(ctx, "web-0", metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(1))}); err != nil {
		t.Fatal(err)
	}
	stale, err := c.CoreV1().Pods("uid-test").Get(ctx, "web-0", metav1.GetOptions{})
	if err != nil || stale.DeletionTimestamp == nil {
		t.Fatalf("premise: pod should be Terminating: %v %v", stale, err)
	}

	// 2. Before shepherd acts, someone else clears it and the controller
	//    recreates a pod with the same name: a different object (new UID).
	if err := c.CoreV1().Pods("uid-test").Delete(ctx, "web-0", metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))}); err != nil {
		t.Fatal(err)
	}
	fresh := newPod()
	if fresh.UID == old.UID {
		t.Fatal("premise: replacement should have a new UID")
	}

	// 3. Shepherd now acts on its stale view.
	r := New(Config{Policy: policy.Config{}}, c, nil, nil, record.NewFakeRecorder(10),
		slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(prometheus.NewRegistry()))
	counted := r.forceDelete(ctx, stale, policy.Decision{Action: policy.ForceDelete, Reason: policy.ReasonDeadNode, Mode: policy.ModeDeadNode, NodeState: "missing"})

	if counted {
		t.Error("forceDelete reported a deletion that must not have happened")
	}
	got, err := c.CoreV1().Pods("uid-test").Get(ctx, "web-0", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		t.Fatal("the replacement pod was deleted through a stale UID: precondition not enforced")
	}
	if err != nil {
		t.Fatal(err)
	}
	if got.UID != fresh.UID || got.DeletionTimestamp != nil {
		t.Errorf("replacement pod was touched: uid=%s deletionTimestamp=%v", got.UID, got.DeletionTimestamp)
	}

	// Control: with the right UID the same call does delete.
	if err := c.CoreV1().Pods("uid-test").Delete(ctx, "web-0", metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(1))}); err != nil {
		t.Fatal(err)
	}
	cur, _ := c.CoreV1().Pods("uid-test").Get(ctx, "web-0", metav1.GetOptions{})
	if !r.forceDelete(ctx, cur, policy.Decision{Action: policy.ForceDelete, Reason: policy.ReasonDeadNode, Mode: policy.ModeDeadNode}) {
		t.Fatal("control: matching UID should have been force-deleted")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.CoreV1().Pods("uid-test").Get(ctx, "web-0", metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("control: pod still present after a matching-UID force delete")
}

func ptr[T any](v T) *T { return &v }
