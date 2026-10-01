package evictedpods

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

	"github.com/magicorntech/shepherd/internal/testenv"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Stop()
	os.Exit(code)
}

// Same guarantee as the stuck-pods job, proven against a real API server (the
// fake clientset ignores preconditions): a stale view of an evicted pod must
// not delete a different pod that has since taken its name.
func TestUIDPreconditionProtectsAReplacementPodWithTheSameName(t *testing.T) {
	c := testenv.Client(t)
	ctx := context.Background()
	if _, err := c.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "evicted-uid-test"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	create := func() *corev1.Pod {
		p, err := c.CoreV1().Pods("evicted-uid-test").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "evicted-uid-test"},
			Spec:       corev1.PodSpec{NodeName: "n", Containers: []corev1.Container{{Name: "c", Image: "example/c:1"}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	// The evicted pod shepherd has decided to delete.
	old := create()
	old.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}
	stale, err := c.CoreV1().Pods("evicted-uid-test").UpdateStatus(ctx, old, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Meanwhile it is removed elsewhere and a new pod takes the name.
	if err := c.CoreV1().Pods("evicted-uid-test").Delete(ctx, "web-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	fresh := create()
	if fresh.UID == stale.UID {
		t.Fatal("premise: the replacement should have a new UID")
	}

	j := New(Config{TTL: time.Hour}, c, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(prometheus.NewRegistry()))
	if j.delete(ctx, stale, Decision{Action: Delete, Reason: ReasonExpired, Age: 2 * time.Hour}) {
		t.Error("delete reported success for a pod that must not have been touched")
	}
	got, err := c.CoreV1().Pods("evicted-uid-test").Get(ctx, "web-0", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		t.Fatal("the replacement pod was deleted through a stale UID: precondition not enforced")
	}
	if err != nil || got.UID != fresh.UID {
		t.Fatalf("replacement pod was touched: %v %v", got, err)
	}
}
