package evictedpods

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

// evicted returns an Evicted pod whose status settled `age` before t0.
func evicted(age time.Duration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", CreationTimestamp: metav1.NewTime(t0.Add(-100 * 24 * time.Hour))},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: memory.",
			Conditions: []corev1.PodCondition{{Type: corev1.DisruptionTarget, LastTransitionTime: metav1.NewTime(t0.Add(-age))}},
		},
	}
}

func TestDecide(t *testing.T) {
	ttl := 24 * time.Hour
	for _, tc := range []struct {
		name       string
		pod        func() *corev1.Pod
		wantAction Action
		wantReason string
	}{
		{"older than the TTL is deleted", func() *corev1.Pod { return evicted(25 * time.Hour) }, Delete, ReasonExpired},
		{"exactly at the TTL is deleted", func() *corev1.Pod { return evicted(24 * time.Hour) }, Delete, ReasonExpired},
		{"younger than the TTL waits", func() *corev1.Pod { return evicted(23 * time.Hour) }, Wait, ReasonWithinTTL},
		{"just evicted waits", func() *corev1.Pod { return evicted(0) }, Wait, ReasonWithinTTL},
		{"Failed for another reason is not ours", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			p.Status.Reason = "NodeAffinity"
			return p
		}, Skip, ReasonNotEvicted},
		{"Evicted reason but not Failed is not ours", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			p.Status.Phase = corev1.PodRunning
			return p
		}, Skip, ReasonNotEvicted},
		{"a Succeeded pod is not ours", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			p.Status.Phase, p.Status.Reason = corev1.PodSucceeded, ""
			return p
		}, Skip, ReasonNotEvicted},
		{"already being deleted is left alone", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			ts := metav1.NewTime(t0)
			p.DeletionTimestamp = &ts
			return p
		}, Skip, ReasonDeleting},
		{"opted out via label", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			p.Labels = map[string]string{OptOutLabel: OptOutValue}
			return p
		}, Skip, ReasonOptedOut},
		{"the label with any other value does not opt out", func() *corev1.Pod {
			p := evicted(48 * time.Hour)
			p.Labels = map[string]string{OptOutLabel: "on"}
			return p
		}, Delete, ReasonExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.pod(), t0, ttl)
			if got.Action != tc.wantAction || got.Reason != tc.wantReason {
				t.Fatalf("got %s/%s, want %s/%s", got.Action, got.Reason, tc.wantAction, tc.wantReason)
			}
		})
	}
}

func TestWaitReportsExactlyHowLongIsLeft(t *testing.T) {
	got := Decide(evicted(23*time.Hour+30*time.Minute), t0, 24*time.Hour)
	if got.Action != Wait || got.RequeueAfter != 30*time.Minute || got.Age != 23*time.Hour+30*time.Minute {
		t.Fatalf("got %+v", got)
	}
}

func TestEvictedAtUsesTheLatestSignalAndFallsBack(t *testing.T) {
	older, newer := t0.Add(-5*time.Hour), t0.Add(-2*time.Hour)

	t.Run("latest of conditions and container termination", func(t *testing.T) {
		p := evicted(5 * time.Hour)
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(newer)}}}}
		if got := EvictedAt(p); !got.Equal(newer) {
			t.Fatalf("got %v, want %v", got, newer)
		}
	})
	t.Run("no conditions or containers: startTime", func(t *testing.T) {
		p := evicted(0)
		p.Status.Conditions = nil
		st := metav1.NewTime(older)
		p.Status.StartTime = &st
		if got := EvictedAt(p); !got.Equal(older) {
			t.Fatalf("got %v, want %v", got, older)
		}
	})
	t.Run("nothing at all: creationTimestamp", func(t *testing.T) {
		p := evicted(0)
		p.Status.Conditions = nil
		if got := EvictedAt(p); !got.Equal(p.CreationTimestamp.Time) {
			t.Fatalf("got %v, want creationTimestamp", got)
		}
	})
}
