// Package evictedpods deletes pods the kubelet evicted (phase Failed, reason
// Evicted) once they have been around for a configurable time.
//
// Eviction leaves the pod object behind on purpose, as evidence of what
// happened. Nothing removes it afterwards except the controller-manager's pod
// GC, which only starts once the cluster holds terminated-pod-gc-threshold
// pods (12500 by default). After a node-pressure incident that is thousands of
// Failed pods clogging `kubectl get pods`, dashboards and list calls.
package evictedpods

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	// OptOutLabel on a pod keeps it from being cleaned up when set to "off"
	// (e.g. to keep one around while someone investigates it).
	OptOutLabel = "shepherd.magicorn.co/evicted-cleanup"
	OptOutValue = "off"

	evictedReason = "Evicted"
)

type Action int

const (
	// Skip: not an evicted pod, or not ours to touch.
	Skip Action = iota
	// Wait: evicted, but younger than the TTL.
	Wait
	// Delete: evicted and older than the TTL.
	Delete
)

func (a Action) String() string {
	switch a {
	case Skip:
		return "skip"
	case Wait:
		return "wait"
	case Delete:
		return "delete"
	}
	return "unknown"
}

// Reasons are stable strings (used as metric labels).
const (
	ReasonNotEvicted = "not-evicted"
	ReasonDeleting   = "already-deleting"
	ReasonOptedOut   = "opted-out"
	ReasonWithinTTL  = "within-ttl"
	ReasonExpired    = "expired"
)

type Decision struct {
	Action Action
	Reason string
	// Age is how long the pod has been evicted. Set for Wait and Delete.
	Age time.Duration
	// RequeueAfter is only meaningful for Wait: time until the TTL is up.
	RequeueAfter time.Duration
}

// IsEvicted reports whether the pod is one the kubelet evicted.
func IsEvicted(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed && pod.Status.Reason == evictedReason
}

// Decide evaluates one pod.
func Decide(pod *corev1.Pod, now time.Time, ttl time.Duration) Decision {
	if !IsEvicted(pod) {
		return Decision{Action: Skip, Reason: ReasonNotEvicted}
	}
	if pod.DeletionTimestamp != nil {
		return Decision{Action: Skip, Reason: ReasonDeleting}
	}
	if pod.Labels[OptOutLabel] == OptOutValue {
		return Decision{Action: Skip, Reason: ReasonOptedOut}
	}
	age := now.Sub(EvictedAt(pod))
	if age < ttl {
		return Decision{Action: Wait, Reason: ReasonWithinTTL, Age: age, RequeueAfter: ttl - age}
	}
	return Decision{Action: Delete, Reason: ReasonExpired, Age: age}
}

// EvictedAt is when the pod reached its terminal state. The kubelet does not
// stamp a single "evicted at" field, so use the latest status transition we
// can find (condition transitions, container termination), then startTime,
// then creationTimestamp as a last resort. A pod's status is frozen once it is
// Failed, so the latest of these is stable from then on.
func EvictedAt(pod *corev1.Pod) time.Time {
	var latest time.Time
	consider := func(t time.Time) {
		if t.After(latest) {
			latest = t
		}
	}
	for _, c := range pod.Status.Conditions {
		consider(c.LastTransitionTime.Time)
	}
	for _, statuses := range [][]corev1.ContainerStatus{
		pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses, pod.Status.EphemeralContainerStatuses,
	} {
		for _, cs := range statuses {
			if t := cs.State.Terminated; t != nil {
				consider(t.FinishedAt.Time)
			}
		}
	}
	if !latest.IsZero() {
		return latest
	}
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return pod.CreationTimestamp.Time
}
