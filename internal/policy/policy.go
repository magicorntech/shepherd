// Package policy decides, for one pod, whether shepherd should force-delete
// it. It is pure (no API calls, no clock of its own) so every rule is unit
// testable; internal/reaper owns the loop, the API client and the safety
// rails that span many pods (rate limit, circuit breaker).
package policy

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// ModeLabel is read from the pod (so it is set once in the workload's pod
// template, no Deployment/ReplicaSet lookup needed).
const ModeLabel = "shepherd.magicorn.co/force-delete"

type Mode string

const (
	// ModeOff: never touch this pod. Use the label to exempt a workload
	// when the cluster-wide default is dead-node or any.
	ModeOff Mode = "off"
	// ModeDeadNode: force-delete only when the pod's node is NotReady/Unknown
	// or its Node object is gone. Safe because a dead node runs nothing.
	ModeDeadNode Mode = "dead-node"
	// ModeAny: additionally force-delete a pod stuck terminating on a node
	// that still reports Ready. The API object goes away immediately but
	// the kubelet may still be tearing the container down, so for a
	// workload with an exclusive resource (a UDP port, a lock) old and new
	// can briefly coexist. Never a default; opt in per pod via the label.
	ModeAny Mode = "any"
)

type Action int

const (
	// Skip: not our business, nothing to report (not terminating, mode off).
	Skip Action = iota
	// Wait: terminating, still inside grace + buffer. Look again later.
	Wait
	// ForceDelete: past the deadline and eligible.
	ForceDelete
	// Alert: past the deadline but stuck for a reason force-delete should
	// not (or cannot) fix. Surfaced to humans, never acted on.
	Alert
)

func (a Action) String() string {
	switch a {
	case Skip:
		return "skip"
	case Wait:
		return "wait"
	case ForceDelete:
		return "force-delete"
	case Alert:
		return "alert"
	}
	return "unknown"
}

// Reasons are stable strings: they become metric labels and Event reasons.
const (
	ReasonNotTerminating = "not-terminating"
	ReasonUnscheduled    = "unscheduled"
	ReasonMirrorPod      = "mirror-pod"
	ReasonModeOff        = "mode-off"
	ReasonModeInvalid    = "mode-invalid"
	ReasonWithinGrace    = "within-grace"
	ReasonFinalizers     = "finalizers"
	ReasonStatefulSet    = "statefulset"
	ReasonPVC            = "pvc"
	ReasonHealthyNode    = "healthy-node"
	ReasonDeadNode       = "dead-node"
	ReasonNodeMissing    = "node-missing"
	ReasonNodeStuckAny   = "stuck-on-healthy-node"
)

type Config struct {
	DefaultMode Mode
	// DeadNodeBuffer is added on top of the pod's own deletion deadline
	// (deletionTimestamp already includes terminationGracePeriodSeconds).
	DeadNodeBuffer time.Duration
	// HealthyNodeBuffer is the (longer) buffer for a pod on a Ready node:
	// there the kubelet is alive and probably just slow, give it room.
	HealthyNodeBuffer time.Duration
	// IncludeStatefulSet / IncludePVC lift the default exclusions. A
	// force-deleted StatefulSet pod breaks the at-most-one guarantee, and a
	// PVC-backed pod's RWO volume stays attached to the dead node, so both
	// are excluded unless explicitly included.
	IncludeStatefulSet bool
	IncludePVC         bool
}

type Decision struct {
	Action Action
	Reason string
	Mode   Mode
	// NodeState is "ready", "not-ready" or "missing", for logs/metrics.
	NodeState string
	// RequeueAfter is only meaningful for Wait.
	RequeueAfter time.Duration
}

// Decide evaluates one pod. node is nil when the Node object does not exist.
func Decide(pod *corev1.Pod, node *corev1.Node, now time.Time, cfg Config) Decision {
	if pod.DeletionTimestamp == nil {
		return Decision{Action: Skip, Reason: ReasonNotTerminating}
	}
	if pod.Spec.NodeName == "" {
		return Decision{Action: Skip, Reason: ReasonUnscheduled}
	}
	if _, mirror := pod.Annotations["kubernetes.io/config.mirror"]; mirror {
		return Decision{Action: Skip, Reason: ReasonMirrorPod}
	}

	mode, ok := resolveMode(pod, cfg.DefaultMode)
	if !ok {
		return Decision{Action: Skip, Reason: ReasonModeInvalid}
	}
	if mode == ModeOff {
		return Decision{Action: Skip, Reason: ReasonModeOff, Mode: mode}
	}

	nodeState, nodeDead := classifyNode(node)
	d := Decision{Mode: mode, NodeState: nodeState}

	buffer := cfg.HealthyNodeBuffer
	if nodeDead {
		buffer = cfg.DeadNodeBuffer
	}
	deadline := pod.DeletionTimestamp.Time.Add(buffer)
	if now.Before(deadline) {
		d.Action, d.Reason, d.RequeueAfter = Wait, ReasonWithinGrace, deadline.Sub(now)
		return d
	}

	// Past the deadline from here on. First, reasons force-delete is the
	// wrong tool: report them instead of acting.
	if len(pod.Finalizers) > 0 {
		// A force delete only sets grace to 0; the object stays until its
		// finalizers are removed, so this would be a no-op that looks like
		// a fix. The finalizer's owner has to act.
		d.Action, d.Reason = Alert, ReasonFinalizers
		return d
	}
	if !cfg.IncludeStatefulSet && ownedByStatefulSet(pod) {
		d.Action, d.Reason = Alert, ReasonStatefulSet
		return d
	}
	if !cfg.IncludePVC && usesPVC(pod) {
		d.Action, d.Reason = Alert, ReasonPVC
		return d
	}

	if !nodeDead && mode == ModeDeadNode {
		d.Action, d.Reason = Alert, ReasonHealthyNode
		return d
	}

	d.Action = ForceDelete
	switch {
	case node == nil:
		d.Reason = ReasonNodeMissing
	case nodeDead:
		d.Reason = ReasonDeadNode
	default:
		d.Reason = ReasonNodeStuckAny
	}
	return d
}

func resolveMode(pod *corev1.Pod, def Mode) (Mode, bool) {
	raw, has := pod.Labels[ModeLabel]
	if !has {
		return def, true
	}
	switch m := Mode(raw); m {
	case ModeOff, ModeDeadNode, ModeAny:
		return m, true
	}
	return ModeOff, false
}

// classifyNode returns a label for logs and whether the node should be
// treated as dead (kubelet not confirming anything).
func classifyNode(node *corev1.Node) (string, bool) {
	if node == nil {
		return "missing", true
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			if c.Status == corev1.ConditionTrue {
				return "ready", false
			}
			return "not-ready", true
		}
	}
	// No Ready condition at all: the kubelet has never reported. Dead.
	return "not-ready", true
}

func ownedByStatefulSet(pod *corev1.Pod) bool {
	for _, o := range pod.OwnerReferences {
		if o.Kind == "StatefulSet" {
			return true
		}
	}
	return false
}

func usesPVC(pod *corev1.Pod) bool {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil || v.Ephemeral != nil {
			return true
		}
	}
	return false
}

// ParseMode validates a flag value.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case ModeOff, ModeDeadNode, ModeAny:
		return m, nil
	}
	return "", fmt.Errorf("invalid mode %q (want off, dead-node or any)", s)
}
