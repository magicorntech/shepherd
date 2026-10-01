package policy

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func cfg() Config {
	return Config{
		DefaultMode:       ModeOff,
		DeadNodeBuffer:    30 * time.Second,
		HealthyNodeBuffer: 5 * time.Minute,
	}
}

// terminating returns a pod whose deletion deadline (grace included) is
// deadline, labelled with mode.
func terminating(mode Mode, deadline time.Time) *corev1.Pod {
	ts := metav1.NewTime(deadline)
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "p",
			Namespace:         "ns",
			DeletionTimestamp: &ts,
			Labels:            map[string]string{},
		},
		Spec: corev1.PodSpec{NodeName: "n1"},
	}
	if mode != "" {
		p.Labels[ModeLabel] = string(mode)
	}
	return p
}

func node(ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: ready},
		}},
	}
}

func TestDecide(t *testing.T) {
	past := t0.Add(-10 * time.Minute) // deadline long gone
	tests := []struct {
		name       string
		pod        func() *corev1.Pod
		node       *corev1.Node
		now        time.Time
		cfg        func(*Config)
		wantAction Action
		wantReason string
	}{
		{
			name:       "not terminating",
			pod:        func() *corev1.Pod { p := terminating(ModeAny, past); p.DeletionTimestamp = nil; return p },
			node:       node(corev1.ConditionTrue),
			wantAction: Skip, wantReason: ReasonNotTerminating,
		},
		{
			name:       "unscheduled",
			pod:        func() *corev1.Pod { p := terminating(ModeAny, past); p.Spec.NodeName = ""; return p },
			wantAction: Skip, wantReason: ReasonUnscheduled,
		},
		{
			name: "mirror pod",
			pod: func() *corev1.Pod {
				p := terminating(ModeAny, past)
				p.Annotations = map[string]string{"kubernetes.io/config.mirror": "x"}
				return p
			},
			wantAction: Skip, wantReason: ReasonMirrorPod,
		},
		{
			name:       "no label defaults to off",
			pod:        func() *corev1.Pod { return terminating("", past) },
			node:       node(corev1.ConditionFalse),
			wantAction: Skip, wantReason: ReasonModeOff,
		},
		{
			name:       "default mode can enable cluster-wide",
			pod:        func() *corev1.Pod { return terminating("", past) },
			node:       node(corev1.ConditionFalse),
			cfg:        func(c *Config) { c.DefaultMode = ModeDeadNode },
			wantAction: ForceDelete, wantReason: ReasonDeadNode,
		},
		{
			name:       "explicit off beats default",
			pod:        func() *corev1.Pod { return terminating(ModeOff, past) },
			node:       node(corev1.ConditionFalse),
			cfg:        func(c *Config) { c.DefaultMode = ModeAny },
			wantAction: Skip, wantReason: ReasonModeOff,
		},
		{
			name: "garbage label never acts",
			pod:  func() *corev1.Pod { return terminating("yes-please", past) },
			node: node(corev1.ConditionFalse),
			// a typo must fail safe
			wantAction: Skip, wantReason: ReasonModeInvalid,
		},
		{
			name:       "dead node, still inside grace+buffer",
			pod:        func() *corev1.Pod { return terminating(ModeDeadNode, t0.Add(-10*time.Second)) },
			node:       node(corev1.ConditionUnknown),
			wantAction: Wait, wantReason: ReasonWithinGrace,
		},
		{
			name:       "dead node, grace passed but buffer not",
			pod:        func() *corev1.Pod { return terminating(ModeDeadNode, t0.Add(-29*time.Second)) },
			node:       node(corev1.ConditionUnknown),
			wantAction: Wait, wantReason: ReasonWithinGrace,
		},
		{
			name:       "dead node, exactly at deadline",
			pod:        func() *corev1.Pod { return terminating(ModeDeadNode, t0.Add(-30*time.Second)) },
			node:       node(corev1.ConditionUnknown),
			wantAction: ForceDelete, wantReason: ReasonDeadNode,
		},
		{
			name:       "node NotReady",
			pod:        func() *corev1.Pod { return terminating(ModeDeadNode, past) },
			node:       node(corev1.ConditionFalse),
			wantAction: ForceDelete, wantReason: ReasonDeadNode,
		},
		{
			name:       "node object deleted",
			pod:        func() *corev1.Pod { return terminating(ModeDeadNode, past) },
			node:       nil,
			wantAction: ForceDelete, wantReason: ReasonNodeMissing,
		},
		{
			name: "node never reported Ready",
			pod:  func() *corev1.Pod { return terminating(ModeDeadNode, past) },
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
			// no Ready condition == kubelet never spoke == dead
			wantAction: ForceDelete, wantReason: ReasonDeadNode,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg()
			if tc.cfg != nil {
				tc.cfg(&c)
			}
			now := tc.now
			if now.IsZero() {
				now = t0
			}
			got := Decide(tc.pod(), tc.node, now, c)
			if got.Action != tc.wantAction || got.Reason != tc.wantReason {
				t.Fatalf("got %s/%s, want %s/%s", got.Action, got.Reason, tc.wantAction, tc.wantReason)
			}
		})
	}
}

func TestHealthyNode(t *testing.T) {
	ready := node(corev1.ConditionTrue)

	t.Run("dead-node mode on healthy node alerts after the longer buffer", func(t *testing.T) {
		got := Decide(terminating(ModeDeadNode, t0.Add(-10*time.Minute)), ready, t0, cfg())
		if got.Action != Alert || got.Reason != ReasonHealthyNode {
			t.Fatalf("got %s/%s", got.Action, got.Reason)
		}
	})

	t.Run("healthy node uses the longer buffer, not the dead-node one", func(t *testing.T) {
		// 2 min past deadline: > 30s dead buffer, < 5min healthy buffer.
		got := Decide(terminating(ModeAny, t0.Add(-2*time.Minute)), ready, t0, cfg())
		if got.Action != Wait {
			t.Fatalf("got %s/%s, want wait", got.Action, got.Reason)
		}
		if got.RequeueAfter != 3*time.Minute {
			t.Fatalf("RequeueAfter = %s, want 3m", got.RequeueAfter)
		}
	})

	t.Run("any mode force-deletes a stuck pod on a healthy node", func(t *testing.T) {
		got := Decide(terminating(ModeAny, t0.Add(-10*time.Minute)), ready, t0, cfg())
		if got.Action != ForceDelete || got.Reason != ReasonNodeStuckAny {
			t.Fatalf("got %s/%s", got.Action, got.Reason)
		}
	})
}

func TestExclusions(t *testing.T) {
	past := t0.Add(-10 * time.Minute)
	dead := node(corev1.ConditionFalse)

	finalizers := terminating(ModeAny, past)
	finalizers.Finalizers = []string{"example.com/cleanup"}

	sts := terminating(ModeAny, past)
	sts.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db"}}

	pvc := terminating(ModeAny, past)
	pvc.Spec.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
	}}}

	for name, tc := range map[string]struct {
		pod    *corev1.Pod
		reason string
		lift   func(*Config)
	}{
		"finalizers":  {finalizers, ReasonFinalizers, nil},
		"statefulset": {sts, ReasonStatefulSet, func(c *Config) { c.IncludeStatefulSet = true }},
		"pvc":         {pvc, ReasonPVC, func(c *Config) { c.IncludePVC = true }},
	} {
		t.Run(name+" alerts by default", func(t *testing.T) {
			got := Decide(tc.pod, dead, t0, cfg())
			if got.Action != Alert || got.Reason != tc.reason {
				t.Fatalf("got %s/%s", got.Action, got.Reason)
			}
		})
		if tc.lift != nil {
			t.Run(name+" can be opted in", func(t *testing.T) {
				c := cfg()
				tc.lift(&c)
				got := Decide(tc.pod, dead, t0, c)
				if got.Action != ForceDelete {
					t.Fatalf("got %s/%s", got.Action, got.Reason)
				}
			})
		}
	}

	t.Run("finalizers are never overridable", func(t *testing.T) {
		c := cfg()
		c.IncludeStatefulSet, c.IncludePVC = true, true
		if got := Decide(finalizers, dead, t0, c); got.Action != Alert {
			t.Fatalf("got %s/%s", got.Action, got.Reason)
		}
	})
}

func TestParseMode(t *testing.T) {
	for _, ok := range []string{"off", "dead-node", "any"} {
		if _, err := ParseMode(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	if _, err := ParseMode("all"); err == nil {
		t.Error("expected error for invalid mode")
	}
}
