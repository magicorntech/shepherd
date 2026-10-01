// Package shepherd wires the pieces together: informers, the reaper, the
// live event triggers, and leader election. It lives outside cmd/ so the
// whole thing can be run against a real API server in tests, exactly the way
// main runs it.
package shepherd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/reaper"
)

type Options struct {
	Version string
	// Namespace restricts the pod informer; "" = all namespaces. Nodes are
	// cluster-scoped and always watched in full.
	Namespace string
	Reaper    reaper.Config
	Metrics   *reaper.Metrics

	LeaderElect    bool
	LeaseName      string
	LeaseNamespace string
	// Identity must be unique per replica (main uses the hostname).
	Identity string
	// Zero values mean the production defaults (30s / 20s / 5s); tests
	// shorten them so a failover takes seconds, not a minute.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	// OnSynced, if set, is called once the informer caches have synced
	// (main uses it for /readyz).
	OnSynced func()
}

// Run blocks until ctx is cancelled (or startup fails).
func Run(ctx context.Context, client kubernetes.Interface, opts Options, log *slog.Logger) error {
	// Pod informers only need to see pods being deleted, but the API can't
	// field-select on deletionTimestamp, so cache them all and filter in
	// the sweep.
	podFactory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(opts.Namespace))
	podInformer := podFactory.Core().V1().Pods()
	nodeFactory := informers.NewSharedInformerFactory(client, 0)
	nodeInformer := nodeFactory.Core().V1().Nodes()

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "shepherd"})

	r := reaper.New(opts.Reaper, client, podInformer.Lister(), nodeInformer.Lister(), recorder, log, opts.Metrics)

	// Live triggers. A pod entering Terminating, or a node flipping its Ready
	// state / disappearing, can change a decision immediately; the reaper
	// also sleeps exactly until the next pod's deadline, so nothing waits on
	// a poll tick.
	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) {
			if p, ok := o.(*corev1.Pod); ok && p.DeletionTimestamp != nil {
				r.Kick()
			}
		},
		UpdateFunc: func(_, o any) {
			if p, ok := o.(*corev1.Pod); ok && p.DeletionTimestamp != nil {
				r.Kick()
			}
		},
	}); err != nil {
		return fmt.Errorf("pod handler: %w", err)
	}
	if _, err := nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(o, n any) {
			on, ok1 := o.(*corev1.Node)
			nn, ok2 := n.(*corev1.Node)
			if ok1 && ok2 && nodeReady(on) != nodeReady(nn) {
				r.Kick()
			}
		},
		DeleteFunc: func(any) { r.Kick() },
	}); err != nil {
		return fmt.Errorf("node handler: %w", err)
	}

	// Informers run on every replica (so a standby is warm); only the
	// leader sweeps.
	podFactory.Start(ctx.Done())
	nodeFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced, nodeInformer.Informer().HasSynced) {
		return errors.New("cancelled before informer caches synced")
	}
	if opts.OnSynced != nil {
		opts.OnSynced()
	}

	log.Info("shepherd started", "version", opts.Version, "dryRun", opts.Reaper.DryRun,
		"defaultMode", string(opts.Reaper.Policy.DefaultMode), "namespace", opts.Namespace, "leaderElect", opts.LeaderElect)
	if opts.Reaper.DryRun {
		log.Warn("DRY RUN: nothing will be deleted. Drop --dry-run to act.")
	}

	if !opts.LeaderElect {
		r.Run(ctx)
		return nil
	}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Name: opts.LeaseName, Namespace: opts.LeaseNamespace},
			Client:     client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: opts.Identity},
		},
		ReleaseOnCancel: true,
		LeaseDuration:   orDefault(opts.LeaseDuration, 30*time.Second),
		RenewDeadline:   orDefault(opts.RenewDeadline, 20*time.Second),
		RetryPeriod:     orDefault(opts.RetryPeriod, 5*time.Second),
		Name:            opts.LeaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) { log.Info("became leader", "id", opts.Identity); r.Run(ctx) },
			OnStoppedLeading: func() { log.Info("stopped leading", "id", opts.Identity) },
		},
	})
	if err != nil {
		return fmt.Errorf("leader election: %w", err)
	}
	le.Run(ctx)
	return nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
