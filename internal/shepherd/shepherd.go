// Package shepherd wires the pieces together: the shared informers, the
// enabled jobs and their live event triggers, and leader election. It lives
// outside cmd/ so the whole thing can be run against a real API server in
// tests, exactly the way main runs it.
//
// To add a job: implement job.Job in its own package under internal/job, add
// its Config to Options and one case to buildJob below.
package shepherd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/job"
	"github.com/magicorntech/shepherd/internal/job/evictedpods"
	"github.com/magicorntech/shepherd/internal/job/stuckpods"
)

// AllJobs lists every job, in the order they are started. All are enabled
// unless excluded.
var AllJobs = []string{stuckpods.JobName, evictedpods.JobName}

// ValidateJobNames rejects names that are not a known job, so a typo in
// --exclude-jobs fails at startup instead of silently excluding nothing.
func ValidateJobNames(names []string) error {
	for _, n := range names {
		if !slices.Contains(AllJobs, n) {
			return fmt.Errorf("unknown job %q (known: %s)", n, strings.Join(AllJobs, ", "))
		}
	}
	return nil
}

type Options struct {
	Version string
	// Namespace restricts the pod informer; "" = all namespaces. Nodes are
	// cluster-scoped and always watched in full.
	Namespace string

	// ExcludeJobs names jobs to leave out. Everything else runs.
	ExcludeJobs []string
	StuckPods   stuckpods.Config
	EvictedPods evictedpods.Config
	// DryRun applies to every job (it overrides their own DryRun field).
	DryRun bool
	// Interval is every job's safety-net sweep interval.
	Interval time.Duration
	// Registry receives the metrics of the enabled jobs.
	Registry prometheus.Registerer

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
	if err := ValidateJobNames(opts.ExcludeJobs); err != nil {
		return err
	}
	opts.StuckPods.DryRun, opts.EvictedPods.DryRun = opts.DryRun, opts.DryRun

	// Pod informers only need to see a few pods, but the API can't
	// field-select on deletionTimestamp or phase reason, so cache them all
	// and filter in each job.
	podFactory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(opts.Namespace))
	podInformer := podFactory.Core().V1().Pods()
	nodeFactory := informers.NewSharedInformerFactory(client, 0)
	nodeInformer := nodeFactory.Core().V1().Nodes()

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "shepherd"})

	var runners []*job.Runner
	var enabled []string
	for _, name := range AllJobs {
		if slices.Contains(opts.ExcludeJobs, name) {
			continue
		}
		j := buildJob(name, opts, client, podInformer, nodeInformer, recorder, log.With("job", name))
		runners = append(runners, job.NewRunner(j, opts.Interval))
		enabled = append(enabled, name)
	}
	if len(runners) == 0 {
		return errors.New("every job is excluded; nothing to do")
	}

	// Live triggers: each job says which events should wake it.
	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) {
			if p, ok := o.(*corev1.Pod); ok {
				for _, r := range runners {
					if t := r.Job.Triggers().Pod; t != nil && t(nil, p) {
						r.Kick()
					}
				}
			}
		},
		UpdateFunc: func(o, n any) {
			old, ok1 := o.(*corev1.Pod)
			cur, ok2 := n.(*corev1.Pod)
			if ok1 && ok2 {
				for _, r := range runners {
					if t := r.Job.Triggers().Pod; t != nil && t(old, cur) {
						r.Kick()
					}
				}
			}
		},
	}); err != nil {
		return fmt.Errorf("pod handler: %w", err)
	}
	if _, err := nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(o, n any) {
			old, ok1 := o.(*corev1.Node)
			cur, ok2 := n.(*corev1.Node)
			if ok1 && ok2 {
				for _, r := range runners {
					if t := r.Job.Triggers().Node; t != nil && t(old, cur) {
						r.Kick()
					}
				}
			}
		},
		DeleteFunc: func(any) {
			for _, r := range runners {
				if r.Job.Triggers().NodeDeleted {
					r.Kick()
				}
			}
		},
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

	log.Info("shepherd started", "version", opts.Version, "jobs", enabled, "excludedJobs", opts.ExcludeJobs,
		"dryRun", opts.DryRun, "namespace", opts.Namespace, "leaderElect", opts.LeaderElect)
	if opts.DryRun {
		log.Warn("DRY RUN: nothing will be deleted. Drop --dry-run to act.")
	}

	runAll := func(ctx context.Context) {
		var wg sync.WaitGroup
		for _, r := range runners {
			wg.Add(1)
			go func() { defer wg.Done(); r.Run(ctx) }()
		}
		wg.Wait()
	}

	if !opts.LeaderElect {
		runAll(ctx)
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
			OnStartedLeading: func(ctx context.Context) { log.Info("became leader", "id", opts.Identity); runAll(ctx) },
			OnStoppedLeading: func() { log.Info("stopped leading", "id", opts.Identity) },
		},
	})
	if err != nil {
		return fmt.Errorf("leader election: %w", err)
	}
	le.Run(ctx)
	return nil
}

func buildJob(name string, opts Options, client kubernetes.Interface,
	pods corev1informers.PodInformer, nodes corev1informers.NodeInformer, recorder record.EventRecorder, log *slog.Logger) job.Job {
	switch name {
	case stuckpods.JobName:
		return stuckpods.New(opts.StuckPods, client, pods.Lister(), nodes.Lister(), recorder, log, stuckpods.NewMetrics(opts.Registry))
	case evictedpods.JobName:
		return evictedpods.New(opts.EvictedPods, client, pods.Lister(), log, evictedpods.NewMetrics(opts.Registry))
	}
	panic("shepherd: no constructor for job " + name) // AllJobs and this switch must agree
}

func orDefault(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}
