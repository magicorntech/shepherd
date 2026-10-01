// shepherd: a small cluster-janitor. Its first job is force-deleting pods
// that are stuck Terminating (dead node, or a kubelet that never confirms),
// so Recreate-strategy rollouts don't wait forever on a pod nobody will ever
// finish deleting.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/magicorntech/shepherd/internal/policy"
	"github.com/magicorntech/shepherd/internal/reaper"
)

var version = "dev"

func main() {
	var (
		kubeconfig   = flag.String("kubeconfig", "", "path to kubeconfig; empty = in-cluster")
		namespace    = flag.String("namespace", "", "only watch this namespace; empty = all")
		interval     = flag.Duration("interval", 60*time.Second, "safety-net sweep interval; sweeps are normally triggered by watch events and per-pod deadlines")
		dryRun       = flag.Bool("dry-run", true, "log what would be force-deleted without deleting; pass --dry-run=false to act")
		defaultMode  = flag.String("default-mode", "off", "mode for pods without the "+policy.ModeLabel+" label: off, dead-node, any")
		deadBuffer   = flag.Duration("dead-node-buffer", 30*time.Second, "extra wait after the pod's deletion deadline before force-deleting a pod on a dead node")
		healthyBuf   = flag.Duration("healthy-node-buffer", 5*time.Minute, "extra wait after the pod's deletion deadline before force-deleting a pod on a Ready node (mode=any only)")
		inclSTS      = flag.Bool("include-statefulset", false, "also force-delete StatefulSet pods (breaks at-most-one; off by default)")
		inclPVC      = flag.Bool("include-pvc", false, "also force-delete pods using PVCs (RWO volumes may fail to re-attach; off by default)")
		maxPerSweep  = flag.Int("max-deletes-per-sweep", 50, "cap on force deletes in a single sweep")
		breakerFrac  = flag.Float64("breaker-not-ready-fraction", 0.3, "stop acting on dead-node pods when more than this fraction of nodes are NotReady; 0 disables")
		breakerNodes = flag.Int("breaker-min-nodes", 5, "breaker only applies in clusters with at least this many nodes")
		metricsAddr  = flag.String("metrics-addr", ":8080", "metrics and health listen address")
		leaderElect  = flag.Bool("leader-elect", true, "use a Lease so only one replica sweeps")
		leaseNS      = flag.String("leader-election-namespace", "", "namespace of the Lease; empty = the pod's own namespace")
		leaseName    = flag.String("leader-election-name", "shepherd", "name of the Lease")
		logJSON      = flag.Bool("log-json", true, "JSON logs")
	)
	flag.Parse()

	var h slog.Handler = slog.NewTextHandler(os.Stderr, nil)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, nil)
	}
	log := slog.New(h)

	mode, err := policy.ParseMode(*defaultMode)
	if err != nil {
		fatal(log, "bad --default-mode", err)
	}

	cfg, err := restConfig(*kubeconfig)
	if err != nil {
		fatal(log, "kube config", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fatal(log, "kube client", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	metrics := reaper.NewMetrics(reg)

	// Pod informers only need to see pods being deleted, but the API can't
	// field-select on deletionTimestamp, so cache them all (metadata is
	// small) and filter in the sweep.
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(*namespace))
	podInformer := factory.Core().V1().Pods()
	podLister := podInformer.Lister()
	podSynced := podInformer.Informer().HasSynced
	// Nodes are cluster-scoped, so they need their own unscoped factory.
	nodeFactory := informers.NewSharedInformerFactory(client, 0)
	nodeInformer := nodeFactory.Core().V1().Nodes()
	nodeLister := nodeInformer.Lister()
	nodeSynced := nodeInformer.Informer().HasSynced

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "shepherd"})

	r := reaper.New(reaper.Config{
		Policy: policy.Config{
			DefaultMode:        mode,
			DeadNodeBuffer:     *deadBuffer,
			HealthyNodeBuffer:  *healthyBuf,
			IncludeStatefulSet: *inclSTS,
			IncludePVC:         *inclPVC,
		},
		Interval:           *interval,
		DryRun:             *dryRun,
		MaxDeletesPerSweep: *maxPerSweep,
		BreakerFraction:    *breakerFrac,
		BreakerMinNodes:    *breakerNodes,
	}, client, podLister, nodeLister, recorder, log, metrics)

	// Live triggers. A pod entering Terminating, or a node flipping its Ready
	// state / disappearing, can change a decision immediately; the reaper
	// also sleeps exactly until the next pod's deadline, so nothing waits on
	// a poll tick.
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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
	})
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(o, n any) {
			on, ok1 := o.(*corev1.Node)
			nn, ok2 := n.(*corev1.Node)
			if ok1 && ok2 && nodeReady(on) != nodeReady(nn) {
				r.Kick()
			}
		},
		DeleteFunc: func(any) { r.Kick() },
	})

	ready := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-ready:
			fmt.Fprintln(w, "ok")
		default:
			http.Error(w, "caches not synced", http.StatusServiceUnavailable)
		}
	})
	srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal(log, "metrics server", err)
		}
	}()
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	// Informers run on every replica (so a standby is warm); only the
	// leader sweeps.
	factory.Start(ctx.Done())
	nodeFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podSynced, nodeSynced) {
		fatal(log, "cache sync", fmt.Errorf("cancelled before caches synced"))
	}
	close(ready)

	log.Info("shepherd started", "version", version, "dryRun", *dryRun, "defaultMode", mode,
		"namespace", *namespace, "leaderElect", *leaderElect)
	if *dryRun {
		log.Warn("DRY RUN: nothing will be deleted. Pass --dry-run=false once the logs look right.")
	}

	if !*leaderElect {
		r.Run(ctx)
		return
	}

	ns := *leaseNS
	if ns == "" {
		ns = podNamespace()
	}
	id, _ := os.Hostname()
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: *leaseName, Namespace: ns},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   30 * time.Second,
		RenewDeadline:   20 * time.Second,
		RetryPeriod:     5 * time.Second,
		Name:            *leaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) { log.Info("became leader", "id", id); r.Run(ctx) },
			OnStoppedLeading: func() { log.Info("stopped leading", "id", id) },
		},
	})
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func podNamespace() string {
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return string(b)
	}
	return "default"
}

func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}
