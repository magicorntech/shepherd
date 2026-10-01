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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/magicorntech/shepherd/internal/policy"
	"github.com/magicorntech/shepherd/internal/reaper"
	"github.com/magicorntech/shepherd/internal/shepherd"
)

var version = "dev"

func main() {
	var (
		kubeconfig   = flag.String("kubeconfig", "", "path to kubeconfig; empty = in-cluster")
		namespace    = flag.String("namespace", "", "only watch this namespace; empty = all")
		interval     = flag.Duration("interval", 60*time.Second, "safety-net sweep interval; sweeps are normally triggered by watch events and per-pod deadlines")
		dryRun       = flag.Bool("dry-run", false, "log what would be force-deleted without deleting")
		defaultMode  = flag.String("default-mode", "dead-node", "mode for pods without the "+policy.ModeLabel+" label: off, dead-node, any")
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
		logLevel     = flag.String("log-level", "info", "debug, info, warn or error; debug also logs every pod that is waiting for its deadline")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "bad --log-level %q (want debug, info, warn or error)\n", *logLevel)
		os.Exit(1)
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, opts)
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

	ns := *leaseNS
	if ns == "" {
		ns = podNamespace()
	}
	id, _ := os.Hostname()

	err = shepherd.Run(ctx, client, shepherd.Options{
		Version:   version,
		Namespace: *namespace,
		Reaper: reaper.Config{
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
		},
		Metrics:        reaper.NewMetrics(reg),
		LeaderElect:    *leaderElect,
		LeaseName:      *leaseName,
		LeaseNamespace: ns,
		Identity:       id,
		OnSynced:       func() { close(ready) },
	}, log)
	if err != nil {
		fatal(log, "run", err)
	}
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
