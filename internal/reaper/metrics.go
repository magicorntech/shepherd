package reaper

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/labels"
)

var everything = labels.Everything()

type Metrics struct {
	forceDeleted *prometheus.CounterVec
	stuck        *prometheus.GaugeVec
	breakerOpen  prometheus.Gauge
	errors       prometheus.Counter
	sweepSeconds prometheus.Histogram
}

// NewMetrics registers shepherd's metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		forceDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shepherd_pods_force_deleted_total",
			Help: "Pods force-deleted (or that would have been, when dry_run=true).",
		}, []string{"reason", "mode", "dry_run"}),
		stuck: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "shepherd_terminating_pods",
			Help: "Terminating pods seen in the last sweep, by what shepherd decided.",
		}, []string{"action", "reason"}),
		breakerOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "shepherd_circuit_breaker_open",
			Help: "1 when too many nodes are NotReady and dead-node force deletes are suspended.",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "shepherd_delete_errors_total",
			Help: "Force deletes that failed with an unexpected API error.",
		}),
		sweepSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "shepherd_sweep_duration_seconds",
			Help:    "Duration of one reaper sweep.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.forceDeleted, m.stuck, m.breakerOpen, m.errors, m.sweepSeconds)
	return m
}
