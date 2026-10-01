package evictedpods

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/labels"
)

var everything = labels.Everything()

type Metrics struct {
	deleted *prometheus.CounterVec
	pods    *prometheus.GaugeVec
	errors  prometheus.Counter
}

// NewMetrics registers the evicted-pods job's metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		deleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shepherd_evicted_pods_deleted_total",
			Help: "Evicted pods deleted (or that would have been, when dry_run=true).",
		}, []string{"dry_run"}),
		pods: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "shepherd_evicted_pods",
			Help: "Evicted pods seen in the last sweep, by state: waiting (younger than the TTL), due (older, about to be deleted) or opted_out.",
		}, []string{"state"}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "shepherd_evicted_delete_errors_total",
			Help: "Evicted-pod deletes that failed with an unexpected API error.",
		}),
	}
	reg.MustRegister(m.deleted, m.pods, m.errors)
	return m
}
