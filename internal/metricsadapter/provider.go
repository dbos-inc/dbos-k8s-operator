// Package metricsadapter implements the External Metrics API for HPA. It
// reads from the shared Store; the poller is the only writer. Only one
// metric is exposed today: dbos_queue_load, identified by the app label
// only. Users enable queue-based autoscaling by adding the metric to their
// HPA with an app selector; the operator returns the maximum load across
// every queue of that app that has worker_concurrency configured.
//
//	type: External
//	external:
//	  metric:
//	    name: dbos_queue_load
//	    selector: { matchLabels: { app: dbos-k8s-app } }
//	  target: { type: AverageValue, averageValue: "1" }
package metricsadapter

import (
	"context"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"

	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// MetricName is the single external metric name exposed.
const MetricName = "dbos_queue_load"

// LabelApp is the user-facing selector label
const LabelApp = "app"

// Provider is a custom-metrics-apiserver ExternalMetricsProvider backed by a store.Store
type Provider struct {
	Store store.Store
}

// New returns a Provider reading from s.
func New(s store.Store) *Provider {
	return &Provider{Store: s}
}

// ListAllExternalMetrics is the discovery handler. We expose exactly one.
func (p *Provider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
	return []provider.ExternalMetricInfo{{Metric: MetricName}}
}

// GetExternalMetric is called on every HPA poll. For each app matching the
// selector we collapse all observed queues into a single value: the maximum
// queue load. Returned labels carry only `app` — the winning queue is logged
// for operator visibility, not exposed as a metric dimension.
//
// Empty value list (no matching app, or no queues observed yet) is returned
// as-is; HPA treats that as "metric unknown" and holds the current replica
// count, which is safer than synthesizing a zero.
func (p *Provider) GetExternalMetric(
	ctx context.Context,
	namespace string,
	selector labels.Selector,
	info provider.ExternalMetricInfo,
) (*external_metrics.ExternalMetricValueList, error) {
	logger := klog.FromContext(ctx).WithValues(
		"metric", info.Metric,
		"namespace", namespace,
		"selector", selector.String(),
	)
	if info.Metric != MetricName {
		return &external_metrics.ExternalMetricValueList{}, nil
	}

	var values []external_metrics.ExternalMetricValue
	for _, app := range p.Store.Apps() {
		if !selector.Matches(labels.Set{LabelApp: app}) {
			continue
		}
		queues := p.Store.ByApp(app)
		if len(queues) == 0 {
			continue
		}
		var (
			winnerQueue string
			winner      store.Sample
		)
		for _, e := range queues {
			if winnerQueue == "" || e.Sample.Load > winner.Load {
				winnerQueue = e.Queue
				winner = e.Sample
			}
		}
		// load is a dimensionless ratio; encode as a milli-quantity to
		// preserve fractional precision through HPA's arithmetic.
		q := resource.NewMilliQuantity(int64(winner.Load*1000), resource.DecimalSI)
		values = append(values, external_metrics.ExternalMetricValue{
			MetricName:   info.Metric,
			MetricLabels: map[string]string{LabelApp: app},
			Timestamp:    metav1.NewTime(winner.ObservedAt),
			Value:        *q,
		})
		logger.Info("aggregated queue load",
			"app", app,
			"queueCount", len(queues),
			"winnerQueue", winnerQueue,
			"load", winner.Load,
			"depth", winner.Depth,
			"workerConcurrency", winner.WorkerConcurrency,
		)
	}

	if len(values) == 0 {
		logger.Info("no queues matched selector; returning empty value list")
		return &external_metrics.ExternalMetricValueList{}, nil
	}
	return &external_metrics.ExternalMetricValueList{Items: values}, nil
}
