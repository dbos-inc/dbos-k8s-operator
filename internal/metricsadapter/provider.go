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

// LabelApp is the only user-facing selector label. The queue dimension is
// internal: the adapter aggregates to one value per app.
const LabelApp = "app"

// Provider is a custom-metrics-apiserver ExternalMetricsProvider backed by a
// store.Store. Construction is straightforward; the library does the rest.
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

	// Match on app
	predicate := func(app, _ string) bool {
		return selector.Matches(labels.Set{LabelApp: app})
	}
	matches := p.Store.Match(predicate)

	type agg struct {
		winnerQueue string
		sample      store.Sample
		queueCount  int
	}
	byApp := make(map[string]*agg)
	for _, m := range matches {
		a, ok := byApp[m.Key.App]
		if !ok {
			a = &agg{}
			byApp[m.Key.App] = a
		}
		a.queueCount++
		if a.winnerQueue == "" || m.Sample.Load > a.sample.Load {
			a.winnerQueue = m.Key.Queue
			a.sample = m.Sample
		}
	}

	if len(byApp) == 0 {
		logger.Info("no queues matched selector; returning empty value list",
			"observedKeyCount", len(matches))
		return &external_metrics.ExternalMetricValueList{}, nil
	}

	values := make([]external_metrics.ExternalMetricValue, 0, len(byApp))
	for app, a := range byApp {
		// load is a dimensionless ratio; encode as a milli-quantity to
		// preserve fractional precision through HPA's arithmetic.
		q := resource.NewMilliQuantity(int64(a.sample.Load*1000), resource.DecimalSI)
		values = append(values, external_metrics.ExternalMetricValue{
			MetricName:   info.Metric,
			MetricLabels: map[string]string{LabelApp: app},
			Timestamp:    metav1.NewTime(a.sample.ObservedAt),
			Value:        *q,
		})
		logger.Info("aggregated queue load",
			"app", app,
			"queueCount", a.queueCount,
			"winnerQueue", a.winnerQueue,
			"load", a.sample.Load,
			"depth", a.sample.Depth,
			"workerConcurrency", a.sample.WorkerConcurrency,
		)
	}
	return &external_metrics.ExternalMetricValueList{Items: values}, nil
}
