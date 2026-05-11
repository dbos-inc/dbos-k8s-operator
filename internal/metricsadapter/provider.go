// Package metricsadapter implements the External Metrics API for HPA. It
// reads from the shared Store; the poller is the only writer. Only one
// metric is exposed today: dbos_queue_load, identified by labels app and
// queue. HPA configurations look like:
//
//	type: External
//	external:
//	  metric:
//	    name: dbos_queue_load
//	    selector: { matchLabels: { queue: orders, app: dbos-k8s-app } }
//	  target: { type: AverageValue, averageValue: "1" }
package metricsadapter

import (
	"context"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"

	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// MetricName is the single external metric name exposed.
const MetricName = "dbos_queue_load"

// Selector label keys. HPA can match on any subset.
const (
	LabelQueue = "queue"
	LabelApp   = "app"
)

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

// GetExternalMetric is called on every HPA poll. The library handles routing,
// authn/authz, TLS, and serialization; we just translate (namespace,
// labelSelector, metricName) into a list of ExternalMetricValue.
func (p *Provider) GetExternalMetric(
	ctx context.Context,
	namespace string,
	selector labels.Selector,
	info provider.ExternalMetricInfo,
) (*external_metrics.ExternalMetricValueList, error) {
	if info.Metric != MetricName {
		return &external_metrics.ExternalMetricValueList{}, nil
	}

	predicate := func(app, queue string) bool {
		return selector.Matches(labels.Set{LabelQueue: queue, LabelApp: app})
	}
	matches := p.Store.MatchByNamespace(namespace, predicate)

	values := make([]external_metrics.ExternalMetricValue, 0, len(matches))
	for _, m := range matches {
		// load is a dimensionless ratio; encode as a milli-quantity to
		// preserve fractional precision through HPA's arithmetic.
		q := resource.NewMilliQuantity(int64(m.Sample.Load*1000), resource.DecimalSI)
		values = append(values, external_metrics.ExternalMetricValue{
			MetricName: info.Metric,
			MetricLabels: map[string]string{
				LabelApp:   m.Key.App,
				LabelQueue: m.Key.Queue,
			},
			Timestamp: metav1.NewTime(m.Sample.ObservedAt),
			Value:     *q,
		})
	}
	return &external_metrics.ExternalMetricValueList{Items: values}, nil
}
