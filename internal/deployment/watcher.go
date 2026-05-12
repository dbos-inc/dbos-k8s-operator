// Package deployment watches a single K8s Deployment for PodTemplate changes
// and reports the resulting ReplicaSet's pod-template-hash to Conductor as
// the corresponding application_version's metadata.
//
// Why split detect from publish? The K8s informer fires the instant a new
// ReplicaSet appears — milliseconds after `kubectl apply`. At that moment,
// the new Python (or Go) pod hasn't started yet, hasn't connected to
// Conductor, hasn't registered an executor under its new application_version.
// If we PATCH metadata immediately, we'd write to whatever Conductor
// considers latest *now*, which is still the previous version. The fix is to
// wait for Conductor to learn about the new version first.
//
// We pivot on the pod hostname. K8s pod names follow the pattern
// "<deployment>-<pod-template-hash>-<random>", and the kubelet sets each
// pod's hostname to its name by default. When the Python pod connects to
// Conductor it sends its hostname; Conductor stores it on the executor row.
// So once *any* executor with one of our pod hostnames appears in Conductor,
// we know two things:
//   - which application_version that K8s rollout corresponds to (the
//     executor's AppVersion field)
//   - that it's safe to PATCH metadata, because the version row exists
//
// Two phases run in the same goroutine:
//
//   - Detect (event-driven): the K8s informer fires; reconcile records the
//     pendingHash in watcher state and clears any prior "published" marker
//     if the hash changed. No Conductor call.
//   - Publish (periodic, every PublishInterval): if a pendingHash is set,
//     list pods with that label in K8s, list executors in Conductor, find an
//     executor whose hostname matches one of our pods, then PATCH that
//     version's metadata with the pendingHash. On success, mark published and
//     clear the pending entry.
//
// Self-healing: on operator restart we don't remember publishedHash. The
// first reconcile re-enqueues a pending write, and the publish tick re-writes
// metadata. Conductor's UPDATE is idempotent so writing the same hash twice
// is harmless.
package deployment

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
)

// revisionAnnotation is the annotation K8s puts on each ReplicaSet to record
// the Deployment revision it corresponds to. The current RS is the one with
// the highest value.
const revisionAnnotation = "deployment.kubernetes.io/revision"

// podTemplateHashLabel is the label K8s puts on each ReplicaSet (and on each
// pod it creates) containing the FNV hash of the PodTemplateSpec.
const podTemplateHashLabel = "pod-template-hash"

// Config is one watcher's configuration.
type Config struct {
	// AppName is the DBOS application name the watched Deployment runs.
	// Used as the path segment when calling Conductor.
	AppName string

	// Namespace + Name identify the Deployment to watch.
	Namespace string
	Name      string

	// ResyncPeriod is the informer resync interval. Resyncs replay cached
	// objects through the handlers and act as a self-heal mechanism.
	ResyncPeriod time.Duration

	// PublishInterval is the cadence at which the publish phase tries to
	// match pods to executors and write metadata. Default 5s if zero.
	PublishInterval time.Duration
}

// Watcher observes one Deployment and publishes pod-template-hash to
// Conductor on a delay that waits for Conductor to learn about the new
// application_version.
type Watcher struct {
	cfg  Config
	k8s  kubernetes.Interface
	cond *conductor.Client

	mu            sync.Mutex
	pendingHash   string    // the hash we still need to publish, "" if nothing pending
	pendingSince  time.Time // when this pending entry was created (for "still waiting" logs)
	publishedHash string    // the hash most recently written to Conductor (no need to re-publish)
}

// New constructs a Watcher.
func New(cfg Config, k8sClient kubernetes.Interface, cond *conductor.Client) *Watcher {
	if cfg.PublishInterval == 0 {
		cfg.PublishInterval = 5 * time.Second
	}
	return &Watcher{cfg: cfg, k8s: k8sClient, cond: cond}
}

// Run blocks until ctx is cancelled. Spawn one of these per configured
// Deployment.
func (w *Watcher) Run(ctx context.Context) {
	logger := klog.FromContext(ctx).WithValues(
		"app", w.cfg.AppName,
		"namespace", w.cfg.Namespace,
		"deployment", w.cfg.Name,
	)
	logger.Info("starting deployment watcher")

	factory := informers.NewSharedInformerFactoryWithOptions(
		w.k8s,
		w.cfg.ResyncPeriod,
		informers.WithNamespace(w.cfg.Namespace),
	)
	rsInformer := factory.Apps().V1().ReplicaSets().Informer()
	rsLister := factory.Apps().V1().ReplicaSets().Lister().ReplicaSets(w.cfg.Namespace)

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ interface{}) { w.detect(logger, rsLister) },
		UpdateFunc: func(_, _ interface{}) { w.detect(logger, rsLister) },
	}
	if _, err := rsInformer.AddEventHandler(handler); err != nil {
		logger.Error(err, "register replicaset event handler")
		return
	}

	factory.Start(ctx.Done())
	for kind, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			logger.Error(nil, "informer cache failed to sync", "type", kind.String())
			return
		}
	}
	logger.V(2).Info("informer caches synced")

	// Initial reconciliation in case all events fired before our handler was
	// registered.
	w.detect(logger, rsLister)

	// Publish phase: ticker that walks pending → publishedHash.
	ticker := time.NewTicker(w.cfg.PublishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping deployment watcher")
			return
		case <-ticker.C:
			w.publish(ctx, logger)
		}
	}
}

// detect inspects the cache for the current ReplicaSet owned by our
// Deployment and updates pendingHash if its hash differs from publishedHash.
func (w *Watcher) detect(logger klog.Logger, rsLister rsListerInterface) {
	current, err := currentReplicaSet(rsLister, w.cfg.Name)
	if err != nil {
		logger.V(2).Info("no current replicaset yet", "err", err)
		return
	}
	hash := current.Labels[podTemplateHashLabel]
	if hash == "" {
		logger.V(2).Info("current replicaset has no pod-template-hash label",
			"replicaset", current.Name)
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if hash == w.publishedHash {
		// Already published; nothing to do.
		w.pendingHash = ""
		return
	}
	if hash == w.pendingHash {
		// Already pending the same hash; informer is just re-firing.
		return
	}
	// New hash to publish.
	w.pendingHash = hash
	w.pendingSince = time.Now()
	logger.Info("deployment template change detected",
		"replicaset", current.Name,
		"podTemplateHash", hash,
		"publishedHash", w.publishedHash)
}

// publish tries to write metadata for any pending hash. The flow:
//  1. Read pendingHash from local state.
//  2. List pods in our namespace with label pod-template-hash=<hash>.
//  3. List executors from Conductor for our app.
//  4. Find an executor whose hostname matches one of our pod names.
//  5. If found, PATCH that version's metadata and mark publishedHash.
//
// If any step yields no match, no error is fatal: the next tick retries.
func (w *Watcher) publish(ctx context.Context, logger klog.Logger) {
	w.mu.Lock()
	pendingHash := w.pendingHash
	pendingSince := w.pendingSince
	w.mu.Unlock()
	if pendingHash == "" {
		return
	}

	// 1. Get pod names with this template hash. The Deployment controller
	//    applies the same `pod-template-hash` label to the RS, the pods, and
	//    every replica from that template — so a label selector is enough.
	pods, err := w.k8s.CoreV1().Pods(w.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", podTemplateHashLabel, pendingHash),
	})
	if err != nil {
		logger.V(2).Info("list pods failed", "err", err)
		return
	}
	if len(pods.Items) == 0 {
		logger.V(2).Info("no pods yet for pending hash",
			"podTemplateHash", pendingHash,
			"elapsed", time.Since(pendingSince).Round(time.Second))
		return
	}
	podNames := make(map[string]struct{}, len(pods.Items))
	for _, p := range pods.Items {
		podNames[p.Name] = struct{}{}
	}

	// 2. List executors from Conductor.
	executors, err := w.cond.ListExecutors(ctx, w.cfg.AppName)
	if err != nil {
		logger.V(2).Info("list executors failed", "err", err)
		return
	}

	// 3. Find executor whose hostname matches one of our pod names.
	var version string
	for _, e := range executors {
		if e.Hostname == nil {
			continue
		}
		if _, ok := podNames[*e.Hostname]; ok {
			version = e.AppVersion
			break
		}
	}
	if version == "" {
		// No matching executor yet. Surface a periodic log so a long wait
		// (e.g., a pod stuck in CrashLoopBackOff) is visible.
		if time.Since(pendingSince) > 30*time.Second {
			logger.Info("waiting for executor registration matching new pods",
				"podTemplateHash", pendingHash,
				"podCount", len(podNames),
				"elapsed", time.Since(pendingSince).Round(time.Second))
		} else {
			logger.V(2).Info("no executor matches our pods yet",
				"podTemplateHash", pendingHash,
				"elapsed", time.Since(pendingSince).Round(time.Second))
		}
		return
	}

	// 4. PATCH the version's metadata.
	if err := w.cond.UpdateVersionMetadata(ctx, w.cfg.AppName, version, map[string]string{
		"pod_template_hash": pendingHash,
	}); err != nil {
		logger.Error(err, "update conductor version metadata",
			"version", version, "podTemplateHash", pendingHash)
		return
	}

	logger.Info("conductor version metadata updated",
		"version", version,
		"podTemplateHash", pendingHash,
		"elapsed", time.Since(pendingSince).Round(time.Second))

	w.mu.Lock()
	w.publishedHash = pendingHash
	if w.pendingHash == pendingHash {
		w.pendingHash = ""
	}
	w.mu.Unlock()
}

// rsListerInterface is the subset of the typed lister we use, narrowed for
// testability.
type rsListerInterface interface {
	List(selector labels.Selector) ([]*appsv1.ReplicaSet, error)
}

// currentReplicaSet returns the ReplicaSet owned by the named Deployment that
// has the highest revision annotation.
func currentReplicaSet(rsLister rsListerInterface, deploymentName string) (*appsv1.ReplicaSet, error) {
	all, err := rsLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	var best *appsv1.ReplicaSet
	var bestRev int64 = -1
	for _, rs := range all {
		if !isOwnedBy(rs, deploymentName) {
			continue
		}
		rev := revisionOf(rs)
		if rev > bestRev {
			bestRev = rev
			best = rs
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no replicaset owned by deployment %q", deploymentName)
	}
	return best, nil
}

// isOwnedBy returns true if rs has an ownerReference to a Deployment named
// deploymentName.
func isOwnedBy(rs *appsv1.ReplicaSet, deploymentName string) bool {
	for _, o := range rs.OwnerReferences {
		if o.Kind == "Deployment" && o.Name == deploymentName {
			return true
		}
	}
	return false
}

// revisionOf parses the deployment.kubernetes.io/revision annotation.
func revisionOf(rs *appsv1.ReplicaSet) int64 {
	v := rs.Annotations[revisionAnnotation]
	if v == "" {
		return -1
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return -1
	}
	return n
}
