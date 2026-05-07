// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package apply_desire implements the ApplyDesireController.
//
// On every sync the controller reads the named ApplyDesire from its lister,
// decodes spec.kubeContent into an unstructured object, resolves the GVR via
// the supplied RESTMapper, and issues a server-side-apply with Force=true and
// FieldManager="kube-applier" via the dynamic client. The outcome is recorded
// on .status.conditions["Successful"] and persisted via the StatusWriter.
package apply_desire

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/database/listers"
	"github.com/Azure/ARO-HCP/internal/utils"

	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/conditions"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/keys"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/statuswriter"
)

// FieldManager is the SSA field-manager name the kube-applier uses when
// applying ApplyDesires. All on-cluster ownership of fields written by the
// kube-applier traces back to this string.
const FieldManager = "kube-applier"

// ApplyDesireController reconciles ApplyDesires by SSA-applying spec.kubeContent.
type ApplyDesireController struct {
	name     string
	informer cache.SharedIndexInformer
	fetcher  *applyDesireFetcher
	dyn      dynamic.Interface
	writer   statuswriter.StatusWriter[kubeapplier.ApplyDesire, keys.ApplyDesireKey]
	queue    workqueue.TypedRateLimitingInterface[keys.ApplyDesireKey]
}

// NewApplyDesireController wires up the informer event handler and returns a
// ready-to-Run controller. SSA writes go through dyn; we don't consult a
// RESTMapper — see applyDesired for the GVR-from-GVK convention.
//
// crudByParent provides a parent-scoped ResourceCRUD per ApplyDesire so
// status replaces can be issued under the desire's own cluster/nodepool
// resource ID rather than a sentinel parent.
func NewApplyDesireController(
	informer cache.SharedIndexInformer,
	lister listers.ApplyDesireLister,
	dyn dynamic.Interface,
	crudByParent database.KubeApplierApplyDesireCRUD,
) (*ApplyDesireController, error) {
	fetcher := &applyDesireFetcher{lister: lister}
	c := &ApplyDesireController{
		name:     "ApplyDesireController",
		informer: informer,
		fetcher:  fetcher,
		dyn:      dyn,
		writer: statuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
			fetcher,
			&applyDesireReplacer{crudByParent: crudByParent},
		),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ApplyDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ApplyDesireKey]{Name: "ApplyDesireController"},
		),
	}

	// Register the event handler at construction so events are delivered to
	// the queue before the informer starts pumping. Adding it inside Run()
	// races with the initial sync.
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueue(obj) },
		UpdateFunc: func(_, obj any) { c.enqueue(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// Run starts threadiness workers. It returns when ctx is cancelled.
func (c *ApplyDesireController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	ctx = utils.ContextWithControllerName(ctx, c.name)
	logger := utils.LoggerFromContext(ctx).WithValues(utils.LogValues{}.AddControllerName(c.name)...)
	ctx = utils.ContextWithLogger(ctx, logger)
	logger.Info("starting ApplyDesireController")
	defer logger.Info("stopped ApplyDesireController")

	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *ApplyDesireController) enqueue(obj any) {
	d, ok := obj.(*kubeapplier.ApplyDesire)
	if !ok {
		return
	}
	key, err := keys.ApplyDesireKeyFromResourceID(d.GetResourceID())
	if err != nil {
		// Should not happen for a desire produced by our own informers, but
		// don't poison the queue if it does.
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *ApplyDesireController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *ApplyDesireController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.SyncOnce(ctx, key); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "sync error; requeuing", "key", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// SyncOnce performs a single reconcile pass for the named ApplyDesire.
// It is idempotent; concurrent invocations on different keys are safe.
func (c *ApplyDesireController) SyncOnce(ctx context.Context, key keys.ApplyDesireKey) error {
	desire, err := c.fetcher.Fetch(ctx, key)
	if err != nil {
		if database.IsNotFoundError(err) {
			return nil
		}
		return err
	}
	if desire == nil {
		return nil
	}

	syncErr := c.applyDesired(ctx, desire)

	return c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ApplyDesire) {
		conditions.SetSuccessful(&d.Status.Conditions, syncErr)
		conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(syncErr))
	})
}

// applyDesired performs the kubeContent decode and SSA call. The GVR comes
// straight from spec.targetItem; we don't consult a RESTMapper or guess. The
// dynamic client surfaces a kube error if the GVR doesn't resolve, and that
// lands in SetSuccessful as KubeAPIError.
//
// PreCheckError is returned for pre-flight failures (parse, missing fields)
// so they classify as PreCheckFailed; everything else is treated as a
// kube-apiserver error.
func (c *ApplyDesireController) applyDesired(ctx context.Context, d *kubeapplier.ApplyDesire) error {
	target := d.Spec.TargetItem
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		return conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
	}
	if len(d.Spec.KubeContent.Raw) == 0 {
		return conditions.NewPreCheckError(errors.New("spec.kubeContent is empty"))
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(d.Spec.KubeContent.Raw); err != nil {
		return conditions.NewPreCheckError(fmt.Errorf("decode kubeContent: %w", err))
	}

	gvr := schema.GroupVersionResource{Group: target.Group, Version: target.Version, Resource: target.Resource}
	resource := c.dyn.Resource(gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if len(target.Namespace) > 0 {
		kubeResourceAccessor = resource.Namespace(target.Namespace)
	}

	_, applyErr := kubeResourceAccessor.Apply(ctx, target.Name, obj, metav1.ApplyOptions{
		FieldManager: FieldManager,
		Force:        true,
	})
	if applyErr != nil {
		// Wrap with a contextual prefix; keep the original kind so SetSuccessful
		// classifies it as a kube-apiserver error (NOT a *PreCheckError).
		return fmt.Errorf("server-side apply: %w", applyErr)
	}
	return nil
}

// classifyAsDegraded picks which sync errors should bubble to the Degraded
// condition. PreCheck failures are status-only signals, not controller-health
// problems, so we suppress them here.
func classifyAsDegraded(err error) error {
	if err == nil {
		return nil
	}
	var preCheck *conditions.PreCheckError
	if errors.As(err, &preCheck) {
		return nil
	}
	// 4xx errors from the apiserver are also user-input problems, not
	// controller wedges. Only 5xx and unclassified errors register as Degraded.
	if isClientError(err) {
		return nil
	}
	return err
}

func isClientError(err error) bool {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		c := statusErr.ErrStatus.Code
		return c >= 400 && c < 500
	}
	return false
}

// applyDesireFetcher implements statuswriter.Fetcher over an ApplyDesireLister,
// dispatching to the correct scope-specific lister method based on the key.
type applyDesireFetcher struct {
	lister listers.ApplyDesireLister
}

var _ statuswriter.Fetcher[kubeapplier.ApplyDesire, keys.ApplyDesireKey] = &applyDesireFetcher{}

func (f *applyDesireFetcher) Fetch(ctx context.Context, key keys.ApplyDesireKey) (*kubeapplier.ApplyDesire, error) {
	if key.IsNodePoolScoped() {
		return f.lister.GetForNodePool(ctx, key.SubscriptionID, key.ResourceGroupName, key.ClusterName, key.NodePoolName, key.Name)
	}
	return f.lister.GetForCluster(ctx, key.SubscriptionID, key.ResourceGroupName, key.ClusterName, key.Name)
}

// applyDesireReplacer implements statuswriter.Replacer over a
// KubeApplierApplyDesireCRUD. It derives the (cluster, [nodepool]) parent
// from each desire's resourceID at Replace time so a single Replacer can
// serve desires across many parents.
type applyDesireReplacer struct {
	crudByParent database.KubeApplierApplyDesireCRUD
}

var _ statuswriter.Replacer[kubeapplier.ApplyDesire] = &applyDesireReplacer{}

func (r *applyDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.ApplyDesire) error {
	key, err := keys.ApplyDesireKeyFromResourceID(desired.GetResourceID())
	if err != nil {
		return fmt.Errorf("derive key for replace: %w", err)
	}
	crud, err := r.crudByParent.ApplyDesires(key.ResourceParent())
	if err != nil {
		return fmt.Errorf("crud for parent %v: %w", key.ResourceParent(), err)
	}
	if _, err := crud.Replace(ctx, desired, nil); err != nil {
		return err
	}
	return nil
}
