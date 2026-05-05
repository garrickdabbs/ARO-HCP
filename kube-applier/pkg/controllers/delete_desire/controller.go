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

// Package delete_desire implements the DeleteDesireController.
//
// On every sync the controller resolves spec.targetItem to a GVR via the
// supplied RESTMapper, gets the object, and either:
//   - reports Successful=True if the object is gone,
//   - reports Successful=False (WaitingForDeletion) if it's there and has a
//     deletionTimestamp (or after issuing a delete that succeeded but the
//     object hasn't fully gone away yet because of finalizers), or
//   - issues a delete and re-checks the same way.
package delete_desire

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// DeleteDesireController reconciles DeleteDesires by deleting their target items
// and reporting WaitingForDeletion until the items actually disappear.
type DeleteDesireController struct {
	name     string
	informer cache.SharedIndexInformer
	fetcher  *deleteDesireFetcher
	dyn      dynamic.Interface
	writer   statuswriter.StatusWriter[kubeapplier.DeleteDesire, keys.DeleteDesireKey]
	queue    workqueue.TypedRateLimitingInterface[keys.DeleteDesireKey]
}

// NewDeleteDesireController wires up the informer event handler and returns a
// ready-to-Run controller. Deletes go through dyn; the GVR comes straight from
// spec.targetItem, no RESTMapper consultation.
//
// crudByParent provides a parent-scoped ResourceCRUD per DeleteDesire so
// status replaces can be issued under the desire's own cluster/nodepool
// resource ID rather than a sentinel parent.
func NewDeleteDesireController(
	informer cache.SharedIndexInformer,
	lister listers.DeleteDesireLister,
	dyn dynamic.Interface,
	crudByParent database.KubeApplierDeleteDesireCRUD,
) (*DeleteDesireController, error) {
	fetcher := &deleteDesireFetcher{lister: lister}
	c := &DeleteDesireController{
		name:     "DeleteDesireController",
		informer: informer,
		fetcher:  fetcher,
		dyn:      dyn,
		writer: statuswriter.New[kubeapplier.DeleteDesire, keys.DeleteDesireKey](
			fetcher,
			&deleteDesireReplacer{crudByParent: crudByParent},
		),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.DeleteDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.DeleteDesireKey]{Name: "DeleteDesireController"},
		),
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueue(obj) },
		UpdateFunc: func(_, obj any) { c.enqueue(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// Run starts threadiness workers. The DeleteDesire informer's 60s resync
// drives the periodic re-check that turns "WaitingForDeletion" into
// "Successful=True" once finalizers complete.
func (c *DeleteDesireController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	ctx = utils.ContextWithControllerName(ctx, c.name)
	logger := utils.LoggerFromContext(ctx).WithValues(utils.LogValues{}.AddControllerName(c.name)...)
	ctx = utils.ContextWithLogger(ctx, logger)
	logger.Info("starting DeleteDesireController")
	defer logger.Info("stopped DeleteDesireController")

	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *DeleteDesireController) enqueue(obj any) {
	d, ok := obj.(*kubeapplier.DeleteDesire)
	if !ok {
		return
	}
	key, err := keys.DeleteDesireKeyFromResourceID(d.GetResourceID())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *DeleteDesireController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *DeleteDesireController) processNext(ctx context.Context) bool {
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

// SyncOnce performs a single reconcile pass for the named DeleteDesire.
func (c *DeleteDesireController) SyncOnce(ctx context.Context, key keys.DeleteDesireKey) error {
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

	mutate, _ := c.evaluate(ctx, desire)
	// The error returned by evaluate is already encoded into the status mutation
	// via SetSuccessful, so we don't propagate it back to the workqueue — the
	// next informer event or resync will redrive the loop if needed.
	return c.writer.UpdateStatus(ctx, key, mutate)
}

// evaluate runs the state machine for one DeleteDesire and returns the status
// mutation function that records the outcome.
//
// State machine (from readme):
//
//	get target
//	  not found             -> SetSuccessful(true)
//	  has deletion timestamp -> WaitingForDeletion
//	  no deletion timestamp -> issue Delete; on error -> KubeAPIError
//	                           re-issue get
//	                             still not found       -> SetSuccessful(true)
//	                             has deletion timestamp -> WaitingForDeletion
//
// We don't consult a RESTMapper: the GVR is taken straight from
// spec.targetItem, and scope is decided by namespace presence. If the GVR
// doesn't resolve, the dynamic client surfaces a kube error that lands in
// SetSuccessful as KubeAPIError.
func (c *DeleteDesireController) evaluate(ctx context.Context, d *kubeapplier.DeleteDesire) (statuswriter.MutateFunc[kubeapplier.DeleteDesire], error) {
	target := d.Spec.TargetItem
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		err := conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	gvr := schema.GroupVersionResource{Group: target.Group, Version: target.Version, Resource: target.Resource}
	resource := c.dyn.Resource(gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if len(target.Namespace) > 0 {
		kubeResourceAccessor = resource.Namespace(target.Namespace)
	}

	got, getErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}
	if getErr != nil {
		err := fmt.Errorf("get target: %w", getErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	if dt := got.GetDeletionTimestamp(); dt != nil {
		uid := got.GetUID()
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}

	if delErr := kubeResourceAccessor.Delete(ctx, target.Name, metav1.DeleteOptions{}); delErr != nil {
		// 404 just before delete is fine — the object disappeared between Get and Delete.
		if apierrors.IsNotFound(delErr) {
			return func(d *kubeapplier.DeleteDesire) {
				conditions.SetSuccessful(&d.Status.Conditions, nil)
				conditions.SetDegraded(&d.Status.Conditions, nil)
			}, nil
		}
		err := fmt.Errorf("delete target: %w", delErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	// Re-read post-delete to capture the deletion-timestamp + UID for the
	// "waiting for finalizers" message — readme requires this verbatim.
	post, postErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(postErr) {
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}
	if postErr != nil {
		err := fmt.Errorf("post-delete get: %w", postErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}
	dt := post.GetDeletionTimestamp()
	uid := post.GetUID()
	if dt == nil {
		// Should not happen — apiserver always sets DT on a successful delete that's not
		// immediate. If it does, treat as still-present.
		now := metav1.NewTime(time.Now())
		dt = &now
	}
	return func(d *kubeapplier.DeleteDesire) {
		conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
		conditions.SetDegraded(&d.Status.Conditions, nil)
	}, nil
}

func classifyAsDegraded(err error) error {
	if err == nil {
		return nil
	}
	var preCheck *conditions.PreCheckError
	if errors.As(err, &preCheck) {
		return nil
	}
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		c := statusErr.ErrStatus.Code
		if c >= 400 && c < 500 {
			return nil
		}
	}
	return err
}

// deleteDesireFetcher implements statuswriter.Fetcher over a DeleteDesireLister.
// Returns a DeepCopy so the StatusWriter can safely mutate it; see the
// apply_desire counterpart for why aliasing the cache would be a bug.
type deleteDesireFetcher struct {
	lister listers.DeleteDesireLister
}

var _ statuswriter.Fetcher[kubeapplier.DeleteDesire, keys.DeleteDesireKey] = &deleteDesireFetcher{}

func (f *deleteDesireFetcher) Fetch(ctx context.Context, key keys.DeleteDesireKey) (*kubeapplier.DeleteDesire, error) {
	var got *kubeapplier.DeleteDesire
	var err error
	if key.IsNodePoolScoped() {
		got, err = f.lister.GetForNodePool(ctx, key.SubscriptionID, key.ResourceGroupName, key.ClusterName, key.NodePoolName, key.Name)
	} else {
		got, err = f.lister.GetForCluster(ctx, key.SubscriptionID, key.ResourceGroupName, key.ClusterName, key.Name)
	}
	if err != nil {
		return nil, err
	}
	return got.DeepCopy(), nil
}

// deleteDesireReplacer implements statuswriter.Replacer over a
// KubeApplierDeleteDesireCRUD. See the apply_desire counterpart for why
// the parent must be derived per-call instead of fixed at construction.
type deleteDesireReplacer struct {
	crudByParent database.KubeApplierDeleteDesireCRUD
}

var _ statuswriter.Replacer[kubeapplier.DeleteDesire] = &deleteDesireReplacer{}

func (r *deleteDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.DeleteDesire) error {
	key, err := keys.DeleteDesireKeyFromResourceID(desired.GetResourceID())
	if err != nil {
		return fmt.Errorf("derive key for replace: %w", err)
	}
	crud, err := r.crudByParent.DeleteDesires(key.ResourceParent())
	if err != nil {
		return fmt.Errorf("crud for parent %v: %w", key.ResourceParent(), err)
	}
	if _, err := crud.Replace(ctx, desired, nil); err != nil {
		return err
	}
	return nil
}
