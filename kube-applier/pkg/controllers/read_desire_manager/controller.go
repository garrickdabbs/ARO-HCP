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

// Package read_desire_manager implements the ReadDesireInformerManagingController.
//
// It watches the ReadDesire informer and, for every key, owns the lifecycle of a
// per-ReadDesire ReadDesireKubernetesController. When a ReadDesire's TargetItem
// changes, the manager stops the old per-instance controller (waiting for its
// goroutine to exit) and starts a fresh one.
package read_desire_manager

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/read_desire_kubernetes"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/statuswriter"
)

// PerInstanceController abstracts the per-ReadDesire kube reflector so the
// manager can be tested with a fake.
type PerInstanceController interface {
	Run(ctx context.Context)
}

// PerInstanceFactory builds a per-ReadDesire controller. The default factory
// constructs a ReadDesireKubernetesController via realPerInstanceFactory;
// tests pass a recording fake.
type PerInstanceFactory interface {
	Build(key keys.ReadDesireKey, target kubeapplier.ResourceReference) (PerInstanceController, error)
}

// ReadDesireInformerManagingController watches ReadDesires and manages the
// per-instance kubernetes reflectors.
type ReadDesireInformerManagingController struct {
	informer cache.SharedIndexInformer
	fetcher  *readDesireFetcher
	factory  PerInstanceFactory
	writer   statuswriter.StatusWriter[kubeapplier.ReadDesire, keys.ReadDesireKey]
	queue    workqueue.TypedRateLimitingInterface[keys.ReadDesireKey]

	mu      sync.Mutex
	running map[keys.ReadDesireKey]*runningInstance
}

type runningInstance struct {
	target kubeapplier.ResourceReference
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReadDesireInformerManagingController constructs a manager that uses the
// supplied dynamic client for every per-instance controller it spawns.
//
// crudByParent provides a parent-scoped ResourceCRUD per ReadDesire so status
// replaces — both the manager's own WatchStarted updates and those of every
// per-instance controller it spawns — can be issued under each desire's own
// cluster/nodepool resource ID rather than a sentinel parent.
func NewReadDesireInformerManagingController(
	informer cache.SharedIndexInformer,
	lister listers.ReadDesireLister,
	dyn dynamic.Interface,
	crudByParent database.KubeApplierReadDesireCRUD,
) (*ReadDesireInformerManagingController, error) {
	fetcher := &readDesireFetcher{lister: lister}
	c := &ReadDesireInformerManagingController{
		informer: informer,
		fetcher:  fetcher,
		factory:  &realPerInstanceFactory{dyn: dyn, lister: lister, crudByParent: crudByParent},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ReadDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ReadDesireKey]{Name: "ReadDesireInformerManagingController"},
		),
		writer: statuswriter.New[kubeapplier.ReadDesire, keys.ReadDesireKey](
			fetcher,
			&readDesireReplacer{crudByParent: crudByParent},
		),
		running: map[keys.ReadDesireKey]*runningInstance{},
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueue(obj) },
		UpdateFunc: func(_, obj any) { c.enqueue(obj) },
		DeleteFunc: func(obj any) { c.enqueue(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// SetFactory swaps the per-instance controller factory. Intended for tests.
func (c *ReadDesireInformerManagingController) SetFactory(f PerInstanceFactory) { c.factory = f }

// realPerInstanceFactory is the production PerInstanceFactory: it builds a
// real ReadDesireKubernetesController against the supplied dynamic client,
// lister, and CRUD provider.
type realPerInstanceFactory struct {
	dyn          dynamic.Interface
	lister       listers.ReadDesireLister
	crudByParent database.KubeApplierReadDesireCRUD
}

var _ PerInstanceFactory = &realPerInstanceFactory{}

func (f *realPerInstanceFactory) Build(
	key keys.ReadDesireKey, target kubeapplier.ResourceReference,
) (PerInstanceController, error) {
	return read_desire_kubernetes.NewReadDesireKubernetesController(key, target, f.dyn, f.lister, f.crudByParent)
}

// Run starts the workers. Threadiness > 1 is supported but not necessary —
// the manager's work is bookkeeping, while the per-instance controllers run
// in their own goroutines.
func (c *ReadDesireInformerManagingController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	defer c.stopAll()

	logger := utils.LoggerFromContext(ctx).WithValues(utils.LogValues{}.AddControllerName("ReadDesireInformerManagingController")...)
	ctx = utils.ContextWithLogger(ctx, logger)
	logger.Info("starting ReadDesireInformerManagingController")
	defer logger.Info("stopped ReadDesireInformerManagingController")

	if threadiness < 1 {
		threadiness = 1
	}
	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *ReadDesireInformerManagingController) enqueue(obj any) {
	d, ok := obj.(*kubeapplier.ReadDesire)
	if !ok {
		// DeleteFinalStateUnknown wraps the real object on cache eviction.
		if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			if d2, ok := t.Obj.(*kubeapplier.ReadDesire); ok {
				d = d2
			}
		}
	}
	if d == nil {
		return
	}
	key, err := keys.ReadDesireKeyFromResourceID(d.GetResourceID())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *ReadDesireInformerManagingController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *ReadDesireInformerManagingController) processNext(ctx context.Context) bool {
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

// SyncOnce reconciles one ReadDesire by ensuring its per-instance controller
// is running with the desired TargetItem.
func (c *ReadDesireInformerManagingController) SyncOnce(ctx context.Context, key keys.ReadDesireKey) error {
	desire, err := c.fetcher.Fetch(ctx, key)
	if err != nil && !database.IsNotFoundError(err) {
		return err
	}
	if desire == nil {
		c.stopByKey(key)
		return nil
	}

	c.mu.Lock()
	cur, exists := c.running[key]
	c.mu.Unlock()

	target := desire.Spec.TargetItem
	if exists && cur.target == target {
		// Already running with the right target — nothing to do.
		return nil
	}
	if exists {
		c.stopByKey(key)
	}

	per, err := c.factory.Build(key, target)
	if err != nil {
		// PreCheckError or any other construction failure: record it on status,
		// don't enter a Running state.
		return c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ReadDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
		})
	}

	childCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.mu.Lock()
	c.running[key] = &runningInstance{target: target, cancel: cancel, done: done}
	c.mu.Unlock()

	go func() {
		defer close(done)
		per.Run(childCtx)
	}()

	return c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ReadDesire) {
		conditions.SetWatchStarted(&d.Status.Conditions, "watch (re)launched")
	})
}

func (c *ReadDesireInformerManagingController) stopByKey(key keys.ReadDesireKey) {
	c.mu.Lock()
	cur, ok := c.running[key]
	if ok {
		delete(c.running, key)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	cur.cancel()
	<-cur.done // wait for the goroutine to actually exit before returning.
}

func (c *ReadDesireInformerManagingController) stopAll() {
	c.mu.Lock()
	allKeys := make([]keys.ReadDesireKey, 0, len(c.running))
	for k := range c.running {
		allKeys = append(allKeys, k)
	}
	c.mu.Unlock()
	for _, k := range allKeys {
		c.stopByKey(k)
	}
}

// Running returns true when key has a per-instance controller in flight. Test-only.
func (c *ReadDesireInformerManagingController) Running(key keys.ReadDesireKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.running[key]
	return ok
}

// readDesireFetcher implements statuswriter.Fetcher over a ReadDesireLister.
// Defined here to keep the manager self-contained; the per-instance
// controller package has its own equivalent struct. Returns a DeepCopy so
// the StatusWriter can safely mutate the result without aliasing the cache.
type readDesireFetcher struct {
	lister listers.ReadDesireLister
}

var _ statuswriter.Fetcher[kubeapplier.ReadDesire, keys.ReadDesireKey] = &readDesireFetcher{}

func (f *readDesireFetcher) Fetch(ctx context.Context, key keys.ReadDesireKey) (*kubeapplier.ReadDesire, error) {
	var got *kubeapplier.ReadDesire
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

// readDesireReplacer implements statuswriter.Replacer over a
// KubeApplierReadDesireCRUD. The manager has its own writer for the
// WatchStarted condition; the spawned per-instance controllers have
// their own writer for KubeContent. Both writers go through a Replacer
// like this one.
type readDesireReplacer struct {
	crudByParent database.KubeApplierReadDesireCRUD
}

var _ statuswriter.Replacer[kubeapplier.ReadDesire] = &readDesireReplacer{}

func (r *readDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.ReadDesire) error {
	key, err := keys.ReadDesireKeyFromResourceID(desired.GetResourceID())
	if err != nil {
		return fmt.Errorf("derive key for replace: %w", err)
	}
	crud, err := r.crudByParent.ReadDesires(key.ResourceParent())
	if err != nil {
		return fmt.Errorf("crud for parent %v: %w", key.ResourceParent(), err)
	}
	if _, err := crud.Replace(ctx, desired, nil); err != nil {
		return err
	}
	return nil
}
