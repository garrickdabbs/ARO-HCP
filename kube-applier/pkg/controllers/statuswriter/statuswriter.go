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

// Package statuswriter offers a generic "read-mutate-replace" helper for
// kube-applier *Desire status updates. The kube-applier never creates desires
// (the backend does), so the helper deliberately omits the create-if-missing
// branch present in backend's controllerutils.WriteController.
//
// Callers supply two collaborators as named structs implementing Fetcher
// and Replacer; the StatusWriter does not accept function-typed adapters.
package statuswriter

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"

	"github.com/Azure/ARO-HCP/internal/database"
)

// Fetcher reads the current state of a single desire by a controller-defined
// typed key. Implementations typically wrap a lister cache.
//
// Implementations MUST return a copy that the caller can mutate safely.
// Returning a pointer into a shared informer cache is incorrect: the
// StatusWriter mutates conditions in place via meta.SetStatusCondition,
// which would corrupt the cache entry and silently defeat the
// "no-op when unchanged" check below.
type Fetcher[T any, K comparable] interface {
	Fetch(ctx context.Context, key K) (*T, error)
}

// Replacer writes back a fully-populated desire. Implementations are free to
// derive the partition / parent scoping from the desire itself (the
// kube-applier controllers parse it out of the desire's resourceID), so the
// StatusWriter does not pass a key here.
type Replacer[T any] interface {
	Replace(ctx context.Context, desired *T) error
}

// MutateFunc deep-mutates a desire to record the latest controller observation.
// It must not perform IO; precompute everything first.
type MutateFunc[T any] func(*T)

// StatusWriter computes the next desired state via mutate and writes it back
// once. It returns nil for a no-op (status already up-to-date) and for an etag
// conflict (the informer will requeue when the new revision lands).
type StatusWriter[T any, K comparable] interface {
	UpdateStatus(ctx context.Context, key K, mutate MutateFunc[T]) error
}

// New returns a StatusWriter that fetches via fetcher and writes via replacer.
func New[T any, K comparable](fetcher Fetcher[T, K], replacer Replacer[T]) StatusWriter[T, K] {
	return &writer[T, K]{fetcher: fetcher, replacer: replacer}
}

type writer[T any, K comparable] struct {
	fetcher  Fetcher[T, K]
	replacer Replacer[T]
}

// UpdateStatus implements the read-mutate-replace cycle. The contract:
//
//   - Skip the write entirely when mutate produces a deeply-equal copy of the
//     existing desire — this is the steady state for healthy reconciles.
//   - Surface every error to the caller, including PreconditionFailed (412),
//     so the controller's workqueue retries with backoff. The kube-applier
//     has multiple controllers writing to the same desire (manager's
//     WatchStarted vs. per-instance Successful/KubeContent); on conflict the
//     loser must retry.
//
// We Fetch twice to obtain two independent deep-copies of the desire: one to
// mutate (desired) and one to compare against (existing). A naive
// `desired := *existing` would share the conditions-slice backing array with
// existing, and meta.SetStatusCondition mutates conditions in place, so the
// comparison would always report no change and silently drop writes.
func (w *writer[T, K]) UpdateStatus(ctx context.Context, key K, mutate MutateFunc[T]) error {
	existing, err := w.fetcher.Fetch(ctx, key)
	if err != nil {
		// NotFound is normal: the desire was deleted between dispatch and now.
		if database.IsNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("fetch %v: %w", key, err)
	}
	if existing == nil {
		return nil
	}

	desired, err := w.fetcher.Fetch(ctx, key)
	if err != nil {
		if database.IsNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("fetch %v: %w", key, err)
	}
	if desired == nil {
		return nil
	}

	mutate(desired)

	if equality.Semantic.DeepEqual(existing, desired) {
		return nil
	}

	if err := w.replacer.Replace(ctx, desired); err != nil {
		return fmt.Errorf("replace status for %v: %w", key, err)
	}
	return nil
}
