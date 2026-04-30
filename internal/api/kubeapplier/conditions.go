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

package kubeapplier

// Condition types reported on every *Desire's .status.conditions.
const (
	// ConditionSuccessful is true when the controller most-recently observed the
	// desired effect of the *Desire achieved against the kube-apiserver.
	ConditionSuccessful = "Successful"

	// ConditionDegraded reports controller-level health for the *Desire.
	// True means the controller failed in a way unrelated to the kube-apiserver
	// rejecting our request.
	ConditionDegraded = "Degraded"

	// ConditionWatchStarted reports that the per-instance ReadDesire informer
	// has been launched. Only meaningful on ReadDesire.
	ConditionWatchStarted = "WatchStarted"
)

// Condition reasons.
const (
	// ReasonKubeAPIError is set when the kube-apiserver returned an error for our request.
	ReasonKubeAPIError = "KubeAPIError"

	// ReasonPreCheckFailed is set when we could not issue the kube-apiserver request
	// (e.g. malformed kubeContent, GVR not present in the RESTMapper, etc.).
	ReasonPreCheckFailed = "PreCheckFailed"

	// ReasonWaitingForDeletion is set on a DeleteDesire when the target item still
	// exists in the cluster, either because finalizers are running or the delete
	// call has just been issued.
	ReasonWaitingForDeletion = "WaitingForDeletion"

	// ReasonLaunched is set on ConditionWatchStarted when the per-instance
	// ReadDesire informer is started.
	ReasonLaunched = "Launched"

	// ReasonNoErrors is the success reason matching the existing controller
	// convention (see backend's controllerutils.ReportSyncError).
	ReasonNoErrors = "NoErrors"

	// ReasonFailed is the failure reason matching the existing controller
	// convention (see backend's controllerutils.ReportSyncError).
	ReasonFailed = "Failed"
)
