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

package fleet

import (
	"path"
	"strings"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
)

// ToManagementClusterResourceID constructs a provider-level resource ID for a
// management cluster keyed by stamp identifier.
func ToManagementClusterResourceID(stampIdentifier string) (*azcorearm.ResourceID, error) {
	return azcorearm.ParseResourceID(ToManagementClusterResourceIDString(stampIdentifier))
}

// ToManagementClusterResourceIDString returns the lowercased provider-level
// resource ID string for a management cluster.
func ToManagementClusterResourceIDString(stampIdentifier string) string {
	return strings.ToLower(path.Join(
		"/providers", ManagementClusterResourceType.String(), stampIdentifier,
	))
}
