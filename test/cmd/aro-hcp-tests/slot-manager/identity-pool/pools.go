// Copyright 2025 Microsoft Corporation
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

package identitypool

import (
	"github.com/Azure/ARO-HCP/test/cmd/aro-hcp-tests/slot-manager/slots"
)

type identityPool struct {
	Environment      string
	Region           string
	SubscriptionName string
	Slots            []slots.ExpandedSlot
}

func loadIdentityPool(catalogPath, environment, subscriptionName, region string) (identityPool, error) {
	catalog, err := slots.LoadCatalog(catalogPath)
	if err != nil {
		return identityPool{}, err
	}

	pool, err := catalog.ResolvePool(environment, subscriptionName, region)
	if err != nil {
		return identityPool{}, err
	}
	expandedSlots := slots.ExpandSlotsForPool(environment, pool)

	return identityPool{
		Environment:      environment,
		Region:           pool.Region,
		SubscriptionName: pool.SubscriptionName,
		Slots:            expandedSlots,
	}, nil
}
