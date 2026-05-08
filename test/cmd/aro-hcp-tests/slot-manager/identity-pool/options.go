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

package identitypool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armdeploymentstacks"

	"github.com/Azure/ARO-HCP/test/cmd/aro-hcp-tests/slot-manager/slots"
	"github.com/Azure/ARO-HCP/test/e2e"
	"github.com/Azure/ARO-HCP/test/util/framework"
)

const (
	deploymentStackNamePrefix = "aro-hcp-slot-msi-pool"
)

func DefaultApplyOptions() *RawApplyOptions {
	return &RawApplyOptions{}
}

func BindApplyOptions(opts *RawApplyOptions, cmd *cobra.Command) error {
	cmd.Flags().StringVar(&opts.Environment, "environment", opts.Environment, "Environment short name. One of: int, stg, dev, prod")
	cmd.Flags().StringVar(&opts.SubscriptionName, "subscription-name", opts.SubscriptionName, "Optional subscription name selector for environments with multiple pools.")
	cmd.Flags().StringVar(&opts.Region, "region", opts.Region, "Optional region selector for environments with multiple pools.")
	cmd.Flags().StringVar(&opts.SlotCatalog, "slot-catalog", opts.SlotCatalog, "Path to the canonical E2E slot catalog")
	if err := cmd.MarkFlagRequired("environment"); err != nil {
		return fmt.Errorf("failed to mark flag %q as required: %w", "environment", err)
	}
	return nil
}

type RawApplyOptions struct {
	Environment      string
	SubscriptionName string
	Region           string
	SlotCatalog      string
}

type validatedApplyOptions struct {
	*RawApplyOptions
}

type ValidatedApplyOptions struct {
	*validatedApplyOptions
}

type completedApplyOptions struct {
	Template        map[string]interface{}
	IdentityPool    identityPool
	SubscriptionID  string
	AzureCredential azcore.TokenCredential
}

type ApplyOptions struct {
	*completedApplyOptions
}

func (o *RawApplyOptions) Validate() (*ValidatedApplyOptions, error) {
	if strings.TrimSpace(o.Environment) == "" {
		return nil, fmt.Errorf("--environment must not be empty")
	}

	return &ValidatedApplyOptions{
		validatedApplyOptions: &validatedApplyOptions{
			RawApplyOptions: o,
		},
	}, nil
}

func (o *ValidatedApplyOptions) Complete(ctx context.Context) (*ApplyOptions, error) {
	pool, err := loadIdentityPool(o.SlotCatalog, o.Environment, o.SubscriptionName, o.Region)
	if err != nil {
		return nil, fmt.Errorf("failed loading identity pool from slot catalog: %w", err)
	}

	tc := framework.NewTestContext()
	cred, err := tc.AzureCredential()
	if err != nil {
		return nil, fmt.Errorf("failed getting Azure credential: %w", err)
	}

	subscriptionID, err := tc.SubscriptionID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting subscription ID: %w", err)
	}
	if err := validateSubscriptionName(pool, os.Getenv("CUSTOMER_SUBSCRIPTION")); err != nil {
		return nil, fmt.Errorf("failed validating customer subscription: %w", err)
	}

	// Deployment Stacks require an ARM JSON template. The test suite embeds pre-generated
	// templates under test/e2e/test-artifacts/generated-test-artifacts.
	template, err := e2e.TestArtifactsFS.ReadFile("test-artifacts/generated-test-artifacts/msi-pools.json")
	if err != nil {
		return nil, fmt.Errorf("failed reading template file: %w", err)
	}
	bicepTemplateMap := map[string]interface{}{}
	if err := json.Unmarshal(template, &bicepTemplateMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Bicep template JSON: %w", err)
	}

	return &ApplyOptions{
		completedApplyOptions: &completedApplyOptions{
			Template:        bicepTemplateMap,
			IdentityPool:    pool,
			SubscriptionID:  subscriptionID,
			AzureCredential: cred,
		},
	}, nil
}

func (o *ApplyOptions) Run(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)

	stackClient, err := armdeploymentstacks.NewClient(o.SubscriptionID, o.AzureCredential, nil)
	if err != nil {
		return fmt.Errorf("failed creating deployment stack client: %w", err)
	}

	for _, slot := range o.IdentityPool.Slots {
		if err := o.applySlot(ctx, logger, stackClient, slot); err != nil {
			return err
		}
	}

	return nil
}

func (o *ApplyOptions) applySlot(ctx context.Context, logger logr.Logger, stackClient *armdeploymentstacks.Client, slot slots.ExpandedSlot) error {
	parameters := map[string]*armdeploymentstacks.DeploymentParameter{
		"poolSize": {
			Value: slot.IdentityContainerCount,
		},
		"resourceGroupBaseName": {
			Value: slot.IdentityContainerPrefix,
		},
	}

	stackName := fmt.Sprintf("%s-%s", deploymentStackNamePrefix, slot.ResourceName)
	stack := armdeploymentstacks.DeploymentStack{
		Location: to.Ptr(o.IdentityPool.Region),
		Properties: &armdeploymentstacks.DeploymentStackProperties{
			Template:   o.Template,
			Parameters: parameters,
			DenySettings: &armdeploymentstacks.DenySettings{
				Mode:               to.Ptr(armdeploymentstacks.DenySettingsModeNone),
				ApplyToChildScopes: to.Ptr(false),
				ExcludedActions:    []*string{},
				ExcludedPrincipals: []*string{},
			},
			ActionOnUnmanage: &armdeploymentstacks.ActionOnUnmanage{
				Resources:        to.Ptr(armdeploymentstacks.DeploymentStacksDeleteDetachEnum("delete")),
				ResourceGroups:   to.Ptr(armdeploymentstacks.DeploymentStacksDeleteDetachEnum("delete")),
				ManagementGroups: to.Ptr(armdeploymentstacks.DeploymentStacksDeleteDetachEnum("delete")),
			},
		},
	}

	logger.Info(
		"applying deployment stack",
		"subscriptionID", o.SubscriptionID,
		"stackName", stackName,
		"location", o.IdentityPool.Region,
		"slotName", slot.ResourceName,
		"identityContainerCount", slot.IdentityContainerCount,
	)

	poller, err := stackClient.BeginCreateOrUpdateAtSubscription(
		ctx,
		stackName,
		stack,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed starting deployment stack apply for slot %q: %w", slot.ResourceName, err)
	}

	_, err = poller.PollUntilDone(ctx, &azruntime.PollUntilDoneOptions{Frequency: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("failed waiting for deployment stack apply for slot %q: %w", slot.ResourceName, err)
	}

	return nil
}

func validateSubscriptionName(pool identityPool, subscriptionName string) error {
	subscriptionName = strings.TrimSpace(subscriptionName)
	if subscriptionName == "" {
		return fmt.Errorf("CUSTOMER_SUBSCRIPTION must not be empty")
	}
	if subscriptionName != pool.SubscriptionName {
		return fmt.Errorf("wrong customer subscription: expected %q, got %q", pool.SubscriptionName, subscriptionName)
	}
	return nil
}
