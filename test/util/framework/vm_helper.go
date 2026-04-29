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

package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
)

// Helper to run command on VM
func RunVMCommand(ctx context.Context, tc interface {
	SubscriptionID(ctx context.Context) (string, error)
	AzureCredential() (azcore.TokenCredential, error)
}, resourceGroup, vmName, command string, pollTimeout time.Duration) (string, error) {
	subscriptionID, err := tc.SubscriptionID(ctx)
	if err != nil {
		return "", err
	}

	azCreds, err := tc.AzureCredential()
	if err != nil {
		return "", err
	}

	computeClient, err := armcompute.NewVirtualMachinesClient(subscriptionID, azCreds, nil)
	if err != nil {
		return "", err
	}

	runCommandInput := armcompute.RunCommandInput{
		CommandID: to.Ptr("RunShellScript"),
		Script: []*string{
			to.Ptr(command),
		},
	}

	poller, err := computeClient.BeginRunCommand(ctx, resourceGroup, vmName, runCommandInput, nil)
	if err != nil {
		return "", err
	}

	// Create a timeout context to avoid waiting too long on VM command failures
	// VM commands should complete quickly (within a few minutes at most)
	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	result, err := poller.PollUntilDone(pollCtx, nil)
	if err != nil {
		return "", err
	}

	if len(result.Value) > 0 && result.Value[0].Message != nil {
		// Azure Run Command returns output in format:
		// "Enable succeeded: \n[stdout]\n<actual output>\n[stderr]\n<errors>"
		// We need to extract stdout and stderr content
		message := *result.Value[0].Message

		// Find the stdout section
		stdoutStart := strings.Index(message, "[stdout]\n")
		if stdoutStart == -1 {
			// If no stdout marker, return the whole message
			return message, nil
		}

		// Skip past the "[stdout]\n" marker
		stdoutStart += len("[stdout]\n")

		// Find where stderr starts (if present)
		stderrStart := strings.Index(message[stdoutStart:], "\n[stderr]")

		var output string
		if stderrStart == -1 {
			// No stderr marker, take everything after stdout
			output = message[stdoutStart:]
		} else {
			// Take only the stdout section
			output = message[stdoutStart : stdoutStart+stderrStart]

			// Extract and inspect stderr
			stderrAbsoluteStart := stdoutStart + stderrStart + len("\n[stderr]\n")
			if stderrAbsoluteStart < len(message) {
				stderr := strings.TrimSpace(message[stderrAbsoluteStart:])
				if stderr != "" {
					// Return an error if stderr is not empty
					return "", fmt.Errorf("%s", stderr)
				}
			}
		}

		return strings.TrimSpace(output), nil
	}

	return "", nil
}

// GetVirtualMachinesInResourceGroup lists all VMs in the given resource group
// with a polling loop to handle ARM replication delays.
func GetVirtualMachinesInResourceGroup(
	ctx context.Context,
	computeClientFactory *armcompute.ClientFactory,
	resourceGroupName string,
	expectedMinimumCount int,
	timeout time.Duration,
) ([]*armcompute.VirtualMachine, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, timeout,
		fmt.Errorf("timed out waiting for %d VMs in resource group %q", expectedMinimumCount, resourceGroupName))
	defer cancel()

	vmClient := computeClientFactory.NewVirtualMachinesClient()
	const pollInterval = 10 * time.Second

	for {
		var vms []*armcompute.VirtualMachine
		pager := vmClient.NewListPager(resourceGroupName, nil)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, fmt.Errorf("caused by: %w, error: %w", context.Cause(ctx), err)
				}
				return nil, fmt.Errorf("failed to list VMs in resource group %q: %w", resourceGroupName, err)
			}
			vms = append(vms, page.Value...)
		}

		if len(vms) >= expectedMinimumCount {
			return vms, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("caused by: %w, error: %w", context.Cause(ctx), ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}
