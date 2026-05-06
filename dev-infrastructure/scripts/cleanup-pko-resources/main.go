package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

const apiGroup = "package-operator.run"

type crdInfo struct {
	Name   string
	Plural string
	Group  string
	Scope  apiextensionsv1.ResourceScope
}

func main() {
	timeout := 120 * time.Second
	if v := os.Getenv("PKO_CLEANUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			timeout = d
		}
	}

	if err := run(timeout); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		fmt.Println("=== PKO cleanup completed with errors (best-effort, not blocking rollout) ===")
		return
	}
}

func run(timeout time.Duration) error {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), nil,
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("building kubeconfig: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	crdClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	ctx := context.Background()
	var errors int

	fmt.Println("=== Package Operator CR + CRD cleanup (best-effort) ===")

	crds, err := discoverPKOCRDs(ctx, crdClient)
	if err != nil {
		return fmt.Errorf("discovering CRDs: %w", err)
	}
	if len(crds) == 0 {
		fmt.Println("No package-operator.run CRDs found. Nothing to do.")
		return nil
	}

	fmt.Printf("Found %d CRD(s):\n", len(crds))
	for _, c := range crds {
		fmt.Printf("  %s (%s)\n", c.Name, c.Scope)
	}

	errors += deleteCRs(ctx, dynamicClient, crds, timeout)

	remaining := waitForDeletion(ctx, dynamicClient, crds, 180*time.Second)

	if remaining > 0 {
		fmt.Printf("\nWARNING: %d CR(s) stuck after 180s — removing finalizers.\n", remaining)
		errors += stripFinalizers(ctx, dynamicClient, crds)

		time.Sleep(10 * time.Second)

		remaining = countAllCRs(ctx, dynamicClient, crds)
		if remaining > 0 {
			fmt.Fprintf(os.Stderr, "[ERROR] %d CR(s) still remain after finalizer removal\n", remaining)
			errors++
		} else {
			fmt.Println("All stuck CRs removed.")
		}
	}

	errors += deleteCRDs(ctx, crdClient, crds, timeout)

	if errors > 0 {
		fmt.Printf("\n=== PKO cleanup completed with %d error(s) (best-effort, not blocking rollout) ===\n", errors)
	} else {
		fmt.Println("\n=== PKO resource cleanup complete ===")
	}
	return nil
}

func discoverPKOCRDs(ctx context.Context, client apiextensionsclient.Interface) ([]crdInfo, error) {
	list, err := client.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var result []crdInfo
	for _, crd := range list.Items {
		if !isPKOGroup(crd.Spec.Group) {
			continue
		}
		result = append(result, crdInfo{
			Name:   crd.Name,
			Plural: crd.Spec.Names.Plural,
			Group:  crd.Spec.Group,
			Scope:  crd.Spec.Scope,
		})
	}
	return result, nil
}

func isPKOGroup(group string) bool {
	return group == apiGroup || strings.HasSuffix(group, "."+apiGroup)
}

func gvr(c crdInfo) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: c.Group, Version: "v1alpha1", Resource: c.Plural}
}

func deleteCRs(ctx context.Context, client dynamic.Interface, crds []crdInfo, timeout time.Duration) int {
	errors := 0
	for _, c := range crds {
		resource := fmt.Sprintf("%s.%s", c.Plural, c.Group)
		fmt.Printf("\n--- Deleting all %s CRs (%s) ---\n", resource, c.Scope)

		deletePolicy := metav1.DeletePropagationBackground
		opts := metav1.DeleteOptions{
			PropagationPolicy: &deletePolicy,
		}

		if c.Scope == apiextensionsv1.NamespaceScoped {
			// The API server does not support cross-namespace DeleteCollection.
			// List to find which namespaces contain CRs, then delete per namespace.
			list, err := client.Resource(gvr(c)).Namespace("").List(ctx, metav1.ListOptions{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] failed to list %s: %v\n", resource, err)
				errors++
				continue
			}

			namespaces := make(map[string]struct{})
			for _, item := range list.Items {
				namespaces[item.GetNamespace()] = struct{}{}
			}

			for ns := range namespaces {
				if err := client.Resource(gvr(c)).Namespace(ns).DeleteCollection(ctx, opts, metav1.ListOptions{}); err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] failed to delete %s in namespace %s: %v\n", resource, ns, err)
					errors++
				}
			}
		} else {
			if err := client.Resource(gvr(c)).DeleteCollection(ctx, opts, metav1.ListOptions{}); err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] failed to delete %s: %v\n", resource, err)
				errors++
			}
		}
	}
	return errors
}

func countCRs(ctx context.Context, client dynamic.Interface, c crdInfo) int {
	var list *unstructured.UnstructuredList
	var err error

	if c.Scope == apiextensionsv1.NamespaceScoped {
		list, err = client.Resource(gvr(c)).Namespace("").List(ctx, metav1.ListOptions{})
	} else {
		list, err = client.Resource(gvr(c)).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] failed to list %s.%s: %v\n", c.Plural, c.Group, err)
		return 0
	}
	return len(list.Items)
}

func countAllCRs(ctx context.Context, client dynamic.Interface, crds []crdInfo) int {
	total := 0
	for _, c := range crds {
		total += countCRs(ctx, client, c)
	}
	return total
}

func waitForDeletion(ctx context.Context, client dynamic.Interface, crds []crdInfo, maxWait time.Duration) int {
	fmt.Println("\nWaiting for cascading deletion to complete...")

	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		remaining := countAllCRs(ctx, client, crds)
		if remaining == 0 {
			fmt.Println("All package-operator CRs have been deleted.")
			return 0
		}

		elapsed := time.Since(deadline.Add(-maxWait)).Truncate(time.Second)
		fmt.Printf("  %d CR(s) still remaining, waiting... (%s / %s)\n",
			remaining, elapsed, maxWait)
		time.Sleep(10 * time.Second)
	}

	return countAllCRs(ctx, client, crds)
}

func stripFinalizers(ctx context.Context, client dynamic.Interface, crds []crdInfo) int {
	patch := []byte(`{"metadata":{"finalizers":[]}}`)
	errors := 0

	for _, c := range crds {
		resource := fmt.Sprintf("%s.%s", c.Plural, c.Group)

		var list *unstructured.UnstructuredList
		var err error
		if c.Scope == apiextensionsv1.NamespaceScoped {
			list, err = client.Resource(gvr(c)).Namespace("").List(ctx, metav1.ListOptions{})
		} else {
			list, err = client.Resource(gvr(c)).List(ctx, metav1.ListOptions{})
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] failed to list %s for finalizer removal: %v\n", resource, err)
			errors++
			continue
		}

		for _, item := range list.Items {
			if len(item.GetFinalizers()) == 0 {
				continue
			}

			ns := item.GetNamespace()
			name := item.GetName()
			if ns != "" {
				fmt.Printf("  Patching finalizers on %s/%s -n %s\n", resource, name, ns)
				_, err = client.Resource(gvr(c)).Namespace(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
			} else {
				fmt.Printf("  Patching finalizers on %s/%s\n", resource, name)
				_, err = client.Resource(gvr(c)).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] failed to patch finalizers on %s/%s: %v\n", resource, name, err)
				errors++
			}
		}
	}
	return errors
}

func deleteCRDs(ctx context.Context, client apiextensionsclient.Interface, crds []crdInfo, timeout time.Duration) int {
	fmt.Println("\nRemoving package-operator.run CRDs...")

	errors := 0
	for _, c := range crds {
		fmt.Printf("  Deleting CRD: %s\n", c.Name)
		if err := client.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, c.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "[ERROR] failed to delete CRD %s: %v\n", c.Name, err)
			errors++
		}
	}
	return errors
}
