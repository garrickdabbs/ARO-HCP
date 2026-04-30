# 03 &mdash; Database wiring (`internal/database`)

## Goals

1. Make a new Cosmos container, `kube-applier`, available alongside
   `Resources`, `Billing`, `Locks`.
2. Provide single-partition `ResourceCRUD[T]` accessors keyed by management
   cluster (one accessor per `*Desire` type).
3. Provide a cross-partition `GlobalLister[T]` for each `*Desire` type so the
   backend can list across all management clusters.
4. Fully isolate the partition-key strategy (mgmt cluster name) from the
   existing subscription-ID strategy.

Reference files:

- `internal/database/database.go:34-38` &mdash; container constants.
- `internal/database/database.go:72-74` &mdash; `NewPartitionKey`.
- `internal/database/crud_nested_resource.go:30-62` &mdash; generic
  `ResourceCRUD` and constructor.
- `internal/database/crud_hcpcluster.go:117-212` &mdash; nested CRUD pattern.
- `internal/database/global_lister.go:38-82` &mdash; `GlobalListers`
  interface and per-type `cosmosGlobalLister`.

## Work items

### 3.1 Add the container constant + client field

Edit `internal/database/database.go`:

```go
const (
    billingContainer    = "Billing"
    locksContainer      = "Locks"
    resourcesContainer  = "Resources"
    kubeApplierContainer = "kube-applier"
)
```

Extend `cosmosDBClient` with a `kubeApplier *azcosmos.ContainerClient` field
and populate it in `NewDBClient`. Mirror the existing pattern exactly for
each of the other three containers.

### 3.2 Custom partition-key helper

Add a sibling helper to `NewPartitionKey`:

```go
// NewKubeApplierPartitionKey builds the partition key for the kube-applier
// container, which is partitioned by the lower-cased management cluster name.
func NewKubeApplierPartitionKey(managementCluster string) azcosmos.PartitionKey {
    return azcosmos.NewPartitionKeyString(strings.ToLower(managementCluster))
}
```

This lives next to `NewPartitionKey` so the deviation is visible to anyone
auditing partition strategy.

### 3.3 New CRUD file: `crud_kube_applier.go`

Define a top-level entry point on the `DBClient` interface:

```go
type DBClient interface {
    // ... existing methods ...
    KubeApplier(managementCluster string) KubeApplierCRUD
}

type KubeApplierCRUD interface {
    ApplyDesires(parent ResourceParent) ResourceCRUD[kubeapplier.ApplyDesire]
    DeleteDesires(parent ResourceParent) ResourceCRUD[kubeapplier.DeleteDesire]
    ReadDesires(parent ResourceParent) ResourceCRUD[kubeapplier.ReadDesire]
}

// ResourceParent identifies what the *Desires are nested under.
// Either a cluster (NodePool == "") or a nodepool under a cluster.
type ResourceParent struct {
    SubscriptionID    string
    ResourceGroupName string
    ClusterName       string
    NodePoolName      string // optional
}
```

Implementation notes:

- The resource-ID prefix that `NewCosmosResourceCRUD` needs is built from
  `ResourceParent` plus the resource type. Mirror
  `crud_hcpcluster.go:NodePools()` exactly &mdash; the nesting is one level
  deeper.
- The `azcosmos.ContainerClient` passed to `NewCosmosResourceCRUD` is the
  new `kubeApplier` container, **not** `resources`.
- Override the partition-key callback so it uses
  `NewKubeApplierPartitionKey(managementCluster)` instead of the generic
  `NewPartitionKey(subscription)`. Read
  `crud_nested_resource.go` to see if this is parameterised today; if not,
  add a constructor variant `NewCosmosResourceCRUDWithPartitionKey(...)`
  that takes a partition-key function.

### 3.4 GlobalListers extension

Add three methods to `GlobalListers`:

```go
type GlobalListers interface {
    // ... existing ...
    ApplyDesires() GlobalLister[kubeapplier.ApplyDesire]
    DeleteDesires() GlobalLister[kubeapplier.DeleteDesire]
    ReadDesires() GlobalLister[kubeapplier.ReadDesire]
}
```

Implementations call `list[InternalAPIType, CosmosAPIType](ctx, l.kubeApplier, "", &resourceType, nil, options, false)` &mdash;
note the empty partition key string forces a cross-partition query, exactly
as the existing `cosmosGlobalLister` does at `global_lister.go:158-161`.

The constructor `NewCosmosGlobalListers` must take the new container:

```go
func NewCosmosGlobalListers(
    resources *azcosmos.ContainerClient,
    billing *azcosmos.ContainerClient,
    kubeApplier *azcosmos.ContainerClient,
) GlobalListers
```

This is a breaking change to one constructor &mdash; update its only call
site in `NewDBClient`.

### 3.5 Cosmos document wrapper types

Each `internal/api` type that lives in Cosmos has a sibling `database.*`
struct (e.g. `database.HCPCluster` for `api.HCPOpenShiftCluster`). Follow
the same convention:

- `internal/database/types_apply_desire.go` &mdash; `KubeApplyDesire`
- `internal/database/types_delete_desire.go` &mdash; `KubeDeleteDesire`
- `internal/database/types_read_desire.go` &mdash; `KubeReadDesire`

Each is a thin envelope that adds the cosmos-document fields (`id`,
`partitionKey`, `_etag`) and round-trips to the internal API type. Mirror
`internal/database/types_management_cluster_content.go`.

### 3.6 Tests

- Unit tests for the partition-key helper (round-trip case, lowercase
  invariant).
- Unit tests for `ResourceParent.ResourceIDString()` producing the exact
  format described in the readme (with and without nodepool).
- Round-trip serialisation tests for each of the three `database.*` envelope
  types.

### 3.7 Mocks

Update `internal/databasetesting`:

- `mock_dbclient.go` &mdash; add a `kubeApplier` keyspace; implement the
  `KubeApplier(...)` accessor returning a mock CRUD that respects the new
  partition-key invariants.
- `mock_global_lister.go` &mdash; add `ApplyDesires`, `DeleteDesires`,
  `ReadDesires` global listers using the existing
  `mockTypedGlobalLister[Internal, Cosmos]` generic pattern.
- `mock_init.go` &mdash; teach `NewMockDBClientWithResources` to accept and
  partition `*kubeapplier.{Apply,Delete,Read}Desire` objects.

## Risks / things to watch

- **Cross-container atomicity.** Cosmos `TransactionalBatch` is per-partition
  and per-container, so we cannot atomically write a `*Desire` and a
  `Resources`-container document in one shot. The backend must be designed
  to tolerate intermediate states. (This is consistent with current ARO-HCP
  behaviour.)
- **Indexing policy.** Confirm the new container's indexing policy (auto vs.
  custom) before we land it &mdash; cross-partition queries on
  `_resourceType` need an index.
- **Container creation pipeline.** The container itself is created by IaC
  (bicep). Adding the container to `dev-infrastructure/` is in scope for
  Doc 06 / 08, not for the database client PR itself.
