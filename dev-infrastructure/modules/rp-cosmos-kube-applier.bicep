param rpCosmosDbAccountId string
param containerName string
param containerMaxScale int
param kubeApplierManagedIdentityPrincipalId string

import * as res from './resource.bicep'

var cosmosDBAccountRef = res.cosmosDBAccountRefFromId(rpCosmosDbAccountId)

module kubeApplierAccess 'rp-cosmos-kube-applier-access.bicep' = {
  name: 'ka-access-${uniqueString(containerName)}'
  scope: resourceGroup(cosmosDBAccountRef.resourceGroup.subscriptionId, cosmosDBAccountRef.resourceGroup.name)
  params: {
    cosmosDBAccountName: cosmosDBAccountRef.name
    containerName: containerName
    containerMaxScale: containerMaxScale
    kubeApplierManagedIdentityPrincipalId: kubeApplierManagedIdentityPrincipalId
  }
}
