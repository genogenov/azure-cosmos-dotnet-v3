//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.QueryClient
{
    using Microsoft.Azure.Documents;

    internal readonly struct ContainerQueryProperties
    {
        public ContainerQueryProperties(
            string resourceId,
            string effectivePartitionKeyString,
            PartitionKeyDefinition partitionKeyDefinition)
        {
            ResourceId = resourceId;
            EffectivePartitionKeyString = effectivePartitionKeyString;
            PartitionKeyDefinition = partitionKeyDefinition;
        }

        public string ResourceId { get; }
        public string EffectivePartitionKeyString { get; }
        public PartitionKeyDefinition PartitionKeyDefinition { get; }
    }
}