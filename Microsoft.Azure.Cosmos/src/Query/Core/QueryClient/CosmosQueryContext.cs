﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Query.Core.QueryClient
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Query.Core.QueryPlan;
    using OperationType = Documents.OperationType;
    using PartitionKeyRangeIdentity = Documents.PartitionKeyRangeIdentity;
    using ResourceType = Documents.ResourceType;

    internal abstract class CosmosQueryContext
    {
        public virtual CosmosQueryClient QueryClient { get; }
        public virtual ResourceType ResourceTypeEnum { get; }
        public virtual OperationType OperationTypeEnum { get; }
        public virtual Type ResourceType { get; }
        public virtual bool IsContinuationExpected { get; }
        public virtual bool AllowNonValueAggregateQuery { get; }
        public virtual string ResourceLink { get; }
        public virtual string ContainerResourceId { get; set; }
        public virtual Guid CorrelatedActivityId { get; }

        internal CosmosQueryContext()
        {
        }

        public CosmosQueryContext(
            CosmosQueryClient client,
            ResourceType resourceTypeEnum,
            OperationType operationType,
            Type resourceType,
            string resourceLink,
            Guid correlatedActivityId,
            bool isContinuationExpected,
            bool allowNonValueAggregateQuery,
            string containerResourceId = null)
        {
            OperationTypeEnum = operationType;
            QueryClient = client ?? throw new ArgumentNullException(nameof(client));
            ResourceTypeEnum = resourceTypeEnum;
            ResourceType = resourceType ?? throw new ArgumentNullException(nameof(resourceType));
            ResourceLink = resourceLink;
            ContainerResourceId = containerResourceId;
            IsContinuationExpected = isContinuationExpected;
            AllowNonValueAggregateQuery = allowNonValueAggregateQuery;
            CorrelatedActivityId = (correlatedActivityId == Guid.Empty) ? throw new ArgumentOutOfRangeException(nameof(correlatedActivityId)) : correlatedActivityId;
        }

        internal abstract IDisposable CreateDiagnosticScope(string name);

        internal abstract Task<QueryResponseCore> ExecuteQueryAsync(
            SqlQuerySpec querySpecForInit,
            string continuationToken,
            PartitionKeyRangeIdentity partitionKeyRange,
            bool isContinuationExpected,
            int pageSize,
            CancellationToken cancellationToken);

        internal abstract Task<PartitionedQueryExecutionInfo> ExecuteQueryPlanRequestAsync(
            string resourceUri,
            Documents.ResourceType resourceType,
            Documents.OperationType operationType,
            SqlQuerySpec sqlQuerySpec,
            PartitionKey? partitionKey,
            string supportedQueryFeatures,
            CancellationToken cancellationToken);
    }
}
