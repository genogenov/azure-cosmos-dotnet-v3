//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Common;
    using Microsoft.Azure.Cosmos.Query.Core.ComparableTask;
    using Microsoft.Azure.Cosmos.Query.Core.QueryPlan;
    using Microsoft.Azure.Cosmos.Routing;
    using Microsoft.Azure.Documents;

    internal sealed class DocumentQueryClient : IDocumentQueryClient
    {
        private readonly DocumentClient innerClient;
        private readonly SemaphoreSlim semaphore;
        private QueryPartitionProvider queryPartitionProvider;

        public DocumentQueryClient(DocumentClient innerClient)
        {
            if (innerClient == null)
            {
                throw new ArgumentNullException("innerClient");
            }

            this.innerClient = innerClient;
            semaphore = new SemaphoreSlim(1, 1);
        }

        public void Dispose()
        {
            innerClient.Dispose();
            if (queryPartitionProvider != null)
            {
                queryPartitionProvider.Dispose();
            }
        }

        QueryCompatibilityMode IDocumentQueryClient.QueryCompatibilityMode
        {
            get
            {
                return innerClient.QueryCompatibilityMode;
            }

            set
            {
                innerClient.QueryCompatibilityMode = value;
            }
        }

        IRetryPolicyFactory IDocumentQueryClient.ResetSessionTokenRetryPolicy
        {
            get
            {
                return innerClient.ResetSessionTokenRetryPolicy;
            }
        }

        Uri IDocumentQueryClient.ServiceEndpoint
        {
            get
            {
                return innerClient.ReadEndpoint;
            }
        }

        ConnectionMode IDocumentQueryClient.ConnectionMode
        {
            get
            {
                return innerClient.ConnectionPolicy.ConnectionMode;
            }
        }

        Action<IQueryable> IDocumentQueryClient.OnExecuteScalarQueryCallback
        {
            get { return innerClient.OnExecuteScalarQueryCallback; }
        }

        async Task<CollectionCache> IDocumentQueryClient.GetCollectionCacheAsync()
        {
            return await innerClient.GetCollectionCacheAsync();
        }

        async Task<IRoutingMapProvider> IDocumentQueryClient.GetRoutingMapProviderAsync()
        {
            return await innerClient.GetPartitionKeyRangeCacheAsync();
        }

        public async Task<QueryPartitionProvider> GetQueryPartitionProviderAsync(CancellationToken cancellationToken)
        {
            if (queryPartitionProvider == null)
            {
                await semaphore.WaitAsync(cancellationToken);

                if (queryPartitionProvider == null)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    queryPartitionProvider = new QueryPartitionProvider(await innerClient.GetQueryEngineConfigurationAsync());
                }

                semaphore.Release();
            }

            return queryPartitionProvider;
        }

        public Task<DocumentServiceResponse> ExecuteQueryAsync(DocumentServiceRequest request, IDocumentClientRetryPolicy retryPolicyInstance, CancellationToken cancellationToken)
        {
            return innerClient.ExecuteQueryAsync(request, retryPolicyInstance, cancellationToken);
        }

        public Task<DocumentServiceResponse> ReadFeedAsync(DocumentServiceRequest request, IDocumentClientRetryPolicy retryPolicyInstance, CancellationToken cancellationToken)
        {
            return innerClient.ReadFeedAsync(request, retryPolicyInstance, cancellationToken);
        }

        public async Task<ConsistencyLevel> GetDefaultConsistencyLevelAsync()
        {
            return (ConsistencyLevel)await innerClient.GetDefaultConsistencyLevelAsync();
        }

        public Task<ConsistencyLevel?> GetDesiredConsistencyLevelAsync()
        {
            return innerClient.GetDesiredConsistencyLevelAsync();
        }

        public Task EnsureValidOverwriteAsync(ConsistencyLevel requestedConsistencyLevel)
        {
            innerClient.EnsureValidOverwrite(requestedConsistencyLevel);
            return CompletedTask.Instance;
        }

        public Task<PartitionKeyRangeCache> GetPartitionKeyRangeCacheAsync()
        {
            return innerClient.GetPartitionKeyRangeCacheAsync();
        }
    }
}