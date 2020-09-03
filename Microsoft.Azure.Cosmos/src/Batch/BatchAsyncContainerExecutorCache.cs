//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Cache to create and share Executor instances across the client's lifetime.
    /// </summary>
    internal class BatchAsyncContainerExecutorCache : IDisposable
    {
        // Keeping same performance tuned value of Bulk V2.
        internal const int DefaultMaxBulkRequestBodySizeInBytes = 220201;
        private readonly ConcurrentDictionary<string, BatchAsyncContainerExecutor> executorsPerContainer = new ConcurrentDictionary<string, BatchAsyncContainerExecutor>();

        public BatchAsyncContainerExecutor GetExecutorForContainer(
            ContainerInternal container,
            CosmosClientContext cosmosClientContext)
        {
            if (!cosmosClientContext.ClientOptions.AllowBulkExecution)
            {
                throw new InvalidOperationException("AllowBulkExecution is not currently enabled.");
            }

            string containerLink = container.LinkUri.ToString();
            if (executorsPerContainer.TryGetValue(containerLink, out BatchAsyncContainerExecutor executor))
            {
                return executor;
            }

            BatchAsyncContainerExecutor newExecutor = new BatchAsyncContainerExecutor(
                container,
                cosmosClientContext,
                Constants.MaxOperationsInDirectModeBatchRequest,
                DefaultMaxBulkRequestBodySizeInBytes);
            if (!executorsPerContainer.TryAdd(containerLink, newExecutor))
            {
                newExecutor.Dispose();
            }

            return executorsPerContainer[containerLink];
        }

        public void Dispose()
        {
            foreach (KeyValuePair<string, BatchAsyncContainerExecutor> cacheEntry in executorsPerContainer)
            {
                cacheEntry.Value.Dispose();
            }
        }
    }
}