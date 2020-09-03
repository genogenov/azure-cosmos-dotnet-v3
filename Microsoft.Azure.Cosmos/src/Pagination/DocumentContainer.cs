// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Pagination
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Query.Core.Monads;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Composes a <see cref="IMonadicDocumentContainer"/> and creates an <see cref="IDocumentContainer"/>.
    /// </summary>
    internal sealed class DocumentContainer : IDocumentContainer
    {
        private readonly IMonadicDocumentContainer monadicDocumentContainer;

        public DocumentContainer(IMonadicDocumentContainer monadicDocumentContainer)
        {
            this.monadicDocumentContainer = monadicDocumentContainer ?? throw new ArgumentNullException(nameof(monadicDocumentContainer));
        }

        public Task<TryCatch<List<PartitionKeyRange>>> MonadicGetChildRangeAsync(
            PartitionKeyRange partitionKeyRange,
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicGetChildRangeAsync(
                partitionKeyRange,
                cancellationToken);

        public Task<List<PartitionKeyRange>> GetChildRangeAsync(
            PartitionKeyRange partitionKeyRange,
            CancellationToken cancellationToken) => TryCatch<List<PartitionKeyRange>>.UnsafeGetResultAsync(
                MonadicGetChildRangeAsync(
                    partitionKeyRange,
                    cancellationToken),
                cancellationToken);

        public Task<TryCatch<List<PartitionKeyRange>>> MonadicGetFeedRangesAsync(
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicGetFeedRangesAsync(
                cancellationToken);

        public Task<List<PartitionKeyRange>> GetFeedRangesAsync(
            CancellationToken cancellationToken) => TryCatch<List<PartitionKeyRange>>.UnsafeGetResultAsync(
                MonadicGetFeedRangesAsync(
                    cancellationToken),
                cancellationToken);

        public Task<TryCatch<Record>> MonadicCreateItemAsync(
            CosmosObject payload,
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicCreateItemAsync(
                payload,
                cancellationToken);

        public Task<Record> CreateItemAsync(
            CosmosObject payload,
            CancellationToken cancellationToken) => TryCatch<List<PartitionKeyRange>>.UnsafeGetResultAsync(
                MonadicCreateItemAsync(
                    payload,
                    cancellationToken),
                cancellationToken);

        public Task<TryCatch<Record>> MonadicReadItemAsync(
            CosmosElement partitionKey,
            Guid identifer,
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicReadItemAsync(
                partitionKey,
                identifer,
                cancellationToken);

        public Task<Record> ReadItemAsync(
            CosmosElement partitionKey,
            Guid identifier,
            CancellationToken cancellationToken) => TryCatch<Record>.UnsafeGetResultAsync(
                MonadicReadItemAsync(
                    partitionKey,
                    identifier,
                    cancellationToken),
                cancellationToken);

        public Task<TryCatch<DocumentContainerPage>> MonadicReadFeedAsync(
            int partitionKeyRangeId,
            long resourceIdentifer,
            int pageSize,
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicReadFeedAsync(
                partitionKeyRangeId,
                resourceIdentifer,
                pageSize,
                cancellationToken);

        public Task<DocumentContainerPage> ReadFeedAsync(
            int partitionKeyRangeId,
            long resourceIdentifier,
            int pageSize,
            CancellationToken cancellationToken) => TryCatch<DocumentContainerPage>.UnsafeGetResultAsync(
                MonadicReadFeedAsync(
                    partitionKeyRangeId,
                    resourceIdentifier,
                    pageSize,
                    cancellationToken),
                cancellationToken);

        public Task<TryCatch> MonadicSplitAsync(
            int partitionKeyRangeId,
            CancellationToken cancellationToken) => monadicDocumentContainer.MonadicSplitAsync(
                partitionKeyRangeId,
                cancellationToken);

        public Task SplitAsync(
            int partitionKeyRangeId,
            CancellationToken cancellationToken) => TryCatch.UnsafeWaitAsync(
                MonadicSplitAsync(
                    partitionKeyRangeId,
                    cancellationToken),
                cancellationToken);
    }
}
