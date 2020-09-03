//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Query.Core;
    using Microsoft.Azure.Cosmos.Query.Core.Monads;
    using Microsoft.Azure.Cosmos.Resource.CosmosExceptions;
    using Microsoft.Azure.Cosmos.Serializer;
    using Microsoft.Azure.Documents;
    using static Microsoft.Azure.Documents.RuntimeConstants;

    /// <summary>
    /// Cosmos feed stream iterator. This is used to get the query responses with a Stream content
    /// </summary>
    internal sealed class FeedRangeIteratorCore : FeedIteratorInternal
    {
        internal readonly FeedRangeInternal FeedRangeInternal;
        internal FeedRangeContinuation FeedRangeContinuation { get; private set; }
        private readonly ContainerInternal containerCore;
        private readonly CosmosClientContext clientContext;
        private readonly QueryRequestOptions queryRequestOptions;
        private readonly AsyncLazy<TryCatch<string>> lazyContainerRid;
        private readonly ResourceType resourceType;
        private readonly SqlQuerySpec querySpec;
        private bool hasMoreResultsInternal;

        public static FeedRangeIteratorCore Create(
            ContainerInternal containerCore,
            FeedRangeInternal feedRangeInternal,
            string continuation,
            QueryRequestOptions options,
            ResourceType resourceType = ResourceType.Document,
            QueryDefinition queryDefinition = null)
        {
            if (!string.IsNullOrEmpty(continuation))
            {
                if (FeedRangeContinuation.TryParse(continuation, out FeedRangeContinuation feedRangeContinuation))
                {
                    return new FeedRangeIteratorCore(containerCore, feedRangeContinuation, options, resourceType, queryDefinition);
                }

                // Backward compatible with old format
                feedRangeInternal = FeedRangeEpk.FullRange;
                feedRangeContinuation = new FeedRangeCompositeContinuation(
                    string.Empty,
                    feedRangeInternal,
                    new List<Documents.Routing.Range<string>>()
                    {
                            new Documents.Routing.Range<string>(
                                Documents.Routing.PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                                Documents.Routing.PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                                isMinInclusive: true,
                                isMaxInclusive: false)
                    },
                    continuation);
                return new FeedRangeIteratorCore(containerCore, feedRangeContinuation, options, resourceType, queryDefinition);
            }

            feedRangeInternal ??= FeedRangeEpk.FullRange;
            return new FeedRangeIteratorCore(containerCore, feedRangeInternal, options, resourceType, queryDefinition);
        }

        /// <summary>
        /// For unit tests
        /// </summary>
        internal FeedRangeIteratorCore(
            ContainerInternal containerCore,
            FeedRangeContinuation feedRangeContinuation,
            QueryRequestOptions options,
            ResourceType resourceType,
            QueryDefinition queryDefinition)
            : this(containerCore, feedRangeContinuation.FeedRange, options, resourceType, queryDefinition)
        {
            FeedRangeContinuation = feedRangeContinuation;
        }

        private FeedRangeIteratorCore(
            ContainerInternal containerCore,
            FeedRangeInternal feedRangeInternal,
            QueryRequestOptions options,
            ResourceType resourceType,
            QueryDefinition queryDefinition)
            : this(containerCore, options, resourceType, queryDefinition)
        {
            FeedRangeInternal = feedRangeInternal ?? throw new ArgumentNullException(nameof(feedRangeInternal));
        }

        private FeedRangeIteratorCore(
            ContainerInternal containerCore,
            QueryRequestOptions options,
            ResourceType resourceType,
            QueryDefinition queryDefinition)
        {
            this.containerCore = containerCore ?? throw new ArgumentNullException(nameof(containerCore));
            clientContext = containerCore.ClientContext;
            queryRequestOptions = options;
            hasMoreResultsInternal = true;
            this.resourceType = resourceType;
            querySpec = queryDefinition?.ToSqlQuerySpec();
            lazyContainerRid = new AsyncLazy<TryCatch<string>>(valueFactory: (innerCancellationToken) =>
            {
                return TryInitializeContainerRIdAsync(innerCancellationToken);
            });
        }

        public override bool HasMoreResults => hasMoreResultsInternal;

        /// <summary>
        /// Get the next set of results from the cosmos service
        /// </summary>
        /// <param name="cancellationToken">(Optional) <see cref="CancellationToken"/> representing request cancellation.</param>
        /// <returns>A query response from cosmos service</returns>
        public override async Task<ResponseMessage> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            CosmosDiagnosticsContext diagnostics = CosmosDiagnosticsContext.Create(queryRequestOptions);
            using (diagnostics.GetOverallScope())
            {
                if (!lazyContainerRid.ValueInitialized)
                {
                    using (diagnostics.CreateScope("InitializeContainerResourceId"))
                    {
                        TryCatch<string> tryInitializeContainerRId = await lazyContainerRid.GetValueAsync(cancellationToken);
                        if (!tryInitializeContainerRId.Succeeded)
                        {
                            CosmosException cosmosException = tryInitializeContainerRId.Exception.InnerException as CosmosException;
                            return cosmosException.ToCosmosResponseMessage(new RequestMessage(method: null, requestUriString: null, diagnosticsContext: diagnostics));
                        }
                    }

                    using (diagnostics.CreateScope("InitializeContinuation"))
                    {
                        if (FeedRangeContinuation != null)
                        {
                            TryCatch validateContainer = FeedRangeContinuation.ValidateContainer(lazyContainerRid.Result.Result);
                            if (!validateContainer.Succeeded)
                            {
                                return CosmosExceptionFactory.CreateBadRequestException(
                                    message: validateContainer.Exception.InnerException.Message,
                                    innerException: validateContainer.Exception.InnerException,
                                    diagnosticsContext: diagnostics).ToCosmosResponseMessage(new RequestMessage(method: null, requestUriString: null, diagnosticsContext: diagnostics));
                            }
                        }
                        else
                        {
                            await InitializeFeedContinuationAsync(cancellationToken);
                        }
                    }
                }

                return await ReadNextInternalAsync(diagnostics, cancellationToken);
            }
        }

        private async Task<ResponseMessage> ReadNextInternalAsync(
            CosmosDiagnosticsContext diagnostics,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Stream stream = null;
            OperationType operation = OperationType.ReadFeed;
            if (querySpec != null)
            {
                stream = clientContext.SerializerCore.ToStreamSqlQuerySpec(querySpec, resourceType);
                operation = OperationType.Query;
            }

            ResponseMessage responseMessage = await clientContext.ProcessResourceOperationStreamAsync(
               resourceUri: containerCore.LinkUri,
               resourceType: resourceType,
               operationType: operation,
               requestOptions: queryRequestOptions,
               cosmosContainerCore: containerCore,
               partitionKey: queryRequestOptions?.PartitionKey,
               streamPayload: stream,
               requestEnricher: request =>
               {
                   // FeedRangeContinuationRequestMessagePopulatorVisitor needs to run before FeedRangeRequestMessagePopulatorVisitor,
                   // since they both set EPK range headers and in the case of split we need to run on the child range and not the parent range.
                   FeedRangeContinuationRequestMessagePopulatorVisitor feedRangeContinuationVisitor = new FeedRangeContinuationRequestMessagePopulatorVisitor(
                       request,
                       QueryRequestOptions.FillContinuationToken);
                   FeedRangeContinuation.Accept(feedRangeContinuationVisitor);

                   FeedRangeRequestMessagePopulatorVisitor feedRangeVisitor = new FeedRangeRequestMessagePopulatorVisitor(request);
                   FeedRangeInternal.Accept(feedRangeVisitor);

                   if (querySpec != null)
                   {
                       request.Headers.Add(HttpConstants.HttpHeaders.ContentType, MediaTypes.QueryJson);
                       request.Headers.Add(HttpConstants.HttpHeaders.IsQuery, bool.TrueString);
                   }
               },
               diagnosticsContext: diagnostics,
               cancellationToken: cancellationToken);

            ShouldRetryResult shouldRetryOnSplit = await FeedRangeContinuation.HandleSplitAsync(containerCore, responseMessage, cancellationToken);
            if (shouldRetryOnSplit.ShouldRetry)
            {
                return await ReadNextInternalAsync(diagnostics, cancellationToken);
            }

            if (responseMessage.Content != null)
            {
                await CosmosElementSerializer.RewriteStreamAsTextAsync(responseMessage, queryRequestOptions);
            }

            if (responseMessage.IsSuccessStatusCode)
            {
                FeedRangeContinuation.ReplaceContinuation(responseMessage.Headers.ContinuationToken);
                hasMoreResultsInternal = !FeedRangeContinuation.IsDone;
                return FeedRangeResponse.CreateSuccess(responseMessage, FeedRangeContinuation);
            }
            else
            {
                hasMoreResultsInternal = false;
                return FeedRangeResponse.CreateFailure(responseMessage);
            }
        }

        private async Task<TryCatch<string>> TryInitializeContainerRIdAsync(CancellationToken cancellationToken)
        {
            try
            {
                string containerRId = await containerCore.GetRIDAsync(cancellationToken);
                return TryCatch<string>.FromResult(containerRId);
            }
            catch (CosmosException cosmosException)
            {
                return TryCatch<string>.FromException(cosmosException);
            }
        }

        private async Task InitializeFeedContinuationAsync(CancellationToken cancellationToken)
        {
            Routing.PartitionKeyRangeCache partitionKeyRangeCache = await clientContext.DocumentClient.GetPartitionKeyRangeCacheAsync();
            List<Documents.Routing.Range<string>> effectiveRanges = await FeedRangeInternal.GetEffectiveRangesAsync(
                routingMapProvider: partitionKeyRangeCache,
                containerRid: lazyContainerRid.Result.Result,
                partitionKeyDefinition: null);

            FeedRangeContinuation = new FeedRangeCompositeContinuation(
                containerRid: lazyContainerRid.Result.Result,
                feedRange: FeedRangeInternal,
                effectiveRanges);
        }

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            throw new NotImplementedException();
        }
    }
}
