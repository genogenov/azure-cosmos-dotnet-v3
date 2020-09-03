//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed
{
    using System;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Query.Core.ContinuationTokens;
    using Microsoft.Azure.Cosmos.Routing;

    /// <summary>
    /// Cosmos Stand-By Feed iterator implementing Composite Continuation Token
    /// </summary>
    /// <remarks>
    /// Legacy, see <see cref="ChangeFeedIteratorCore"/>.
    /// </remarks>
    /// <seealso cref="ChangeFeedIteratorCore"/>
    internal class StandByFeedIteratorCore : FeedIteratorInternal
    {
        internal StandByFeedContinuationToken compositeContinuationToken;

        private readonly CosmosClientContext clientContext;
        private readonly ContainerInternal container;
        private ChangeFeedStartFrom changeFeedStartFrom;
        private string containerRid;

        internal StandByFeedIteratorCore(
            CosmosClientContext clientContext,
            ContainerCore container,
            ChangeFeedStartFrom changeFeedStartFrom,
            ChangeFeedRequestOptions options)
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }

            this.clientContext = clientContext;
            this.container = container;
            this.changeFeedStartFrom = changeFeedStartFrom ?? throw new ArgumentNullException(nameof(changeFeedStartFrom));
            changeFeedOptions = options;
        }

        /// <summary>
        /// The query options for the result set
        /// </summary>
        protected readonly ChangeFeedRequestOptions changeFeedOptions;

        public override bool HasMoreResults => true;

        /// <summary>
        /// Get the next set of results from the cosmos service
        /// </summary>
        /// <param name="cancellationToken">(Optional) <see cref="CancellationToken"/> representing request cancellation.</param>
        /// <returns>A query response from cosmos service</returns>
        public override async Task<ResponseMessage> ReadNextAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            string firstNotModifiedKeyRangeId = null;
            string currentKeyRangeId;
            string nextKeyRangeId;
            ResponseMessage response;
            do
            {
                (currentKeyRangeId, response) = await ReadNextInternalAsync(cancellationToken);
                // Read only one range at a time - Breath first
                compositeContinuationToken.MoveToNextToken();
                (_, nextKeyRangeId) = await compositeContinuationToken.GetCurrentTokenAsync();
                if (response.StatusCode != HttpStatusCode.NotModified)
                {
                    break;
                }

                // HttpStatusCode.NotModified
                if (string.IsNullOrEmpty(firstNotModifiedKeyRangeId))
                {
                    // First NotModified Response
                    firstNotModifiedKeyRangeId = currentKeyRangeId;
                }
            }
            // We need to keep checking across all ranges until one of them returns OK or we circle back to the start
            while (!firstNotModifiedKeyRangeId.Equals(nextKeyRangeId, StringComparison.InvariantCultureIgnoreCase));

            // Send to the user the composite state for all ranges
            response.Headers.ContinuationToken = compositeContinuationToken.ToString();
            return response;
        }

        internal async Task<(string, ResponseMessage)> ReadNextInternalAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (compositeContinuationToken == null)
            {
                PartitionKeyRangeCache pkRangeCache = await clientContext.DocumentClient.GetPartitionKeyRangeCacheAsync();
                containerRid = await container.GetRIDAsync(cancellationToken);

                if (changeFeedStartFrom is ChangeFeedStartFromContinuation startFromContinuation)
                {
                    compositeContinuationToken = await StandByFeedContinuationToken.CreateAsync(
                        containerRid,
                        startFromContinuation.Continuation,
                        pkRangeCache.TryGetOverlappingRangesAsync);
                    (CompositeContinuationToken token, string id) = await compositeContinuationToken.GetCurrentTokenAsync();

                    if (token.Token != null)
                    {
                        changeFeedStartFrom = ChangeFeedStartFrom.ContinuationToken(token.Token);
                    }
                    else
                    {
                        changeFeedStartFrom = ChangeFeedStartFrom.Beginning();
                    }
                }
                else
                {
                    compositeContinuationToken = await StandByFeedContinuationToken.CreateAsync(
                        containerRid,
                        initialStandByFeedContinuationToken: null,
                        pkRangeCache.TryGetOverlappingRangesAsync);
                }
            }

            (CompositeContinuationToken currentRangeToken, string rangeId) = await compositeContinuationToken.GetCurrentTokenAsync();
            FeedRange feedRange = new FeedRangePartitionKeyRange(rangeId);
            if (currentRangeToken.Token != null)
            {
                changeFeedStartFrom = new ChangeFeedStartFromContinuationAndFeedRange(currentRangeToken.Token, (FeedRangeInternal)feedRange);
            }
            else
            {
                changeFeedStartFrom = ChangeFeedStartFrom.Beginning(feedRange);
            }

            ResponseMessage response = await NextResultSetDelegateAsync(changeFeedOptions, cancellationToken);
            if (await ShouldRetryFailureAsync(response, cancellationToken))
            {
                return await ReadNextInternalAsync(cancellationToken);
            }

            if (response.IsSuccessStatusCode
                || response.StatusCode == HttpStatusCode.NotModified)
            {
                // Change Feed read uses Etag for continuation
                currentRangeToken.Token = response.Headers.ETag;
            }

            return (rangeId, response);
        }

        /// <summary>
        /// During Feed read, split can happen or Max Item count can go beyond the max response size
        /// </summary>
        internal async Task<bool> ShouldRetryFailureAsync(
            ResponseMessage response,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.NotModified)
            {
                return false;
            }

            bool partitionSplit = response.StatusCode == HttpStatusCode.Gone
                && (response.Headers.SubStatusCode == Documents.SubStatusCodes.PartitionKeyRangeGone || response.Headers.SubStatusCode == Documents.SubStatusCodes.CompletingSplit);
            if (partitionSplit)
            {
                // Forcing stale refresh of Partition Key Ranges Cache
                await compositeContinuationToken.GetCurrentTokenAsync(forceRefresh: true);
                return true;
            }

            return false;
        }

        internal virtual Task<ResponseMessage> NextResultSetDelegateAsync(
            ChangeFeedRequestOptions options,
            CancellationToken cancellationToken)
        {
            string resourceUri = container.LinkUri;
            return clientContext.ProcessResourceOperationAsync<ResponseMessage>(
                resourceUri: resourceUri,
                resourceType: Documents.ResourceType.Document,
                operationType: Documents.OperationType.ReadFeed,
                requestOptions: options,
                containerInternal: container,
                requestEnricher: (request) =>
                {
                    ChangeFeedStartFromRequestOptionPopulator visitor = new ChangeFeedStartFromRequestOptionPopulator(request);
                    changeFeedStartFrom.Accept(visitor);
                },
                responseCreator: response => response,
                partitionKey: default,
                streamPayload: default,
                diagnosticsContext: default,
                cancellationToken: cancellationToken);
        }

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            throw new NotImplementedException();
        }
    }
}