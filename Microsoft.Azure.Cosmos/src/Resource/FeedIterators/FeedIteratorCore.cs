//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Query.Core;
    using Microsoft.Azure.Cosmos.Serializer;
    using Microsoft.Azure.Documents;
    using static Microsoft.Azure.Documents.RuntimeConstants;

    /// <summary>
    /// Cosmos feed stream iterator. This is used to get the query responses with a Stream content for non-partitioned results
    /// </summary>
    internal sealed class FeedIteratorCore : FeedIteratorInternal
    {
        private readonly CosmosClientContext clientContext;
        private readonly string resourceLink;
        private readonly ResourceType resourceType;
        private readonly SqlQuerySpec querySpec;
        private bool hasMoreResultsInternal;

        public FeedIteratorCore(
            CosmosClientContext clientContext,
            string resourceLink,
            ResourceType resourceType,
            QueryDefinition queryDefinition,
            string continuationToken,
            QueryRequestOptions options)
        {
            this.resourceLink = resourceLink;
            this.clientContext = clientContext;
            this.resourceType = resourceType;
            querySpec = queryDefinition?.ToSqlQuerySpec();
            ContinuationToken = continuationToken;
            requestOptions = options;
            hasMoreResultsInternal = true;
        }

        public override bool HasMoreResults => hasMoreResultsInternal;

        /// <summary>
        /// The query options for the result set
        /// </summary>
        public QueryRequestOptions requestOptions { get; }

        /// <summary>
        /// The Continuation Token
        /// </summary>
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Get the next set of results from the cosmos service
        /// </summary>
        /// <param name="cancellationToken">(Optional) <see cref="CancellationToken"/> representing request cancellation.</param>
        /// <returns>A query response from cosmos service</returns>
        public override async Task<ResponseMessage> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            CosmosDiagnosticsContext diagnosticsContext = CosmosDiagnosticsContext.Create(requestOptions);
            using (diagnosticsContext.GetOverallScope())
            {
                return await ReadNextInternalAsync(diagnosticsContext, cancellationToken);
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
               resourceUri: resourceLink,
               resourceType: resourceType,
               operationType: operation,
               requestOptions: requestOptions,
               cosmosContainerCore: null,
               partitionKey: requestOptions?.PartitionKey,
               streamPayload: stream,
               requestEnricher: request =>
               {
                   QueryRequestOptions.FillContinuationToken(request, ContinuationToken);
                   if (querySpec != null)
                   {
                       request.Headers.Add(HttpConstants.HttpHeaders.ContentType, MediaTypes.QueryJson);
                       request.Headers.Add(HttpConstants.HttpHeaders.IsQuery, bool.TrueString);
                   }
               },
               diagnosticsContext: diagnostics,
               cancellationToken: cancellationToken);

            ContinuationToken = responseMessage.Headers.ContinuationToken;
            hasMoreResultsInternal = ContinuationToken != null && responseMessage.StatusCode != HttpStatusCode.NotModified;

            if (responseMessage.Content != null)
            {
                await CosmosElementSerializer.RewriteStreamAsTextAsync(responseMessage, requestOptions);
            }

            return responseMessage;
        }

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Cosmos feed iterator that keeps track of the continuation token when retrieving results form a query.
    /// </summary>
    /// <typeparam name="T">The response object type that can be deserialized</typeparam>
    internal sealed class FeedIteratorCore<T> : FeedIteratorInternal<T>
    {
        private readonly FeedIteratorInternal feedIterator;
        private readonly Func<ResponseMessage, FeedResponse<T>> responseCreator;

        internal FeedIteratorCore(
            FeedIteratorInternal feedIterator,
            Func<ResponseMessage, FeedResponse<T>> responseCreator)
        {
            this.responseCreator = responseCreator;
            this.feedIterator = feedIterator;
        }

        public override bool HasMoreResults => feedIterator.HasMoreResults;

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            return feedIterator.GetCosmosElementContinuationToken();
        }

        /// <summary>
        /// Get the next set of results from the cosmos service
        /// </summary>
        /// <param name="cancellationToken">(Optional) <see cref="CancellationToken"/> representing request cancellation.</param>
        /// <returns>A query response from cosmos service</returns>
        public override async Task<FeedResponse<T>> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ResponseMessage response = await feedIterator.ReadNextAsync(cancellationToken);
            return responseCreator(response);
        }

        protected override void Dispose(bool disposing)
        {
            feedIterator.Dispose();
            base.Dispose(disposing);
        }
    }
}
