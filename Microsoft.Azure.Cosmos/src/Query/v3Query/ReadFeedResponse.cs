//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos
{
    using System.Collections.Generic;
    using System.Net;

    internal class ReadFeedResponse<T> : FeedResponse<T>
    {
        protected ReadFeedResponse(
            HttpStatusCode httpStatusCode,
            IReadOnlyCollection<T> resources,
            Headers responseMessageHeaders,
            CosmosDiagnostics diagnostics)
        {
            Count = resources?.Count ?? 0;
            Headers = responseMessageHeaders;
            StatusCode = httpStatusCode;
            Diagnostics = diagnostics;
            Resource = resources;
        }

        public override int Count { get; }

        public override string ContinuationToken => Headers?.ContinuationToken;

        public override Headers Headers { get; }

        public override IEnumerable<T> Resource { get; }

        public override HttpStatusCode StatusCode { get; }

        public override CosmosDiagnostics Diagnostics { get; }

        public override IEnumerator<T> GetEnumerator()
        {
            return Resource.GetEnumerator();
        }

        internal static ReadFeedResponse<TInput> CreateResponse<TInput>(
            ResponseMessage responseMessage,
            CosmosSerializerCore serializerCore)
        {
            using (responseMessage)
            {
                // ReadFeed can return 304 on some scenarios (Change Feed for example)
                if (responseMessage.StatusCode != HttpStatusCode.NotModified)
                {
                    responseMessage.EnsureSuccessStatusCode();
                }

                IReadOnlyCollection<TInput> resources = CosmosFeedResponseSerializer.FromFeedResponseStream<TInput>(
                        serializerCore,
                        responseMessage.Content);

                ReadFeedResponse<TInput> readFeedResponse = new ReadFeedResponse<TInput>(
                    httpStatusCode: responseMessage.StatusCode,
                    resources: resources,
                    responseMessageHeaders: responseMessage.Headers,
                    diagnostics: responseMessage.Diagnostics);

                return readFeedResponse;
            }
        }
    }
}