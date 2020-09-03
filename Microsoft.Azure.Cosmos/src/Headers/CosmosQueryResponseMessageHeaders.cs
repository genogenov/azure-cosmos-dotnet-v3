//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// HTTP headers in a <see cref="ResponseMessage"/>.
    /// </summary>
    internal class CosmosQueryResponseMessageHeaders : Headers
    {
        public CosmosQueryResponseMessageHeaders(
            string continauationToken,
            string disallowContinuationTokenMessage,
            ResourceType resourceType,
            string containerRid)
        {
            base.ContinuationToken = continauationToken;
            DisallowContinuationTokenMessage = disallowContinuationTokenMessage;
            ResourceType = resourceType;
            ContainerRid = containerRid;
        }

        internal string DisallowContinuationTokenMessage { get; }

        public override string ContinuationToken
        {
            get
            {
                if (DisallowContinuationTokenMessage != null)
                {
                    throw new ArgumentException(DisallowContinuationTokenMessage);
                }

                return base.ContinuationToken;
            }

            internal set
            {
                throw new InvalidOperationException("To prevent the different aggregate context from impacting each other only allow updating the continuation token via clone method.");
            }
        }

        internal virtual string ContainerRid { get; }

        internal virtual ResourceType ResourceType { get; }

        internal string InternalContinuationToken => base.ContinuationToken;

        internal CosmosQueryResponseMessageHeaders CloneKnownProperties()
        {
            return CloneKnownProperties(
                InternalContinuationToken,
                DisallowContinuationTokenMessage);
        }

        internal CosmosQueryResponseMessageHeaders CloneKnownProperties(
            string continauationToken,
            string disallowContinuationTokenMessage)
        {
            return new CosmosQueryResponseMessageHeaders(
                continauationToken,
                disallowContinuationTokenMessage,
                ResourceType,
                ContainerRid)
            {
                RequestCharge = RequestCharge,
                ContentLength = ContentLength,
                ActivityId = ActivityId,
                ETag = ETag,
                Location = Location,
                RetryAfterLiteral = RetryAfterLiteral,
                SubStatusCodeLiteral = SubStatusCodeLiteral,
                ContentType = ContentType,
                QueryMetricsText = QueryMetricsText
            };
        }

        internal static CosmosQueryResponseMessageHeaders ConvertToQueryHeaders(
            Headers sourceHeaders,
            ResourceType resourceType,
            string containerRid)
        {
            if (sourceHeaders == null)
            {
                return new CosmosQueryResponseMessageHeaders(
                    continauationToken: null,
                    disallowContinuationTokenMessage: null,
                    resourceType: resourceType,
                    containerRid: containerRid);
            }

            return new CosmosQueryResponseMessageHeaders(
                continauationToken: sourceHeaders.ContinuationToken,
                disallowContinuationTokenMessage: null,
                resourceType: resourceType,
                containerRid: containerRid)
            {
                RequestCharge = sourceHeaders.RequestCharge,
                ContentLength = sourceHeaders.ContentLength,
                ActivityId = sourceHeaders.ActivityId,
                ETag = sourceHeaders.ETag,
                Location = sourceHeaders.Location,
                RetryAfterLiteral = sourceHeaders.RetryAfterLiteral,
                SubStatusCodeLiteral = sourceHeaders.SubStatusCodeLiteral,
                ContentType = sourceHeaders.ContentType,
                QueryMetricsText = sourceHeaders.QueryMetricsText
            };
        }
    }
}