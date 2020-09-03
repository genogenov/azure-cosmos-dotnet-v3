// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;

    /// <summary>
    /// Visitor to populate RequestMessage headers and properties based on FeedRange.
    /// </summary>
    internal sealed class FeedRangeRequestMessagePopulatorVisitor : IFeedRangeVisitor
    {
        private readonly RequestMessage request;

        public FeedRangeRequestMessagePopulatorVisitor(RequestMessage request)
        {
            this.request = request ?? throw new ArgumentNullException(nameof(request));
        }

        public void Visit(FeedRangePartitionKey feedRange)
        {
            request.Headers.PartitionKey = feedRange.PartitionKey.ToJsonString();
        }

        public void Visit(FeedRangePartitionKeyRange feedRange)
        {
            request.PartitionKeyRangeId = new Documents.PartitionKeyRangeIdentity(feedRange.PartitionKeyRangeId);
        }

        public void Visit(FeedRangeEpk feedRange)
        {
            // In case EPK has already been set by compute
            if (!request.Properties.ContainsKey(HandlerConstants.StartEpkString))
            {
                request.Properties[HandlerConstants.StartEpkString] = feedRange.Range.Min;
                request.Properties[HandlerConstants.EndEpkString] = feedRange.Range.Max;
            }
        }
    }
}
