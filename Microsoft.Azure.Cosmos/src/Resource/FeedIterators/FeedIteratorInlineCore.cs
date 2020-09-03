﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;

    internal sealed class FeedIteratorInlineCore : FeedIteratorInternal
    {
        private readonly FeedIteratorInternal feedIteratorInternal;

        internal FeedIteratorInlineCore(
            FeedIterator feedIterator)
        {
            if (!(feedIterator is FeedIteratorInternal feedIteratorInternal))
            {
                throw new ArgumentNullException(nameof(feedIterator));
            }

            this.feedIteratorInternal = feedIteratorInternal;
        }

        internal FeedIteratorInlineCore(
            FeedIteratorInternal feedIteratorInternal)
        {
            this.feedIteratorInternal = feedIteratorInternal ?? throw new ArgumentNullException(nameof(feedIteratorInternal));
        }

        public override bool HasMoreResults => feedIteratorInternal.HasMoreResults;

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            return feedIteratorInternal.GetCosmosElementContinuationToken();
        }

        public override Task<ResponseMessage> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            return TaskHelper.RunInlineIfNeededAsync(() => feedIteratorInternal.ReadNextAsync(cancellationToken));
        }

        protected override void Dispose(bool disposing)
        {
            feedIteratorInternal.Dispose();
            base.Dispose(disposing);
        }
    }
}
