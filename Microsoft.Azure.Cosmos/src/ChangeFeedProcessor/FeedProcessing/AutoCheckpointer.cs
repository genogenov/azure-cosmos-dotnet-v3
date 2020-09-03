//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.FeedProcessing
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.Configuration;

    internal sealed class AutoCheckpointer<T> : ChangeFeedObserver<T>
    {
        private readonly CheckpointFrequency checkpointFrequency;
        private readonly ChangeFeedObserver<T> observer;
        private int processedDocCount;
        private DateTime lastCheckpointTime = DateTime.UtcNow;

        public AutoCheckpointer(CheckpointFrequency checkpointFrequency, ChangeFeedObserver<T> observer)
        {
            if (checkpointFrequency == null)
            {
                throw new ArgumentNullException(nameof(checkpointFrequency));
            }

            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            this.checkpointFrequency = checkpointFrequency;
            this.observer = observer;
        }

        public override Task OpenAsync(ChangeFeedObserverContext context)
        {
            return observer.OpenAsync(context);
        }

        public override Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return observer.CloseAsync(context, reason);
        }

        public override async Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyCollection<T> docs, CancellationToken cancellationToken)
        {
            await observer.ProcessChangesAsync(context, docs, cancellationToken).ConfigureAwait(false);
            processedDocCount += docs.Count;

            if (IsCheckpointNeeded())
            {
                await context.CheckpointAsync().ConfigureAwait(false);
                processedDocCount = 0;
                lastCheckpointTime = DateTime.UtcNow;
            }
        }

        private bool IsCheckpointNeeded()
        {
            if (!checkpointFrequency.ProcessedDocumentCount.HasValue && !checkpointFrequency.TimeInterval.HasValue)
            {
                return true;
            }

            if (processedDocCount >= checkpointFrequency.ProcessedDocumentCount)
            {
                return true;
            }

            TimeSpan delta = DateTime.UtcNow - lastCheckpointTime;
            if (delta >= checkpointFrequency.TimeInterval)
            {
                return true;
            }

            return false;
        }
    }
}