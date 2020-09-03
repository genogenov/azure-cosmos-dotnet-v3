﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.FeedProcessing
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.FeedManagement;

    internal sealed class FeedEstimatorCore : FeedEstimator
    {
        private static TimeSpan defaultMonitoringDelay = TimeSpan.FromSeconds(5);
        private readonly ChangeFeedEstimatorDispatcher dispatcher;
        private readonly RemainingWorkEstimator remainingWorkEstimator;
        private readonly TimeSpan monitoringDelay;

        public FeedEstimatorCore(ChangeFeedEstimatorDispatcher dispatcher, RemainingWorkEstimator remainingWorkEstimator)
        {
            this.dispatcher = dispatcher;
            this.remainingWorkEstimator = remainingWorkEstimator;
            monitoringDelay = dispatcher.DispatchPeriod ?? FeedEstimatorCore.defaultMonitoringDelay;
        }

        public override async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TimeSpan delay = monitoringDelay;

                try
                {
                    long estimation = await remainingWorkEstimator.GetEstimatedRemainingWorkAsync(cancellationToken).ConfigureAwait(false);
                    await dispatcher.DispatchEstimationAsync(estimation, cancellationToken);
                }
                catch (TaskCanceledException canceledException)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }

                    Extensions.TraceException(new Exception("exception within estimator", canceledException));

                    // ignore as it is caused by client
                }

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}