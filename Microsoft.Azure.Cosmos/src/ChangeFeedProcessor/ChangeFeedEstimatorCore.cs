//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.Configuration;
    using Microsoft.Azure.Cosmos.ChangeFeed.FeedManagement;
    using Microsoft.Azure.Cosmos.ChangeFeed.FeedProcessing;
    using Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement;
    using Microsoft.Azure.Cosmos.ChangeFeed.Utils;
    using Microsoft.Azure.Cosmos.Core.Trace;
    using static Microsoft.Azure.Cosmos.Container;

    internal sealed class ChangeFeedEstimatorCore : ChangeFeedProcessor
    {
        private const string EstimatorDefaultHostName = "Estimator";

        private readonly ChangesEstimationHandler initialEstimateDelegate;
        private CancellationTokenSource shutdownCts;
        private ContainerInternal leaseContainer;
        private string monitoredContainerRid;
        private TimeSpan? estimatorPeriod = null;
        private ContainerInternal monitoredContainer;
        private DocumentServiceLeaseStoreManager documentServiceLeaseStoreManager;
        private FeedEstimator feedEstimator;
        private RemainingWorkEstimator remainingWorkEstimator;
        private ChangeFeedLeaseOptions changeFeedLeaseOptions;
        private bool initialized = false;

        private Task runAsync;

        public ChangeFeedEstimatorCore(
            ChangesEstimationHandler initialEstimateDelegate,
            TimeSpan? estimatorPeriod)
        {
            if (initialEstimateDelegate == null)
            {
                throw new ArgumentNullException(nameof(initialEstimateDelegate));
            }

            if (estimatorPeriod.HasValue && estimatorPeriod.Value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(estimatorPeriod));
            }

            this.initialEstimateDelegate = initialEstimateDelegate;
            this.estimatorPeriod = estimatorPeriod;
        }

        internal ChangeFeedEstimatorCore(
            ChangesEstimationHandler initialEstimateDelegate,
            TimeSpan? estimatorPeriod,
            RemainingWorkEstimator remainingWorkEstimator)
            : this(initialEstimateDelegate, estimatorPeriod)
        {
            this.remainingWorkEstimator = remainingWorkEstimator;
        }

        public void ApplyBuildConfiguration(
            DocumentServiceLeaseStoreManager customDocumentServiceLeaseStoreManager,
            ContainerInternal leaseContainer,
            string monitoredContainerRid,
            string instanceName,
            ChangeFeedLeaseOptions changeFeedLeaseOptions,
            ChangeFeedProcessorOptions changeFeedProcessorOptions,
            ContainerInternal monitoredContainer)
        {
            if (monitoredContainer == null)
            {
                throw new ArgumentNullException(nameof(monitoredContainer));
            }

            if (leaseContainer == null && customDocumentServiceLeaseStoreManager == null)
            {
                throw new ArgumentNullException(nameof(leaseContainer));
            }

            documentServiceLeaseStoreManager = customDocumentServiceLeaseStoreManager;
            this.leaseContainer = leaseContainer;
            this.monitoredContainerRid = monitoredContainerRid;
            this.monitoredContainer = monitoredContainer;
            this.changeFeedLeaseOptions = changeFeedLeaseOptions;
        }

        public override async Task StartAsync()
        {
            if (!initialized)
            {
                await InitializeAsync().ConfigureAwait(false);
            }

            shutdownCts = new CancellationTokenSource();
            DefaultTrace.TraceInformation("Starting estimator...");
            runAsync = feedEstimator.RunAsync(shutdownCts.Token);
        }

        public override async Task StopAsync()
        {
            DefaultTrace.TraceInformation("Stopping estimator...");
            shutdownCts.Cancel();
            try
            {
                await runAsync.ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                // Expected during shutdown
            }
        }

        private async Task InitializeAsync()
        {
            string monitoredContainerRid = await monitoredContainer.GetMonitoredContainerRidAsync(this.monitoredContainerRid);
            this.monitoredContainerRid = monitoredContainer.GetLeasePrefix(changeFeedLeaseOptions, monitoredContainerRid);
            documentServiceLeaseStoreManager = await ChangeFeedProcessorCore<dynamic>.InitializeLeaseStoreManagerAsync(documentServiceLeaseStoreManager, leaseContainer, this.monitoredContainerRid, ChangeFeedEstimatorCore.EstimatorDefaultHostName).ConfigureAwait(false);
            feedEstimator = BuildFeedEstimator();
            initialized = true;
        }

        private FeedEstimator BuildFeedEstimator()
        {
            if (remainingWorkEstimator == null)
            {
                Func<string, string, bool, FeedIterator> feedCreator = (string partitionKeyRangeId, string continuationToken, bool startFromBeginning) =>
                {
                    return ResultSetIteratorUtils.BuildResultSetIterator(
                        partitionKeyRangeId: partitionKeyRangeId,
                        continuationToken: continuationToken,
                        maxItemCount: 1,
                        container: monitoredContainer,
                        startTime: null,
                        startFromBeginning: string.IsNullOrEmpty(continuationToken));
                };

                remainingWorkEstimator = new RemainingWorkEstimatorCore(
                   documentServiceLeaseStoreManager.LeaseContainer,
                   feedCreator,
                   monitoredContainer.ClientContext.Client.ClientOptions?.GatewayModeMaxConnectionLimit ?? 1);
            }

            ChangeFeedEstimatorDispatcher estimatorDispatcher = new ChangeFeedEstimatorDispatcher(initialEstimateDelegate, estimatorPeriod);

            return new FeedEstimatorCore(estimatorDispatcher, remainingWorkEstimator);
        }
    }
}