//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using Microsoft.Azure.Cosmos.ChangeFeed.Configuration;
    using Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement;

    /// <summary>
    /// Provides a flexible way to create an instance of <see cref="ChangeFeedProcessor"/> with custom set of parameters.
    /// </summary>
    public class ChangeFeedProcessorBuilder
    {
        private const string InMemoryDefaultHostName = "InMemory";

        private readonly ContainerInternal monitoredContainer;
        private readonly ChangeFeedProcessor changeFeedProcessor;
        private readonly ChangeFeedLeaseOptions changeFeedLeaseOptions;
        private readonly Action<DocumentServiceLeaseStoreManager,
                ContainerInternal,
                string,
                string,
                ChangeFeedLeaseOptions,
                ChangeFeedProcessorOptions,
                ContainerInternal> applyBuilderConfiguration;

        private ChangeFeedProcessorOptions changeFeedProcessorOptions;

        private ContainerInternal leaseContainer;
        private string InstanceName;
        private DocumentServiceLeaseStoreManager LeaseStoreManager;
        private string monitoredContainerRid;
        private bool isBuilt;

        internal ChangeFeedProcessorBuilder(
            string processorName,
            ContainerInternal container,
            ChangeFeedProcessor changeFeedProcessor,
            Action<DocumentServiceLeaseStoreManager,
                ContainerInternal,
                string,
                string,
                ChangeFeedLeaseOptions,
                ChangeFeedProcessorOptions,
                ContainerInternal> applyBuilderConfiguration)
        {
            changeFeedLeaseOptions = new ChangeFeedLeaseOptions
            {
                LeasePrefix = processorName
            };
            monitoredContainer = container;
            this.changeFeedProcessor = changeFeedProcessor;
            this.applyBuilderConfiguration = applyBuilderConfiguration;
        }

        /// <summary>
        /// Sets the Host name.
        /// </summary>
        /// <param name="instanceName">Name to be used for the processor instance. When using multiple processor hosts, each host must have a unique name.</param>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        public ChangeFeedProcessorBuilder WithInstanceName(string instanceName)
        {
            InstanceName = instanceName;
            return this;
        }

        /// <summary>
        /// Sets a custom configuration to be used by this instance of <see cref="ChangeFeedProcessor"/> to control how leases are maintained in a container when using <see cref="WithLeaseContainer"/>.
        /// </summary>
        /// <param name="acquireInterval">Interval to kick off a task to verify if leases are distributed evenly among known host instances.</param>
        /// <param name="expirationInterval">Interval for which the lease is taken. If the lease is not renewed within this interval, it will cause it to expire and ownership of the lease will move to another processor instance.</param>
        /// <param name="renewInterval">Renew interval for all leases currently held by a particular processor instance.</param>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        public ChangeFeedProcessorBuilder WithLeaseConfiguration(
            TimeSpan? acquireInterval = null,
            TimeSpan? expirationInterval = null,
            TimeSpan? renewInterval = null)
        {
            changeFeedLeaseOptions.LeaseRenewInterval = renewInterval ?? ChangeFeedLeaseOptions.DefaultRenewInterval;
            changeFeedLeaseOptions.LeaseAcquireInterval = acquireInterval ?? ChangeFeedLeaseOptions.DefaultAcquireInterval;
            changeFeedLeaseOptions.LeaseExpirationInterval = expirationInterval ?? ChangeFeedLeaseOptions.DefaultExpirationInterval;
            return this;
        }

        /// <summary>
        /// Gets or sets the delay in between polling the change feed for new changes, after all current changes are drained.
        /// </summary>
        /// <remarks>
        /// Applies only after a read on the change feed yielded no results.
        /// </remarks>
        /// <param name="pollInterval">Polling interval value.</param>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        public ChangeFeedProcessorBuilder WithPollInterval(TimeSpan pollInterval)
        {
            if (pollInterval == null)
            {
                throw new ArgumentNullException(nameof(pollInterval));
            }

            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
            changeFeedProcessorOptions.FeedPollDelay = pollInterval;
            return this;
        }

        /// <summary>
        /// Indicates whether change feed in the Azure Cosmos DB service should start from beginning.
        /// By default it's start from current time.
        /// </summary>
        /// <remarks>
        /// This is only used when:
        /// (1) Lease store is not initialized and is ignored if a lease exists and has continuation token.
        /// (2) StartContinuation is not specified.
        /// (3) StartTime is not specified.
        /// </remarks>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        internal virtual ChangeFeedProcessorBuilder WithStartFromBeginning()
        {
            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
            changeFeedProcessorOptions.StartFromBeginning = true;
            return this;
        }

        /// <summary>
        /// Sets the time (exclusive) to start looking for changes after.
        /// </summary>
        /// <remarks>
        /// This is only used when:
        /// (1) Lease store is not initialized and is ignored if a lease exists and has continuation token.
        /// (2) StartContinuation is not specified.
        /// If this is specified, StartFromBeginning is ignored.
        /// </remarks>
        /// <param name="startTime">Date and time when to start looking for changes.</param>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        public ChangeFeedProcessorBuilder WithStartTime(DateTime startTime)
        {
            if (startTime == null)
            {
                throw new ArgumentNullException(nameof(startTime));
            }

            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
            changeFeedProcessorOptions.StartTime = startTime;
            return this;
        }

        /// <summary>
        /// Sets the maximum number of items to be returned in the enumeration operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="maxItemCount">Maximum amount of items to be returned in a Change Feed request.</param>
        /// <returns>An instance of <see cref="ChangeFeedProcessorBuilder"/>.</returns>
        public ChangeFeedProcessorBuilder WithMaxItems(int maxItemCount)
        {
            if (maxItemCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxItemCount));
            }

            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
            changeFeedProcessorOptions.MaxItemCount = maxItemCount;
            return this;
        }

        /// <summary>
        /// Sets the Cosmos Container to hold the leases state
        /// </summary>
        /// <param name="leaseContainer">Instance of a Cosmos Container to hold the leases.</param>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        public ChangeFeedProcessorBuilder WithLeaseContainer(Container leaseContainer)
        {
            if (leaseContainer == null)
            {
                throw new ArgumentNullException(nameof(leaseContainer));
            }

            if (this.leaseContainer != null)
            {
                throw new InvalidOperationException("The builder already defined a lease container.");
            }

            if (LeaseStoreManager != null)
            {
                throw new InvalidOperationException("The builder already defined an in-memory lease container instance.");
            }

            this.leaseContainer = (ContainerInternal)leaseContainer;
            return this;
        }

        /// <summary>
        /// Uses an in-memory container to maintain state of the leases
        /// </summary>
        /// <remarks>
        /// Using an in-memory container restricts the scaling capability to just the instance running the current processor.
        /// </remarks>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        internal virtual ChangeFeedProcessorBuilder WithInMemoryLeaseContainer()
        {
            if (leaseContainer != null)
            {
                throw new InvalidOperationException("The builder already defined a lease container.");
            }

            if (LeaseStoreManager != null)
            {
                throw new InvalidOperationException("The builder already defined an in-memory lease container instance.");
            }

            if (string.IsNullOrEmpty(InstanceName))
            {
                InstanceName = ChangeFeedProcessorBuilder.InMemoryDefaultHostName;
            }

            LeaseStoreManager = new DocumentServiceLeaseStoreManagerInMemory();
            return this;
        }

        /// <summary>
        /// Sets the start request session continuation token to start looking for changes after.
        /// </summary>
        /// <remarks>
        /// This is only used when lease store is not initialized and is ignored if a lease exists and has continuation token.
        /// If this is specified, both StartTime and StartFromBeginning are ignored.
        /// </remarks>
        /// <returns>The instance of <see cref="ChangeFeedProcessorBuilder"/> to use.</returns>
        internal virtual ChangeFeedProcessorBuilder WithSessionContinuationToken(string startContinuation)
        {
            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
            changeFeedProcessorOptions.StartContinuation = startContinuation;
            return this;
        }

        internal virtual ChangeFeedProcessorBuilder WithMonitoredContainerRid(string monitoredContainerRid)
        {
            if (monitoredContainerRid != null)
            {
                throw new ArgumentNullException(nameof(monitoredContainerRid));
            }

            this.monitoredContainerRid = monitoredContainerRid;
            return this;
        }

        /// <summary>
        /// Builds a new instance of the <see cref="ChangeFeedProcessor"/> with the specified configuration.
        /// </summary>
        /// <returns>An instance of <see cref="ChangeFeedProcessor"/>.</returns>
        public ChangeFeedProcessor Build()
        {
            if (isBuilt)
            {
                throw new InvalidOperationException("This builder instance has already been used to build a processor. Create a new instance to build another.");
            }

            if (monitoredContainer == null)
            {
                throw new InvalidOperationException(nameof(monitoredContainer) + " was not specified");
            }

            if (leaseContainer == null && LeaseStoreManager == null)
            {
                throw new InvalidOperationException($"Defining the lease store by WithLeaseContainer or WithInMemoryLeaseContainer is required.");
            }

            if (changeFeedLeaseOptions.LeasePrefix == null)
            {
                throw new InvalidOperationException("Processor name not specified during creation.");
            }

            InitializeDefaultOptions();
            applyBuilderConfiguration(LeaseStoreManager, leaseContainer, monitoredContainerRid, InstanceName, changeFeedLeaseOptions, changeFeedProcessorOptions, monitoredContainer);

            isBuilt = true;
            return changeFeedProcessor;
        }

        private void InitializeDefaultOptions()
        {
            changeFeedProcessorOptions = changeFeedProcessorOptions ?? new ChangeFeedProcessorOptions();
        }
    }
}
