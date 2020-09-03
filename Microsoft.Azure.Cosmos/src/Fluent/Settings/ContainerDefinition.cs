//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Fluent
{
    using System;

    /// <summary>
    /// Azure Cosmos container fluent definition.
    /// </summary>
    /// <seealso cref="Container"/>
    public abstract class ContainerDefinition<T>
        where T : ContainerDefinition<T>
    {
        private readonly string containerName;
        private readonly string partitionKeyPath;
        private int? defaultTimeToLive;
        private IndexingPolicy indexingPolicy;
        private string timeToLivePropertyPath;
        private PartitionKeyDefinitionVersion? partitionKeyDefinitionVersion = null;

        /// <summary>
        /// Creates an instance for unit-testing
        /// </summary>
        public ContainerDefinition()
        {
        }

        internal ContainerDefinition(
            string name,
            string partitionKeyPath = null)
        {
            containerName = name;
            this.partitionKeyPath = partitionKeyPath;
        }

        /// <summary>
        /// Sets the <see cref="Cosmos.PartitionKeyDefinitionVersion"/>
        ///
        /// The partition key definition version 1 uses a hash function that computes
        /// hash based on the first 100 bytes of the partition key. This can cause
        /// conflicts for documents with partition keys greater than 100 bytes.
        /// 
        /// The partition key definition version 2 uses a hash function that computes
        /// hash based on the first 2 KB of the partition key.
        /// </summary>
        /// <param name="partitionKeyDefinitionVersion">The partition key definition version</param>
        /// <returns>An instance of the current Fluent builder.</returns>
        /// <seealso cref="ContainerProperties.PartitionKeyDefinitionVersion"/>
        public T WithPartitionKeyDefinitionVersion(PartitionKeyDefinitionVersion partitionKeyDefinitionVersion)
        {
            this.partitionKeyDefinitionVersion = partitionKeyDefinitionVersion;
            return (T)this;
        }

        /// <summary>
        /// <see cref="ContainerProperties.DefaultTimeToLive"/> will be applied to all the items in the container as the default time-to-live policy.
        /// The individual item could override the default time-to-live policy by setting its time to live.
        /// </summary>
        /// <param name="defaultTtlTimeSpan">The default Time To Live.</param>
        /// <returns>An instance of the current Fluent builder.</returns>
        /// <seealso cref="ContainerProperties.DefaultTimeToLive"/>
        public T WithDefaultTimeToLive(TimeSpan defaultTtlTimeSpan)
        {
            if (defaultTtlTimeSpan == null)
            {
                throw new ArgumentNullException(nameof(defaultTtlTimeSpan));
            }

            defaultTimeToLive = (int)defaultTtlTimeSpan.TotalSeconds;
            return (T)this;
        }

        /// <summary>
        /// <see cref="ContainerProperties.DefaultTimeToLive"/> will be applied to all the items in the container as the default time-to-live policy.
        /// The individual item could override the default time-to-live policy by setting its time to live.
        /// </summary>
        /// <param name="defaultTtlInSeconds">The default Time To Live.</param>
        /// <returns>An instance of the current Fluent builder.</returns>
        /// <seealso cref="ContainerProperties.DefaultTimeToLive"/>
        public T WithDefaultTimeToLive(int defaultTtlInSeconds)
        {
            if (defaultTtlInSeconds < -1)
            {
                throw new ArgumentOutOfRangeException(nameof(defaultTtlInSeconds));
            }

            defaultTimeToLive = defaultTtlInSeconds;
            return (T)this;
        }

        /// <summary>
        /// Sets the time to live base timestamp property path.
        /// </summary>
        /// <param name="propertyPath">This property should be only present when DefaultTimeToLive is set. When this property is present, time to live for a item is decided based on the value of this property in an item. By default, time to live is based on the _ts property in an item. Example: /property</param>
        /// <returns>An instance of the current Fluent builder.</returns>
        /// <seealso cref="ContainerProperties.TimeToLivePropertyPath"/>
        public T WithTimeToLivePropertyPath(string propertyPath)
        {
            if (string.IsNullOrEmpty(propertyPath))
            {
                throw new ArgumentNullException(nameof(propertyPath));
            }

            timeToLivePropertyPath = propertyPath;
            return (T)this;
        }

        /// <summary>
        /// <see cref="Cosmos.IndexingPolicy"/> definition for the current Azure Cosmos container.
        /// </summary>
        /// <returns>An instance of <see cref="IndexingPolicyDefinition{T}"/>.</returns>
        public IndexingPolicyDefinition<T> WithIndexingPolicy()
        {
            if (indexingPolicy != null)
            {
                // Overwrite
                throw new NotSupportedException();
            }

            return new IndexingPolicyDefinition<T>(
                (T)this,
                (indexingPolicy) => WithIndexingPolicy(indexingPolicy));
        }

        /// <summary>
        /// Applies the current Fluent definition and creates a container configuration.
        /// </summary>
        /// <returns>Builds the current Fluent configuration into an instance of <see cref="ContainerProperties"/>.</returns>
        public ContainerProperties Build()
        {
            ContainerProperties containerProperties = new ContainerProperties(id: containerName, partitionKeyPath: partitionKeyPath);
            if (indexingPolicy != null)
            {
                containerProperties.IndexingPolicy = indexingPolicy;
            }

            if (defaultTimeToLive.HasValue)
            {
                containerProperties.DefaultTimeToLive = defaultTimeToLive.Value;
            }

            if (timeToLivePropertyPath != null)
            {
#pragma warning disable 0612
                containerProperties.TimeToLivePropertyPath = timeToLivePropertyPath;
#pragma warning restore 0612
            }

            if (partitionKeyDefinitionVersion.HasValue)
            {
                containerProperties.PartitionKeyDefinitionVersion = partitionKeyDefinitionVersion.Value;
            }

            containerProperties.ValidateRequiredProperties();

            return containerProperties;
        }

        private void WithIndexingPolicy(IndexingPolicy indexingPolicy)
        {
            this.indexingPolicy = indexingPolicy;
        }
    }
}
