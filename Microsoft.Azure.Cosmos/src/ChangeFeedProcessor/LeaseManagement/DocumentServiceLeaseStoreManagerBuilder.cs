//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// Provides flexible way to build lease manager constructor parameters.
    /// For the actual creation of lease manager instance, delegates to lease manager factory.
    /// </summary>
    internal class DocumentServiceLeaseStoreManagerBuilder
    {
        private readonly DocumentServiceLeaseStoreManagerOptions options = new DocumentServiceLeaseStoreManagerOptions();
        private Container container;
        private RequestOptionsFactory requestOptionsFactory;

        public DocumentServiceLeaseStoreManagerBuilder WithLeaseContainer(Container leaseContainer)
        {
            if (leaseContainer == null)
            {
                throw new ArgumentNullException(nameof(leaseContainer));
            }

            container = leaseContainer;
            return this;
        }

        public DocumentServiceLeaseStoreManagerBuilder WithLeasePrefix(string leasePrefix)
        {
            if (leasePrefix == null)
            {
                throw new ArgumentNullException(nameof(leasePrefix));
            }

            options.ContainerNamePrefix = leasePrefix;
            return this;
        }

        public DocumentServiceLeaseStoreManagerBuilder WithRequestOptionsFactory(RequestOptionsFactory requestOptionsFactory)
        {
            if (requestOptionsFactory == null)
            {
                throw new ArgumentNullException(nameof(requestOptionsFactory));
            }

            this.requestOptionsFactory = requestOptionsFactory;
            return this;
        }

        public DocumentServiceLeaseStoreManagerBuilder WithHostName(string hostName)
        {
            if (hostName == null)
            {
                throw new ArgumentNullException(nameof(hostName));
            }

            options.HostName = hostName;
            return this;
        }

        public Task<DocumentServiceLeaseStoreManager> BuildAsync()
        {
            if (container == null)
            {
                throw new InvalidOperationException(nameof(container) + " was not specified");
            }

            if (requestOptionsFactory == null)
            {
                throw new InvalidOperationException(nameof(requestOptionsFactory) + " was not specified");
            }

            var leaseStoreManager = new DocumentServiceLeaseStoreManagerCosmos(options, container, requestOptionsFactory);
            return Task.FromResult<DocumentServiceLeaseStoreManager>(leaseStoreManager);
        }
    }
}
