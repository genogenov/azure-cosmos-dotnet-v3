//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.FeedManagement;
    using Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement;
    using Microsoft.Azure.Cosmos.ChangeFeed.Monitoring;
    using Microsoft.Azure.Documents;

    internal class HealthMonitoringPartitionControllerDecorator : PartitionController
    {
        private readonly PartitionController inner;
        private readonly HealthMonitor monitor;

        public HealthMonitoringPartitionControllerDecorator(PartitionController inner, HealthMonitor monitor)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
            this.monitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
        }

        public override async Task AddOrUpdateLeaseAsync(DocumentServiceLease lease)
        {
            try
            {
                await inner.AddOrUpdateLeaseAsync(lease);
                await monitor.InspectAsync(new HealthMonitoringRecord(HealthSeverity.Informational, MonitoredOperation.AcquireLease, lease, null));
            }
            catch (DocumentClientException)
            {
                throw;
            }
            catch (Exception exception)
            {
                await monitor.InspectAsync(new HealthMonitoringRecord(HealthSeverity.Error, MonitoredOperation.AcquireLease, lease, exception));

                throw;
            }
        }

        public override Task InitializeAsync()
        {
            return inner.InitializeAsync();
        }

        public override Task ShutdownAsync()
        {
            return inner.ShutdownAsync();
        }
    }
}