﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.FeedManagement
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.Bootstrapping;

    internal sealed class PartitionManagerCore : PartitionManager
    {
        private readonly Bootstrapper bootstrapper;
        private readonly PartitionController partitionController;
        private readonly PartitionLoadBalancer partitionLoadBalancer;

        public PartitionManagerCore(Bootstrapper bootstrapper, PartitionController partitionController, PartitionLoadBalancer partitionLoadBalancer)
        {
            this.bootstrapper = bootstrapper;
            this.partitionController = partitionController;
            this.partitionLoadBalancer = partitionLoadBalancer;
        }

        public override async Task StartAsync()
        {
            await bootstrapper.InitializeAsync().ConfigureAwait(false);
            await partitionController.InitializeAsync().ConfigureAwait(false);
            partitionLoadBalancer.Start();
        }

        public override async Task StopAsync()
        {
            await partitionLoadBalancer.StopAsync().ConfigureAwait(false);
            await partitionController.ShutdownAsync().ConfigureAwait(false);
        }
    }
}