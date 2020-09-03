//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.ChangeFeed.Utils;
    using Newtonsoft.Json;

    /// <summary>
    /// Implementation of <see cref="DocumentServiceLeaseStore"/> for state in Azure Cosmos DB
    /// </summary>
    internal sealed class DocumentServiceLeaseStoreCosmos : DocumentServiceLeaseStore
    {
        private readonly Container container;
        private readonly string containerNamePrefix;
        private readonly RequestOptionsFactory requestOptionsFactory;
        private string lockETag;

        public DocumentServiceLeaseStoreCosmos(
            Container container,
            string containerNamePrefix,
            RequestOptionsFactory requestOptionsFactory)
        {
            this.container = container;
            this.containerNamePrefix = containerNamePrefix;
            this.requestOptionsFactory = requestOptionsFactory;
        }

        public override async Task<bool> IsInitializedAsync()
        {
            string markerDocId = GetStoreMarkerName();

            return await container.ItemExistsAsync(requestOptionsFactory.GetPartitionKey(markerDocId), markerDocId).ConfigureAwait(false);
        }

        public override async Task MarkInitializedAsync()
        {
            string markerDocId = GetStoreMarkerName();
            dynamic containerDocument = new { id = markerDocId };

            using (Stream itemStream = CosmosContainerExtensions.DefaultJsonSerializer.ToStream(containerDocument))
            {
                using (ResponseMessage responseMessage = await container.CreateItemStreamAsync(
                    itemStream,
                    requestOptionsFactory.GetPartitionKey(markerDocId)).ConfigureAwait(false))
                {
                    responseMessage.EnsureSuccessStatusCode();
                }
            }
        }

        public override async Task<bool> AcquireInitializationLockAsync(TimeSpan lockTime)
        {
            string lockId = GetStoreLockName();
            LockDocument containerDocument = new LockDocument() { Id = lockId, TimeToLive = (int)lockTime.TotalSeconds };
            ItemResponse<LockDocument> document = await container.TryCreateItemAsync<LockDocument>(
                requestOptionsFactory.GetPartitionKey(lockId),
                containerDocument).ConfigureAwait(false);

            if (document != null)
            {
                lockETag = document.ETag;
                return true;
            }

            return false;
        }

        public override async Task<bool> ReleaseInitializationLockAsync()
        {
            string lockId = GetStoreLockName();
            ItemRequestOptions requestOptions = new ItemRequestOptions()
            {
                IfMatchEtag = lockETag,
            };

            bool deleted = await container.TryDeleteItemAsync<LockDocument>(
                requestOptionsFactory.GetPartitionKey(lockId),
                lockId,
                requestOptions).ConfigureAwait(false);

            if (deleted)
            {
                lockETag = null;
                return true;
            }

            return false;
        }

        private string GetStoreMarkerName()
        {
            return containerNamePrefix + ".info";
        }

        private string GetStoreLockName()
        {
            return containerNamePrefix + ".lock";
        }

        private class LockDocument
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("ttl")]
            public int TimeToLive { get; set; }
        }
    }
}