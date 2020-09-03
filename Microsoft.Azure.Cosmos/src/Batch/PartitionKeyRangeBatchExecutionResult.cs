//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System.Collections.Generic;
    using System.Net;

    internal class PartitionKeyRangeBatchExecutionResult
    {
        public string PartitionKeyRangeId { get; }

        public TransactionalBatchResponse ServerResponse { get; }

        public IEnumerable<ItemBatchOperation> Operations { get; }

        public PartitionKeyRangeBatchExecutionResult(
            string pkRangeId,
            IEnumerable<ItemBatchOperation> operations,
            TransactionalBatchResponse serverResponse)
        {
            PartitionKeyRangeId = pkRangeId;
            ServerResponse = serverResponse;
            Operations = operations;
        }

        internal bool IsSplit() => ServerResponse != null &&
                                            ServerResponse.StatusCode == HttpStatusCode.Gone
                                                && (ServerResponse.SubStatusCode == Documents.SubStatusCodes.CompletingSplit
                                                || ServerResponse.SubStatusCode == Documents.SubStatusCodes.CompletingPartitionMigration
                                                || ServerResponse.SubStatusCode == Documents.SubStatusCodes.PartitionKeyRangeGone);
    }
}