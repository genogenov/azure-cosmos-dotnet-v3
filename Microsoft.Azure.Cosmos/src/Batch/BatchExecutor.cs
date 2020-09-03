//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;

    internal sealed class BatchExecutor
    {
        private readonly ContainerInternal container;

        private readonly CosmosClientContext clientContext;

        private readonly IReadOnlyList<ItemBatchOperation> inputOperations;

        private readonly PartitionKey partitionKey;

        private readonly RequestOptions batchOptions;

        private readonly CosmosDiagnosticsContext diagnosticsContext;

        public BatchExecutor(
            ContainerInternal container,
            PartitionKey partitionKey,
            IReadOnlyList<ItemBatchOperation> operations,
            RequestOptions batchOptions,
            CosmosDiagnosticsContext diagnosticsContext)
        {
            this.container = container;
            clientContext = this.container.ClientContext;
            inputOperations = operations;
            this.partitionKey = partitionKey;
            this.batchOptions = batchOptions;
            this.diagnosticsContext = diagnosticsContext;
        }

        public async Task<TransactionalBatchResponse> ExecuteAsync(CancellationToken cancellationToken)
        {
            using (diagnosticsContext.GetOverallScope())
            {
                BatchExecUtils.EnsureValid(inputOperations, batchOptions);

                foreach (ItemBatchOperation operation in inputOperations)
                {
                    operation.DiagnosticsContext = diagnosticsContext;
                }

                PartitionKey? serverRequestPartitionKey = partitionKey;
                if (batchOptions != null && batchOptions.IsEffectivePartitionKeyRouting)
                {
                    serverRequestPartitionKey = null;
                }

                SinglePartitionKeyServerBatchRequest serverRequest;
                using (diagnosticsContext.CreateScope("CreateBatchRequest"))
                {
                    serverRequest = await SinglePartitionKeyServerBatchRequest.CreateAsync(
                          serverRequestPartitionKey,
                          new ArraySegment<ItemBatchOperation>(inputOperations.ToArray()),
                          clientContext.SerializerCore,
                          cancellationToken);
                }

                return await ExecuteServerRequestAsync(serverRequest, cancellationToken);
            }
        }

        /// <summary>
        /// Makes a single batch request to the server.
        /// </summary>
        /// <param name="serverRequest">A server request with a set of operations on items.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> representing request cancellation.</param>
        /// <returns>Response from the server.</returns>
        private async Task<TransactionalBatchResponse> ExecuteServerRequestAsync(
            SinglePartitionKeyServerBatchRequest serverRequest,
            CancellationToken cancellationToken)
        {
            using (Stream serverRequestPayload = serverRequest.TransferBodyStream())
            {
                Debug.Assert(serverRequestPayload != null, "Server request payload expected to be non-null");
                ResponseMessage responseMessage = await clientContext.ProcessResourceOperationStreamAsync(
                    container.LinkUri,
                    ResourceType.Document,
                    OperationType.Batch,
                    batchOptions,
                    container,
                    serverRequest.PartitionKey,
                    serverRequestPayload,
                    requestMessage =>
                    {
                        requestMessage.Headers.Add(HttpConstants.HttpHeaders.IsBatchRequest, bool.TrueString);
                        requestMessage.Headers.Add(HttpConstants.HttpHeaders.IsBatchAtomic, bool.TrueString);
                        requestMessage.Headers.Add(HttpConstants.HttpHeaders.IsBatchOrdered, bool.TrueString);
                    },
                    diagnosticsContext: diagnosticsContext,
                    cancellationToken);

                using (diagnosticsContext.CreateScope("TransactionalBatchResponse"))
                {
                    return await TransactionalBatchResponse.FromResponseMessageAsync(
                        responseMessage,
                        serverRequest,
                        clientContext.SerializerCore,
                        shouldPromoteOperationStatus: true,
                        cancellationToken);
                }
            }
        }
    }
}
