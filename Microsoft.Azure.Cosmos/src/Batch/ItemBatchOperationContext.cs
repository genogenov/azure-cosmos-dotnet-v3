//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Core.Trace;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Context for a particular Batch operation.
    /// </summary>
    internal class ItemBatchOperationContext : IDisposable
    {
        public string PartitionKeyRangeId { get; }

        public BatchAsyncBatcher CurrentBatcher { get; set; }

        public Task<TransactionalBatchOperationResult> OperationTask => taskCompletionSource.Task;

        private readonly IDocumentClientRetryPolicy retryPolicy;

        private readonly TaskCompletionSource<TransactionalBatchOperationResult> taskCompletionSource = new TaskCompletionSource<TransactionalBatchOperationResult>();

        public ItemBatchOperationContext(
            string partitionKeyRangeId,
            IDocumentClientRetryPolicy retryPolicy = null)
        {
            PartitionKeyRangeId = partitionKeyRangeId;
            this.retryPolicy = retryPolicy;
        }

        /// <summary>
        /// Based on the Retry Policy, if a failed response should retry.
        /// </summary>
        public Task<ShouldRetryResult> ShouldRetryAsync(
            TransactionalBatchOperationResult batchOperationResult,
            CancellationToken cancellationToken)
        {
            if (retryPolicy == null
                || batchOperationResult.IsSuccessStatusCode)
            {
                return Task.FromResult(ShouldRetryResult.NoRetry());
            }

            ResponseMessage responseMessage = batchOperationResult.ToResponseMessage();
            return retryPolicy.ShouldRetryAsync(responseMessage, cancellationToken);
        }

        public void Complete(
            BatchAsyncBatcher completer,
            TransactionalBatchOperationResult result)
        {
            if (AssertBatcher(completer))
            {
                taskCompletionSource.SetResult(result);
            }

            Dispose();
        }

        public void Fail(
            BatchAsyncBatcher completer,
            Exception exception)
        {
            if (AssertBatcher(completer, exception))
            {
                taskCompletionSource.SetException(exception);
            }

            Dispose();
        }

        public void Dispose()
        {
            CurrentBatcher = null;
        }

        private bool AssertBatcher(
            BatchAsyncBatcher completer,
            Exception innerException = null)
        {
            if (!object.ReferenceEquals(completer, CurrentBatcher))
            {
                DefaultTrace.TraceCritical($"Operation was completed by incorrect batcher.");
                taskCompletionSource.SetException(new Exception($"Operation was completed by incorrect batcher.", innerException));
                return false;
            }

            return true;
        }
    }
}
