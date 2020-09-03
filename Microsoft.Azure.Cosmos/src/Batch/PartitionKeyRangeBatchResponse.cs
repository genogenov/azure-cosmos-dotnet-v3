//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Text;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Response of a cross partition key batch request.
    /// </summary>
    internal class PartitionKeyRangeBatchResponse : TransactionalBatchResponse
    {
        // Results sorted in the order operations had been added.
        private readonly TransactionalBatchOperationResult[] resultsByOperationIndex;
        private readonly TransactionalBatchResponse serverResponse;
        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionKeyRangeBatchResponse"/> class.
        /// </summary>
        /// <param name="originalOperationsCount">Original operations that generated the server responses.</param>
        /// <param name="serverResponse">Response from the server.</param>
        /// <param name="serializerCore">Serializer to deserialize response resource body streams.</param>
        internal PartitionKeyRangeBatchResponse(
            int originalOperationsCount,
            TransactionalBatchResponse serverResponse,
            CosmosSerializerCore serializerCore)
        {
            StatusCode = serverResponse.StatusCode;

            this.serverResponse = serverResponse;
            resultsByOperationIndex = new TransactionalBatchOperationResult[originalOperationsCount];

            StringBuilder errorMessageBuilder = new StringBuilder();
            List<ItemBatchOperation> itemBatchOperations = new List<ItemBatchOperation>();
            // We expect number of results == number of operations here
            for (int index = 0; index < serverResponse.Operations.Count; index++)
            {
                int operationIndex = serverResponse.Operations[index].OperationIndex;
                if (resultsByOperationIndex[operationIndex] == null
                    || resultsByOperationIndex[operationIndex].StatusCode == (HttpStatusCode)StatusCodes.TooManyRequests)
                {
                    resultsByOperationIndex[operationIndex] = serverResponse[index];
                }
            }

            itemBatchOperations.AddRange(serverResponse.Operations);
            Headers = serverResponse.Headers;

            if (!string.IsNullOrEmpty(serverResponse.ErrorMessage))
            {
                errorMessageBuilder.AppendFormat("{0}; ", serverResponse.ErrorMessage);
            }

            ErrorMessage = errorMessageBuilder.Length > 2 ? errorMessageBuilder.ToString(0, errorMessageBuilder.Length - 2) : null;
            Operations = itemBatchOperations;
            SerializerCore = serializerCore;
        }

        /// <summary>
        /// Gets the ActivityId that identifies the server request made to execute the batch request.
        /// </summary>
        public override string ActivityId => serverResponse.ActivityId;

        /// <inheritdoc />
        public override CosmosDiagnostics Diagnostics => serverResponse.Diagnostics;

        internal override CosmosDiagnosticsContext DiagnosticsContext => serverResponse.DiagnosticsContext;

        internal override CosmosSerializerCore SerializerCore { get; }

        /// <summary>
        /// Gets the number of operation results.
        /// </summary>
        public override int Count => resultsByOperationIndex.Length;

        /// <inheritdoc />
        public override TransactionalBatchOperationResult this[int index] => resultsByOperationIndex[index];

        /// <summary>
        /// Gets the result of the operation at the provided index in the batch - the returned result has a Resource of provided type.
        /// </summary>
        /// <typeparam name="T">Type to which the Resource in the operation result needs to be deserialized to, when present.</typeparam>
        /// <param name="index">0-based index of the operation in the batch whose result needs to be returned.</param>
        /// <returns>Result of batch operation that contains a Resource deserialized to specified type.</returns>
        public override TransactionalBatchOperationResult<T> GetOperationResultAtIndex<T>(int index)
        {
            if (index >= Count)
            {
                throw new IndexOutOfRangeException();
            }

            TransactionalBatchOperationResult result = resultsByOperationIndex[index];

            T resource = default(T);
            if (result.ResourceStream != null)
            {
                resource = SerializerCore.FromStream<T>(result.ResourceStream);
            }

            return new TransactionalBatchOperationResult<T>(result, resource);
        }

        /// <summary>
        /// Gets an enumerator over the operation results.
        /// </summary>
        /// <returns>Enumerator over the operation results.</returns>
        public override IEnumerator<TransactionalBatchOperationResult> GetEnumerator()
        {
            foreach (TransactionalBatchOperationResult result in resultsByOperationIndex)
            {
                yield return result;
            }
        }

#if INTERNAL
        public 
#else
        internal
#endif
        override IEnumerable<string> GetActivityIds()
        {
            return new string[1] { ActivityId };
        }

        /// <summary>
        /// Disposes the disposable members held.
        /// </summary>
        /// <param name="disposing">Indicates whether to dispose managed resources or not.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && !isDisposed)
            {
                isDisposed = true;
                serverResponse?.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}