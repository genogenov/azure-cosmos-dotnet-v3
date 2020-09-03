//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Serialization.HybridRow;
    using Microsoft.Azure.Cosmos.Serialization.HybridRow.IO;
    using Microsoft.Azure.Cosmos.Serialization.HybridRow.Layouts;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Represents an operation on an item which will be executed as part of a batch request
    /// on a container.
    /// </summary>
    internal class ItemBatchOperation : IDisposable
    {
#pragma warning disable SA1401 // Fields should be private
        protected Memory<byte> body;
#pragma warning restore SA1401 // Fields should be private
        private bool isDisposed;

        public ItemBatchOperation(
            OperationType operationType,
            int operationIndex,
            PartitionKey partitionKey,
            string id = null,
            Stream resourceStream = null,
            TransactionalBatchItemRequestOptions requestOptions = null,
            CosmosDiagnosticsContext diagnosticsContext = null)
        {
            OperationType = operationType;
            OperationIndex = operationIndex;
            PartitionKey = partitionKey;
            Id = id;
            ResourceStream = resourceStream;
            RequestOptions = requestOptions;
            DiagnosticsContext = diagnosticsContext;
        }

        public ItemBatchOperation(
            OperationType operationType,
            int operationIndex,
            ContainerInternal containerCore,
            string id = null,
            Stream resourceStream = null,
            TransactionalBatchItemRequestOptions requestOptions = null)
        {
            OperationType = operationType;
            OperationIndex = operationIndex;
            ContainerInternal = containerCore;
            Id = id;
            ResourceStream = resourceStream;
            RequestOptions = requestOptions;
            DiagnosticsContext = null;
        }

        public PartitionKey? PartitionKey { get; internal set; }

        public string Id { get; }

        public OperationType OperationType { get; }

        public Stream ResourceStream { get; protected set; }

        public TransactionalBatchItemRequestOptions RequestOptions { get; }

        public int OperationIndex { get; internal set; }

        internal ContainerInternal ContainerInternal { get; }

        internal CosmosDiagnosticsContext DiagnosticsContext { get; set; }

        internal string PartitionKeyJson { get; set; }

        internal Documents.PartitionKey ParsedPartitionKey { get; set; }

        internal Memory<byte> ResourceBody
        {
            get
            {
                Debug.Assert(
                    ResourceStream == null || !body.IsEmpty,
                    "ResourceBody read without materialization of ResourceStream");

                return body;
            }

            set
            {
                body = value;
            }
        }

        /// <summary>
        /// Operational context used in stream operations.
        /// </summary>
        /// <seealso cref="BatchAsyncBatcher"/>
        /// <seealso cref="BatchAsyncStreamer"/>
        /// <seealso cref="BatchAsyncContainerExecutor"/>
        internal ItemBatchOperationContext Context { get; private set; }

        /// <summary>
        /// Disposes the current <see cref="ItemBatchOperation"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        internal static Result WriteOperation(ref RowWriter writer, TypeArgument typeArg, ItemBatchOperation operation)
        {
            bool pkWritten = false;
            Result r = writer.WriteInt32("operationType", (int)operation.OperationType);
            if (r != Result.Success)
            {
                return r;
            }

            r = writer.WriteInt32("resourceType", (int)ResourceType.Document);
            if (r != Result.Success)
            {
                return r;
            }

            if (operation.PartitionKeyJson != null)
            {
                r = writer.WriteString("partitionKey", operation.PartitionKeyJson);
                if (r != Result.Success)
                {
                    return r;
                }

                pkWritten = true;
            }

            if (operation.Id != null)
            {
                r = writer.WriteString("id", operation.Id);
                if (r != Result.Success)
                {
                    return r;
                }
            }

            if (!operation.ResourceBody.IsEmpty)
            {
                r = writer.WriteBinary("resourceBody", operation.ResourceBody.Span);
                if (r != Result.Success)
                {
                    return r;
                }
            }

            if (operation.RequestOptions != null)
            {
                TransactionalBatchItemRequestOptions options = operation.RequestOptions;
                if (options.IndexingDirective.HasValue)
                {
                    string indexingDirectiveString = IndexingDirectiveStrings.FromIndexingDirective(options.IndexingDirective.Value);
                    r = writer.WriteString("indexingDirective", indexingDirectiveString);
                    if (r != Result.Success)
                    {
                        return r;
                    }
                }

                if (ItemRequestOptions.ShouldSetNoContentHeader(
                    options.EnableContentResponseOnWrite,
                    options.EnableContentResponseOnRead,
                    operation.OperationType))
                {
                    r = writer.WriteBool("minimalReturnPreference", true);
                    if (r != Result.Success)
                    {
                        return r;
                    }
                }

                if (options.IfMatchEtag != null)
                {
                    r = writer.WriteString("ifMatch", options.IfMatchEtag);
                    if (r != Result.Success)
                    {
                        return r;
                    }
                }
                else if (options.IfNoneMatchEtag != null)
                {
                    r = writer.WriteString("ifNoneMatch", options.IfNoneMatchEtag);
                    if (r != Result.Success)
                    {
                        return r;
                    }
                }

                if (options.Properties != null)
                {
                    if (options.Properties.TryGetValue(WFConstants.BackendHeaders.BinaryId, out object binaryIdObj))
                    {
                        if (binaryIdObj is byte[] binaryId)
                        {
                            r = writer.WriteBinary("binaryId", binaryId);
                            if (r != Result.Success)
                            {
                                return r;
                            }
                        }
                    }

                    if (options.Properties.TryGetValue(WFConstants.BackendHeaders.EffectivePartitionKey, out object epkObj))
                    {
                        if (epkObj is byte[] epk)
                        {
                            r = writer.WriteBinary("effectivePartitionKey", epk);
                            if (r != Result.Success)
                            {
                                return r;
                            }
                        }
                    }

                    if (!pkWritten && options.Properties.TryGetValue(
                            HttpConstants.HttpHeaders.PartitionKey,
                            out object pkStrObj))
                    {
                        if (pkStrObj is string pkString)
                        {
                            r = writer.WriteString("partitionKey", pkString);
                            if (r != Result.Success)
                            {
                                return r;
                            }
                        }
                    }

                    if (options.Properties.TryGetValue(WFConstants.BackendHeaders.TimeToLiveInSeconds, out object ttlObj))
                    {
                        if (ttlObj is string ttlStr && int.TryParse(ttlStr, out int ttl))
                        {
                            r = writer.WriteInt32("timeToLiveInSeconds", ttl);
                            if (r != Result.Success)
                            {
                                return r;
                            }
                        }
                    }
                }
            }

            return Result.Success;
        }

        /// <summary>
        /// Computes and returns an approximation for the length of this <see cref="ItemBatchOperation"/>.
        /// when serialized.
        /// </summary>
        /// <returns>An under-estimate of the length.</returns>
        internal int GetApproximateSerializedLength()
        {
            int length = 0;

            if (PartitionKeyJson != null)
            {
                length += PartitionKeyJson.Length;
            }

            if (Id != null)
            {
                length += Id.Length;
            }

            length += body.Length;

            if (RequestOptions != null)
            {
                if (RequestOptions.IfMatchEtag != null)
                {
                    length += RequestOptions.IfMatchEtag.Length;
                }

                if (RequestOptions.IfNoneMatchEtag != null)
                {
                    length += RequestOptions.IfNoneMatchEtag.Length;
                }

                if (RequestOptions.IndexingDirective.HasValue)
                {
                    length += 7; // "Default", "Include", "Exclude" are possible values
                }

                if (RequestOptions.Properties != null)
                {
                    if (RequestOptions.Properties.TryGetValue(WFConstants.BackendHeaders.BinaryId, out object binaryIdObj))
                    {
                        if (binaryIdObj is byte[] binaryId)
                        {
                            length += binaryId.Length;
                        }
                    }

                    if (RequestOptions.Properties.TryGetValue(WFConstants.BackendHeaders.EffectivePartitionKey, out object epkObj))
                    {
                        if (epkObj is byte[] epk)
                        {
                            length += epk.Length;
                        }
                    }
                }
            }

            return length;
        }

        /// <summary>
        /// Materializes the operation's resource into a Memory{byte} wrapping a byte array.
        /// </summary>
        /// <param name="serializerCore">Serializer to serialize user provided objects to JSON.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> for cancellation.</param>
        internal virtual async Task MaterializeResourceAsync(CosmosSerializerCore serializerCore, CancellationToken cancellationToken)
        {
            if (body.IsEmpty && ResourceStream != null)
            {
                body = await BatchExecUtils.StreamToMemoryAsync(ResourceStream, cancellationToken);
            }
        }

        /// <summary>
        /// Attached a context to the current operation to track resolution.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the operation already had an attached context.</exception>
        internal void AttachContext(ItemBatchOperationContext context)
        {
            if (Context != null)
            {
                throw new InvalidOperationException("Cannot modify the current context of an operation.");
            }

            Context = context;
        }

        /// <summary>
        /// Disposes the disposable members held by this class.
        /// </summary>
        /// <param name="disposing">Indicates whether to dispose managed resources or not.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing && !isDisposed)
            {
                isDisposed = true;
                if (ResourceStream != null)
                {
                    ResourceStream.Dispose();
                    ResourceStream = null;
                }
            }
        }
    }

#pragma warning disable SA1402 // File may only contain a single type
    internal class ItemBatchOperation<T> : ItemBatchOperation
#pragma warning restore SA1402 // File may only contain a single type
    {
        public ItemBatchOperation(
            OperationType operationType,
            int operationIndex,
            PartitionKey partitionKey,
            T resource,
            string id = null,
            TransactionalBatchItemRequestOptions requestOptions = null)
            : base(operationType, operationIndex, partitionKey: partitionKey, id: id, requestOptions: requestOptions)
        {
            Resource = resource;
        }

        public ItemBatchOperation(
            OperationType operationType,
            int operationIndex,
            T resource,
            ContainerInternal containerCore,
            string id = null,
            TransactionalBatchItemRequestOptions requestOptions = null)
            : base(operationType, operationIndex, containerCore: containerCore, id: id, requestOptions: requestOptions)
        {
            Resource = resource;
        }

        public T Resource { get; private set; }

        /// <summary>
        /// Materializes the operation's resource into a Memory{byte} wrapping a byte array.
        /// </summary>
        /// <param name="serializerCore">Serializer to serialize user provided objects to JSON.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> for cancellation.</param>
        internal override Task MaterializeResourceAsync(CosmosSerializerCore serializerCore, CancellationToken cancellationToken)
        {
            if (body.IsEmpty && Resource != null)
            {
                ResourceStream = serializerCore.ToStream(Resource);
                return base.MaterializeResourceAsync(serializerCore, cancellationToken);
            }

            return Task.CompletedTask;
        }
    }
}