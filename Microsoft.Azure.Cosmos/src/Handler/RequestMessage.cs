//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Common;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Represents a request in the processing pipeline of the Azure Cosmos DB SDK.
    /// </summary>
    /// <remarks>
    /// It is expected that direct property access is used for properties that will be read and used within the Azure Cosmos SDK pipeline, for example <see cref="RequestMessage.OperationType"/>.
    /// <see cref="RequestMessage.Properties"/> should be used for any other property that needs to be sent to the backend but will not be read nor used within the Azure Cosmos DB SDK pipeline.
    /// <see cref="RequestMessage.Headers"/> should be used for HTTP headers that need to be passed down and sent to the backend.
    /// </remarks>
    public class RequestMessage : IDisposable
    {
        /// <summary>
        /// Create a <see cref="RequestMessage"/>
        /// </summary>
        public RequestMessage()
        {
            DiagnosticsContext = new CosmosDiagnosticsContextCore();
        }

        /// <summary>
        /// Create a <see cref="RequestMessage"/>
        /// </summary>
        /// <param name="method">The http method</param>
        /// <param name="requestUri">The requested URI</param>
        public RequestMessage(HttpMethod method, Uri requestUri)
        {
            Method = method;
            RequestUriString = requestUri?.OriginalString;
            InternalRequestUri = requestUri;
            DiagnosticsContext = new CosmosDiagnosticsContextCore();
        }

        /// <summary>
        /// Create a <see cref="RequestMessage"/>
        /// </summary>
        /// <param name="method">The http method</param>
        /// <param name="requestUriString">The requested URI</param>
        /// <param name="diagnosticsContext">The diagnostics object used to track the request</param>
        internal RequestMessage(
            HttpMethod method,
            string requestUriString,
            CosmosDiagnosticsContext diagnosticsContext)
        {
            Method = method;
            RequestUriString = requestUriString;
            DiagnosticsContext = diagnosticsContext ?? throw new ArgumentNullException(nameof(diagnosticsContext));
        }

        /// <summary>
        /// Gets the <see cref="HttpMethod"/> for the current request.
        /// </summary>
        public virtual HttpMethod Method { get; private set; }

        /// <summary>
        /// Gets the <see cref="Uri"/> for the current request.
        /// </summary>
        public virtual Uri RequestUri
        {
            get
            {
                if (InternalRequestUri == null)
                {
                    InternalRequestUri = new Uri(RequestUriString, UriKind.Relative);
                }

                return InternalRequestUri;
            }
        }

        /// <summary>
        /// Gets the current <see cref="RequestMessage"/> HTTP headers.
        /// </summary>
        public virtual Headers Headers => headers.Value;

        /// <summary>
        /// Gets or sets the current <see cref="RequestMessage"/> payload.
        /// </summary>
        public virtual Stream Content
        {
            get => content;
            set
            {
                CheckDisposed();
                content = value;
            }
        }

        internal string RequestUriString { get; }

        internal Uri InternalRequestUri { get; private set; }

        internal CosmosDiagnosticsContext DiagnosticsContext { get; }

        internal RequestOptions RequestOptions { get; set; }

        internal ResourceType ResourceType { get; set; }

        internal OperationType OperationType { get; set; }

        internal PartitionKeyRangeIdentity PartitionKeyRangeId { get; set; }

        /// <summary>
        /// Used to override the client default. This is used for scenarios
        /// in query where the service interop is not present.
        /// </summary>
        internal bool? UseGatewayMode { get; set; }

        internal DocumentServiceRequest DocumentServiceRequest { get; set; }

        internal Action<DocumentServiceRequest> OnBeforeSendRequestActions { get; set; }

        internal bool IsPropertiesInitialized => properties.IsValueCreated;

        /// <summary>
        /// The partition key range handler is only needed for read feed on partitioned resources 
        /// where the partition key range needs to be computed. 
        /// </summary>
        internal bool IsPartitionKeyRangeHandlerRequired => OperationType == OperationType.ReadFeed &&
            ResourceType.IsPartitioned() && PartitionKeyRangeId == null &&
            Headers.PartitionKey == null;

        /// <summary>
        /// Request properties Per request context available to handlers. 
        /// These will not be automatically included into the wire.
        /// </summary>
        public virtual Dictionary<string, object> Properties => properties.Value;

        private readonly Lazy<Dictionary<string, object>> properties = new Lazy<Dictionary<string, object>>(RequestMessage.CreateDictionary);

        private readonly Lazy<Headers> headers = new Lazy<Headers>(RequestMessage.CreateHeaders);

        private bool disposed;

        private Stream content;

        /// <summary>
        /// Disposes the current <see cref="RequestMessage"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes of the request message content
        /// </summary>
        /// <param name="disposing">True to dispose content</param>
        protected virtual void Dispose(bool disposing)
        {
            // The reason for this type to implement IDisposable is that it contains instances of types that implement
            // IDisposable (content). 
            if (disposing && !disposed)
            {
                disposed = true;
                if (Content != null)
                {
                    Content.Dispose();
                }
            }
        }

        internal void AddThroughputHeader(int? throughputValue)
        {
            if (throughputValue.HasValue)
            {
                Headers.OfferThroughput = throughputValue.Value.ToString(CultureInfo.InvariantCulture);
            }
        }

        internal void AddThroughputPropertiesHeader(ThroughputProperties throughputProperties)
        {
            if (throughputProperties == null)
            {
                return;
            }

            if (throughputProperties.Throughput.HasValue &&
                (throughputProperties.AutoscaleMaxThroughput.HasValue || throughputProperties.AutoUpgradeMaxThroughputIncrementPercentage.HasValue))
            {
                throw new InvalidOperationException("Autoscale provisioned throughput can not be configured with fixed offer");
            }

            if (throughputProperties.Throughput.HasValue)
            {
                AddThroughputHeader(throughputProperties.Throughput);
            }
            else if (throughputProperties?.Content?.OfferAutoscaleSettings != null)
            {
                Headers.Add(HttpConstants.HttpHeaders.OfferAutopilotSettings, throughputProperties.Content.OfferAutoscaleSettings.GetJsonString());
            }
        }

        internal async Task AssertPartitioningDetailsAsync(CosmosClient client, CancellationToken cancellationToken)
        {
            if (IsMasterOperation())
            {
                return;
            }

#if DEBUG
            try
            {
                CollectionCache collectionCache = await client.DocumentClient.GetCollectionCacheAsync();
                ContainerProperties collectionFromCache =
                    await collectionCache.ResolveCollectionAsync(ToDocumentServiceRequest(), cancellationToken);
                if (collectionFromCache.PartitionKey?.Paths?.Count > 0)
                {
                    Debug.Assert(AssertPartitioningPropertiesAndHeaders());
                }
            }
            catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Ignore container non-existence
            }
#else
            await Task.CompletedTask;
#endif
        }

        internal DocumentServiceRequest ToDocumentServiceRequest()
        {
            if (DocumentServiceRequest == null)
            {
                DocumentServiceRequest serviceRequest;
                if (OperationType == OperationType.ReadFeed && ResourceType == ResourceType.Database)
                {
                    serviceRequest = new DocumentServiceRequest(
                        operationType: OperationType,
                        resourceIdOrFullName: null,
                        resourceType: ResourceType,
                        body: Content,
                        headers: Headers.CosmosMessageHeaders,
                        isNameBased: false,
                        authorizationTokenType: AuthorizationTokenType.PrimaryMasterKey);
                }
                else
                {
                    serviceRequest = new DocumentServiceRequest(OperationType, ResourceType, RequestUriString, Content, AuthorizationTokenType.PrimaryMasterKey, Headers.CosmosMessageHeaders);
                }

                if (UseGatewayMode.HasValue)
                {
                    serviceRequest.UseGatewayMode = UseGatewayMode.Value;
                }

                serviceRequest.RequestContext.ClientRequestStatistics = new CosmosClientSideRequestStatistics(DiagnosticsContext);
                serviceRequest.UseStatusCodeForFailures = true;
                serviceRequest.UseStatusCodeFor429 = true;
                serviceRequest.Properties = Properties;
                DocumentServiceRequest = serviceRequest;
            }

            // Routing to a particular PartitionKeyRangeId
            if (PartitionKeyRangeId != null)
            {
                DocumentServiceRequest.RouteTo(PartitionKeyRangeId);
            }

            OnBeforeRequestHandler(DocumentServiceRequest);
            return DocumentServiceRequest;
        }

        private static Dictionary<string, object> CreateDictionary()
        {
            return new Dictionary<string, object>();
        }

        private static Headers CreateHeaders()
        {
            return new Headers();
        }

        private void OnBeforeRequestHandler(DocumentServiceRequest serviceRequest)
        {
            if (OnBeforeSendRequestActions != null)
            {
                OnBeforeSendRequestActions(serviceRequest);
            }
        }

        private bool AssertPartitioningPropertiesAndHeaders()
        {
            // Either PK/key-range-id is assumed
            bool pkExists = !string.IsNullOrEmpty(Headers.PartitionKey);
            bool epkExists = Properties.ContainsKey(WFConstants.BackendHeaders.EffectivePartitionKeyString);
            if (pkExists && epkExists)
            {
                throw new ArgumentNullException(RMResources.PartitionKeyAndEffectivePartitionKeyBothSpecified);
            }

            bool isPointOperation = OperationType != OperationType.ReadFeed;
            if (!pkExists && !epkExists && OperationType.IsPointOperation())
            {
                throw new ArgumentNullException(RMResources.MissingPartitionKeyValue);
            }

            bool partitionKeyRangeIdExists = !string.IsNullOrEmpty(Headers.PartitionKeyRangeId);
            if (partitionKeyRangeIdExists)
            {
                // Assert operation type is not write
                if (OperationType != OperationType.Query && OperationType != OperationType.ReadFeed && OperationType != OperationType.Batch)
                {
                    throw new ArgumentOutOfRangeException(RMResources.UnexpectedPartitionKeyRangeId);
                }
            }

            if (pkExists && partitionKeyRangeIdExists)
            {
                throw new ArgumentOutOfRangeException(RMResources.PartitionKeyAndPartitionKeyRangeRangeIdBothSpecified);
            }

            return true;
        }

        private bool IsMasterOperation()
        {
            return ResourceType != ResourceType.Document;
        }

        private void CheckDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(GetType().ToString());
            }
        }
    }
}