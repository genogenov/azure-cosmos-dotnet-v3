//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.Azure.Cosmos.Handlers;

    internal class ClientPipelineBuilder
    {
        private readonly CosmosClient client;
        private readonly ConsistencyLevel? requestedClientConsistencyLevel;
        private readonly DiagnosticsHandler diagnosticsHandler;
        private readonly RequestHandler invalidPartitionExceptionRetryHandler;
        private readonly RequestHandler transportHandler;
        private IReadOnlyCollection<RequestHandler> customHandlers;
        private RequestHandler retryHandler;

        public ClientPipelineBuilder(
            CosmosClient client,
            ConsistencyLevel? requestedClientConsistencyLevel,
            IReadOnlyCollection<RequestHandler> customHandlers)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
            this.requestedClientConsistencyLevel = requestedClientConsistencyLevel;
            transportHandler = new TransportHandler(client);
            Debug.Assert(transportHandler.InnerHandler == null, nameof(transportHandler));

            invalidPartitionExceptionRetryHandler = new NamedCacheRetryHandler();
            Debug.Assert(invalidPartitionExceptionRetryHandler.InnerHandler == null, "The invalidPartitionExceptionRetryHandler.InnerHandler must be null to allow other handlers to be linked.");

            PartitionKeyRangeHandler = new PartitionKeyRangeHandler(client);
            Debug.Assert(PartitionKeyRangeHandler.InnerHandler == null, "The PartitionKeyRangeHandler.InnerHandler must be null to allow other handlers to be linked.");

            diagnosticsHandler = new DiagnosticsHandler();
            Debug.Assert(diagnosticsHandler.InnerHandler == null, nameof(diagnosticsHandler));

            UseRetryPolicy();
            AddCustomHandlers(customHandlers);
        }

        internal IReadOnlyCollection<RequestHandler> CustomHandlers
        {
            get => customHandlers;
            private set
            {
                if (value != null && value.Any(x => x?.InnerHandler != null))
                {
                    throw new ArgumentOutOfRangeException(nameof(CustomHandlers));
                }

                customHandlers = value;
            }
        }

        internal RequestHandler PartitionKeyRangeHandler { get; set; }

        /// <summary>
        /// This is the cosmos pipeline logic for the operations. 
        /// 
        ///                                    +-----------------------------+
        ///                                    |                             |
        ///                                    |    RequestInvokerHandler    |
        ///                                    |                             |
        ///                                    +-----------------------------+
        ///                                                 |
        ///                                                 |
        ///                                                 |
        ///                                    +-----------------------------+
        ///                                    |                             |
        ///                                    |       UserHandlers          |
        ///                                    |                             |
        ///                                    +-----------------------------+
        ///                                                 |
        ///                                                 |
        ///                                                 |
        ///                                    +-----------------------------+
        ///                                    |                             |
        ///                                    |       RetryHandler          |-> RetryPolicy -> ResetSessionTokenRetryPolicyFactory -> ClientRetryPolicy -> ResourceThrottleRetryPolicy
        ///                                    |                             |
        ///                                    +-----------------------------+
        ///                                                 |
        ///                                                 |
        ///                                                 |
        ///                                    +-----------------------------+
        ///                                    |                             |
        ///                                    |       RouteHandler          | 
        ///                                    |                             |
        ///                                    +-----------------------------+
        ///                                    |                             |
        ///                                    |                             |
        ///                                    |                             |
        ///                  +-----------------------------+         +---------------------------------------+
        ///                  | !IsPartitionedFeedOperation |         |    IsPartitionedFeedOperation         |
        ///                  |      TransportHandler       |         | invalidPartitionExceptionRetryHandler |
        ///                  |                             |         |                                       |
        ///                  +-----------------------------+         +---------------------------------------+
        ///                                                                          |
        ///                                                                          |
        ///                                                                          |
        ///                                                          +---------------------------------------+
        ///                                                          |                                       |
        ///                                                          |     PartitionKeyRangeHandler          |
        ///                                                          |                                       |
        ///                                                          +---------------------------------------+
        ///                                                                          |
        ///                                                                          |
        ///                                                                          |
        ///                                                          +---------------------------------------+
        ///                                                          |                                       |
        ///                                                          |         TransportHandler              |
        ///                                                          |                                       |
        ///                                                          +---------------------------------------+
        /// </summary>
        /// <returns>The request invoker handler used to do calls to Cosmos DB</returns>
        public RequestInvokerHandler Build()
        {
            RequestInvokerHandler root = new RequestInvokerHandler(
                client,
                requestedClientConsistencyLevel);

            RequestHandler current = root;
            if (CustomHandlers != null && CustomHandlers.Any())
            {
                foreach (RequestHandler handler in CustomHandlers)
                {
                    current.InnerHandler = handler;
                    current = current.InnerHandler;
                }
            }

            Debug.Assert(diagnosticsHandler != null, nameof(diagnosticsHandler));
            current.InnerHandler = diagnosticsHandler;
            current = current.InnerHandler;

            Debug.Assert(retryHandler != null, nameof(retryHandler));
            current.InnerHandler = retryHandler;
            current = current.InnerHandler;

            // Have a router handler
            RequestHandler feedHandler = CreateDocumentFeedPipeline();

            Debug.Assert(feedHandler != null, nameof(feedHandler));
            Debug.Assert(transportHandler.InnerHandler == null, nameof(transportHandler));
            RequestHandler routerHandler = new RouterHandler(
                documentFeedHandler: feedHandler,
                pointOperationHandler: transportHandler);

            current.InnerHandler = routerHandler;
            current = current.InnerHandler;

            return root;
        }

        internal static RequestHandler CreatePipeline(params RequestHandler[] requestHandlers)
        {
            RequestHandler head = null;
            int handlerCount = requestHandlers.Length;
            for (int i = handlerCount - 1; i >= 0; i--)
            {
                RequestHandler indexHandler = requestHandlers[i];
                if (indexHandler.InnerHandler != null)
                {
                    throw new ArgumentOutOfRangeException($"The requestHandlers[{i}].InnerHandler is required to be null to allow the pipeline to chain the handlers.");
                }

                if (head != null)
                {
                    indexHandler.InnerHandler = head;
                }
                head = indexHandler;
            }

            return head;
        }

        private ClientPipelineBuilder UseRetryPolicy()
        {
            retryHandler = new RetryHandler(client);
            Debug.Assert(retryHandler.InnerHandler == null, "The retryHandler.InnerHandler must be null to allow other handlers to be linked.");
            return this;
        }

        private ClientPipelineBuilder AddCustomHandlers(IReadOnlyCollection<RequestHandler> customHandlers)
        {
            CustomHandlers = customHandlers;
            return this;
        }

        private RequestHandler CreateDocumentFeedPipeline()
        {
            RequestHandler[] feedPipeline = new RequestHandler[]
                {
                    invalidPartitionExceptionRetryHandler,
                    PartitionKeyRangeHandler,
                    transportHandler,
                };

            return ClientPipelineBuilder.CreatePipeline(feedPipeline);
        }
    }
}
