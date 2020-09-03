//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Scripts
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;

    internal abstract class ScriptsCore : Scripts
    {
        private readonly ContainerInternal container;

        internal ScriptsCore(
            ContainerInternal container,
            CosmosClientContext clientContext)
        {
            this.container = container;
            ClientContext = clientContext;
        }

        protected CosmosClientContext ClientContext { get; }

        public Task<StoredProcedureResponse> CreateStoredProcedureAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            StoredProcedureProperties storedProcedureProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            return ProcessScriptsCreateOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: container.LinkUri,
                resourceType: ResourceType.StoredProcedure,
                operationType: OperationType.Create,
                streamPayload: ClientContext.SerializerCore.ToStream(storedProcedureProperties),
                requestOptions: requestOptions,
                responseFunc: ClientContext.ResponseFactory.CreateStoredProcedureResponse,
                cancellationToken: cancellationToken);
        }

        public override FeedIterator GetStoredProcedureQueryStreamIterator(
           string queryText = null,
           string continuationToken = null,
           QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetStoredProcedureQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator<T> GetStoredProcedureQueryIterator<T>(
            string queryText = null,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetStoredProcedureQueryIterator<T>(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator GetStoredProcedureQueryStreamIterator(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            return new FeedIteratorCore(
               clientContext: ClientContext,
               container.LinkUri,
               resourceType: ResourceType.StoredProcedure,
               queryDefinition: queryDefinition,
               continuationToken: continuationToken,
               options: requestOptions);
        }

        public override FeedIterator<T> GetStoredProcedureQueryIterator<T>(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            if (!(GetStoredProcedureQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions) is FeedIteratorInternal databaseStreamIterator))
            {
                throw new InvalidOperationException($"Expected a FeedIteratorInternal.");
            }

            return new FeedIteratorCore<T>(
                databaseStreamIterator,
                (response) => ClientContext.ResponseFactory.CreateQueryFeedResponse<T>(
                    responseMessage: response,
                    resourceType: ResourceType.StoredProcedure));
        }

        public Task<StoredProcedureResponse> ReadStoredProcedureAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessStoredProcedureOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Read,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<StoredProcedureResponse> ReplaceStoredProcedureAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            StoredProcedureProperties storedProcedureProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            return ProcessStoredProcedureOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: storedProcedureProperties.Id,
                operationType: OperationType.Replace,
                streamPayload: ClientContext.SerializerCore.ToStream(storedProcedureProperties),
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<StoredProcedureResponse> DeleteStoredProcedureAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessStoredProcedureOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Delete,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public async Task<StoredProcedureExecuteResponse<TOutput>> ExecuteStoredProcedureAsync<TOutput>(
            CosmosDiagnosticsContext diagnosticsContext,
            string storedProcedureId,
            Cosmos.PartitionKey partitionKey,
            dynamic[] parameters,
            StoredProcedureRequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            ResponseMessage response = await ExecuteStoredProcedureStreamAsync(
                diagnosticsContext: diagnosticsContext,
                storedProcedureId: storedProcedureId,
                partitionKey: partitionKey,
                parameters: parameters,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);

            return ClientContext.ResponseFactory.CreateStoredProcedureExecuteResponse<TOutput>(response);
        }

        public Task<ResponseMessage> ExecuteStoredProcedureStreamAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string storedProcedureId,
            Cosmos.PartitionKey partitionKey,
            dynamic[] parameters,
            StoredProcedureRequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            Stream streamPayload = null;
            if (parameters != null)
            {
                streamPayload = ClientContext.SerializerCore.ToStream<dynamic[]>(parameters);
            }

            return ExecuteStoredProcedureStreamAsync(
                diagnosticsContext: diagnosticsContext,
                storedProcedureId: storedProcedureId,
                partitionKey: partitionKey,
                streamPayload: streamPayload,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<ResponseMessage> ExecuteStoredProcedureStreamAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string storedProcedureId,
            Stream streamPayload,
            Cosmos.PartitionKey partitionKey,
            StoredProcedureRequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(storedProcedureId))
            {
                throw new ArgumentNullException(nameof(storedProcedureId));
            }

            ContainerInternal.ValidatePartitionKey(partitionKey, requestOptions);

            string linkUri = ClientContext.CreateLink(
                parentLink: container.LinkUri,
                uriPathSegment: Paths.StoredProceduresPathSegment,
                id: storedProcedureId);

            return ProcessStreamOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: linkUri,
                resourceType: ResourceType.StoredProcedure,
                operationType: OperationType.ExecuteJavaScript,
                partitionKey: partitionKey,
                streamPayload: streamPayload,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<TriggerResponse> CreateTriggerAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            TriggerProperties triggerProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (triggerProperties == null)
            {
                throw new ArgumentNullException(nameof(triggerProperties));
            }

            if (string.IsNullOrEmpty(triggerProperties.Id))
            {
                throw new ArgumentNullException(nameof(triggerProperties.Id));
            }

            if (string.IsNullOrEmpty(triggerProperties.Body))
            {
                throw new ArgumentNullException(nameof(triggerProperties.Body));
            }

            return ProcessScriptsCreateOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: container.LinkUri,
                resourceType: ResourceType.Trigger,
                operationType: OperationType.Create,
                streamPayload: ClientContext.SerializerCore.ToStream(triggerProperties),
                requestOptions: requestOptions,
                responseFunc: ClientContext.ResponseFactory.CreateTriggerResponse,
                cancellationToken: cancellationToken);
        }

        public override FeedIterator GetTriggerQueryStreamIterator(
           string queryText = null,
           string continuationToken = null,
           QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetTriggerQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator<T> GetTriggerQueryIterator<T>(
            string queryText = null,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetTriggerQueryIterator<T>(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator GetTriggerQueryStreamIterator(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            return new FeedIteratorCore(
               clientContext: ClientContext,
               container.LinkUri,
               resourceType: ResourceType.Trigger,
               queryDefinition: queryDefinition,
               continuationToken: continuationToken,
               options: requestOptions);
        }

        public override FeedIterator<T> GetTriggerQueryIterator<T>(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            if (!(GetTriggerQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions) is FeedIteratorInternal databaseStreamIterator))
            {
                throw new InvalidOperationException($"Expected a FeedIteratorInternal.");
            }

            return new FeedIteratorCore<T>(
                databaseStreamIterator,
                (response) => ClientContext.ResponseFactory.CreateQueryFeedResponse<T>(
                    responseMessage: response,
                    resourceType: ResourceType.Trigger));
        }

        public Task<TriggerResponse> ReadTriggerAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessTriggerOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Read,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<TriggerResponse> ReplaceTriggerAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            TriggerProperties triggerProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (triggerProperties == null)
            {
                throw new ArgumentNullException(nameof(triggerProperties));
            }

            if (string.IsNullOrEmpty(triggerProperties.Id))
            {
                throw new ArgumentNullException(nameof(triggerProperties.Id));
            }

            if (string.IsNullOrEmpty(triggerProperties.Body))
            {
                throw new ArgumentNullException(nameof(triggerProperties.Body));
            }

            return ProcessTriggerOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: triggerProperties.Id,
                operationType: OperationType.Replace,
                streamPayload: ClientContext.SerializerCore.ToStream(triggerProperties),
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<TriggerResponse> DeleteTriggerAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessTriggerOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Delete,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<UserDefinedFunctionResponse> CreateUserDefinedFunctionAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            UserDefinedFunctionProperties userDefinedFunctionProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (userDefinedFunctionProperties == null)
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties));
            }

            if (string.IsNullOrEmpty(userDefinedFunctionProperties.Id))
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties.Id));
            }

            if (string.IsNullOrEmpty(userDefinedFunctionProperties.Body))
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties.Body));
            }

            return ProcessScriptsCreateOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: container.LinkUri,
                resourceType: ResourceType.UserDefinedFunction,
                operationType: OperationType.Create,
                streamPayload: ClientContext.SerializerCore.ToStream(userDefinedFunctionProperties),
                requestOptions: requestOptions,
                responseFunc: ClientContext.ResponseFactory.CreateUserDefinedFunctionResponse,
                cancellationToken: cancellationToken);
        }

        public override FeedIterator GetUserDefinedFunctionQueryStreamIterator(
           string queryText = null,
           string continuationToken = null,
           QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetUserDefinedFunctionQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator<T> GetUserDefinedFunctionQueryIterator<T>(
            string queryText = null,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            QueryDefinition queryDefinition = null;
            if (queryText != null)
            {
                queryDefinition = new QueryDefinition(queryText);
            }

            return GetUserDefinedFunctionQueryIterator<T>(
                queryDefinition,
                continuationToken,
                requestOptions);
        }

        public override FeedIterator GetUserDefinedFunctionQueryStreamIterator(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            return new FeedIteratorCore(
               clientContext: ClientContext,
               container.LinkUri,
               resourceType: ResourceType.UserDefinedFunction,
               queryDefinition: queryDefinition,
               continuationToken: continuationToken,
               options: requestOptions);
        }

        public override FeedIterator<T> GetUserDefinedFunctionQueryIterator<T>(
            QueryDefinition queryDefinition,
            string continuationToken = null,
            QueryRequestOptions requestOptions = null)
        {
            if (!(GetUserDefinedFunctionQueryStreamIterator(
                queryDefinition,
                continuationToken,
                requestOptions) is FeedIteratorInternal databaseStreamIterator))
            {
                throw new InvalidOperationException($"Expected a FeedIteratorInternal.");
            }

            return new FeedIteratorCore<T>(
                databaseStreamIterator,
                (response) => ClientContext.ResponseFactory.CreateQueryFeedResponse<T>(
                    responseMessage: response,
                    resourceType: ResourceType.UserDefinedFunction));
        }

        public Task<UserDefinedFunctionResponse> ReadUserDefinedFunctionAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessUserDefinedFunctionOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Read,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<UserDefinedFunctionResponse> ReplaceUserDefinedFunctionAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            UserDefinedFunctionProperties userDefinedFunctionProperties,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (userDefinedFunctionProperties == null)
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties));
            }

            if (string.IsNullOrEmpty(userDefinedFunctionProperties.Id))
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties.Id));
            }

            if (string.IsNullOrEmpty(userDefinedFunctionProperties.Body))
            {
                throw new ArgumentNullException(nameof(userDefinedFunctionProperties.Body));
            }

            return ProcessUserDefinedFunctionOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: userDefinedFunctionProperties.Id,
                operationType: OperationType.Replace,
                streamPayload: ClientContext.SerializerCore.ToStream(userDefinedFunctionProperties),
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        public Task<UserDefinedFunctionResponse> DeleteUserDefinedFunctionAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentNullException(nameof(id));
            }

            return ProcessUserDefinedFunctionOperationAsync(
                diagnosticsContext: diagnosticsContext,
                id: id,
                operationType: OperationType.Delete,
                streamPayload: null,
                requestOptions: requestOptions,
                cancellationToken: cancellationToken);
        }

        private async Task<StoredProcedureResponse> ProcessStoredProcedureOperationAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            OperationType operationType,
            Stream streamPayload,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            string linkUri = ClientContext.CreateLink(
                parentLink: container.LinkUri,
                uriPathSegment: Paths.StoredProceduresPathSegment,
                id: id);

            ResponseMessage response = await ProcessStreamOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: linkUri,
                resourceType: ResourceType.StoredProcedure,
                operationType: operationType,
                requestOptions: requestOptions,
                partitionKey: null,
                streamPayload: streamPayload,
                cancellationToken: cancellationToken);

            return ClientContext.ResponseFactory.CreateStoredProcedureResponse(response);
        }

        private async Task<TriggerResponse> ProcessTriggerOperationAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            OperationType operationType,
            Stream streamPayload,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            string linkUri = ClientContext.CreateLink(
                parentLink: container.LinkUri,
                uriPathSegment: Paths.TriggersPathSegment,
                id: id);

            ResponseMessage response = await ProcessStreamOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: linkUri,
                resourceType: ResourceType.Trigger,
                operationType: operationType,
                requestOptions: requestOptions,
                partitionKey: null,
                streamPayload: streamPayload,
                cancellationToken: cancellationToken);

            return ClientContext.ResponseFactory.CreateTriggerResponse(response);
        }

        private Task<ResponseMessage> ProcessStreamOperationAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string resourceUri,
            ResourceType resourceType,
            OperationType operationType,
            Cosmos.PartitionKey? partitionKey,
            Stream streamPayload,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            return ClientContext.ProcessResourceOperationStreamAsync(
                resourceUri: resourceUri,
                resourceType: resourceType,
                operationType: operationType,
                requestOptions: requestOptions,
                cosmosContainerCore: container,
                partitionKey: partitionKey,
                streamPayload: streamPayload,
                requestEnricher: null,
                diagnosticsContext: diagnosticsContext,
                cancellationToken: cancellationToken);
        }

        private async Task<T> ProcessScriptsCreateOperationAsync<T>(
            CosmosDiagnosticsContext diagnosticsContext,
            string resourceUri,
            ResourceType resourceType,
            OperationType operationType,
            Stream streamPayload,
            RequestOptions requestOptions,
            Func<ResponseMessage, T> responseFunc,
            CancellationToken cancellationToken)
        {
            ResponseMessage response = await ProcessStreamOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: resourceUri,
                resourceType: resourceType,
                operationType: operationType,
                requestOptions: requestOptions,
                partitionKey: null,
                streamPayload: streamPayload,
                cancellationToken: cancellationToken);

            return responseFunc(response);
        }

        private async Task<UserDefinedFunctionResponse> ProcessUserDefinedFunctionOperationAsync(
            CosmosDiagnosticsContext diagnosticsContext,
            string id,
            OperationType operationType,
            Stream streamPayload,
            RequestOptions requestOptions,
            CancellationToken cancellationToken)
        {
            string linkUri = ClientContext.CreateLink(
                parentLink: container.LinkUri,
                uriPathSegment: Paths.UserDefinedFunctionsPathSegment,
                id: id);

            ResponseMessage response = await ProcessStreamOperationAsync(
                diagnosticsContext: diagnosticsContext,
                resourceUri: linkUri,
                resourceType: ResourceType.UserDefinedFunction,
                operationType: operationType,
                requestOptions: requestOptions,
                partitionKey: null,
                streamPayload: streamPayload,
                cancellationToken: cancellationToken);

            return ClientContext.ResponseFactory.CreateUserDefinedFunctionResponse(response);
        }
    }
}
