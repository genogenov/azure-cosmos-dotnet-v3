//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using Microsoft.Azure.Cosmos.Scripts;

    internal sealed class CosmosResponseFactoryCore : CosmosResponseFactoryInternal
    {
        /// <summary>
        /// This is used for all meta data types
        /// </summary>
        private readonly CosmosSerializerCore serializerCore;

        public CosmosResponseFactoryCore(
            CosmosSerializerCore jsonSerializerCore)
        {
            serializerCore = jsonSerializerCore;
        }

        public override FeedResponse<T> CreateItemFeedResponse<T>(ResponseMessage responseMessage)
        {
            return CreateQueryFeedResponseHelper<T>(
                responseMessage,
                Documents.ResourceType.Document);
        }

        public override FeedResponse<T> CreateChangeFeedUserTypeResponse<T>(
            ResponseMessage responseMessage)
        {
            return CreateChangeFeedResponseHelper<T>(
                responseMessage);
        }

        public override FeedResponse<T> CreateQueryFeedUserTypeResponse<T>(
            ResponseMessage responseMessage)
        {
            return CreateQueryFeedResponseHelper<T>(
                responseMessage,
                Documents.ResourceType.Document);
        }

        public override FeedResponse<T> CreateQueryFeedResponse<T>(
            ResponseMessage responseMessage,
            Documents.ResourceType resourceType)
        {
            return CreateQueryFeedResponseHelper<T>(
                responseMessage,
                resourceType);
        }

        private FeedResponse<T> CreateQueryFeedResponseHelper<T>(
            ResponseMessage cosmosResponseMessage,
            Documents.ResourceType resourceType)
        {
            if (cosmosResponseMessage is QueryResponse queryResponse)
            {
                return QueryResponse<T>.CreateResponse<T>(
                    cosmosQueryResponse: queryResponse,
                    serializerCore: serializerCore);
            }

            return ReadFeedResponse<T>.CreateResponse<T>(
                       cosmosResponseMessage,
                       serializerCore);
        }

        private FeedResponse<T> CreateChangeFeedResponseHelper<T>(
            ResponseMessage cosmosResponseMessage)
        {
            return ReadFeedResponse<T>.CreateResponse<T>(
                       cosmosResponseMessage,
                       serializerCore);
        }

        public override ItemResponse<T> CreateItemResponse<T>(
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                T item = ToObjectpublic<T>(cosmosResponseMessage);
                return new ItemResponse<T>(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    item,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override ContainerResponse CreateContainerResponse(
            Container container,
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                ContainerProperties containerProperties = ToObjectpublic<ContainerProperties>(cosmosResponseMessage);
                return new ContainerResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    containerProperties,
                    container,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override UserResponse CreateUserResponse(
            User user,
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                UserProperties userProperties = ToObjectpublic<UserProperties>(cosmosResponseMessage);
                return new UserResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    userProperties,
                    user,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override PermissionResponse CreatePermissionResponse(
            Permission permission,
            ResponseMessage responseMessage)
        {
            return ProcessMessage<PermissionResponse>(responseMessage, (cosmosResponseMessage) =>
            {
                PermissionProperties permissionProperties = ToObjectpublic<PermissionProperties>(cosmosResponseMessage);
                return new PermissionResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    permissionProperties,
                    permission,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override DatabaseResponse CreateDatabaseResponse(
            Database database,
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                DatabaseProperties databaseProperties = ToObjectpublic<DatabaseProperties>(cosmosResponseMessage);

                return new DatabaseResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    databaseProperties,
                    database,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override ThroughputResponse CreateThroughputResponse(
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                ThroughputProperties throughputProperties = ToObjectpublic<ThroughputProperties>(cosmosResponseMessage);
                return new ThroughputResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    throughputProperties,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override StoredProcedureExecuteResponse<T> CreateStoredProcedureExecuteResponse<T>(ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                T item = ToObjectpublic<T>(cosmosResponseMessage);
                return new StoredProcedureExecuteResponse<T>(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    item,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override StoredProcedureResponse CreateStoredProcedureResponse(ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                StoredProcedureProperties cosmosStoredProcedure = ToObjectpublic<StoredProcedureProperties>(cosmosResponseMessage);
                return new StoredProcedureResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    cosmosStoredProcedure,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override TriggerResponse CreateTriggerResponse(ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                TriggerProperties triggerProperties = ToObjectpublic<TriggerProperties>(cosmosResponseMessage);
                return new TriggerResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    triggerProperties,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public override UserDefinedFunctionResponse CreateUserDefinedFunctionResponse(
            ResponseMessage responseMessage)
        {
            return ProcessMessage(responseMessage, (cosmosResponseMessage) =>
            {
                UserDefinedFunctionProperties settings = ToObjectpublic<UserDefinedFunctionProperties>(cosmosResponseMessage);
                return new UserDefinedFunctionResponse(
                    cosmosResponseMessage.StatusCode,
                    cosmosResponseMessage.Headers,
                    settings,
                    cosmosResponseMessage.Diagnostics);
            });
        }

        public T ProcessMessage<T>(ResponseMessage responseMessage, Func<ResponseMessage, T> createResponse)
        {
            using (ResponseMessage message = responseMessage)
            {
                //Throw the exception
                message.EnsureSuccessStatusCode();

                return createResponse(message);
            }
        }

        public T ToObjectpublic<T>(ResponseMessage responseMessage)
        {
            if (responseMessage.Content == null)
            {
                return default(T);
            }

            return serializerCore.FromStream<T>(responseMessage.Content);
        }
    }
}