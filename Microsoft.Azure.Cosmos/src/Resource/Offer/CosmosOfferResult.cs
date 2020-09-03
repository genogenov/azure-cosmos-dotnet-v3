//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos
{
    using System.Net;

    internal class CosmosOfferResult
    {
        public CosmosOfferResult(int? throughput)
        {
            Throughput = throughput;
            StatusCode = throughput.HasValue ? HttpStatusCode.OK : HttpStatusCode.NotFound;
        }

        public CosmosOfferResult(
            HttpStatusCode statusCode,
            CosmosException cosmosRequestException)
        {
            StatusCode = statusCode;
            CosmosException = cosmosRequestException;
        }

        public CosmosException CosmosException { get; }

        public HttpStatusCode StatusCode { get; }

        public int? Throughput { get; }
    }
}
