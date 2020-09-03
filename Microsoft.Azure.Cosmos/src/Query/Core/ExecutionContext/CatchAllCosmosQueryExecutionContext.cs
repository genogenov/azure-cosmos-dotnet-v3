// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.ExecutionContext
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Query.Core.QueryClient;

    internal sealed class CatchAllCosmosQueryExecutionContext : CosmosQueryExecutionContext
    {
        private readonly CosmosQueryExecutionContext cosmosQueryExecutionContext;
        private bool hitException;

        public CatchAllCosmosQueryExecutionContext(
            CosmosQueryExecutionContext cosmosQueryExecutionContext)
        {
            this.cosmosQueryExecutionContext = cosmosQueryExecutionContext ?? throw new ArgumentNullException(nameof(cosmosQueryExecutionContext));
        }

        public override bool IsDone => hitException || cosmosQueryExecutionContext.IsDone;

        public override void Dispose()
        {
            cosmosQueryExecutionContext.Dispose();
        }

        public override async Task<QueryResponseCore> ExecuteNextAsync(CancellationToken cancellationToken)
        {
            if (IsDone)
            {
                throw new InvalidOperationException(
                    $"Can not {nameof(ExecuteNextAsync)} from a {nameof(CosmosQueryExecutionContext)} where {nameof(IsDone)}.");
            }

            cancellationToken.ThrowIfCancellationRequested();

            QueryResponseCore queryResponseCore;
            try
            {
                queryResponseCore = await cosmosQueryExecutionContext.ExecuteNextAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Per cancellationToken.ThrowIfCancellationRequested(); line above, this function should still throw OperationCanceledException.
                throw;
            }
            catch (Exception ex)
            {
                queryResponseCore = QueryResponseFactory.CreateFromException(ex);
            }

            if (!queryResponseCore.IsSuccess)
            {
                hitException = true;
            }

            return queryResponseCore;
        }

        public override CosmosElement GetCosmosElementContinuationToken()
        {
            return cosmosQueryExecutionContext.GetCosmosElementContinuationToken();
        }
    }
}
