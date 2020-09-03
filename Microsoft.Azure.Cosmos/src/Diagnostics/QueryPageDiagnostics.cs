//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Diagnostics
{
    using System;

    internal sealed class QueryPageDiagnostics : CosmosDiagnosticsInternal
    {
        public QueryPageDiagnostics(
            Guid clientQueryCorrelationId,
            string partitionKeyRangeId,
            string queryMetricText,
            string indexUtilizationText,
            CosmosDiagnosticsContext diagnosticsContext)
        {
            ClientCorrelationId = clientQueryCorrelationId;
            PartitionKeyRangeId = partitionKeyRangeId ?? throw new ArgumentNullException(nameof(partitionKeyRangeId));
            QueryMetricText = queryMetricText ?? string.Empty;
            IndexUtilizationText = indexUtilizationText ?? string.Empty;
            DiagnosticsContext = diagnosticsContext;
        }

        /// <summary>
        /// A client id for the query. This can be used to
        /// correlate multiple query responses to a single
        /// query iterator.
        /// </summary>
        public Guid ClientCorrelationId { get; }

        public string PartitionKeyRangeId { get; }

        public string QueryMetricText { get; }

        public string IndexUtilizationText { get; }

        public CosmosDiagnosticsContext DiagnosticsContext { get; }

        public override void Accept(CosmosDiagnosticsInternalVisitor visitor)
        {
            visitor.Visit(this);
        }

        public override TResult Accept<TResult>(CosmosDiagnosticsInternalVisitor<TResult> visitor)
        {
            return visitor.Visit(this);
        }
    }
}
