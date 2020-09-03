//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Cosmos.Diagnostics;

    /// <summary>
    /// This represents the core diagnostics object used in the SDK.
    /// This object gets created on the initial request and passed down
    /// through the pipeline appending information as it goes into a list
    /// where it is lazily converted to a JSON string.
    /// </summary>
    internal sealed class CosmosDiagnosticsContextCore : CosmosDiagnosticsContext
    {
        private static readonly string DefaultUserAgentString;
        private readonly CosmosDiagnosticScope overallScope;

        /// <summary>
        /// Detailed view of all the operations.
        /// </summary>
        private List<CosmosDiagnosticsInternal> ContextList { get; }

        private int totalRequestCount = 0;
        private int failedRequestCount = 0;

        static CosmosDiagnosticsContextCore()
        {
            // Default user agent string does not contain client id or features.
            UserAgentContainer userAgentContainer = new UserAgentContainer();
            CosmosDiagnosticsContextCore.DefaultUserAgentString = userAgentContainer.UserAgent;
        }

        public CosmosDiagnosticsContextCore()
            : this(nameof(CosmosDiagnosticsContextCore),
                  CosmosDiagnosticsContextCore.DefaultUserAgentString)
        {
        }

        public CosmosDiagnosticsContextCore(
            string operationName,
            string userAgentString)
        {
            UserAgent = userAgentString ?? throw new ArgumentNullException(nameof(userAgentString));
            OperationName = operationName ?? throw new ArgumentNullException(nameof(operationName));
            StartUtc = DateTime.UtcNow;
            ContextList = new List<CosmosDiagnosticsInternal>();
            Diagnostics = new CosmosDiagnosticsCore(this);
            overallScope = new CosmosDiagnosticScope("Overall");
        }

        public override DateTime StartUtc { get; }

        public override string UserAgent { get; }

        public override string OperationName { get; }

        internal override CosmosDiagnostics Diagnostics { get; }

        internal override IDisposable GetOverallScope()
        {
            return overallScope;
        }

        internal override TimeSpan GetRunningElapsedTime()
        {
            return overallScope.GetElapsedTime();
        }

        internal override bool TryGetTotalElapsedTime(out TimeSpan timeSpan)
        {
            return overallScope.TryGetElapsedTime(out timeSpan);
        }

        internal override bool IsComplete()
        {
            return overallScope.IsComplete();
        }

        public override int GetTotalRequestCount()
        {
            return totalRequestCount;
        }

        public override int GetFailedRequestCount()
        {
            return failedRequestCount;
        }

        internal override IDisposable CreateScope(string name)
        {
            CosmosDiagnosticScope scope = new CosmosDiagnosticScope(name);

            ContextList.Add(scope);
            return scope;
        }

        internal override IDisposable CreateRequestHandlerScopeScope(RequestHandler requestHandler)
        {
            RequestHandlerScope requestHandlerScope = new RequestHandlerScope(requestHandler);
            ContextList.Add(requestHandlerScope);
            return requestHandlerScope;
        }

        internal override void AddDiagnosticsInternal(CosmosSystemInfo processInfo)
        {
            if (processInfo == null)
            {
                throw new ArgumentNullException(nameof(processInfo));
            }

            ContextList.Add(processInfo);
        }

        internal override void AddDiagnosticsInternal(PointOperationStatistics pointOperationStatistics)
        {
            if (pointOperationStatistics == null)
            {
                throw new ArgumentNullException(nameof(pointOperationStatistics));
            }

            AddRequestCount((int)pointOperationStatistics.StatusCode);

            ContextList.Add(pointOperationStatistics);
        }

        internal override void AddDiagnosticsInternal(StoreResponseStatistics storeResponseStatistics)
        {
            if (storeResponseStatistics.StoreResult != null)
            {
                AddRequestCount((int)storeResponseStatistics.StoreResult.StatusCode);
            }

            ContextList.Add(storeResponseStatistics);
        }

        internal override void AddDiagnosticsInternal(AddressResolutionStatistics addressResolutionStatistics)
        {
            ContextList.Add(addressResolutionStatistics);
        }

        internal override void AddDiagnosticsInternal(CosmosClientSideRequestStatistics clientSideRequestStatistics)
        {
            ContextList.Add(clientSideRequestStatistics);
        }

        internal override void AddDiagnosticsInternal(FeedRangeStatistics feedRangeStatistics)
        {
            ContextList.Add(feedRangeStatistics);
        }

        internal override void AddDiagnosticsInternal(QueryPageDiagnostics queryPageDiagnostics)
        {
            if (queryPageDiagnostics == null)
            {
                throw new ArgumentNullException(nameof(queryPageDiagnostics));
            }

            if (queryPageDiagnostics.DiagnosticsContext != null)
            {
                AddSummaryInfo(queryPageDiagnostics.DiagnosticsContext);
            }

            ContextList.Add(queryPageDiagnostics);
        }

        internal override void AddDiagnosticsInternal(CosmosDiagnosticsContext newContext)
        {
            AddSummaryInfo(newContext);

            ContextList.AddRange(newContext);
        }

        public override void Accept(CosmosDiagnosticsInternalVisitor cosmosDiagnosticsInternalVisitor)
        {
            cosmosDiagnosticsInternalVisitor.Visit(this);
        }

        public override TResult Accept<TResult>(CosmosDiagnosticsInternalVisitor<TResult> visitor)
        {
            return visitor.Visit(this);
        }

        public override IEnumerator<CosmosDiagnosticsInternal> GetEnumerator()
        {
            // Using a for loop with a yield prevents Issue #1467 which causes
            // ThrowInvalidOperationException if a new diagnostics is getting added
            // while the enumerator is being used.
            for (int i = 0; i < ContextList.Count; i++)
            {
                yield return ContextList[i];
            }
        }

        private void AddRequestCount(int statusCode)
        {
            totalRequestCount++;
            if (statusCode < 200 || statusCode > 299)
            {
                failedRequestCount++;
            }
        }

        private void AddSummaryInfo(CosmosDiagnosticsContext newContext)
        {
            if (Object.ReferenceEquals(this, newContext))
            {
                return;
            }

            totalRequestCount += newContext.GetTotalRequestCount();
            failedRequestCount += newContext.GetFailedRequestCount();
        }
    }
}
