//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Diagnostics
{
    using System;
    using System.Diagnostics;

    internal sealed class RequestHandlerScope : CosmosDiagnosticsInternal, IDisposable
    {
        private readonly Stopwatch ElapsedTimeStopWatch;
        private bool isDisposed = false;

        public RequestHandlerScope(RequestHandler handler)
        {
            if (handler == null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            Id = handler.GetType().FullName;
            ElapsedTimeStopWatch = Stopwatch.StartNew();
        }

        public string Id { get; }

        public bool TryGetTotalElapsedTime(out TimeSpan elapsedTime)
        {
            if (!isDisposed)
            {
                return false;
            }

            elapsedTime = ElapsedTimeStopWatch.Elapsed;
            return true;
        }

        internal TimeSpan GetCurrentElapsedTime()
        {
            return ElapsedTimeStopWatch.Elapsed;
        }

        internal bool IsComplete()
        {
            return !ElapsedTimeStopWatch.IsRunning;
        }

        public void Dispose()
        {
            if (isDisposed)
            {
                return;
            }

            ElapsedTimeStopWatch.Stop();
            isDisposed = true;
        }

        public override void Accept(CosmosDiagnosticsInternalVisitor cosmosDiagnosticsInternalVisitor)
        {
            cosmosDiagnosticsInternalVisitor.Visit(this);
        }

        public override TResult Accept<TResult>(CosmosDiagnosticsInternalVisitor<TResult> visitor)
        {
            return visitor.Visit(this);
        }
    }
}
