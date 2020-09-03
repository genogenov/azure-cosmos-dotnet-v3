//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Diagnostics
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// This represents a single scope in the diagnostics.
    /// A scope is a section of code that is important to track.
    /// For example there is a scope for serialization, retry handlers, etc..
    /// </summary>
    internal sealed class CosmosDiagnosticScope : CosmosDiagnosticsInternal, IDisposable
    {
        private readonly Stopwatch ElapsedTimeStopWatch;
        private bool isDisposed = false;

        public CosmosDiagnosticScope(
            string name)
        {
            Id = name;
            ElapsedTimeStopWatch = Stopwatch.StartNew();
        }

        public string Id { get; }

        public bool TryGetElapsedTime(out TimeSpan elapsedTime)
        {
            if (!isDisposed)
            {
                return false;
            }

            elapsedTime = ElapsedTimeStopWatch.Elapsed;
            return true;
        }

        internal TimeSpan GetElapsedTime()
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
