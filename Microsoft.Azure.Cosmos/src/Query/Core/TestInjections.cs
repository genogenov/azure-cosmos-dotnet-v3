// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core
{
    internal sealed class TestInjections
    {
        public enum PipelineType
        {
            Passthrough,
            Specialized,
        }

        public TestInjections(bool simulate429s, bool simulateEmptyPages, ResponseStats responseStats = null)
        {
            SimulateThrottles = simulate429s;
            SimulateEmptyPages = simulateEmptyPages;
            Stats = responseStats;
        }

        public bool SimulateThrottles { get; }

        public bool SimulateEmptyPages { get; }

        public ResponseStats Stats { get; }

        public sealed class ResponseStats
        {
            public PipelineType? PipelineType { get; set; }
        }
    }
}
