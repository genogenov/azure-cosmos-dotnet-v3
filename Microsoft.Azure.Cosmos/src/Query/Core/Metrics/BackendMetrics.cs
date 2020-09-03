//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Metrics
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Metrics received for queries from the backend.
    /// </summary>
#if INTERNAL
#pragma warning disable SA1600
#pragma warning disable CS1591
    public
#else
    internal
#endif
    sealed class BackendMetrics
    {
        /// <summary>
        /// QueryMetrics that with all members having default (but not null) members.
        /// </summary>
        public static readonly BackendMetrics Empty = new BackendMetrics(
            retrievedDocumentCount: default,
            retrievedDocumentSize: default,
            outputDocumentCount: default,
            outputDocumentSize: default,
            indexHitRatio: default,
            totalQueryExecutionTime: default,
            queryPreparationTimes: QueryPreparationTimes.Zero,
            indexLookupTime: default,
            documentLoadTime: default,
            vmExecutionTime: default,
            runtimeExecutionTimes: RuntimeExecutionTimes.Empty,
            documentWriteTime: default);

        public BackendMetrics(
           long retrievedDocumentCount,
           long retrievedDocumentSize,
           long outputDocumentCount,
           long outputDocumentSize,
           double indexHitRatio,
           TimeSpan totalQueryExecutionTime,
           QueryPreparationTimes queryPreparationTimes,
           TimeSpan indexLookupTime,
           TimeSpan documentLoadTime,
           TimeSpan vmExecutionTime,
           RuntimeExecutionTimes runtimeExecutionTimes,
           TimeSpan documentWriteTime)
        {
            RetrievedDocumentCount = retrievedDocumentCount;
            RetrievedDocumentSize = retrievedDocumentSize;
            OutputDocumentCount = outputDocumentCount;
            OutputDocumentSize = outputDocumentSize;
            IndexHitRatio = indexHitRatio;
            TotalTime = totalQueryExecutionTime;
            QueryPreparationTimes = queryPreparationTimes ?? throw new ArgumentNullException($"{nameof(queryPreparationTimes)} can not be null.");
            IndexLookupTime = indexLookupTime;
            DocumentLoadTime = documentLoadTime;
            VMExecutionTime = vmExecutionTime;
            RuntimeExecutionTimes = runtimeExecutionTimes ?? throw new ArgumentNullException($"{nameof(runtimeExecutionTimes)} can not be null.");
            DocumentWriteTime = documentWriteTime;
        }

        /// <summary>
        /// Gets the total query time in the Azure Cosmos database service.
        /// </summary>
        public TimeSpan TotalTime { get; }

        /// <summary>
        /// Gets the number of documents retrieved during query in the Azure Cosmos database service.
        /// </summary>
        public long RetrievedDocumentCount { get; }

        /// <summary>
        /// Gets the size of documents retrieved in bytes during query in the Azure Cosmos DB service.
        /// </summary>
        public long RetrievedDocumentSize { get; }

        /// <summary>
        /// Gets the number of documents returned by query in the Azure Cosmos DB service.
        /// </summary>
        public long OutputDocumentCount { get; }

        /// <summary>
        /// Gets the size of documents outputted in bytes during query in the Azure Cosmos database service.
        /// </summary>
        public long OutputDocumentSize { get; }

        /// <summary>
        /// Gets the query QueryPreparationTimes in the Azure Cosmos database service.
        /// </summary>
        public QueryPreparationTimes QueryPreparationTimes { get; }

        /// <summary>
        /// Gets the query index lookup time in the Azure Cosmos database service.
        /// </summary>
        public TimeSpan IndexLookupTime { get; }

        /// <summary>
        /// Gets the document loading time during query in the Azure Cosmos database service.
        /// </summary>
        public TimeSpan DocumentLoadTime { get; }

        /// <summary>
        /// Gets the query runtime execution times during query in the Azure Cosmos database service.
        /// </summary>
        public RuntimeExecutionTimes RuntimeExecutionTimes { get; }

        /// <summary>
        /// Gets the output writing/serializing time during query in the Azure Cosmos database service.
        /// </summary>
        public TimeSpan DocumentWriteTime { get; }

        /// <summary>
        /// Gets the index hit ratio by query in the Azure Cosmos database service.
        /// </summary>
        public double IndexHitRatio { get; }

        /// <summary>
        /// Gets the VMExecution Time.
        /// </summary>
        public TimeSpan VMExecutionTime { get; }

        public override string ToString()
        {
            return $"totalExecutionTimeInMs={TotalTime.TotalMilliseconds};queryCompileTimeInMs={QueryPreparationTimes.QueryCompilationTime.TotalMilliseconds};queryLogicalPlanBuildTimeInMs={QueryPreparationTimes.LogicalPlanBuildTime.TotalMilliseconds};queryPhysicalPlanBuildTimeInMs={QueryPreparationTimes.PhysicalPlanBuildTime.TotalMilliseconds};queryOptimizationTimeInMs={QueryPreparationTimes.QueryOptimizationTime.TotalMilliseconds};indexLookupTimeInMs={IndexLookupTime.TotalMilliseconds};documentLoadTimeInMs={DocumentLoadTime.TotalMilliseconds};systemFunctionExecuteTimeInMs={RuntimeExecutionTimes.SystemFunctionExecutionTime.TotalMilliseconds};userFunctionExecuteTimeInMs={RuntimeExecutionTimes.UserDefinedFunctionExecutionTime.TotalMilliseconds};retrievedDocumentCount={RetrievedDocumentCount};retrievedDocumentSize={RetrievedDocumentSize};outputDocumentCount={OutputDocumentCount};outputDocumentSize={OutputDocumentSize};writeOutputTimeInMs={DocumentWriteTime.TotalMilliseconds};indexUtilizationRatio={IndexHitRatio}";
        }

        public static BackendMetrics CreateFromIEnumerable(IEnumerable<BackendMetrics> backendMetricsEnumerable)
        {
            BackendMetrics.Accumulator accumulator = default;
            foreach (BackendMetrics backendMetrics in backendMetricsEnumerable)
            {
                accumulator = accumulator.Accumulate(backendMetrics);
            }

            return BackendMetrics.Accumulator.ToBackendMetrics(accumulator);
        }

        public static bool TryParseFromDelimitedString(string delimitedString, out BackendMetrics backendMetrics)
        {
            return BackendMetricsParser.TryParse(delimitedString, out backendMetrics);
        }

        public static BackendMetrics ParseFromDelimitedString(string delimitedString)
        {
            if (!BackendMetricsParser.TryParse(delimitedString, out BackendMetrics backendMetrics))
            {
                throw new FormatException();
            }

            return backendMetrics;
        }

        public ref struct Accumulator
        {
            public Accumulator(
                TimeSpan totalTime,
                long retrievedDocumentCount,
                long retrievedDocumentSize,
                long outputDocumentCount,
                long outputDocumentSize,
                double indexHitRatio,
                QueryPreparationTimes.Accumulator queryPreparationTimesAccumulator,
                TimeSpan indexLookupTime,
                TimeSpan documentLoadTime,
                RuntimeExecutionTimes.Accumulator runtimeExecutionTimesAccumulator,
                TimeSpan documentWriteTime,
                TimeSpan vmExecutionTime)
            {
                TotalTime = totalTime;
                RetrievedDocumentCount = retrievedDocumentCount;
                RetrievedDocumentSize = retrievedDocumentSize;
                OutputDocumentCount = outputDocumentCount;
                OutputDocumentSize = outputDocumentSize;
                IndexHitRatio = indexHitRatio;
                QueryPreparationTimesAccumulator = queryPreparationTimesAccumulator;
                IndexLookupTime = indexLookupTime;
                DocumentLoadTime = documentLoadTime;
                RuntimeExecutionTimesAccumulator = runtimeExecutionTimesAccumulator;
                DocumentWriteTime = documentWriteTime;
                VMExecutionTime = vmExecutionTime;
            }

            public TimeSpan TotalTime { get; }
            public long RetrievedDocumentCount { get; }
            public long RetrievedDocumentSize { get; }
            public long OutputDocumentCount { get; }
            public long OutputDocumentSize { get; }
            public double IndexHitRatio { get; }
            public QueryPreparationTimes.Accumulator QueryPreparationTimesAccumulator { get; }
            public TimeSpan IndexLookupTime { get; }
            public TimeSpan DocumentLoadTime { get; }
            public RuntimeExecutionTimes.Accumulator RuntimeExecutionTimesAccumulator { get; }
            public TimeSpan DocumentWriteTime { get; }
            public TimeSpan VMExecutionTime { get; }

            public Accumulator Accumulate(BackendMetrics backendMetrics)
            {
                return new Accumulator(
                    totalTime: TotalTime + backendMetrics.TotalTime,
                    retrievedDocumentCount: RetrievedDocumentCount + backendMetrics.RetrievedDocumentCount,
                    retrievedDocumentSize: RetrievedDocumentSize + backendMetrics.RetrievedDocumentSize,
                    outputDocumentCount: OutputDocumentCount + backendMetrics.OutputDocumentCount,
                    outputDocumentSize: OutputDocumentSize + backendMetrics.OutputDocumentSize,
                    indexHitRatio: ((OutputDocumentCount * IndexHitRatio) + (backendMetrics.OutputDocumentCount * backendMetrics.IndexHitRatio)) / (RetrievedDocumentCount + backendMetrics.RetrievedDocumentCount),
                    queryPreparationTimesAccumulator: QueryPreparationTimesAccumulator.Accumulate(backendMetrics.QueryPreparationTimes),
                    indexLookupTime: IndexLookupTime + backendMetrics.IndexLookupTime,
                    documentLoadTime: DocumentLoadTime + backendMetrics.DocumentLoadTime,
                    runtimeExecutionTimesAccumulator: RuntimeExecutionTimesAccumulator.Accumulate(backendMetrics.RuntimeExecutionTimes),
                    documentWriteTime: DocumentWriteTime + backendMetrics.DocumentWriteTime,
                    vmExecutionTime: VMExecutionTime + backendMetrics.VMExecutionTime);

            }

            public static BackendMetrics ToBackendMetrics(BackendMetrics.Accumulator accumulator)
            {
                return new BackendMetrics(
                   retrievedDocumentCount: accumulator.RetrievedDocumentCount,
                   retrievedDocumentSize: accumulator.RetrievedDocumentSize,
                   outputDocumentCount: accumulator.OutputDocumentCount,
                   outputDocumentSize: accumulator.OutputDocumentSize,
                   indexHitRatio: accumulator.IndexHitRatio,
                   totalQueryExecutionTime: accumulator.TotalTime,
                   queryPreparationTimes: QueryPreparationTimes.Accumulator.ToQueryPreparationTimes(accumulator.QueryPreparationTimesAccumulator),
                   indexLookupTime: accumulator.IndexLookupTime,
                   documentLoadTime: accumulator.DocumentLoadTime,
                   vmExecutionTime: accumulator.VMExecutionTime,
                   runtimeExecutionTimes: RuntimeExecutionTimes.Accumulator.ToRuntimeExecutionTimes(accumulator.RuntimeExecutionTimesAccumulator),
                   documentWriteTime: accumulator.DocumentWriteTime);
            }
        }
    }
}
