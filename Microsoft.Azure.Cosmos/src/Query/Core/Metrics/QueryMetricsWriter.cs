//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Query.Core.Metrics
{
    using System;
    using System.Linq;

    /// <summary>
    /// Base class for visiting and serializing a <see cref="QueryMetrics"/>.
    /// </summary>
#if INTERNAL
#pragma warning disable SA1600
#pragma warning disable CS1591
    public
#else
    internal
#endif
    abstract class QueryMetricsWriter
    {
        public void WriteQueryMetrics(QueryMetrics queryMetrics)
        {
            WriteBeforeQueryMetrics();

            // Top Level Properties
            WriteRetrievedDocumentCount(queryMetrics.BackendMetrics.RetrievedDocumentCount);
            WriteRetrievedDocumentSize(queryMetrics.BackendMetrics.RetrievedDocumentSize);
            WriteOutputDocumentCount(queryMetrics.BackendMetrics.OutputDocumentCount);
            WriteOutputDocumentSize(queryMetrics.BackendMetrics.OutputDocumentSize);
            WriteIndexHitRatio(queryMetrics.BackendMetrics.IndexHitRatio);

            WriteTotalQueryExecutionTime(queryMetrics.BackendMetrics.TotalTime);

            // QueryPreparationTimes
            WriteQueryPreparationTimes(queryMetrics.BackendMetrics.QueryPreparationTimes);

            WriteIndexLookupTime(queryMetrics.BackendMetrics.IndexLookupTime);
            WriteDocumentLoadTime(queryMetrics.BackendMetrics.DocumentLoadTime);
            WriteVMExecutionTime(queryMetrics.BackendMetrics.VMExecutionTime);

            // RuntimesExecutionTimes
            WriteRuntimesExecutionTimes(queryMetrics.BackendMetrics.RuntimeExecutionTimes);

            WriteDocumentWriteTime(queryMetrics.BackendMetrics.DocumentWriteTime);

            // ClientSideMetrics
            WriteClientSideMetrics(queryMetrics.ClientSideMetrics);

            // IndexUtilizationInfo
            WriteBeforeIndexUtilizationInfo();

            WriteIndexUtilizationInfo(queryMetrics.IndexUtilizationInfo);

            WriteAfterQueryMetrics();
        }

        protected abstract void WriteBeforeQueryMetrics();

        protected abstract void WriteRetrievedDocumentCount(long retrievedDocumentCount);

        protected abstract void WriteRetrievedDocumentSize(long retrievedDocumentSize);

        protected abstract void WriteOutputDocumentCount(long outputDocumentCount);

        protected abstract void WriteOutputDocumentSize(long outputDocumentSize);

        protected abstract void WriteIndexHitRatio(double indexHitRatio);

        protected abstract void WriteTotalQueryExecutionTime(TimeSpan totalQueryExecutionTime);

        #region QueryPreparationTimes
        private void WriteQueryPreparationTimes(QueryPreparationTimes queryPreparationTimes)
        {
            WriteBeforeQueryPreparationTimes();

            WriteQueryCompilationTime(queryPreparationTimes.QueryCompilationTime);
            WriteLogicalPlanBuildTime(queryPreparationTimes.LogicalPlanBuildTime);
            WritePhysicalPlanBuildTime(queryPreparationTimes.PhysicalPlanBuildTime);
            WriteQueryOptimizationTime(queryPreparationTimes.QueryOptimizationTime);

            WriteAfterQueryPreparationTimes();
        }

        protected abstract void WriteBeforeQueryPreparationTimes();

        protected abstract void WriteQueryCompilationTime(TimeSpan queryCompilationTime);

        protected abstract void WriteLogicalPlanBuildTime(TimeSpan logicalPlanBuildTime);

        protected abstract void WritePhysicalPlanBuildTime(TimeSpan physicalPlanBuildTime);

        protected abstract void WriteQueryOptimizationTime(TimeSpan queryOptimizationTime);

        protected abstract void WriteAfterQueryPreparationTimes();
        #endregion

        protected abstract void WriteIndexLookupTime(TimeSpan indexLookupTime);

        protected abstract void WriteDocumentLoadTime(TimeSpan documentLoadTime);

        protected abstract void WriteVMExecutionTime(TimeSpan vMExecutionTime);

        #region RuntimeExecutionTimes
        private void WriteRuntimesExecutionTimes(RuntimeExecutionTimes runtimeExecutionTimes)
        {
            WriteBeforeRuntimeExecutionTimes();

            WriteQueryEngineExecutionTime(runtimeExecutionTimes.QueryEngineExecutionTime);
            WriteSystemFunctionExecutionTime(runtimeExecutionTimes.SystemFunctionExecutionTime);
            WriteUserDefinedFunctionExecutionTime(runtimeExecutionTimes.UserDefinedFunctionExecutionTime);

            WriteAfterRuntimeExecutionTimes();
        }

        protected abstract void WriteBeforeRuntimeExecutionTimes();

        protected abstract void WriteQueryEngineExecutionTime(TimeSpan queryEngineExecutionTime);

        protected abstract void WriteSystemFunctionExecutionTime(TimeSpan systemFunctionExecutionTime);

        protected abstract void WriteUserDefinedFunctionExecutionTime(TimeSpan userDefinedFunctionExecutionTime);

        protected abstract void WriteAfterRuntimeExecutionTimes();
        #endregion

        protected abstract void WriteDocumentWriteTime(TimeSpan documentWriteTime);

        #region ClientSideMetrics
        private void WriteClientSideMetrics(ClientSideMetrics clientSideMetrics)
        {
            WriteBeforeClientSideMetrics();

            WriteRetries(clientSideMetrics.Retries);
            WriteRequestCharge(clientSideMetrics.RequestCharge);
            WritePartitionExecutionTimeline(clientSideMetrics);

            WriteAfterClientSideMetrics();
        }

        protected abstract void WriteBeforeClientSideMetrics();

        protected abstract void WriteRetries(long retries);

        protected abstract void WriteRequestCharge(double requestCharge);

        private void WritePartitionExecutionTimeline(ClientSideMetrics clientSideMetrics)
        {
            WriteBeforePartitionExecutionTimeline();

            foreach (FetchExecutionRange fetchExecutionRange in clientSideMetrics.FetchExecutionRanges.OrderBy(fetchExecutionRange => fetchExecutionRange.StartTime))
            {
                WriteFetchExecutionRange(fetchExecutionRange);
            }

            WriteAfterPartitionExecutionTimeline();
        }

        protected abstract void WriteBeforePartitionExecutionTimeline();

        private void WriteFetchExecutionRange(FetchExecutionRange fetchExecutionRange)
        {
            WriteBeforeFetchExecutionRange();

            WriteFetchPartitionKeyRangeId(fetchExecutionRange.PartitionId);
            WriteActivityId(fetchExecutionRange.ActivityId);
            WriteStartTime(fetchExecutionRange.StartTime);
            WriteEndTime(fetchExecutionRange.EndTime);
            WriteFetchDocumentCount(fetchExecutionRange.NumberOfDocuments);
            WriteFetchRetryCount(fetchExecutionRange.RetryCount);

            WriteAfterFetchExecutionRange();
        }

        protected abstract void WriteBeforeFetchExecutionRange();

        protected abstract void WriteFetchPartitionKeyRangeId(string partitionId);

        protected abstract void WriteActivityId(string activityId);

        protected abstract void WriteStartTime(DateTime startTime);

        protected abstract void WriteEndTime(DateTime endTime);

        protected abstract void WriteFetchDocumentCount(long numberOfDocuments);

        protected abstract void WriteFetchRetryCount(long retryCount);

        protected abstract void WriteAfterFetchExecutionRange();

        protected abstract void WriteAfterPartitionExecutionTimeline();

        protected abstract void WriteBeforeSchedulingMetrics();

        private void WritePartitionSchedulingTimeSpan(string partitionId, SchedulingTimeSpan schedulingTimeSpan)
        {
            WriteBeforePartitionSchedulingTimeSpan();

            WritePartitionSchedulingTimeSpanId(partitionId);
            WriteResponseTime(schedulingTimeSpan.ResponseTime);
            WriteRunTime(schedulingTimeSpan.RunTime);
            WriteWaitTime(schedulingTimeSpan.WaitTime);
            WriteTurnaroundTime(schedulingTimeSpan.TurnaroundTime);
            WriteNumberOfPreemptions(schedulingTimeSpan.NumPreemptions);

            WriteAfterPartitionSchedulingTimeSpan();
        }

        protected abstract void WriteBeforePartitionSchedulingTimeSpan();

        protected abstract void WritePartitionSchedulingTimeSpanId(string partitionId);

        protected abstract void WriteResponseTime(TimeSpan responseTime);

        protected abstract void WriteRunTime(TimeSpan runTime);

        protected abstract void WriteWaitTime(TimeSpan waitTime);

        protected abstract void WriteTurnaroundTime(TimeSpan turnaroundTime);

        protected abstract void WriteNumberOfPreemptions(long numPreemptions);

        protected abstract void WriteAfterPartitionSchedulingTimeSpan();

        protected abstract void WriteAfterSchedulingMetrics();

        protected abstract void WriteAfterClientSideMetrics();
        #endregion

        #region IndexUtilizationInfo

        protected abstract void WriteBeforeIndexUtilizationInfo();

        protected abstract void WriteIndexUtilizationInfo(IndexUtilizationInfo indexUtilizationInfo);

        protected abstract void WriteAfterIndexUtilizationInfo();
        #endregion

        protected abstract void WriteAfterQueryMetrics();
    }
}
