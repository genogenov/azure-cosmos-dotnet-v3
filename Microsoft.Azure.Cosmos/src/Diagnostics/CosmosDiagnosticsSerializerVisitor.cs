//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Diagnostics
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using Microsoft.Azure.Documents.Rntbd;
    using Newtonsoft.Json;

    internal sealed class CosmosDiagnosticsSerializerVisitor : CosmosDiagnosticsInternalVisitor
    {
        private const string DiagnosticsVersion = "2";
        private readonly JsonWriter jsonWriter;

        public CosmosDiagnosticsSerializerVisitor(TextWriter textWriter)
        {
            jsonWriter = new JsonTextWriter(textWriter ?? throw new ArgumentNullException(nameof(textWriter)));
        }

        public override void Visit(PointOperationStatistics pointOperationStatistics)
        {
            jsonWriter.WriteStartObject();
            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue("PointOperationStatistics");

            jsonWriter.WritePropertyName("ActivityId");
            jsonWriter.WriteValue(pointOperationStatistics.ActivityId);

            jsonWriter.WritePropertyName("ResponseTimeUtc");
            jsonWriter.WriteValue(pointOperationStatistics.ResponseTimeUtc.ToString("o", CultureInfo.InvariantCulture));

            jsonWriter.WritePropertyName("StatusCode");
            jsonWriter.WriteValue((int)pointOperationStatistics.StatusCode);

            jsonWriter.WritePropertyName("SubStatusCode");
            jsonWriter.WriteValue((int)pointOperationStatistics.SubStatusCode);

            jsonWriter.WritePropertyName("RequestCharge");
            jsonWriter.WriteValue(pointOperationStatistics.RequestCharge);

            jsonWriter.WritePropertyName("RequestUri");
            jsonWriter.WriteValue(pointOperationStatistics.RequestUri);

            if (!string.IsNullOrEmpty(pointOperationStatistics.ErrorMessage))
            {
                jsonWriter.WritePropertyName("ErrorMessage");
                jsonWriter.WriteValue(pointOperationStatistics.ErrorMessage);
            }

            jsonWriter.WritePropertyName("RequestSessionToken");
            jsonWriter.WriteValue(pointOperationStatistics.RequestSessionToken);

            jsonWriter.WritePropertyName("ResponseSessionToken");
            jsonWriter.WriteValue(pointOperationStatistics.ResponseSessionToken);

            jsonWriter.WriteEndObject();
        }

        public override void Visit(CosmosDiagnosticsContext cosmosDiagnosticsContext)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("DiagnosticVersion");
            jsonWriter.WriteValue(DiagnosticsVersion);

            jsonWriter.WritePropertyName("Summary");
            jsonWriter.WriteStartObject();
            jsonWriter.WritePropertyName("StartUtc");
            jsonWriter.WriteValue(cosmosDiagnosticsContext.StartUtc.ToString("o", CultureInfo.InvariantCulture));

            if (cosmosDiagnosticsContext.TryGetTotalElapsedTime(out TimeSpan totalElapsedTime))
            {
                jsonWriter.WritePropertyName("TotalElapsedTimeInMs");
                jsonWriter.WriteValue(totalElapsedTime.TotalMilliseconds);
            }
            else
            {
                jsonWriter.WritePropertyName("RunningElapsedTimeInMs");
                jsonWriter.WriteValue(cosmosDiagnosticsContext.GetRunningElapsedTime().TotalMilliseconds);
            }

            jsonWriter.WritePropertyName("UserAgent");
            jsonWriter.WriteValue(cosmosDiagnosticsContext.UserAgent);

            jsonWriter.WritePropertyName("TotalRequestCount");
            jsonWriter.WriteValue(cosmosDiagnosticsContext.GetTotalRequestCount());

            jsonWriter.WritePropertyName("FailedRequestCount");
            jsonWriter.WriteValue(cosmosDiagnosticsContext.GetFailedRequestCount());

            jsonWriter.WriteEndObject();

            jsonWriter.WritePropertyName("Context");
            jsonWriter.WriteStartArray();

            foreach (CosmosDiagnosticsInternal cosmosDiagnosticsInternal in cosmosDiagnosticsContext)
            {
                cosmosDiagnosticsInternal.Accept(this);
            }

            jsonWriter.WriteEndArray();

            jsonWriter.WriteEndObject();
        }

        public override void Visit(CosmosDiagnosticScope cosmosDiagnosticScope)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue(cosmosDiagnosticScope.Id);

            if (cosmosDiagnosticScope.IsComplete())
            {
                jsonWriter.WritePropertyName("ElapsedTimeInMs");
            }
            else
            {
                jsonWriter.WritePropertyName("RunningElapsedTimeInMs");
            }

            jsonWriter.WriteValue(cosmosDiagnosticScope.GetElapsedTime().TotalMilliseconds);

            jsonWriter.WriteEndObject();
        }

        public override void Visit(QueryPageDiagnostics queryPageDiagnostics)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("PKRangeId");
            jsonWriter.WriteValue(queryPageDiagnostics.PartitionKeyRangeId);

            jsonWriter.WritePropertyName("StartUtc");
            jsonWriter.WriteValue(queryPageDiagnostics.DiagnosticsContext.StartUtc.ToString("o", CultureInfo.InvariantCulture));

            jsonWriter.WritePropertyName("QueryMetric");
            jsonWriter.WriteValue(queryPageDiagnostics.QueryMetricText);

            jsonWriter.WritePropertyName("IndexUtilization");
            jsonWriter.WriteValue(queryPageDiagnostics.IndexUtilizationText);

            jsonWriter.WritePropertyName("ClientCorrelationId");
            jsonWriter.WriteValue(queryPageDiagnostics.ClientCorrelationId);

            jsonWriter.WritePropertyName("Context");
            jsonWriter.WriteStartArray();

            foreach (CosmosDiagnosticsInternal cosmosDiagnosticsInternal in queryPageDiagnostics.DiagnosticsContext)
            {
                cosmosDiagnosticsInternal.Accept(this);
            }

            jsonWriter.WriteEndArray();

            jsonWriter.WriteEndObject();
        }

        public override void Visit(AddressResolutionStatistics addressResolutionStatistics)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue("AddressResolutionStatistics");

            jsonWriter.WritePropertyName("StartTimeUtc");
            jsonWriter.WriteValue(addressResolutionStatistics.StartTime.ToString("o", CultureInfo.InvariantCulture));

            jsonWriter.WritePropertyName("EndTimeUtc");
            if (addressResolutionStatistics.EndTime.HasValue)
            {
                jsonWriter.WriteValue(addressResolutionStatistics.EndTime.Value.ToString("o", CultureInfo.InvariantCulture));

                jsonWriter.WritePropertyName("ElapsedTimeInMs");
                TimeSpan totaltime = addressResolutionStatistics.EndTime.Value - addressResolutionStatistics.StartTime;
                jsonWriter.WriteValue(totaltime.TotalMilliseconds);
            }
            else
            {
                jsonWriter.WriteValue("EndTime Never Set.");
            }

            jsonWriter.WritePropertyName("TargetEndpoint");
            jsonWriter.WriteValue(addressResolutionStatistics.TargetEndpoint);

            jsonWriter.WriteEndObject();
        }

        public override void Visit(StoreResponseStatistics storeResponseStatistics)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue("StoreResponseStatistics");

            jsonWriter.WritePropertyName("StartTimeUtc");
            if (storeResponseStatistics.RequestStartTime.HasValue)
            {
                jsonWriter.WriteValue(storeResponseStatistics.RequestStartTime.Value.ToString("o", CultureInfo.InvariantCulture));
            }
            else
            {
                jsonWriter.WriteValue("Start time never set");
            }

            jsonWriter.WritePropertyName("ResponseTimeUtc");
            jsonWriter.WriteValue(storeResponseStatistics.RequestResponseTime.ToString("o", CultureInfo.InvariantCulture));

            if (storeResponseStatistics.RequestStartTime.HasValue)
            {
                jsonWriter.WritePropertyName("ElapsedTimeInMs");
                TimeSpan totaltime = storeResponseStatistics.RequestResponseTime - storeResponseStatistics.RequestStartTime.Value;
                jsonWriter.WriteValue(totaltime.TotalMilliseconds);
            }

            jsonWriter.WritePropertyName("ResourceType");
            jsonWriter.WriteValue(storeResponseStatistics.RequestResourceType.ToString());

            jsonWriter.WritePropertyName("OperationType");
            jsonWriter.WriteValue(storeResponseStatistics.RequestOperationType.ToString());

            jsonWriter.WritePropertyName("LocationEndpoint");
            jsonWriter.WriteValue(storeResponseStatistics.LocationEndpoint);

            if (storeResponseStatistics.StoreResult != null)
            {
                jsonWriter.WritePropertyName("ActivityId");
                jsonWriter.WriteValue(storeResponseStatistics.StoreResult.ActivityId);

                jsonWriter.WritePropertyName("StoreResult");
                jsonWriter.WriteValue(storeResponseStatistics.StoreResult.ToString());
            }

            jsonWriter.WriteEndObject();
        }

        public override void Visit(CosmosClientSideRequestStatistics clientSideRequestStatistics)
        {
            jsonWriter.WriteStartObject();
            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue("AggregatedClientSideRequestStatistics");

            WriteJsonUriArrayWithDuplicatesCounted("ContactedReplicas", clientSideRequestStatistics.ContactedReplicas);

            WriteJsonUriArray("RegionsContacted", clientSideRequestStatistics.RegionsContacted);
            WriteJsonUriArray("FailedReplicas", clientSideRequestStatistics.FailedReplicas);

            jsonWriter.WriteEndObject();
        }

        public override void Visit(FeedRangeStatistics feedRangeStatistics)
        {
            jsonWriter.WriteStartObject();
            jsonWriter.WritePropertyName("FeedRange");
            jsonWriter.WriteValue(feedRangeStatistics.FeedRange.ToString());
            jsonWriter.WriteEndObject();
        }

        public override void Visit(RequestHandlerScope requestHandlerScope)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue(requestHandlerScope.Id);

            if (requestHandlerScope.TryGetTotalElapsedTime(out TimeSpan handlerOnlyElapsedTime))
            {
                jsonWriter.WritePropertyName("HandlerElapsedTimeInMs");
                jsonWriter.WriteValue(handlerOnlyElapsedTime.TotalMilliseconds);
            }
            else
            {
                jsonWriter.WritePropertyName("HandlerRunningElapsedTimeInMs");
                jsonWriter.WriteValue(requestHandlerScope.GetCurrentElapsedTime());
            }

            jsonWriter.WriteEndObject();
        }

        public override void Visit(CosmosSystemInfo processInfo)
        {
            jsonWriter.WriteStartObject();

            jsonWriter.WritePropertyName("Id");
            jsonWriter.WriteValue("SystemInfo");

            jsonWriter.WritePropertyName("CpuHistory");
            CpuLoadHistory cpuLoadHistory = processInfo.CpuLoadHistory;
            jsonWriter.WriteValue(cpuLoadHistory.ToString());

            jsonWriter.WriteEndObject();
        }

        private void WriteJsonUriArray(string propertyName, IEnumerable<Uri> uris)
        {
            jsonWriter.WritePropertyName(propertyName);
            jsonWriter.WriteStartArray();

            if (uris != null)
            {
                foreach (Uri contactedReplica in uris)
                {
                    jsonWriter.WriteValue(contactedReplica);
                }
            }

            jsonWriter.WriteEndArray();
        }

        /// <summary>
        /// Writes the list of URIs to JSON.
        /// Sequential duplicates are counted and written as a single object to prevent
        /// writing the same URI multiple times.
        /// </summary>
        private void WriteJsonUriArrayWithDuplicatesCounted(string propertyName, List<Uri> uris)
        {
            jsonWriter.WritePropertyName(propertyName);
            jsonWriter.WriteStartArray();

            if (uris != null)
            {
                Uri previous = null;
                int duplicateCount = 1;
                int totalCount = uris.Count;
                for (int i = 0; i < totalCount; i++)
                {
                    Uri contactedReplica = uris[i];
                    if (contactedReplica.Equals(previous))
                    {
                        duplicateCount++;
                        // Don't continue for last link so it get's printed
                        if (i < totalCount - 1)
                        {
                            continue;
                        }
                    }

                    // The URI is not a duplicate.
                    // Write previous URI and count.
                    // Then update them to the new URI and count
                    if (previous != null)
                    {
                        jsonWriter.WriteStartObject();
                        jsonWriter.WritePropertyName("Count");
                        jsonWriter.WriteValue(duplicateCount);
                        jsonWriter.WritePropertyName("Uri");
                        jsonWriter.WriteValue(contactedReplica);
                        jsonWriter.WriteEndObject();
                    }

                    previous = contactedReplica;
                    duplicateCount = 1;
                }
            }

            jsonWriter.WriteEndArray();
        }
    }
}
