//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using Microsoft.Azure.Cosmos.Diagnostics;
    using Microsoft.Azure.Documents;

    internal sealed class CosmosClientSideRequestStatistics : CosmosDiagnosticsInternal, IClientSideRequestStatistics
    {
        public const string DefaultToStringMessage = "Please see CosmosDiagnostics";
        private readonly object lockObject = new object();
        private readonly long clientSideRequestStatisticsCreateTime;

        private long? firstStartRequestTimestamp;
        private long? lastStartRequestTimestamp;
        private long cumulativeEstimatedDelayDueToRateLimitingInStopwatchTicks = 0;
        private bool received429ResponseSinceLastStartRequest = false;

        public CosmosClientSideRequestStatistics(CosmosDiagnosticsContext diagnosticsContext = null)
        {
            RequestStartTimeUtc = DateTime.UtcNow;
            RequestEndTimeUtc = null;
            EndpointToAddressResolutionStatistics = new Dictionary<string, AddressResolutionStatistics>();
            ContactedReplicas = new List<Uri>();
            FailedReplicas = new HashSet<Uri>();
            RegionsContacted = new HashSet<Uri>();
            DiagnosticsContext = diagnosticsContext ?? CosmosDiagnosticsContextCore.Create(requestOptions: null);
            DiagnosticsContext.AddDiagnosticsInternal(this);
            clientSideRequestStatisticsCreateTime = Stopwatch.GetTimestamp();
        }

        private DateTime RequestStartTimeUtc { get; }

        private DateTime? RequestEndTimeUtc { get; set; }

        private Dictionary<string, AddressResolutionStatistics> EndpointToAddressResolutionStatistics { get; }

        private readonly Dictionary<int, DateTime> RecordRequestHashCodeToStartTime = new Dictionary<int, DateTime>();

        public List<Uri> ContactedReplicas { get; set; }

        public HashSet<Uri> FailedReplicas { get; }

        public HashSet<Uri> RegionsContacted { get; }

        public TimeSpan RequestLatency
        {
            get
            {
                if (RequestEndTimeUtc.HasValue)
                {
                    return RequestEndTimeUtc.Value - RequestStartTimeUtc;
                }

                return TimeSpan.MaxValue;
            }
        }

        public bool IsCpuOverloaded { get; private set; } = false;

        public CosmosDiagnosticsContext DiagnosticsContext { get; }

        public TimeSpan EstimatedClientDelayFromRateLimiting => TimeSpan.FromSeconds(cumulativeEstimatedDelayDueToRateLimitingInStopwatchTicks / (double)Stopwatch.Frequency);

        public TimeSpan EstimatedClientDelayFromAllCauses
        {
            get
            {
                if (!lastStartRequestTimestamp.HasValue || !firstStartRequestTimestamp.HasValue)
                {
                    return TimeSpan.Zero;
                }

                // Stopwatch ticks are not equivalent to DateTime ticks
                long clientDelayInStopWatchTicks = lastStartRequestTimestamp.Value - firstStartRequestTimestamp.Value;
                return TimeSpan.FromSeconds(clientDelayInStopWatchTicks / (double)Stopwatch.Frequency);
            }
        }

        public void RecordRequest(DocumentServiceRequest request)
        {
            lock (lockObject)
            {
                long timestamp = Stopwatch.GetTimestamp();
                if (received429ResponseSinceLastStartRequest)
                {
                    long lastTimestamp = lastStartRequestTimestamp ?? clientSideRequestStatisticsCreateTime;
                    cumulativeEstimatedDelayDueToRateLimitingInStopwatchTicks += timestamp - lastTimestamp;
                }

                if (!firstStartRequestTimestamp.HasValue)
                {
                    firstStartRequestTimestamp = timestamp;
                }

                lastStartRequestTimestamp = timestamp;
                received429ResponseSinceLastStartRequest = false;
            }

            RecordRequestHashCodeToStartTime[request.GetHashCode()] = DateTime.UtcNow;
        }

        public void RecordResponse(DocumentServiceRequest request, StoreResult storeResult)
        {
            // One DocumentServiceRequest can map to multiple store results
            DateTime? startDateTime = null;
            if (RecordRequestHashCodeToStartTime.TryGetValue(request.GetHashCode(), out DateTime startRequestTime))
            {
                startDateTime = startRequestTime;
            }
            else
            {
                Debug.Fail("DocumentServiceRequest start time not recorded");
            }

            DateTime responseTime = DateTime.UtcNow;
            Uri locationEndpoint = request.RequestContext.LocationEndpointToRoute;
            StoreResponseStatistics responseStatistics = new StoreResponseStatistics(
                startDateTime,
                responseTime,
                storeResult,
                request.ResourceType,
                request.OperationType,
                locationEndpoint);

            if (storeResult?.IsClientCpuOverloaded ?? false)
            {
                IsCpuOverloaded = true;
            }

            lock (lockObject)
            {
                if (!RequestEndTimeUtc.HasValue || responseTime > RequestEndTimeUtc)
                {
                    RequestEndTimeUtc = responseTime;
                }

                if (locationEndpoint != null)
                {
                    RegionsContacted.Add(locationEndpoint);
                }

                DiagnosticsContext.AddDiagnosticsInternal(responseStatistics);

                if (!received429ResponseSinceLastStartRequest &&
                    storeResult.StatusCode == StatusCodes.TooManyRequests)
                {
                    received429ResponseSinceLastStartRequest = true;
                }
            }
        }

        public string RecordAddressResolutionStart(Uri targetEndpoint)
        {
            string identifier = Guid.NewGuid().ToString();
            AddressResolutionStatistics resolutionStats = new AddressResolutionStatistics(
                startTime: DateTime.UtcNow,
                endTime: DateTime.MaxValue,
                targetEndpoint: targetEndpoint == null ? "<NULL>" : targetEndpoint.ToString());

            lock (lockObject)
            {
                EndpointToAddressResolutionStatistics.Add(identifier, resolutionStats);
                DiagnosticsContext.AddDiagnosticsInternal(resolutionStats);
            }

            return identifier;
        }

        public void RecordAddressResolutionEnd(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
            {
                return;
            }

            DateTime responseTime = DateTime.UtcNow;
            lock (lockObject)
            {
                if (!EndpointToAddressResolutionStatistics.ContainsKey(identifier))
                {
                    throw new ArgumentException("Identifier {0} does not exist. Please call start before calling end.", identifier);
                }

                if (!RequestEndTimeUtc.HasValue || responseTime > RequestEndTimeUtc)
                {
                    RequestEndTimeUtc = responseTime;
                }

                EndpointToAddressResolutionStatistics[identifier].EndTime = responseTime;
            }
        }

        /// <summary>
        /// The new Cosmos Exception always includes the diagnostics and the
        /// document client exception message. Some of the older document client exceptions
        /// include the request statistics in the message causing a circle reference.
        /// This always returns empty string to prevent the circle reference which
        /// would cause the diagnostic string to grow exponentially.
        /// </summary>
        public override string ToString()
        {
            return DefaultToStringMessage;
        }

        /// <summary>
        /// Please see ToString() documentation
        /// </summary>
        public void AppendToBuilder(StringBuilder stringBuilder)
        {
            stringBuilder.Append(DefaultToStringMessage);
        }

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