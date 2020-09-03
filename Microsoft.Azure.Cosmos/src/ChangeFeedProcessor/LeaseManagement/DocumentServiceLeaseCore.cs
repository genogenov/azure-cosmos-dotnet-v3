//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Newtonsoft.Json;

    [Serializable]
    internal sealed class DocumentServiceLeaseCore : DocumentServiceLease
    {
        private static readonly DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        // Used to detect if the user is migrating from a V2 CFP schema
        private bool isMigratingFromV2 = false;

        public DocumentServiceLeaseCore()
        {
        }

        public DocumentServiceLeaseCore(DocumentServiceLeaseCore other)
        {
            LeaseId = other.LeaseId;
            LeaseToken = other.LeaseToken;
            Owner = other.Owner;
            ContinuationToken = other.ContinuationToken;
            ETag = other.ETag;
            TS = other.TS;
            ExplicitTimestamp = other.ExplicitTimestamp;
            Properties = other.Properties;
        }

        [JsonProperty("id")]
        public string LeaseId { get; set; }

        [JsonIgnore]
        public override string Id => LeaseId;

        [JsonProperty("_etag")]
        public string ETag { get; set; }

        [JsonProperty("LeaseToken")]
        public string LeaseToken { get; set; }

        [JsonProperty("PartitionId", NullValueHandling = NullValueHandling.Ignore)]
        private string PartitionId
        {
            get
            {
                if (isMigratingFromV2)
                {
                    // If the user migrated the lease from V2 schema, we maintain the PartitionId property for backward compatibility
                    return LeaseToken;
                }

                return null;
            }
            set
            {
                LeaseToken = value;
                isMigratingFromV2 = true;
            }
        }

        [JsonIgnore]
        public override string CurrentLeaseToken => LeaseToken;

        [JsonProperty("Owner")]
        public override string Owner { get; set; }

        /// <summary>
        /// Gets or sets the current value for the offset in the stream.
        /// </summary>
        [JsonProperty("ContinuationToken")]
        public override string ContinuationToken { get; set; }

        [JsonIgnore]
        public override DateTime Timestamp
        {
            get { return ExplicitTimestamp ?? UnixStartTime.AddSeconds(TS); }
            set { ExplicitTimestamp = value; }
        }

        [JsonIgnore]
        public override string ConcurrencyToken => ETag;

        [JsonProperty("properties")]
        public override Dictionary<string, string> Properties { get; set; } = new Dictionary<string, string>();

        [JsonProperty("timestamp")]
        private DateTime? ExplicitTimestamp { get; set; }

        [JsonProperty("_ts")]
        private long TS { get; set; }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} Owner='{1}' Continuation={2} Timestamp(local)={3} Timestamp(server)={4}",
                Id,
                Owner,
                ContinuationToken,
                Timestamp.ToUniversalTime(),
                UnixStartTime.AddSeconds(TS).ToUniversalTime());
        }
    }
}