//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Azure.Cosmos.Query.Core.Metrics;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Collections;

    /// <summary>
    /// Represents the template class used by feed methods (enumeration operations) for the Azure Cosmos DB service.
    /// </summary>
    /// <typeparam name="T">The feed type.</typeparam>
    internal class DocumentFeedResponse<T> : IEnumerable<T>, IDynamicMetaObjectProvider, IDocumentFeedResponse<T>
    {
        internal readonly string disallowContinuationTokenMessage;
        private readonly IEnumerable<T> inner;
        private readonly Dictionary<string, long> usageHeaders;
        private readonly Dictionary<string, long> quotaHeaders;
        private readonly bool useETagAsContinuation;
        private readonly IReadOnlyDictionary<string, QueryMetrics> queryMetrics;
        private INameValueCollection responseHeaders;

        /// <summary>
        /// Constructor exposed for mocking purposes.
        /// </summary>
        public DocumentFeedResponse()
        {
        }

        /// <summary>
        /// Constructor exposed for mocking purposes.
        /// </summary>
        /// <param name="result"></param>
        public DocumentFeedResponse(IEnumerable<T> result)
            : this()
        {
            inner = result != null ? result : Enumerable.Empty<T>();
        }

        internal DocumentFeedResponse(
            IEnumerable<T> result,
            int count,
            INameValueCollection responseHeaders,
            bool useETagAsContinuation = false,
            IReadOnlyDictionary<string, QueryMetrics> queryMetrics = null,
            IClientSideRequestStatistics requestStats = null,
            string disallowContinuationTokenMessage = null,
            long responseLengthBytes = 0)
            : this(result)
        {
            Count = count;
            this.responseHeaders = responseHeaders.Clone();
            usageHeaders = new Dictionary<string, long>();
            quotaHeaders = new Dictionary<string, long>();
            this.useETagAsContinuation = useETagAsContinuation;
            this.queryMetrics = queryMetrics;
            this.disallowContinuationTokenMessage = disallowContinuationTokenMessage;
            ResponseLengthBytes = responseLengthBytes;
        }

        internal DocumentFeedResponse(IEnumerable<T> result, int count, INameValueCollection responseHeaders, long responseLengthBytes)
    : this(result, count, responseHeaders)
        {
            ResponseLengthBytes = responseLengthBytes;
        }

        internal DocumentFeedResponse(
            IEnumerable<T> result,
            int count,
            INameValueCollection responseHeaders,
            IClientSideRequestStatistics requestStats,
            long responseLengthBytes)
            : this(result, count, responseHeaders, false, null, requestStats, responseLengthBytes: responseLengthBytes)
        {
        }

        /// <summary>
        /// Get the client side request statistics for the current request.
        /// </summary>
        /// <remarks>
        /// This value is currently used for tracking replica Uris.
        /// </remarks>
        internal IClientSideRequestStatistics RequestStatistics { get; private set; }

        /// <summary>
        /// Gets the response length in bytes
        /// </summary>
        /// <remarks>
        /// This value is only set for Direct mode.
        /// </remarks>
        internal long ResponseLengthBytes { get; private set; }

        /// <summary>
        /// Gets the maximum quota for database resources within the account from the Azure Cosmos DB service. 
        /// </summary>
        /// <value>
        /// The maximum quota for the account.
        /// </value>
        public long DatabaseQuota => GetMaxQuotaHeader(Constants.Quota.Database);

        /// <summary>
        /// Gets the current number of database resources within the account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The number of databases.
        /// </value>
        public long DatabaseUsage => GetCurrentQuotaHeader(Constants.Quota.Database);

        /// <summary>
        /// Gets the maximum quota for collection resources within an account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The maximum quota for the account.
        /// </value>
        public long CollectionQuota => GetMaxQuotaHeader(Constants.Quota.Collection);

        /// <summary>
        /// Gets the current number of collection resources within the account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The number of collections.
        /// </value>
        public long CollectionUsage => GetCurrentQuotaHeader(Constants.Quota.Collection);

        /// <summary>
        /// Gets the maximum quota for user resources within an account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The maximum quota for the account.
        /// </value>
        public long UserQuota => GetMaxQuotaHeader(Constants.Quota.User);

        /// <summary>
        /// Gets the current number of user resources within the account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The number of users.
        /// </value>
        public long UserUsage => GetCurrentQuotaHeader(Constants.Quota.User);

        /// <summary>
        /// Gets the maximum quota for permission resources within an account from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The maximum quota for the account.
        /// </value>
        public long PermissionQuota => GetMaxQuotaHeader(Constants.Quota.Permission);

        /// <summary>
        /// Gets the current number of permission resources within the account from the Azure Cosmos DB service. 
        /// </summary>
        /// <value>
        /// The number of permissions.
        /// </value>
        public long PermissionUsage => GetCurrentQuotaHeader(Constants.Quota.Permission);

        /// <summary>
        /// Gets the maximum size of a collection in kilobytes from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Quota in kilobytes.
        /// </value>
        public long CollectionSizeQuota => GetMaxQuotaHeader(Constants.Quota.CollectionSize);

        /// <summary>
        /// Gets the current size of a collection in kilobytes from the Azure Cosmos DB service. 
        /// </summary>
        /// <vallue>
        /// Current collection size in kilobytes.
        /// </vallue>
        public long CollectionSizeUsage => GetCurrentQuotaHeader(Constants.Quota.CollectionSize);

        /// <summary>
        /// Gets the maximum quota of stored procedures for a collection from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The maximum quota.
        /// </value>
        public long StoredProceduresQuota => GetMaxQuotaHeader(Constants.Quota.StoredProcedure);

        /// <summary>
        /// Gets the current number of stored procedures for a collection from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Current number of stored procedures.
        /// </value>
        public long StoredProceduresUsage => GetCurrentQuotaHeader(Constants.Quota.StoredProcedure);

        /// <summary>
        /// Gets the maximum quota of triggers for a collection from the Azure Cosmos DB service. 
        /// </summary>
        /// <value>
        /// The maximum quota.
        /// </value>
        public long TriggersQuota => GetMaxQuotaHeader(Constants.Quota.Trigger);

        /// <summary>
        /// Get the current number of triggers for a collection from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Current number of triggers.
        /// </value>
        public long TriggersUsage => GetCurrentQuotaHeader(Constants.Quota.Trigger);

        /// <summary>
        /// Gets the maximum quota of user defined functions for a collection from the Azure Cosmos DB service. 
        /// </summary>
        /// <value>
        /// Maximum quota.
        /// </value>
        public long UserDefinedFunctionsQuota => GetMaxQuotaHeader(Constants.Quota.UserDefinedFunction);

        /// <summary>
        /// Gets the current number of user defined functions for a collection from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Current number of user defined functions.
        /// </value>
        public long UserDefinedFunctionsUsage => GetCurrentQuotaHeader(Constants.Quota.UserDefinedFunction);

        /// <summary>
        /// Gets the number of items returned in the response from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Count of items in the response.
        /// </value>
        public int Count
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the maximum size limit for this entity from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The maximum size limit for this entity. Measured in kilobytes for document resources 
        /// and in counts for other resources.
        /// </value>
        public string MaxResourceQuota => responseHeaders[HttpConstants.HttpHeaders.MaxResourceQuota];

        /// <summary>
        /// Gets the current size of this entity from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The current size for this entity. Measured in kilobytes for document resources 
        /// and in counts for other resources.
        /// </value>
        public string CurrentResourceQuotaUsage => responseHeaders[HttpConstants.HttpHeaders.CurrentResourceQuotaUsage];

        /// <summary>
        /// Gets the request charge for this request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The request charge measured in reqest units.
        /// </value>
        public double RequestCharge => Helpers.GetHeaderValueDouble(
                    responseHeaders,
                    HttpConstants.HttpHeaders.RequestCharge,
                    0);

        /// <summary>
        /// Gets the activity ID for the request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The activity ID for the request.
        /// </value>
        public string ActivityId => responseHeaders[HttpConstants.HttpHeaders.ActivityId];

        /// <summary>
        /// Gets the continuation token to be used for continuing enumeration of the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The continuation token to be used for continuing enumeration.
        /// </value>
        public string ResponseContinuation
        {
            get
            {
                if (disallowContinuationTokenMessage != null)
                {
                    throw new ArgumentException(disallowContinuationTokenMessage);
                }

                return InternalResponseContinuation;
            }

            internal set
            {
                if (disallowContinuationTokenMessage != null)
                {
                    throw new ArgumentException(disallowContinuationTokenMessage);
                }

                Debug.Assert(!useETagAsContinuation);
                responseHeaders[HttpConstants.HttpHeaders.Continuation] = value;
            }
        }

        /// <summary>
        /// Gets the session token for use in sesssion consistency reads from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The session token for use in session consistency.
        /// </value>
        public string SessionToken => responseHeaders[HttpConstants.HttpHeaders.SessionToken];

        /// <summary>
        /// Gets the content parent location, for example, dbs/foo/colls/bar, from the Azure Cosmos DB service.
        /// </summary>
        public string ContentLocation => responseHeaders[HttpConstants.HttpHeaders.OwnerFullName];

        /// <summary>
        /// Gets the entity tag associated with last transaction in the Azure Cosmos DB service,
        /// which can be used as If-Non-Match Access condition for ReadFeed REST request or 
        /// ContinuationToken property of <see cref="ChangeFeedOptions"/> parameter for
        /// <see cref="DocumentClient.CreateDocumentChangeFeedQuery(string, ChangeFeedOptions)"/> 
        /// to get feed changes since the transaction specified by this entity tag.
        /// </summary>
        public string ETag => responseHeaders[HttpConstants.HttpHeaders.ETag];

        internal INameValueCollection Headers
        {
            get => responseHeaders;
            set => responseHeaders = value;
        }

        /// <summary>
        /// Gets the response headers from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The response headers.
        /// </value>
        public NameValueCollection ResponseHeaders => responseHeaders.ToNameValueCollection();

        /// <summary>
        /// Get QueryMetrics for each individual partition in the Azure Cosmos DB service
        /// </summary>
        public IReadOnlyDictionary<string, QueryMetrics> QueryMetrics => queryMetrics;

        /// <summary>
        /// Gets the continuation token to be used for continuing enumeration of the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The continuation token to be used for continuing enumeration.
        /// </value>
        internal string InternalResponseContinuation => useETagAsContinuation ?
                    ETag :
                    responseHeaders[HttpConstants.HttpHeaders.Continuation];

        /// <summary>
        /// Gets a dump for troubleshooting the request.
        /// </summary>
        public string RequestDiagnosticsString
        {
            get
            {
                StringBuilder requestDiagnosticsStringBuilder = new StringBuilder();
                requestDiagnosticsStringBuilder.AppendFormat("QueryMetrics: {0}", QueryMetrics);
                requestDiagnosticsStringBuilder.AppendLine();
                requestDiagnosticsStringBuilder.AppendFormat("ClientSideRequestStatistics: {0}", RequestStatistics);
                requestDiagnosticsStringBuilder.AppendLine();
                return requestDiagnosticsStringBuilder.ToString();
            }
        }

        // This is used by FeedResponseBinder.
        internal bool UseETagAsContinuation => useETagAsContinuation;

        internal string DisallowContinuationTokenMessage => disallowContinuationTokenMessage;

        /// <summary>
        /// Returns an enumerator that iterates through a collection from the Azure Cosmos DB service.
        /// </summary>
        /// <returns>An IEnumerator object that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<T> GetEnumerator()
        {
            return inner.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return inner.GetEnumerator();
        }

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
        {
            return new ResourceFeedDynamicObject(this, parameter);
        }

        private long GetCurrentQuotaHeader(string headerName)
        {
            if (usageHeaders.Count == 0 && !string.IsNullOrEmpty(MaxResourceQuota) && !string.IsNullOrEmpty(CurrentResourceQuotaUsage))
            {
                PopulateQuotaHeader(MaxResourceQuota, CurrentResourceQuotaUsage);
            }

            if (usageHeaders.TryGetValue(headerName, out long headerValue))
            {
                return headerValue;
            }

            return 0;
        }

        private long GetMaxQuotaHeader(string headerName)
        {
            if (quotaHeaders.Count == 0 && !string.IsNullOrEmpty(MaxResourceQuota) && !string.IsNullOrEmpty(CurrentResourceQuotaUsage))
            {
                PopulateQuotaHeader(MaxResourceQuota, CurrentResourceQuotaUsage);
            }

            if (quotaHeaders.TryGetValue(headerName, out long headerValue))
            {
                return headerValue;
            }

            return 0;
        }

        private void PopulateQuotaHeader(string headerMaxQuota, string headerCurrentUsage)
        {
            string[] headerMaxQuotaWords = headerMaxQuota.Split(Constants.Quota.DelimiterChars, StringSplitOptions.RemoveEmptyEntries);
            string[] headerCurrentUsageWords = headerCurrentUsage.Split(Constants.Quota.DelimiterChars, StringSplitOptions.RemoveEmptyEntries);

            Debug.Assert(headerMaxQuotaWords.Length == headerCurrentUsageWords.Length, "Headers returned should be consistent for max and current usage");

            for (int i = 0; i < headerMaxQuotaWords.Length; ++i)
            {
                if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.Database,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.Database, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.Database, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.Collection,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.Collection, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.Collection, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));

                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.User,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.User, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.User, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.Permission,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.Permission, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.Permission, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.CollectionSize,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.CollectionSize, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.CollectionSize, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.StoredProcedure,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.StoredProcedure, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.StoredProcedure, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.Trigger,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.Trigger, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.Trigger, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
                else if (string.Equals(
                    headerMaxQuotaWords[i],
                    Constants.Quota.UserDefinedFunction,
                    StringComparison.OrdinalIgnoreCase))
                {
                    quotaHeaders.Add(Constants.Quota.UserDefinedFunction, long.Parse(headerMaxQuotaWords[i + 1], CultureInfo.InvariantCulture));
                    usageHeaders.Add(Constants.Quota.UserDefinedFunction, long.Parse(headerCurrentUsageWords[i + 1], CultureInfo.InvariantCulture));
                }
            }
        }

        private class ResourceFeedDynamicObject : DynamicMetaObject
        {
            public ResourceFeedDynamicObject(DocumentFeedResponse<T> parent, Expression expression)
                : base(expression, BindingRestrictions.Empty, parent)
            {
            }

            public override DynamicMetaObject BindConvert(ConvertBinder binder)
            {
                Type baseFeedType = typeof(DocumentFeedResponse<bool>).GetGenericTypeDefinition();

                if (binder.Type != typeof(IEnumerable) && (!binder.Type.IsGenericType() || (binder.Type.GetGenericTypeDefinition() != baseFeedType &&
                    binder.Type.GetGenericTypeDefinition() != typeof(IEnumerable<string>).GetGenericTypeDefinition() &&
                    binder.Type.GetGenericTypeDefinition() != typeof(IQueryable<string>).GetGenericTypeDefinition())))
                {
                    return base.BindConvert(binder); //We allow cast only to IResourceFeed<>
                }

                // Setup the 'this' reference
                Expression self = Expression.Convert(Expression, LimitType);

                MethodCallExpression methodExpression = Expression.Call(
                    typeof(FeedResponseBinder).GetMethod("Convert",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(
                    binder.Type.GetGenericArguments()[0]),
                    self);

                //Create a meta object to invoke AsType later.
                DynamicMetaObject castOperator = new DynamicMetaObject(
                    methodExpression,
                    BindingRestrictions.GetTypeRestriction(Expression, LimitType));

                return castOperator;
            }
        }
    }
}
