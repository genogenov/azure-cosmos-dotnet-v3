//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Collections;

    /// <summary>
    /// Header implementation used for Request and Responses
    /// </summary>
    /// <seealso cref="ResponseMessage"/>
    /// <seealso cref="RequestMessage"/>
    public class Headers : IEnumerable
    {
        private string GetString(string keyName)
        {
            TryGetValue(keyName, out string valueTuple);
            return valueTuple;
        }

        internal SubStatusCodes SubStatusCode
        {
            get => Headers.GetSubStatusCodes(SubStatusCodeLiteral);
            set => SubStatusCodeLiteral = ((uint)value).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the Continuation Token in the current <see cref="ResponseMessage"/>.
        /// </summary>
        public virtual string ContinuationToken
        {
            get => GetString(HttpConstants.HttpHeaders.Continuation);

            internal set => Set(HttpConstants.HttpHeaders.Continuation, value);
        }

        /// <summary>
        /// Gets the request charge for this request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The request charge measured in request units.
        /// </value>
        public virtual double RequestCharge
        {
            get
            {
                string value = GetString(HttpConstants.HttpHeaders.RequestCharge);
                if (value == null)
                {
                    return 0;
                }

                return double.Parse(value, CultureInfo.InvariantCulture);
            }
            internal set => Set(HttpConstants.HttpHeaders.RequestCharge, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Gets the activity ID for the request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The activity ID for the request.
        /// </value>
        public virtual string ActivityId
        {
            get => GetString(HttpConstants.HttpHeaders.ActivityId);
            internal set => Set(HttpConstants.HttpHeaders.ActivityId, value);
        }

        /// <summary>
        /// Gets the entity tag associated with the resource from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The entity tag associated with the resource.
        /// </value>
        /// <remarks>
        /// ETags are used for concurrency checking when updating resources. 
        /// </remarks>
        public virtual string ETag
        {
            get => GetString(HttpConstants.HttpHeaders.ETag);
            internal set => Set(HttpConstants.HttpHeaders.ETag, value);
        }

        /// <summary>
        /// Gets the Content Type for the current content in the <see cref="ResponseMessage"/>.
        /// </summary>
        public virtual string ContentType
        {
            get => GetString(HttpConstants.HttpHeaders.ContentType);
            internal set => Set(HttpConstants.HttpHeaders.ContentType, value);
        }

        /// <summary>
        /// Gets the Session Token for the current <see cref="ResponseMessage"/>.
        /// </summary>
        /// <remarks>
        /// Session Token is used along with Session Consistency.
        /// </remarks>
        public virtual string Session
        {
            get => GetString(HttpConstants.HttpHeaders.SessionToken);
            internal set => Set(HttpConstants.HttpHeaders.SessionToken, value);
        }

        /// <summary>
        /// Gets the Content Length for the current content in the <see cref="ResponseMessage"/>.
        /// </summary>
        public virtual string ContentLength
        {
            get => GetString(HttpConstants.HttpHeaders.ContentLength);
            internal set => Set(HttpConstants.HttpHeaders.ContentLength, value);
        }

        /// <summary>
        /// Gets the Location for the current content in the <see cref="ResponseMessage"/>.
        /// </summary>
        public virtual string Location
        {
            get => GetString(HttpConstants.HttpHeaders.Location);
            internal set => Set(HttpConstants.HttpHeaders.Location, value);
        }

        internal string SubStatusCodeLiteral
        {
            get => GetString(WFConstants.BackendHeaders.SubStatus);
            set => Set(WFConstants.BackendHeaders.SubStatus, value);
        }

        internal TimeSpan? RetryAfter
        {
            get => Headers.GetRetryAfter(RetryAfterLiteral);
            set
            {
                if (value.HasValue)
                {
                    RetryAfterLiteral = value.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
                    return;
                }

                RetryAfterLiteral = null;
            }
        }

        internal string RetryAfterLiteral
        {
            get => GetString(HttpConstants.HttpHeaders.RetryAfterInMilliseconds);
            set => Set(HttpConstants.HttpHeaders.RetryAfterInMilliseconds, value);
        }

        internal string PartitionKey
        {
            get => GetString(HttpConstants.HttpHeaders.PartitionKey);
            set => Set(HttpConstants.HttpHeaders.PartitionKey, value);
        }

        internal string PartitionKeyRangeId
        {
            get => GetString(HttpConstants.HttpHeaders.PartitionKeyRangeId);
            set => Set(HttpConstants.HttpHeaders.PartitionKeyRangeId, value);
        }

        internal string IsUpsert
        {
            get => GetString(HttpConstants.HttpHeaders.IsUpsert);
            set => Set(HttpConstants.HttpHeaders.IsUpsert, value);
        }

        internal string OfferThroughput
        {
            get => GetString(HttpConstants.HttpHeaders.OfferThroughput);
            set => Set(HttpConstants.HttpHeaders.OfferThroughput, value);
        }

        internal string IfNoneMatch
        {
            get => GetString(HttpConstants.HttpHeaders.IfNoneMatch);
            set => Set(HttpConstants.HttpHeaders.IfNoneMatch, value);
        }

        internal string PageSize
        {
            get => GetString(HttpConstants.HttpHeaders.PageSize);
            set => Set(HttpConstants.HttpHeaders.PageSize, value);
        }

        internal string QueryMetricsText
        {
            get => GetString(HttpConstants.HttpHeaders.QueryMetrics);
            set => Set(HttpConstants.HttpHeaders.QueryMetrics, value);
        }

        /// <summary>
        /// Creates a new instance of <see cref="Headers"/>.
        /// </summary>
        public Headers()
        {
            CosmosMessageHeaders = new CosmosMessageHeadersInternal();
        }

        internal Headers(INameValueCollection nameValue)
        {
            CosmosMessageHeaders = nameValue;
        }

        /// <summary>
        /// Gets the value of a particular header.
        /// </summary>
        /// <param name="headerName">Header name to look for.</param>
        /// <returns>The header value.</returns>
        public virtual string this[string headerName]
        {
            get => CosmosMessageHeaders[headerName];
            set => CosmosMessageHeaders[headerName] = value;
        }

        /// <summary>
        /// Adds a header to the Header collection.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        /// <param name="value">Header value.</param>
        public virtual void Add(string headerName, string value)
        {
            CosmosMessageHeaders.Add(headerName, value);
        }

        /// <summary>
        /// Adds a header to the Header collection.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        /// <param name="values">List of values to be added as a comma-separated list.</param>
        public virtual void Add(string headerName, IEnumerable<string> values)
        {
            CosmosMessageHeaders.Add(headerName, values);
        }

        /// <summary>
        /// Adds or updates a header in the Header collection.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        /// <param name="value">Header value.</param>
        public virtual void Set(string headerName, string value)
        {
            CosmosMessageHeaders.Set(headerName, value);
        }

        /// <summary>
        /// Gets the value of a particular header.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        /// <returns>The header value.</returns>
        public virtual string Get(string headerName)
        {
            return CosmosMessageHeaders.Get(headerName);
        }

        /// <summary>
        /// Tries to get the value for a particular header.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        /// <param name="value">Header value.</param>
        /// <returns>True or false if the header name existed in the header collection.</returns>
        public virtual bool TryGetValue(string headerName, out string value)
        {
            value = CosmosMessageHeaders.Get(headerName);
            return value != null;
        }

        /// <summary>
        /// Returns the header value or the default(string)
        /// </summary>
        /// <param name="headerName">Header Name</param>
        /// <returns>Returns the header value or the default(string).</returns>
        public virtual string GetValueOrDefault(string headerName)
        {
            if (TryGetValue(headerName, out string value))
            {
                return value;
            }

            return default(string);
        }

        /// <summary>
        /// Removes a header from the header collection.
        /// </summary>
        /// <param name="headerName">Header name.</param>
        public virtual void Remove(string headerName)
        {
            CosmosMessageHeaders.Remove(headerName);
        }

        /// <summary>
        /// Obtains a list of all header names.
        /// </summary>
        /// <returns>An array with all the header names.</returns>
        public virtual string[] AllKeys()
        {
            return CosmosMessageHeaders.AllKeys();
        }

        /// <summary>
        /// Gets a header value with a particular type.
        /// </summary>
        /// <typeparam name="T">Type of the header value.</typeparam>
        /// <param name="headerName">Header name.</param>
        /// <returns>The header value parsed for a particular type.</returns>
        public virtual T GetHeaderValue<T>(string headerName)
        {
            return CosmosMessageHeaders.GetHeaderValue<T>(headerName);
        }

        /// <summary>
        /// Enumerates all the HTTP headers names in the <see cref="Headers"/>.
        /// </summary>
        /// <returns>An enumator for all headers.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return CosmosMessageHeaders.GetEnumerator();
        }

        internal string[] GetValues(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            string value = this[key];
            if (value == null)
            {
                return null;
            }

            return new string[1] { this[key] };
        }

        internal INameValueCollection CosmosMessageHeaders { get; }

        internal static SubStatusCodes GetSubStatusCodes(string value)
        {
            if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out uint nSubStatus))
            {
                return (SubStatusCodes)nSubStatus;
            }

            return SubStatusCodes.Unknown;
        }

        internal static TimeSpan? GetRetryAfter(string value)
        {
            if (long.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out long retryIntervalInMilliseconds))
            {
                return TimeSpan.FromMilliseconds(retryIntervalInMilliseconds);
            }

            return null;
        }
    }
}