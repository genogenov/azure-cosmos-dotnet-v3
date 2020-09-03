//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Net;
    using System.Text;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// The Cosmos Client exception
    /// </summary>
    public class CosmosException : Exception
    {
        private readonly string stackTrace;

        internal CosmosException(
            HttpStatusCode statusCodes,
            string message,
            int subStatusCode,
            string stackTrace,
            string activityId,
            double requestCharge,
            TimeSpan? retryAfter,
            Headers headers,
            CosmosDiagnosticsContext diagnosticsContext,
            Error error,
            Exception innerException)
            : base(CosmosException.GetMessageHelper(
                statusCodes,
                subStatusCode,
                message,
                activityId), innerException)
        {
            ResponseBody = message;
            this.stackTrace = stackTrace;
            ActivityId = activityId;
            StatusCode = statusCodes;
            SubStatusCode = subStatusCode;
            RetryAfter = retryAfter;
            RequestCharge = requestCharge;
            Headers = headers;
            Error = error;

            // Always have a diagnostic context. A new diagnostic will have useful info like user agent
            DiagnosticsContext = diagnosticsContext ?? new CosmosDiagnosticsContextCore();
        }

        /// <summary>
        /// Create a <see cref="CosmosException"/>
        /// </summary>
        /// <param name="message">The message associated with the exception.</param>
        /// <param name="statusCode">The <see cref="HttpStatusCode"/> associated with the exception.</param>
        /// <param name="subStatusCode">A sub status code associated with the exception.</param>
        /// <param name="activityId">An ActivityId associated with the operation that generated the exception.</param>
        /// <param name="requestCharge">A request charge associated with the operation that generated the exception.</param>
        public CosmosException(
            string message,
            HttpStatusCode statusCode,
            int subStatusCode,
            string activityId,
            double requestCharge)
            : base(message)
        {
            stackTrace = null;
            SubStatusCode = subStatusCode;
            StatusCode = statusCode;
            RequestCharge = requestCharge;
            ActivityId = activityId;
            Headers = new Headers();
            DiagnosticsContext = new CosmosDiagnosticsContextCore();
        }

        /// <summary>
        /// The body of the cosmos response message as a string
        /// </summary>
        public virtual string ResponseBody { get; }

        /// <summary>
        /// Gets the request completion status code from the Azure Cosmos DB service.
        /// </summary>
        /// <value>The request completion status code</value>
        public virtual HttpStatusCode StatusCode { get; }

        /// <summary>
        /// Gets the request completion sub status code from the Azure Cosmos DB service.
        /// </summary>
        /// <value>The request completion status code</value>
        public virtual int SubStatusCode { get; }

        /// <summary>
        /// Gets the request charge for this request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The request charge measured in request units.
        /// </value>
        public virtual double RequestCharge { get; }

        /// <summary>
        /// Gets the activity ID for the request from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The activity ID for the request.
        /// </value>
        public virtual string ActivityId { get; }

        /// <summary>
        /// Gets the retry after time. This tells how long a request should wait before doing a retry.
        /// </summary>
        public virtual TimeSpan? RetryAfter { get; }

        /// <summary>
        /// Gets the response headers
        /// </summary>
        public virtual Headers Headers { get; }

        /// <summary>
        /// Gets the diagnostics for the request
        /// </summary>
        public virtual CosmosDiagnostics Diagnostics => DiagnosticsContext.Diagnostics;

        /// <inheritdoc/>
        public override string StackTrace
        {
            get
            {
                if (stackTrace != null)
                {
                    return stackTrace;
                }
                else
                {
                    return base.StackTrace;
                }
            }
        }

        internal virtual CosmosDiagnosticsContext DiagnosticsContext { get; }

        /// <summary>
        /// Gets the internal error object.
        /// </summary>
        internal virtual Documents.Error Error { get; set; }

        /// <summary>
        /// Try to get a header from the cosmos response message
        /// </summary>
        /// <param name="headerName"></param>
        /// <param name="value"></param>
        /// <returns>A value indicating if the header was read.</returns>
        public virtual bool TryGetHeader(string headerName, out string value)
        {
            if (Headers == null)
            {
                value = null;
                return false;
            }

            return Headers.TryGetValue(headerName, out value);
        }

        /// <summary>
        /// Create a custom string with all the relevant exception information
        /// </summary>
        /// <returns>A string representation of the exception.</returns>
        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(GetType().FullName);
            stringBuilder.Append(" : ");

            ToStringHelper(stringBuilder);

            return stringBuilder.ToString();
        }

        internal ResponseMessage ToCosmosResponseMessage(RequestMessage request)
        {
            return new ResponseMessage(
                 headers: Headers,
                 requestMessage: request,
                 cosmosException: this,
                 statusCode: StatusCode,
                 diagnostics: DiagnosticsContext);
        }

        private static string GetMessageHelper(
            HttpStatusCode statusCode,
            int subStatusCode,
            string responseBody,
            string activityId)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append($"Response status code does not indicate success: ");
            stringBuilder.Append($"{statusCode} ({(int)statusCode})");
            stringBuilder.Append("; Substatus: ");
            stringBuilder.Append(subStatusCode);
            stringBuilder.Append("; ActivityId: ");
            stringBuilder.Append(activityId ?? string.Empty);
            stringBuilder.Append("; Reason: (");
            stringBuilder.Append(responseBody ?? string.Empty);
            stringBuilder.Append(");");

            return stringBuilder.ToString();
        }

        private string ToStringHelper(
        StringBuilder stringBuilder)
        {
            if (stringBuilder == null)
            {
                throw new ArgumentNullException(nameof(stringBuilder));
            }

            stringBuilder.Append(Message);
            stringBuilder.AppendLine();

            if (InnerException != null)
            {
                stringBuilder.Append(" ---> ");
                stringBuilder.Append(InnerException);
                stringBuilder.AppendLine();
                stringBuilder.Append("   ");
                stringBuilder.Append("--- End of inner exception stack trace ---");
                stringBuilder.AppendLine();
            }

            if (StackTrace != null)
            {
                stringBuilder.Append(StackTrace);
                stringBuilder.AppendLine();
            }

            if (Diagnostics != null)
            {
                stringBuilder.Append("--- Cosmos Diagnostics ---");
                stringBuilder.Append(Diagnostics);
            }

            return stringBuilder.ToString();
        }
    }
}