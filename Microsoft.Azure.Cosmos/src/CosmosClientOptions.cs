//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data.Common;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using Microsoft.Azure.Cosmos.Fluent;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;

    /// <summary>
    /// Defines all the configurable options that the CosmosClient requires.
    /// </summary>
    /// <example>
    /// An example on how to configure the serialization option to ignore null values.
    /// <code language="c#">
    /// <![CDATA[
    /// CosmosClientOptions clientOptions = new CosmosClientOptions()
    /// {
    ///     SerializerOptions = new CosmosSerializationOptions(){
    ///         IgnoreNullValues = true
    ///     },
    ///     ConnectionMode = ConnectionMode.Gateway,
    /// };
    /// 
    /// CosmosClient client = new CosmosClient("endpoint", "key", clientOptions);
    /// ]]>
    /// </code>
    /// </example>
    public class CosmosClientOptions
    {
        /// <summary>
        /// Default connection mode
        /// </summary>
        private const ConnectionMode DefaultConnectionMode = ConnectionMode.Direct;

        /// <summary>
        /// Default Protocol mode
        /// </summary>
        private const Protocol DefaultProtocol = Protocol.Tcp;

        private const string ConnectionStringAccountEndpoint = "AccountEndpoint";
        private const string ConnectionStringAccountKey = "AccountKey";

        private const ApiType DefaultApiType = ApiType.None;

        /// <summary>
        /// Default request timeout
        /// </summary>
        private int gatewayModeMaxConnectionLimit;
        private CosmosSerializationOptions serializerOptions;
        private CosmosSerializer serializerInternal;

        private ConnectionMode connectionMode;
        private Protocol connectionProtocol;
        private TimeSpan? idleTcpConnectionTimeout;
        private TimeSpan? openTcpConnectionTimeout;
        private int? maxRequestsPerTcpConnection;
        private int? maxTcpConnectionsPerEndpoint;
        private PortReuseMode? portReuseMode;
        private IWebProxy webProxy;
        private Func<HttpClient> httpClientFactory;

        /// <summary>
        /// Creates a new CosmosClientOptions
        /// </summary>
        public CosmosClientOptions()
        {
            GatewayModeMaxConnectionLimit = ConnectionPolicy.Default.MaxConnectionLimit;
            RequestTimeout = ConnectionPolicy.Default.RequestTimeout;
            ConnectionMode = CosmosClientOptions.DefaultConnectionMode;
            ConnectionProtocol = CosmosClientOptions.DefaultProtocol;
            ApiType = CosmosClientOptions.DefaultApiType;
            CustomHandlers = new Collection<RequestHandler>();
        }

        /// <summary>
        /// Get or set user-agent suffix to include with every Azure Cosmos DB service interaction.
        /// </summary>
        /// <remarks>
        /// Setting this property after sending any request won't have any effect.
        /// </remarks>
        public string ApplicationName { get; set; }

        /// <summary>
        /// Get or set session container for the client
        /// </summary>
        internal ISessionContainer SessionContainer { get; set; }

        /// <summary>
        /// Get or set the preferred geo-replicated region to be used for Azure Cosmos DB service interaction.
        /// </summary>
        /// <remarks>
        /// When this property is specified, the SDK prefers the region to perform operations. Also SDK auto-selects 
        /// fallback geo-replicated regions for high availability. 
        /// When this property is not specified, the SDK uses the write region as the preferred region for all operations.
        /// </remarks>
        /// <seealso cref="CosmosClientBuilder.WithApplicationRegion(string)"/>
        /// <seealso href="https://docs.microsoft.com/azure/cosmos-db/high-availability#high-availability-with-cosmos-db-in-the-event-of-regional-outages">High availability on regional outages</seealso>
        public string ApplicationRegion { get; set; }

        /// <summary>
        /// Gets and sets the preferred regions for geo-replicated database accounts in the Azure Cosmos DB service. 
        /// </summary>
        /// <remarks>
        /// When this property is specified, the SDK will use the region list in the provided order to define the endpoint failover order.
        /// This configuration is an alternative to <see cref="ApplicationRegion"/>, either one can be set but not both.
        /// </remarks>
        /// <seealso href="https://docs.microsoft.com/azure/cosmos-db/high-availability#high-availability-with-cosmos-db-in-the-event-of-regional-outages">High availability on regional outages</seealso>
        public IReadOnlyList<string> ApplicationPreferredRegions { get; set; }

        /// <summary>
        /// Get or set the maximum number of concurrent connections allowed for the target
        /// service endpoint in the Azure Cosmos DB service.
        /// </summary>
        /// <remarks>
        /// This setting is only applicable in Gateway mode.
        /// </remarks>
        /// <value>Default value is 50.</value>
        /// <seealso cref="CosmosClientBuilder.WithConnectionModeGateway(int?, IWebProxy)"/>
        public int GatewayModeMaxConnectionLimit
        {
            get => gatewayModeMaxConnectionLimit;
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value));
                }

                if (HttpClientFactory != null && value != ConnectionPolicy.Default.MaxConnectionLimit)
                {
                    throw new ArgumentException($"{nameof(httpClientFactory)} can not be set along with {nameof(GatewayModeMaxConnectionLimit)}. This must be set on the HttpClientHandler.MaxConnectionsPerServer property.");
                }

                gatewayModeMaxConnectionLimit = value;
            }
        }

        /// <summary>
        /// Gets the request timeout in seconds when connecting to the Azure Cosmos DB service.
        /// The number specifies the time to wait for response to come back from network peer.
        /// </summary>
        /// <value>Default value is 1 minute.</value>
        /// <seealso cref="CosmosClientBuilder.WithRequestTimeout(TimeSpan)"/>
        public TimeSpan RequestTimeout { get; set; }

        /// <summary>
        /// Gets the handlers run before the process
        /// </summary>
        /// <seealso cref="CosmosClientBuilder.AddCustomHandlers(RequestHandler[])"/>
        [JsonConverter(typeof(ClientOptionJsonConverter))]
        public Collection<RequestHandler> CustomHandlers { get; }

        /// <summary>
        /// Get or set the connection mode used by the client when connecting to the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// Default value is <see cref="Cosmos.ConnectionMode.Direct"/>
        /// </value>
        /// <remarks>
        /// For more information, see <see href="https://docs.microsoft.com/azure/documentdb/documentdb-performance-tips#direct-connection">Connection policy: Use direct connection mode</see>.
        /// </remarks>
        /// <seealso cref="CosmosClientBuilder.WithConnectionModeDirect()"/>
        /// <seealso cref="CosmosClientBuilder.WithConnectionModeGateway(int?, IWebProxy)"/>
        public ConnectionMode ConnectionMode
        {
            get => connectionMode;
            set
            {
                if (value == ConnectionMode.Gateway)
                {
                    ConnectionProtocol = Protocol.Https;
                }
                else if (value == ConnectionMode.Direct)
                {
                    connectionProtocol = Protocol.Tcp;
                }

                ValidateDirectTCPSettings();
                connectionMode = value;
            }
        }

        /// <summary>
        /// This can be used to weaken the database account consistency level for read operations.
        /// If this is not set the database account consistency level will be used for all requests.
        /// </summary>
        public ConsistencyLevel? ConsistencyLevel { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of retries in the case where the request fails
        /// because the Azure Cosmos DB service has applied rate limiting on the client.
        /// </summary>
        /// <value>
        /// The default value is 9. This means in the case where the request is rate limited,
        /// the same request will be issued for a maximum of 10 times to the server before
        /// an error is returned to the application.
        ///
        /// If the value of this property is set to 0, there will be no automatic retry on rate
        /// limiting requests from the client and the exception needs to be handled at the
        /// application level.
        /// </value>
        /// <remarks>
        /// <para>
        /// When a client is sending requests faster than the allowed rate,
        /// the service will return HttpStatusCode 429 (Too Many Requests) to rate limit the client. The current
        /// implementation in the SDK will then wait for the amount of time the service tells it to wait and
        /// retry after the time has elapsed.
        /// </para>
        /// <para>
        /// For more information, see <see href="https://docs.microsoft.com/azure/cosmos-db/performance-tips#throughput">Handle rate limiting/request rate too large</see>.
        /// </para>
        /// </remarks>
        /// <seealso cref="CosmosClientBuilder.WithThrottlingRetryOptions(TimeSpan, int)"/>
        public int? MaxRetryAttemptsOnRateLimitedRequests { get; set; }

        /// <summary>
        /// Gets or sets the maximum retry time in seconds for the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The default value is 30 seconds. 
        /// </value>
        /// <remarks>
        /// <para>
        /// The minimum interval is seconds. Any interval that is smaller will be ignored.
        /// </para>
        /// <para>
        /// When a request fails due to a rate limiting error, the service sends back a response that
        /// contains a value indicating the client should not retry before the <see cref="Microsoft.Azure.Cosmos.CosmosException.RetryAfter"/> time period has
        /// elapsed.
        ///
        /// This property allows the application to set a maximum wait time for all retry attempts.
        /// If the cumulative wait time exceeds the this value, the client will stop retrying and return the error to the application.
        /// </para>
        /// <para>
        /// For more information, see <see href="https://docs.microsoft.com/azure/cosmos-db/performance-tips#throughput">Handle rate limiting/request rate too large</see>.
        /// </para>
        /// </remarks>
        /// <seealso cref="CosmosClientBuilder.WithThrottlingRetryOptions(TimeSpan, int)"/>
        public TimeSpan? MaxRetryWaitTimeOnRateLimitedRequests { get; set; }

        /// <summary>
        /// (Direct/TCP) Controls the amount of idle time after which unused connections are closed.
        /// </summary>
        /// <value>
        /// By default, idle connections are kept open indefinitely. Value must be greater than or equal to 10 minutes. Recommended values are between 20 minutes and 24 hours.
        /// </value>
        /// <remarks>
        /// Mainly useful for sparse infrequent access to a large database account.
        /// </remarks>
        public TimeSpan? IdleTcpConnectionTimeout
        {
            get => idleTcpConnectionTimeout;
            set
            {
                idleTcpConnectionTimeout = value;
                ValidateDirectTCPSettings();
            }
        }

        /// <summary>
        /// (Direct/TCP) Controls the amount of time allowed for trying to establish a connection.
        /// </summary>
        /// <value>
        /// The default timeout is 5 seconds. Recommended values are greater than or equal to 5 seconds.
        /// </value>
        /// <remarks>
        /// When the time elapses, the attempt is cancelled and an error is returned. Longer timeouts will delay retries and failures.
        /// </remarks>
        public TimeSpan? OpenTcpConnectionTimeout
        {
            get => openTcpConnectionTimeout;
            set
            {
                openTcpConnectionTimeout = value;
                ValidateDirectTCPSettings();
            }
        }

        /// <summary>
        /// (Direct/TCP) Controls the number of requests allowed simultaneously over a single TCP connection. When more requests are in flight simultaneously, the direct/TCP client will open additional connections.
        /// </summary>
        /// <value>
        /// The default settings allow 30 simultaneous requests per connection.
        /// Do not set this value lower than 4 requests per connection or higher than 50-100 requests per connection.       
        /// The former can lead to a large number of connections to be created. 
        /// The latter can lead to head of line blocking, high latency and timeouts.
        /// </value>
        /// <remarks>
        /// Applications with a very high degree of parallelism per connection, with large requests or responses, or with very tight latency requirements might get better performance with 8-16 requests per connection.
        /// </remarks>
        public int? MaxRequestsPerTcpConnection
        {
            get => maxRequestsPerTcpConnection;
            set
            {
                maxRequestsPerTcpConnection = value;
                ValidateDirectTCPSettings();
            }
        }

        /// <summary>
        /// (Direct/TCP) Controls the maximum number of TCP connections that may be opened to each Cosmos DB back-end.
        /// Together with MaxRequestsPerTcpConnection, this setting limits the number of requests that are simultaneously sent to a single Cosmos DB back-end(MaxRequestsPerTcpConnection x MaxTcpConnectionPerEndpoint).
        /// </summary>
        /// <value>
        /// The default value is 65,535. Value must be greater than or equal to 16.
        /// </value>
        public int? MaxTcpConnectionsPerEndpoint
        {
            get => maxTcpConnectionsPerEndpoint;
            set
            {
                maxTcpConnectionsPerEndpoint = value;
                ValidateDirectTCPSettings();
            }
        }

        /// <summary>
        /// (Direct/TCP) Controls the client port reuse policy used by the transport stack.
        /// </summary>
        /// <value>
        /// The default value is PortReuseMode.ReuseUnicastPort.
        /// </value>
        /// <remarks>
        /// ReuseUnicastPort and PrivatePortPool are not mutually exclusive.
        /// When PrivatePortPool is enabled, the client first tries to reuse a port it already has.
        /// It falls back to allocating a new port if the initial attempts failed. If this fails, too, the client then falls back to ReuseUnicastPort.
        /// </remarks>
        public PortReuseMode? PortReuseMode
        {
            get => portReuseMode;
            set
            {
                portReuseMode = value;
                ValidateDirectTCPSettings();
            }
        }

        /// <summary>
        /// (Gateway/Https) Get or set the proxy information used for web requests.
        /// </summary>
        [JsonIgnore]
        public IWebProxy WebProxy
        {
            get => webProxy;
            set
            {
                webProxy = value;
                if (ConnectionMode != ConnectionMode.Gateway)
                {
                    throw new ArgumentException($"{nameof(WebProxy)} requires {nameof(ConnectionMode)} to be set to {nameof(ConnectionMode.Gateway)}");
                }

                if (HttpClientFactory != null)
                {
                    throw new ArgumentException($"{nameof(WebProxy)} cannot be set along {nameof(HttpClientFactory)}");
                }
            }
        }

        /// <summary>
        /// Get to set optional serializer options.
        /// </summary>
        /// <example>
        /// An example on how to configure the serialization option to ignore null values
        /// CosmosClientOptions clientOptions = new CosmosClientOptions()
        /// {
        ///     SerializerOptions = new CosmosSerializationOptions(){
        ///         IgnoreNullValues = true
        ///     }
        /// };
        /// 
        /// CosmosClient client = new CosmosClient("endpoint", "key", clientOptions);
        /// </example>
        public CosmosSerializationOptions SerializerOptions
        {
            get => serializerOptions;
            set
            {
                if (Serializer != null)
                {
                    throw new ArgumentException(
                        $"{nameof(SerializerOptions)} is not compatible with {nameof(Serializer)}. Only one can be set.  ");
                }

                serializerOptions = value;
            }
        }

        /// <summary>
        /// Get to set an optional JSON serializer. The client will use it to serialize or de-serialize user's cosmos request/responses.
        /// SDK owned types such as DatabaseProperties and ContainerProperties will always use the SDK default serializer.
        /// </summary>
        /// <example>
        /// // An example on how to set a custom serializer. For basic serializer options look at CosmosSerializationOptions
        /// CosmosSerializer ignoreNullSerializer = new MyCustomIgnoreNullSerializer();
        ///         
        /// CosmosClientOptions clientOptions = new CosmosClientOptions()
        /// {
        ///     Serializer = ignoreNullSerializer
        /// };
        /// 
        /// CosmosClient client = new CosmosClient("endpoint", "key", clientOptions);
        /// </example>
        [JsonConverter(typeof(ClientOptionJsonConverter))]
        public CosmosSerializer Serializer
        {
            get => serializerInternal;
            set
            {
                if (SerializerOptions != null)
                {
                    throw new ArgumentException(
                        $"{nameof(Serializer)} is not compatible with {nameof(SerializerOptions)}. Only one can be set.  ");
                }

                serializerInternal = value;
            }
        }

        /// <summary>
        /// Limits the operations to the provided endpoint on the CosmosClient.
        /// </summary>
        /// <value>
        /// Default value is false.
        /// </value>
        /// <remarks>
        /// When the value of this property is false, the SDK will automatically discover write and read regions, and use them when the configured application region is not available.
        /// When set to true, availability is limited to the endpoint specified on the CosmosClient constructor.
        /// Defining the <see cref="ApplicationRegion"/> or <see cref="ApplicationPreferredRegions"/>  is not allowed when setting the value to true.
        /// </remarks>
        /// <seealso href="https://docs.microsoft.com/azure/cosmos-db/high-availability">High availability</seealso>
        public bool LimitToEndpoint { get; set; } = false;

        /// <summary>
        /// Allows optimistic batching of requests to service. Setting this option might impact the latency of the operations. Hence this option is recommended for non-latency sensitive scenarios only.
        /// </summary>
        public bool AllowBulkExecution { get; set; }

        /// <summary>
        /// Gets or sets the flag to enable address cache refresh on TCP connection reset notification.
        /// </summary>
        /// <remarks>
        /// Does not apply if <see cref="ConnectionMode.Gateway"/> is used.
        /// </remarks>
        /// <value>
        /// The default value is false
        /// </value>
        public bool EnableTcpConnectionEndpointRediscovery { get; set; } = false;

        /// <summary>
        /// Gets or sets a delegate to use to obtain an HttpClient instance to be used for HTTPS communication.
        /// </summary>
        /// <remarks>
        /// <para>
        /// HTTPS communication is used when <see cref="ConnectionMode"/> is set to <see cref="ConnectionMode.Gateway"/> for all operations and when <see cref="ConnectionMode"/> is <see cref="ConnectionMode.Direct"/> (default) for metadata operations.
        /// </para>
        /// <para>
        /// Useful in scenarios where the application is using a pool of HttpClient instances to be shared, like ASP.NET Core applications with IHttpClientFactory or Blazor WebAssembly applications.
        /// </para>
        /// <para>
        /// For .NET core applications the default GatewayConnectionLimit will be ignored. It must be set on the HttpClientHandler.MaxConnectionsPerServer to limit the number of connections
        /// </para>
        /// </remarks>
        [JsonIgnore]
        public Func<HttpClient> HttpClientFactory
        {
            get => httpClientFactory;
            set
            {
                if (WebProxy != null)
                {
                    throw new ArgumentException($"{nameof(HttpClientFactory)} cannot be set along {nameof(WebProxy)}");
                }

                if (GatewayModeMaxConnectionLimit != ConnectionPolicy.Default.MaxConnectionLimit)
                {
                    throw new ArgumentException($"{nameof(httpClientFactory)} can not be set along with {nameof(GatewayModeMaxConnectionLimit)}. This must be set on the HttpClientHandler.MaxConnectionsPerServer property.");
                }

                httpClientFactory = value;
            }
        }

        /// <summary>
        /// Gets or sets the connection protocol when connecting to the Azure Cosmos service.
        /// </summary>
        /// <value>
        /// Default value is <see cref="Protocol.Tcp"/>.
        /// </value>
        /// <remarks>
        /// This setting is not used when <see cref="ConnectionMode"/> is set to <see cref="Cosmos.ConnectionMode.Gateway"/>.
        /// Gateway mode only supports HTTPS.
        /// For more information, see <see href="https://docs.microsoft.com/azure/documentdb/documentdb-performance-tips#use-tcp">Connection policy: Use the TCP protocol</see>.
        /// </remarks>
        internal Protocol ConnectionProtocol
        {
            get => connectionProtocol;
            set
            {
                ValidateDirectTCPSettings();
                connectionProtocol = value;
            }
        }

        /// <summary>
        /// The event handler to be invoked before the request is sent.
        /// </summary>
        internal EventHandler<SendingRequestEventArgs> SendingRequestEventArgs { get; set; }

        /// <summary>
        /// (Optional) transport interceptor factory
        /// </summary>
        internal Func<TransportClient, TransportClient> TransportClientHandlerFactory { get; set; }

        /// <summary>
        /// API type for the account
        /// </summary>
        internal ApiType ApiType { get; set; }

        /// <summary>
        /// Optional store client factory instance to use for all transport requests.
        /// </summary>
        internal IStoreClientFactory StoreClientFactory { get; set; }

        /// <summary>
        /// Gets or sets the initial delay retry time in milliseconds for the Azure Cosmos DB service
        /// for requests that hit RetryWithExceptions. This covers errors that occur due to concurrency errors in the store.
        /// </summary>
        /// <value>
        /// The default value is 1 second. For an example on how to set this value, please refer to <see cref="ConnectionPolicy.RetryOptions"/>.
        /// </value>
        /// <remarks>
        /// <para>
        /// When a request fails due to a RetryWith error, the client delays and retries the request. This configures the client
        /// to delay the time specified before retrying the request.
        /// </para>
        /// </remarks>
        internal int? InitialRetryForRetryWithMilliseconds { get; set; }

        /// <summary>
        /// Gets or sets the maximum delay retry time in milliseconds for the Azure Cosmos DB service
        /// for requests that hit RetryWithExceptions. This covers errors that occur due to concurrency errors in the store.
        /// </summary>
        /// <value>
        /// The default value is 30 seconds. For an example on how to set this value, please refer to <see cref="ConnectionPolicy.RetryOptions"/>.
        /// </value>
        /// <remarks>
        /// <para>
        /// When a request fails due to a RetryWith error, the client delays and retries the request. This configures the maximum time
        /// the client should delay before failing the request.
        /// </para>
        /// </remarks>
        internal int? MaximumRetryForRetryWithMilliseconds { get; set; }

        /// <summary>
        /// Gets or sets the interval to salt retry with value. This will spread the retry values from 1..n from the exponential back-off
        /// subscribed.
        /// </summary>
        /// <value>
        /// The default value is to not salt.
        /// </value>
        /// <remarks>
        /// <para>
        /// When a request fails due to a RetryWith error, the client delays and retries the request. This configures the jitter on the retry attempted.
        /// </para>
        /// </remarks>
        internal int? RandomSaltForRetryWithMilliseconds { get; set; }

        /// <summary>
        /// Gets or sets the total time to wait before failing the request for retry with failures.
        /// subscribed.
        /// </summary>
        /// <value>
        /// The default value 30 seconds.
        /// </value>
        /// <remarks>
        /// <para>
        /// When a request fails due to a RetryWith error, the client delays and retries the request. This configures total time spent waiting on the request.
        /// </para>
        /// </remarks>
        internal int? TotalWaitTimeForRetryWithMilliseconds { get; set; }

        /// <summary>
        /// Flag that controls whether CPU monitoring thread is created to enrich timeout exceptions with additional diagnostic. Default value is true.
        /// </summary>
        internal bool? EnableCpuMonitor { get; set; }

        internal void SetSerializerIfNotConfigured(CosmosSerializer serializer)
        {
            if (serializerInternal == null)
            {
                serializerInternal = serializer ?? throw new ArgumentNullException(nameof(serializer));
            }
        }

        internal CosmosClientOptions Clone()
        {
            CosmosClientOptions cloneConfiguration = (CosmosClientOptions)MemberwiseClone();
            return cloneConfiguration;
        }

        internal ConnectionPolicy GetConnectionPolicy()
        {
            ValidateDirectTCPSettings();
            ValidateLimitToEndpointSettings();
            UserAgentContainer userAgent = BuildUserAgentContainer();

            ConnectionPolicy connectionPolicy = new ConnectionPolicy()
            {
                MaxConnectionLimit = GatewayModeMaxConnectionLimit,
                RequestTimeout = RequestTimeout,
                ConnectionMode = ConnectionMode,
                ConnectionProtocol = ConnectionProtocol,
                UserAgentContainer = userAgent,
                UseMultipleWriteLocations = true,
                IdleTcpConnectionTimeout = IdleTcpConnectionTimeout,
                OpenTcpConnectionTimeout = OpenTcpConnectionTimeout,
                MaxRequestsPerTcpConnection = MaxRequestsPerTcpConnection,
                MaxTcpConnectionsPerEndpoint = MaxTcpConnectionsPerEndpoint,
                EnableEndpointDiscovery = !LimitToEndpoint,
                PortReuseMode = portReuseMode,
                EnableTcpConnectionEndpointRediscovery = EnableTcpConnectionEndpointRediscovery,
                HttpClientFactory = httpClientFactory,
            };

            if (ApplicationRegion != null)
            {
                connectionPolicy.SetCurrentLocation(ApplicationRegion);
            }

            if (ApplicationPreferredRegions != null)
            {
                connectionPolicy.SetPreferredLocations(ApplicationPreferredRegions);
            }

            if (MaxRetryAttemptsOnRateLimitedRequests != null)
            {
                connectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = MaxRetryAttemptsOnRateLimitedRequests.Value;
            }

            if (MaxRetryWaitTimeOnRateLimitedRequests != null)
            {
                connectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = (int)MaxRetryWaitTimeOnRateLimitedRequests.Value.TotalSeconds;
            }

            if (InitialRetryForRetryWithMilliseconds != null)
            {
                connectionPolicy.RetryOptions.InitialRetryForRetryWithMilliseconds =
                    InitialRetryForRetryWithMilliseconds;
            }

            if (MaximumRetryForRetryWithMilliseconds != null)
            {
                connectionPolicy.RetryOptions.MaximumRetryForRetryWithMilliseconds =
                    MaximumRetryForRetryWithMilliseconds;
            }

            if (RandomSaltForRetryWithMilliseconds != null)
            {
                connectionPolicy.RetryOptions.RandomSaltForRetryWithMilliseconds
                    = RandomSaltForRetryWithMilliseconds;
            }

            if (TotalWaitTimeForRetryWithMilliseconds != null)
            {
                connectionPolicy.RetryOptions.TotalWaitTimeForRetryWithMilliseconds
                    = TotalWaitTimeForRetryWithMilliseconds;
            }

            return connectionPolicy;
        }

        internal Documents.ConsistencyLevel? GetDocumentsConsistencyLevel()
        {
            if (!ConsistencyLevel.HasValue)
            {
                return null;
            }

            switch (ConsistencyLevel.Value)
            {
                case Cosmos.ConsistencyLevel.BoundedStaleness:
                    return Documents.ConsistencyLevel.BoundedStaleness;
                case Cosmos.ConsistencyLevel.ConsistentPrefix:
                    return Documents.ConsistencyLevel.BoundedStaleness;
                case Cosmos.ConsistencyLevel.Eventual:
                    return Documents.ConsistencyLevel.Eventual;
                case Cosmos.ConsistencyLevel.Session:
                    return Documents.ConsistencyLevel.Session;
                case Cosmos.ConsistencyLevel.Strong:
                    return Documents.ConsistencyLevel.Strong;
                default:
                    throw new ArgumentException($"Unsupported ConsistencyLevel {ConsistencyLevel.Value}");
            }
        }

        internal static string GetAccountEndpoint(string connectionString)
        {
            return CosmosClientOptions.GetValueFromConnectionString(connectionString, CosmosClientOptions.ConnectionStringAccountEndpoint);
        }

        internal static string GetAccountKey(string connectionString)
        {
            return CosmosClientOptions.GetValueFromConnectionString(connectionString, CosmosClientOptions.ConnectionStringAccountKey);
        }

        private static string GetValueFromConnectionString(string connectionString, string keyName)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            if (builder.TryGetValue(keyName, out object value))
            {
                string keyNameValue = value as string;
                if (!string.IsNullOrEmpty(keyNameValue))
                {
                    return keyNameValue;
                }
            }

            throw new ArgumentException("The connection string is missing a required property: " + keyName);
        }

        private void ValidateLimitToEndpointSettings()
        {
            if (!string.IsNullOrEmpty(ApplicationRegion) && LimitToEndpoint)
            {
                throw new ArgumentException($"Cannot specify {nameof(ApplicationRegion)} and enable {nameof(LimitToEndpoint)}. Only one can be set.");
            }

            if (ApplicationPreferredRegions?.Count > 0 && LimitToEndpoint)
            {
                throw new ArgumentException($"Cannot specify {nameof(ApplicationPreferredRegions)} and enable {nameof(LimitToEndpoint)}. Only one can be set.");
            }

            if (!string.IsNullOrEmpty(ApplicationRegion) && ApplicationPreferredRegions?.Count > 0)
            {
                throw new ArgumentException($"Cannot specify {nameof(ApplicationPreferredRegions)} and {nameof(ApplicationRegion)}. Only one can be set.");
            }
        }

        private void ValidateDirectTCPSettings()
        {
            string settingName = string.Empty;
            if (ConnectionMode != ConnectionMode.Direct)
            {
                if (IdleTcpConnectionTimeout.HasValue)
                {
                    settingName = nameof(IdleTcpConnectionTimeout);
                }
                else if (OpenTcpConnectionTimeout.HasValue)
                {
                    settingName = nameof(OpenTcpConnectionTimeout);
                }
                else if (MaxRequestsPerTcpConnection.HasValue)
                {
                    settingName = nameof(MaxRequestsPerTcpConnection);
                }
                else if (MaxTcpConnectionsPerEndpoint.HasValue)
                {
                    settingName = nameof(MaxTcpConnectionsPerEndpoint);
                }
                else if (PortReuseMode.HasValue)
                {
                    settingName = nameof(PortReuseMode);
                }
            }

            if (!string.IsNullOrEmpty(settingName))
            {
                throw new ArgumentException($"{settingName} requires {nameof(ConnectionMode)} to be set to {nameof(ConnectionMode.Direct)}");
            }
        }

        internal UserAgentContainer BuildUserAgentContainer()
        {
            UserAgentContainer userAgent = new UserAgentContainer();
            string features = GetUserAgentFeatures();

            if (!string.IsNullOrEmpty(features))
            {
                userAgent.SetFeatures(features.ToString());
            }

            if (!string.IsNullOrEmpty(ApplicationName))
            {
                userAgent.Suffix = ApplicationName;
            }

            return userAgent;
        }

        private string GetUserAgentFeatures()
        {
            CosmosClientOptionsFeatures features = CosmosClientOptionsFeatures.NoFeatures;
            if (AllowBulkExecution)
            {
                features |= CosmosClientOptionsFeatures.AllowBulkExecution;
            }

            if (HttpClientFactory != null)
            {
                features |= CosmosClientOptionsFeatures.HttpClientFactory;
            }

            if (features == CosmosClientOptionsFeatures.NoFeatures)
            {
                return null;
            }

            return Convert.ToString((int)features, 2).PadLeft(8, '0');
        }

        /// <summary>
        /// Serialize the current configuration into a JSON string
        /// </summary>
        /// <returns>Returns a JSON string of the current configuration.</returns>
        internal string GetSerializedConfiguration()
        {
            return JsonConvert.SerializeObject(this);
        }

        /// <summary>
        /// The complex object passed in by the user can contain objects that can not be serialized. Instead just log the types.
        /// </summary>
        private class ClientOptionJsonConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                if (value is Collection<RequestHandler> handlers)
                {
                    writer.WriteValue(string.Join(":", handlers.Select(x => x.GetType())));
                    return;
                }

                CosmosJsonSerializerWrapper cosmosJsonSerializerWrapper = value as CosmosJsonSerializerWrapper;
                if (value is CosmosJsonSerializerWrapper)
                {
                    writer.WriteValue(cosmosJsonSerializerWrapper.InternalJsonSerializer.GetType().ToString());
                }

                CosmosSerializer cosmosSerializer = value as CosmosSerializer;
                if (cosmosSerializer is CosmosSerializer)
                {
                    writer.WriteValue(cosmosSerializer.GetType().ToString());
                }
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                throw new NotImplementedException("Unnecessary because CanRead is false. The type will skip the converter.");
            }

            public override bool CanRead => false;

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(DateTime);
            }
        }
    }
}
