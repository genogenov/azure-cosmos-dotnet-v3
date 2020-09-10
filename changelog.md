Preview features are treated as a separate branch and will not be included in the official release until the feature is ready. Each preview release lists all the additional features that are enabled.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### <a name="3.13.0-preview"/> [3.13.0-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.13.0-preview) - 2020-08-12

#### Added
- [#1725](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1725) ChangeFeed : Adds ChangeFeedStartFrom to support StartTimes x FeedRanges. WARNING: This is breaking change for preview SDK
- [#1764](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1764) Performance: Adds compiler optimize flag
- [#1768](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1768) SessionToken: Adds optimization to reduce header size by removing session token for CRUD on stored procedure, triggers, and UDFs
- [#1489](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1489) RequestOptions: Add ability for applications to specify request context

#### Fixed

- [#1757](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1757) Batch API: Fixes the size limit to reduce timeouts
- [#1758](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1758) Connectivity: Fixes address resolution calls when using EnableTcpConnectionEndpointRediscovery

### <a name="3.12.0"/> [3.12.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.12.0) - 2020-08-06

#### Added 

- [#1548](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1548) Transport: Adds an optimization to unify HttpClient usage across Gateway classes
- [#1569](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1569) Batch API: Adds support of request options for transactional batch
- [#1693](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1693) Performance: Reduces lock contention on GlobalAddress Resolver
- [#1712](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1712) Performance: Adds optimization to reduce AuthorizationHelper memory allocations
- [#1715](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1715) Availability: Adds cross-region retry mechanism on transient connectivity issues
- [#1721](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1721) LINQ : Adds support for case-insensitive searches (Thanks to jeffpardy)
- [#1733](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1733) Change Feed Processor: Adds backward compatibility of lease store

#### Fixed

- [#1720](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1720) Gateway Trace: Fixes a bug where the ActivityId is being set to Guid.Empty
- [#1728](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1728) Diagnostics: Fixes ActivityScope by moving it to operation level
- [#1740](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1740) Connection limits: Fixes .NET core to honor gateway connection limit
- [#1744](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1744) Transport: Fixes use of PortReuseMode and other Direct configuration settings



### <a name="3.11.0"/> [3.11.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.11.0) - 2020-07-07

#### Added 

- [#1587](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1587) & [1643](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1643) & [1667](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1667)  Diagnostics: Adds synchronization context tracing to all request
- [#1617](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1617) Performance: Fixes Object Model hierarchy to use strings for relative paths instead of URI
- [#1639](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1639) CosmosClient: Adds argument check for empty key to prevent ambiguous 401 not authorized exception
- [#1640](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1640) Bulk: Adds TimerWheel to Bulk to improve latency
- [#1678](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1678) Autoscale: Adds to container builder

#### Fixed

- [#1638](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1638) Documentation : Fixes all examples to add using statement to FeedIterator
- [#1666](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1666) CosmosOperationCanceledException: Fixes handler to catch all operation cancelled exceptions
- [#1682](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1682) Performance: Fixes high CPU consumption caused by EnableTcpConnectionEndpointRediscovery


### <a name="3.10.1"/> [3.10.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.10.1) - 2020-06-18

- [#1637](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1637) TransportHandler : Removes stack trace print. Introduced in 3.10.0 PR 1587 

### <a name="3.10.0"/> [3.10.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.10.0) - 2020-06-18

#### Added

- [#1613](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1613) Query FeedIterator: Adds IDisposable to fix memory leak. WARNING: This will require changes to fix static anlysis tools checking for dispose.
- [#1550](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1550) CosmosOperationCanceledException: This enables users to access the diagnsotics when an operation is canceled via the cancellation token. The new type extends OperationCanceledException so it does not break current exception handling and includes the CosmosDiagnostic in the ToString().
- [#1578](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1578) Query: Adds memory optimization to prevent coping the buffer
- [#1578](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1578) Query: Adds support for ignore case for [Contains](https://docs.microsoft.com/azure/cosmos-db/sql-query-contains) and [StartsWith](https://docs.microsoft.com/azure/cosmos-db/sql-query-startswith) functions.
- [#1602](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1602) Diagnostics: Adds CPU usage to all operations
- [#1603](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1603) Documentation: Adds new exception handling documentation


#### Fixed

- [#1530](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1530) ContainerDefinition : Fixes WithDefaultTimeToLive argument spelling (Thanks to tony-xia)
- [#1547](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1547) & [#1582](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1582) Query and Readfeed: Fix exceptions caused by not properly handling splits
- [#1578](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1578) ApplicationRegion: Fixes ApplicationRegion to ensure the correct order is being used for failover scenarios
- [#1585](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1585) Query : Fixes Multi- ORDER BY continuation token support with QueryExecutionInfo response headers

### <a name="3.9.1"/> [3.9.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.9.1) - 2020-05-19
### <a name="3.9.1-preview"/> [3.9.1-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.9.1-preview) - 2020-05-19

#### Fixed

- [#1539](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1539) CosmosException and Diagnostics: Fixes ToString() to not grow exponentially with retries. Introduced in 3.7.0 in PR [#1189](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1189).

### <a name="3.9.0"/> [3.9.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.9.0) - 2020-05-18

#### Added

- [#1356](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1356) & [#1407](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1407) & [#1428](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1428) & [#1407](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1407) Adds autoscale support.
- [#1398](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1398) Diagnostics: Adds CPU monitoring for .NET Core.
- [#1441](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1441) Transport: Adds `HttpClientFactory` support on `CosmosClientOptions`.
- [#1457](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1457)  Container: Adds database reference to the container.
- [#1455](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1454) Serializer: Adds SDK serializer to `Client.ClientOptions.Serializer`.
- [#1397](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1397) CosmosClientBuilder: Adds preferred regions and `WithConnectionModeDirect()`.
- [#1439](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1439) No content on Response: Adds the ability to have operations return no content from Azure Cosmos DB. 
- [#1398](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1398) & [#1516](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1516) Read feed and change feed: Adds serialization optimization to reduce memory and CPU utilization up to 90%. Objects are now passed as an array to the serializer. 
- [#1516](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1516) Query: Adds serialization optimization to reduce memory up to %50 and CPU utilization up to 25%. Objects are now passed as an array to the serializer.

#### Fixed

- [#1401](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1401) & [#1437](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1437): Response type: Fix deadlock on scenarios with `SynchronizationContext` when using `Response.Container`.
- [#1445](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1445) Transport: Fix `ServicePoint` for `WebAssembly`.
- [#1462](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1462) UserAgent: Fix feature usage tracking.
- [#1469](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1469) Diagnostics: Fix `InvalidOperationException` and converts elapsed time to millisecond.
- [#1512](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1512) PartitionRoutingHelper: Fix ReadFeed `ArgumentNullException` due to container cache miss.
- [#1530](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1530) CosmosClientBuilder: Fix WithDefaultTimeToLive parameter spelling.

### <a name="3.9.0-preview3"/> [3.9.0-preview3](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.9.0-preview3) - 2020-05-11

#### Added

- [#1356](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1356) & [#1407](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1407) & [#1428](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1428) Autoscale preview release.
- [#1407](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1407) Autoscale: Adds `CreateDatabaseIfNotExistsAsync` and `CreateContainerIfNotExistsAsync` methods.
- [#1410](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1410) FeedRange: Adds Json serialization support.
- [#1398](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1398) Diagnostics: Adds CPU monitoring for .NET Core.
- [#1441](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1441) Transport: Adds `HttpClientFactory` support on `CosmosClientOptions`.
- [#1457](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1457) Container: Adds database reference to the container.
- [#1453](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1453) Response factory: Adds a response factory to the public API.
- [#1455](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1454) Serializer: Adds SDK serializer to `Client.ClientOptions.Serializer`.
- [#1397](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1397) CosmosClientBuilder: Adds preferred regions and public internal func `WithConnectionModeDirect()`.
- [#1439](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1439) No content on response: Adds the ability to have operation return no content from Azure Cosmos DB.
- [#1469](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1469) Diagnostics: Fixes the `InvalidOperationException` and converts elapsed time to millisecond.

#### Fixed

- [#1398](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1398) Reduced memory allocations on query deserialization.
- [#1401](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1401) & [#1437](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1437): Response type: Fixes deadlock on scenarios with `SynchronizationContext` when using `Response.Container`.
- [#1445](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1445) Transport: Fixes `ServicePoint` for WebAssembly.
- [#1462](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1462) UserAgent: Fixes feature usage tracking.

### <a name="3.9.0-preview"/> [3.9.0-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.9.0-preview) - 2020-04-17

#### Added

- [#1356](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1356) Autoscale: Adds to public preview release

### <a name="3.8.0-preview"/> [3.8.0-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.8.0-preview) - 2020-04-16

#### Added

- [#1331](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1331) Enabled client encryption / decryption for transactional batch requests

#### Fixed

- [#1369](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1369) Fixes decryption for 'order by' query results

### <a name="3.8.0"/> [3.8.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.8.0) - 2020-04-07

#### Added

- [#1314](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1314) Adds configuration for proactive TCP end-of-connection detection.
- [#1305](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1305) Adds support for preferred region customization.

#### Fixed

- [#1312](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1312) Fixes null reference when using default(PartitionKey).
- [#1296](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1296) Decrypts the encrypted properties before returning query result.
- [#1345](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1345) Fixes get query plan diagnostics.

### <a name="3.7.1-preview"/> [3.7.1-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.7.1-preview) - 2020-03-30

- [#1210](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1210) Adds change feed pull model.
- [#1242](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1242) Client encryption - fixes bug in read path without encrypted properties.
- [#1314](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1314) Adds configuration for proactive TCP end-of-connection detection.
- [#1312](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1312) Fixes null reference when using default(PartitionKey).
- [#1296](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1296) Decrypts the encrypted properties before returning query result.

### <a name="3.7.0"/> [3.7.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.7.0) - 2020-03-26

#### Added

- [#1268](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1268) Adds `GetElapsedClientLatency` to `CosmosDiagnostics`.
- [#1239](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1239) Made `MultiPolygon` and `PolygonCoordinates` classes public.
- [#1233](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1265) Partition key now supports operators `==, !=` for equality comparison.
- [#1285](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1285) Add query plan retrieval to diagnostics.
- [#1289](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1289) Query `ORDER BY` resume optimization.
- [#1074](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1074) Bulk API congestion control.

#### Fixed

- [#1213](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1213) `CosmosException` now returns the original stack trace.
- [#1213](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1213) `ResponseMessage.ErrorMessage` is now always correctly populated. There was bug in some scenarios where the error message was left in the content stream.
- [#1298](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1298) `CosmosException.Message` contains the same information as `CosmosException.ToString()` to ensure all the information is being tracked.
- [#1242](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1242) Client encryption - Fixes bug in read path without encrypted properties.
- [#1189](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1189) Query diagnostics shows correct overall time.
- [#1189](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1189) Fixes a bug that caused duplicate information in diagnostic context.
- [#1263](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1263) Fixes a bug where retry after interval did not get set on query stream responses.
- [#1198](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1198) Fixes null reference exception when calling a disposed `CosmosClient`.
- [#1274](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1274) `ObjectDisposedException` is thrown when calling all SDK objects like Database and Container that reference a disposed client.
- [#1268](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1268) Fixes bug where Request Options was getting lost for `Database.ReadStreamAsync` and `Database.DeleteStreamAsync` methods.
- [#1304](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1304) Fixes XML documentation so it now is visible in Visual Studio.

### <a name="3.7.0-preview2"/> [3.7.0-preview2](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.7.0-preview2) - 2020-03-09

- [#1210](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1210) Change feed pull model.
- [#1242](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1242) Client encryption - fixes bug in read path without encrypted properties.

### <a name="3.7.0-preview"/> [3.7.0-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.7.0-preview) - 2020-02-25

- [#1074](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1074) Bulk API congestion control.
- [#1210](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1210) Change feed pull model.

### <a name="3.6.0"/> [3.6.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.6.0) - 2020-01-23

- [#1105](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1198) CosmosClient Immutability + Disposable Fixes

#### Added

- [#1097](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1097) `GeospatialConfig` to `ContainerProperties`, `BoundingBoxProperties` to `SpatialPath`.
- [#1061](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1061) Stream payload to `ExecuteStoredProcedureStreamAsync`.
- [#1062](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1062) Additional diagnostic information including the ability to track time through the different SDK layers.
- [#1107](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1107) Source Link support.
- [#1121](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1121) `StandByFeedIterator` breath-first read strategy.

#### Fixed

- [#1105](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1105) Custom serializer no longer calls SDK owned types that would cause serialization exceptions.
- [#1112](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1112) Fixes SDK properties like `DatabaseProperties` to have the same JSON attributes.
- [#1116](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1116) Fixes a deadlock on scenarios with `SynchronizationContext` while executing async query operations.
- [#1143](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1143) Fixes permission resource link and authorization issue when doing a query with resource token for a specific partition key.
- [#1150](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1150) Fixes `NullReferenceException` when using a non-existent Lease Container.

### <a name="3.5.1"/> [3.5.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.5.1) - 2019-12-11

#### Fixed

- [#1060](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1060) Fixes unicode encoding bug in DISTINCT queries.
- [#1070](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1070) `CreateItem` will only retry for auto-extracted partition key in-case of collection re-creation.
- [#1075](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1075) Including header size details for bad request with large headers.
- [#1078](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1078) Fixes a deadlock on scenarios with `SynchronizationContext` while executing async SDK API.
- [#1081](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1081) Fixes race condition in serializer caused by null reference exception.
- [#1086](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1086) Fix possible `NullReferenceException` on a `TransactionalBatch` code path.
- [#1091](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1091) Fixes a bug in query when a partition split occurs that causes a `NotImplementedException` to be thrown.
- [#1089](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1089) Fixes a `NullReferenceException` when using Bulk with items without partition key.

### <a name="3.5.0"/> [3.5.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.5.0) - 2019-12-03

#### Added

- [#979](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/979) Make `SessionToken` on `QueryRequestOptions` public.
- [#995](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/995) Included session token in diagnostics.
- [#1000](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1000) Adds `PortReuseMode` to `CosmosClientOptions`.
- [#1017](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1017) Adds `ClientSideRequestStatistics` to gateway calls and making end time nullable.
- [#1038](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1038) Adds self-link to resource properties.

#### Fixed

- [#921](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/921) Fixes error handling to preserve stack trace in certain scenarios.
- [#944](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/944) Change feed processor won't use user serializer for internal operations.
- [#988](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/988) Fixes query mutating due to retry of gone / name cache is stale.
- [#954](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/954) Support start from beginning for change feed processor in multi master accounts.
- [#999](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/999) Fixes grabbing extra page, updated continuation token on exception path, and non-Ascii character in order by continuation token.
- [#1013](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1013) Gateway `OperationCanceledExceptions` are now returned as request timeouts.
- [#1020](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1020) Direct package update removes debug statements.
- [#1023](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1023) Fixes `ThroughputResponse.IsReplacePending` header mapping.
- [#1036](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1036) Fixes query responses to return null Content if it is a failure.
- [#1045](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1045) Adds stack trace and inner exception to `CosmosException`.
- [#1050](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/1050) Adds mocking constructors to `TransactionalBatchOperationResult`.

### <a name="3.4.1"/> [3.4.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.4.1) - 2019-11-06

#### Fixed

- [#978](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/978) Fixes mocking for `FeedIterator` and response classes.

### <a name="3.4.0"/> [3.4.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.4.0) - 2019-11-04

#### Added

- [#853](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/853) ORDER BY arrays and object support.
- [#877](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/877) Query diagnostics now contains client-side request diagnostics information.
- [#923](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/923) Bulk support is now public.
- [#922](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/922) Included information of bulk support usage in user agent.
- [#934](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/934) Preserved the ordering of projections in a `GROUP BY` query.
- [#952](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/952) ORDER BY undefined and mixed type `ORDER BY` support.
- [#965](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/965) Batch API is now public.

#### Fixed

- [#901](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/901) Fixes a bug causing query response to create a new stream for each content call.
- [#918](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/918) Fixes serializer being used for scripts, permissions, and conflict-related iterators.
- [#936](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/936) Fixes bulk requests with large resources to have natural exception.


### <a name="3.3.3"/> [3.3.3](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.3.3) - 2019-10-30

- [#837](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/837) Fixes group by bug for non-Windows platforms.
- [#927](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/927) Fixes query returning partial results instead of error.

### <a name="3.3.2"/> [3.3.2](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.3.2) - 2019-10-16

#### Fixed

- [#905](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/909) Fixes lINQ camel case bug.

### <a name="3.3.1"/> [3.3.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.3.1) - 2019-10-11

#### Fixed

- [#895](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/895) Fixes user agent bug that caused format exceptions on non-Windows platforms.


### <a name="3.3.0"/> [3.3.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.3.0) - 2019-10-09

#### Added

- [#801](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/801) Enabled LINQ `ThenBy` operator after `OrderBy`.
- [#814](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/814) Ability to limit to configured endpoint only.
- [#822](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/822) `GROUP BY` query support.
- [#844](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/844) Adds `PartitionKeyDefinitionVersion` to container builder.

#### Fixed

- [#835](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/835) Fixes a bug that caused sorted ranges exceptions.
- [#846](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/846) Statistics not getting populated correctly on `CosmosException`.
- [#857](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/857) Fixes reusability of the bulk support across container instances.
- [#860](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/860) Fixes base user agent string.
- [#876](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/876) Default connection time out reduced from 60 s to 10 s.

### <a name="3.2.0"/> [3.2.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.2.0) - 2019-09-17

#### Added

- [#100](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/100) Configurable Tcp settings to `CosmosClientOptions`.
- [#615](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/615), [#775](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/775) Adds request diagnostics to response.
- [#622](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/622) Adds CRUD and query operations for users and permissions, which enables [ResourceToken](https://docs.microsoft.com/azure/cosmos-db/secure-access-to-data#resource-tokens) support.
- [#716](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/716) Added camel case serialization on LINQ query generation.
- [#729](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/729), [#776](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/776) Adds aggregate (CountAsync/SumAsync etc.) extensions for LINQ query.
- [#743](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/743) Adds `WebProxy` to `CosmosClientOptions`.

#### Fixed

- [#726](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/726) Query iterator `HasMoreResults` now returns false if an exception is hit.
- [#705](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/705) User agent suffix gets truncated.
- [#753](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/753) Reason was not being propagated for conflict exceptions.
- [#756](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/756) Change feed processor with `WithStartTime` would execute the delegate the first time with no items.
- [#761](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/761) `CosmosClient` deadlocks when using a custom task scheduler like Orleans.
- [#769](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/769) Session consistency and gateway mode session-token bug fix- under few rare non-success cases session token might be in-correct.
- [#772](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/772) Fixes throughput throwing when custom serializer used or offer doesn't exists.
- [#785](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/785) Incorrect key to throw `CosmosExceptions` with `HttpStatusCode.Unauthorized` status code.


### <a name="3.2.0-preview2"/> [3.2.0-preview2](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.2.0-preview2) - 2019-09-10

- [#585](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/585), [#741](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/741) Bulk execution support.
- [#427](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/427) Transactional batch support (Item CRUD).


### <a name="3.2.0-preview"/> [3.2.0-preview](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.2.0-preview) - 2019-08-09

- [#427](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/427) Transactional batch support (Item CRUD).


### <a name="3.1.1"/> [3.1.1](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.1.1) - 2019-08-12

#### Added

- [#650](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/650) `CosmosSerializerOptions` to customize serialization.

#### Fixed

- [#612](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/612) Bug fix for `ReadFeed` with partition key.
- [#614](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/614) Fixes spatial path serialization and compatibility with older index versions.
- [#619](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/619) Fixes `PInvokeStackImbalance` exception for .NET framework.
- [#626](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/626) `FeedResponse<T>` status codes now return OK for success instead of the invalid status code 0 or Accepted.
- [#629](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/629) Fixes `CreateContainerIfNotExistsAsync` validation to limited to partition key path only.
- [#630](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/630) Fixes user agent to contain environment and package information.


### <a name="3.1.0"/> 3.1.0 - 2019-07-29 - Unlisted

#### Added

- [#541](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/541) Adds consistency level to client and query options.
- [#544](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/544) Adds continuation token support for LINQ.
- [#557](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/557) Adds trigger options to item request options.
- [#572](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/572) Adds partition key validation on `CreateContainerIfNotExistsAsync`.
- [#581](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/581) Adds LINQ to `QueryDefinition` API.
- [#592](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/592) Adds `CreateIfNotExistsAsync` to container builder.
- [#597](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/597) Adds continuation token property to `ResponseMessage`.
- [#604](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/604) Adds LINQ `ToStreamIterator` extension method.

#### Fixed

- [#548](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/548) Fixes mis-typed message in `CosmosException.ToString()`.
- [#558](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/558) Location cache `ConcurrentDict` lock contentions fix.
- [#561](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/561) `GetItemLinqQueryable` now works with null query.
- [#567](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/567) Query correctly handles different language cultures.
- [#574](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/574) Fixes empty error message if query parsing fails from unexpected exception.
- [#576](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/576) Query correctly serializes the input into a stream.


### <a name="3.0.0"/> [3.0.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/3.0.0) - 2019-07-15

- General availability of [Version 3.0.0](https://www.nuget.org/packages/Microsoft.Azure.Cosmos/) of the .NET SDK.
- Targets .NET Standard 2.0, which supports .NET framework 4.6.1+ and .NET Core 2.0+.
- New object model, with top level `CosmosClient` and methods split across relevant database and container classes.
- New stream APIs that have high performance.
- Built-in support for change feed processor APIs.
- Fluent builder APIs for `CosmosClient`, container, and change feed processor.
- Idiomatic throughput management APIs.
- Granular `RequestOptions` and `ResponseTypes` for database, container, item, query, and throughput requests.
- Ability to scale non-partitioned containers.
- Extensible and customizable serializer.
- Extensible request pipeline with support for custom handlers.

## Release & Retirement dates

Microsoft provides notification at least **12 months** in advance of retiring an SDK in order to smooth the transition to a newer/supported version. New features and functionality and optimizations are only added to the current SDK, as such it is recommended that you always upgrade to the latest SDK version as early as possible.

After **31 August 2022**, Azure Cosmos DB will no longer make bug fixes, add new features, and provide support for versions 1.x of the Azure Cosmos DB .NET or .NET Core SDK for SQL API. If you prefer not to upgrade, requests sent from version 1.x of the SDK will continue to be served by the Azure Cosmos DB service.

| Version | Release Date | Retirement Date |
| --- | --- | --- |
| [3.6.0](#3.6.0) |January 23, 2020 |--- |
| [3.5.1](#3.5.1) |December 11, 2019 |--- |
| [3.5.0](#3.5.0) |December 03, 2019 |--- |
| [3.4.1](#3.4.1) |November 06, 2019 |--- |
| [3.4.0](#3.4.0) |November 04, 2019 |--- |
| [3.3.3](#3.3.3) |October 30, 2019 |--- |
| [3.3.2](#3.3.2) |October 16, 2019 |--- |
| [3.3.1](#3.3.1) |October 11, 2019 |--- |
| [3.3.0](#3.3.0) |October 8, 2019 |--- |
| [3.2.0](#3.2.0) |September 18, 2019 |--- |
| [3.1.1](#3.1.1) |August 12, 2019 |--- |
| [3.1.0](#3.1.0) |July 29, 2019 |--- |
| [3.0.0](#3.0.0) |July 15, 2019 |--- |
