//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Handles operation queueing and dispatching.
    /// Fills batches efficiently and maintains a timer for early dispatching in case of partially-filled batches and to optimize for throughput.
    /// </summary>
    /// <remarks>
    /// There is always one batch at a time being filled. Locking is in place to avoid concurrent threads trying to Add operations while the timer might be Dispatching the current batch.
    /// The current batch is dispatched and a new one is readied to be filled by new operations, the dispatched batch runs independently through a fire and forget pattern.
    /// </remarks>
    /// <seealso cref="BatchAsyncBatcher"/>
    internal class BatchAsyncStreamer : IDisposable
    {
        private static readonly TimeSpan congestionControllerDelay = TimeSpan.FromMilliseconds(1000);
        private static readonly TimeSpan batchTimeout = TimeSpan.FromMilliseconds(100);

        private readonly object dispatchLimiter = new object();
        private readonly int maxBatchOperationCount;
        private readonly int maxBatchByteSize;
        private readonly BatchAsyncBatcherExecuteDelegate executor;
        private readonly BatchAsyncBatcherRetryDelegate retrier;
        private readonly CosmosSerializerCore serializerCore;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly int congestionIncreaseFactor = 1;
        private readonly int congestionDecreaseFactor = 5;
        private readonly int maxDegreeOfConcurrency;
        private readonly TimerWheel timerWheel;

        private volatile BatchAsyncBatcher currentBatcher;
        private TimerWheelTimer currentTimer;
        private Task timerTask;

        private TimerWheelTimer congestionControlTimer;
        private Task congestionControlTask;
        private readonly SemaphoreSlim limiter;

        private int congestionDegreeOfConcurrency = 1;
        private long congestionWaitTimeInMilliseconds = 1000;
        private readonly BatchPartitionMetric oldPartitionMetric;
        private readonly BatchPartitionMetric partitionMetric;

        public BatchAsyncStreamer(
            int maxBatchOperationCount,
            int maxBatchByteSize,
            TimerWheel timerWheel,
            SemaphoreSlim limiter,
            int maxDegreeOfConcurrency,
            CosmosSerializerCore serializerCore,
            BatchAsyncBatcherExecuteDelegate executor,
            BatchAsyncBatcherRetryDelegate retrier)
        {
            if (maxBatchOperationCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBatchOperationCount));
            }

            if (maxBatchByteSize < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBatchByteSize));
            }

            if (executor == null)
            {
                throw new ArgumentNullException(nameof(executor));
            }

            if (retrier == null)
            {
                throw new ArgumentNullException(nameof(retrier));
            }

            if (serializerCore == null)
            {
                throw new ArgumentNullException(nameof(serializerCore));
            }

            if (limiter == null)
            {
                throw new ArgumentNullException(nameof(limiter));
            }

            if (maxDegreeOfConcurrency < 1)
            {
                throw new ArgumentNullException(nameof(maxDegreeOfConcurrency));
            }

            this.maxBatchOperationCount = maxBatchOperationCount;
            this.maxBatchByteSize = maxBatchByteSize;
            this.executor = executor;
            this.retrier = retrier;
            this.timerWheel = timerWheel;
            this.serializerCore = serializerCore;
            currentBatcher = CreateBatchAsyncBatcher();
            ResetTimer();

            this.limiter = limiter;
            oldPartitionMetric = new BatchPartitionMetric();
            partitionMetric = new BatchPartitionMetric();
            this.maxDegreeOfConcurrency = maxDegreeOfConcurrency;

            StartCongestionControlTimer();
        }

        public void Add(ItemBatchOperation operation)
        {
            BatchAsyncBatcher toDispatch = null;
            lock (dispatchLimiter)
            {
                while (!currentBatcher.TryAdd(operation))
                {
                    // Batcher is full
                    toDispatch = GetBatchToDispatchAndCreate();
                }
            }

            if (toDispatch != null)
            {
                // Discarded for Fire & Forget
                _ = toDispatch.DispatchAsync(partitionMetric, cancellationTokenSource.Token);
            }
        }

        public void Dispose()
        {
            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();

            currentTimer.CancelTimer();
            currentTimer = null;
            timerTask = null;

            if (congestionControlTimer != null)
            {
                congestionControlTimer.CancelTimer();
                congestionControlTimer = null;
                congestionControlTask = null;
            }
        }

        private void ResetTimer()
        {
            currentTimer = timerWheel.CreateTimer(BatchAsyncStreamer.batchTimeout);
            timerTask = currentTimer.StartTimerAsync().ContinueWith((task) =>
            {
                if (task.IsCompleted)
                {
                    DispatchTimer();
                }
            }, cancellationTokenSource.Token);
        }

        private void StartCongestionControlTimer()
        {
            congestionControlTimer = timerWheel.CreateTimer(BatchAsyncStreamer.congestionControllerDelay);
            congestionControlTask = congestionControlTimer.StartTimerAsync().ContinueWith(async (task) =>
            {
                await RunCongestionControlAsync();
            }, cancellationTokenSource.Token);
        }

        private void DispatchTimer()
        {
            if (cancellationTokenSource.IsCancellationRequested)
            {
                return;
            }

            BatchAsyncBatcher toDispatch;
            lock (dispatchLimiter)
            {
                toDispatch = GetBatchToDispatchAndCreate();
            }

            if (toDispatch != null)
            {
                // Discarded for Fire & Forget
                _ = toDispatch.DispatchAsync(partitionMetric, cancellationTokenSource.Token);
            }

            ResetTimer();
        }

        private BatchAsyncBatcher GetBatchToDispatchAndCreate()
        {
            if (currentBatcher.IsEmpty)
            {
                return null;
            }

            BatchAsyncBatcher previousBatcher = currentBatcher;
            currentBatcher = CreateBatchAsyncBatcher();
            return previousBatcher;
        }

        private BatchAsyncBatcher CreateBatchAsyncBatcher()
        {
            return new BatchAsyncBatcher(maxBatchOperationCount, maxBatchByteSize, serializerCore, executor, retrier);
        }

        private async Task RunCongestionControlAsync()
        {
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                long elapsedTimeInMilliseconds = partitionMetric.TimeTakenInMilliseconds - oldPartitionMetric.TimeTakenInMilliseconds;

                if (elapsedTimeInMilliseconds >= congestionWaitTimeInMilliseconds)
                {
                    long diffThrottle = partitionMetric.NumberOfThrottles - oldPartitionMetric.NumberOfThrottles;
                    long changeItemsCount = partitionMetric.NumberOfItemsOperatedOn - oldPartitionMetric.NumberOfItemsOperatedOn;
                    oldPartitionMetric.Add(changeItemsCount, elapsedTimeInMilliseconds, diffThrottle);

                    if (diffThrottle > 0)
                    {
                        // Decrease should not lead to degreeOfConcurrency 0 as this will just block the thread here and no one would release it.
                        int decreaseCount = Math.Min(congestionDecreaseFactor, congestionDegreeOfConcurrency / 2);

                        // We got a throttle so we need to back off on the degree of concurrency.
                        for (int i = 0; i < decreaseCount; i++)
                        {
                            await limiter.WaitAsync(cancellationTokenSource.Token);
                        }

                        congestionDegreeOfConcurrency -= decreaseCount;

                        // In case of throttling increase the wait time, so as to converge max degreeOfConcurrency
                        congestionWaitTimeInMilliseconds += 1000;
                    }

                    if (changeItemsCount > 0 && diffThrottle == 0)
                    {
                        if (congestionDegreeOfConcurrency + congestionIncreaseFactor <= maxDegreeOfConcurrency)
                        {
                            // We aren't getting throttles, so we should bump up the degree of concurrency.
                            limiter.Release(congestionIncreaseFactor);
                            congestionDegreeOfConcurrency += congestionIncreaseFactor;
                        }
                    }
                }
                else
                {
                    break;
                }
            }

            StartCongestionControlTimer();
        }
    }
}
