// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Timers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using Microsoft.Azure.Cosmos.Core.Trace;

#nullable enable
    internal sealed class TimerWheelCore : TimerWheel, IDisposable
    {
        private readonly ConcurrentDictionary<int, ConcurrentQueue<TimerWheelTimer>> timers;
        private readonly int resolutionInTicks;
        private readonly int resolutionInMs;
        private readonly int buckets;
        private readonly Timer timer;
        private readonly object subscriptionLock;
        private readonly object timerConcurrencyLock;
        private bool isDisposed = false;
        private bool isRunning = false;
        private int expirationIndex = 0;

#pragma warning disable CS8618 // Non-nullable field is uninitialized. Consider declaring as nullable.
        private TimerWheelCore(
            double resolution,
            int buckets)
#pragma warning restore CS8618 // Non-nullable field is uninitialized. Consider declaring as nullable.
        {
            if (resolution <= 20)
            {
                throw new ArgumentOutOfRangeException(nameof(resolution), "Value is too low, machine resolution less than 20 ms has unexpected results https://docs.microsoft.com/dotnet/api/system.threading.timer");
            }

            if (buckets <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(buckets));
            }

            resolutionInMs = (int)resolution;
            resolutionInTicks = (int)TimeSpan.FromMilliseconds(resolutionInMs).Ticks;
            this.buckets = buckets;
            timers = new ConcurrentDictionary<int, ConcurrentQueue<TimerWheelTimer>>();
            subscriptionLock = new object();
            timerConcurrencyLock = new object();
        }

        internal TimerWheelCore(
            TimeSpan resolution,
            int buckets)
            : this(resolution.TotalMilliseconds, buckets)
        {
            timer = new Timer(OnTimer, state: null, resolutionInMs, resolutionInMs);
        }

        /// <summary>
        /// Used only for unit tests.
        /// </summary>
        internal TimerWheelCore(
            TimeSpan resolution,
            int buckets,
            Timer timer)
            : this(resolution.TotalMilliseconds, buckets)
        {
            this.timer = timer;
        }

        public override void Dispose()
        {
            if (isDisposed)
            {
                return;
            }

            DisposeAllTimers();

            isDisposed = true;
        }

        public override TimerWheelTimer CreateTimer(TimeSpan timeout)
        {
            ThrowIfDisposed();
            int timeoutInMs = (int)timeout.TotalMilliseconds;
            if (timeoutInMs < resolutionInMs)
            {
                throw new ArgumentOutOfRangeException(nameof(timeoutInMs), $"TimerWheel configured with {resolutionInMs} resolution, cannot use a smaller timeout of {timeoutInMs}.");
            }

            if (timeoutInMs % resolutionInMs != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(timeoutInMs), $"TimerWheel configured with {resolutionInMs} resolution, cannot use a different resolution of {timeoutInMs}.");
            }

            if (timeoutInMs > resolutionInMs * buckets)
            {
                throw new ArgumentOutOfRangeException(nameof(timeoutInMs), $"TimerWheel configured with {resolutionInMs * buckets} max, cannot use a larger timeout of {timeoutInMs}.");
            }

            return new TimerWheelTimerCore(TimeSpan.FromMilliseconds(timeoutInMs), this);
        }

        public override void SubscribeForTimeouts(TimerWheelTimer timer)
        {
            ThrowIfDisposed();
            long timerTimeoutInTicks = timer.Timeout.Ticks;
            int bucket = (int)timerTimeoutInTicks / resolutionInTicks;
            lock (subscriptionLock)
            {
                int index = GetIndexForTimeout(bucket);
                ConcurrentQueue<TimerWheelTimer> timerQueue = timers.GetOrAdd(index,
                        _ =>
                        {
                            return new ConcurrentQueue<TimerWheelTimer>();
                        });
                timerQueue.Enqueue(timer);
            }
        }

        public void OnTimer(object stateInfo)
        {
            lock (timerConcurrencyLock)
            {
                if (!isRunning)
                {
                    isRunning = true;
                }
                else
                {
                    return;
                }
            }

            try
            {
                if (timers.TryGetValue(expirationIndex, out ConcurrentQueue<TimerWheelTimer> timerQueue))
                {
                    while (timerQueue.TryDequeue(out TimerWheelTimer timer))
                    {
                        timer.FireTimeout();
                    }
                }

                if (++expirationIndex == buckets)
                {
                    expirationIndex = 0;
                }
            }
            catch (Exception ex)
            {
                DefaultTrace.TraceWarning($"TimerWheel: OnTimer error : {ex.Message}\n, stack: {ex.StackTrace}");
            }
            finally
            {
                lock (timerConcurrencyLock)
                {
                    isRunning = false;
                }
            }
        }

        private int GetIndexForTimeout(int bucket)
        {
            int index = bucket + expirationIndex;
            if (index > buckets)
            {
                index -= buckets;
            }

            return index - 1; // zero based
        }

        private void DisposeAllTimers()
        {
            foreach (KeyValuePair<int, ConcurrentQueue<TimerWheelTimer>> kv in timers)
            {
                ConcurrentQueue<TimerWheelTimer> pooledTimerQueue = kv.Value;
                while (pooledTimerQueue.TryDequeue(out TimerWheelTimer timer))
                {
                    timer.CancelTimer();
                }
            }

            timer?.Dispose();
        }

        private void ThrowIfDisposed()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("TimerWheel is disposed.");
            }
        }
    }
}