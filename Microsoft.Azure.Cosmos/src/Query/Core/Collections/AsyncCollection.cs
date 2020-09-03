//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Collections
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary> 
    /// Provides awaitable and bounding capabilities for thread-safe collections that implement IProducerConsumerCollection&lt;T&gt;.
    /// </summary>
    internal sealed class AsyncCollection<T>
    {
        private delegate bool TryPeekDelegate(out T item);
        private readonly IProducerConsumerCollection<T> collection;
        private readonly int boundingCapacity;
        private readonly SemaphoreSlim notFull;
        private readonly SemaphoreSlim notEmpty;
        private readonly TryPeekDelegate tryPeekDelegate;

        public AsyncCollection()
            : this(new ConcurrentQueue<T>(), int.MaxValue)
        {
        }

        public AsyncCollection(int boundingCapacity)
            : this(new ConcurrentQueue<T>(), boundingCapacity)
        {
        }

        public AsyncCollection(IProducerConsumerCollection<T> collection)
            : this(collection, int.MaxValue)
        {
        }

        public AsyncCollection(IProducerConsumerCollection<T> collection, int boundingCapacity)
        {
            if (collection == null)
            {
                throw new ArgumentNullException("collection");
            }

            if (boundingCapacity < 1)
            {
                throw new ArgumentOutOfRangeException("boundedCapacity is not a positive value.");
            }

            int count = collection.Count;

            if (boundingCapacity < count)
            {
                throw new ArgumentOutOfRangeException("boundedCapacity is less than the size of collection.");
            }

            this.collection = collection;
            this.boundingCapacity = boundingCapacity;
            notFull = IsUnbounded ? null : new SemaphoreSlim(boundingCapacity - count, boundingCapacity);
            notEmpty = new SemaphoreSlim(count);
            if (collection is ConcurrentQueue<T> concurrentQueue)
            {
                tryPeekDelegate = concurrentQueue.TryPeek;
                return;
            }

            if (collection is PriorityQueue<T> priorityQueue)
            {
                tryPeekDelegate = priorityQueue.TryPeek;
                return;
            }

            throw new NotSupportedException($"The IProducerConsumerCollection type of {typeof(T)} is not supported.");
        }

        public int Count
        {
            get
            {
                return collection.Count;
            }
        }

        public bool IsUnbounded
        {
            get
            {
                return boundingCapacity >= int.MaxValue;
            }
        }

        public async Task AddAsync(T item, CancellationToken token = default)
        {
            if (!IsUnbounded)
            {
                await notFull.WaitAsync(token);
            }

            if (collection.TryAdd(item))
            {
                notEmpty.Release();
            }
        }

        public async Task AddRangeAsync(IEnumerable<T> items, CancellationToken token = default)
        {
            if (!IsUnbounded)
            {
                foreach (T item in items)
                {
                    await AddAsync(item);
                }
            }
            else
            {
                int count = 0;
                foreach (T item in items)
                {
                    if (collection.TryAdd(item))
                    {
                        ++count;
                    }
                }

                if (count > 0)
                {
                    notEmpty.Release(count);
                }
            }
        }

        public async Task<T> TakeAsync(CancellationToken token = default)
        {
            await notEmpty.WaitAsync(token);
            if (collection.TryTake(out T item))
            {
                if (!IsUnbounded)
                {
                    notFull.Release();
                }
            }

            return item;
        }

        public async Task<T> PeekAsync(CancellationToken token = default)
        {
            if (tryPeekDelegate == null)
            {
                throw new NotImplementedException();
            }

            await notEmpty.WaitAsync(token);
            // Do nothing if tryPeekFunc returns false
            tryPeekDelegate(out T item);
            notEmpty.Release();

            return item;
        }

        public bool TryPeek(out T item)
        {
            if (tryPeekDelegate == null)
            {
                throw new NotImplementedException();
            }

            return tryPeekDelegate(out item);
        }

        public async Task<IReadOnlyList<T>> DrainAsync(
            int maxElements = int.MaxValue,
            TimeSpan timeout = default,
            Func<T, bool> callback = null,
            CancellationToken token = default)
        {
            if (maxElements < 1)
            {
                throw new ArgumentOutOfRangeException("maxElements is not a positive value.");
            }

            List<T> elements = new List<T>();

            Stopwatch stopWatch = Stopwatch.StartNew();
            while (elements.Count < maxElements && await notEmpty.WaitAsync(timeout, token))
            {
                if (collection.TryTake(out T item) && (callback == null || callback(item)))
                {
                    elements.Add(item);
                }
                else
                {
                    break;
                }

                timeout.Subtract(TimeSpan.FromTicks(Math.Min(stopWatch.ElapsedTicks, timeout.Ticks)));
                stopWatch.Restart();
            }

            if (!IsUnbounded && elements.Count > 0)
            {
                notFull.Release(elements.Count);
            }

            return elements;
        }
    }
}