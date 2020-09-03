//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Collections
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    /// <summary> 
    /// An implementation of <a href="https://en.wikipedia.org/wiki/Binary_heap">Binary Heap</a>
    /// </summary>
    internal sealed class PriorityQueue<T> : IProducerConsumerCollection<T>
    {
        private const int DefaultInitialCapacity = 17;
        private readonly List<T> queue;

        public PriorityQueue(bool isSynchronized = false)
            : this(DefaultInitialCapacity, isSynchronized)
        {
        }

        public PriorityQueue(int initialCapacity, bool isSynchronized = false)
            : this(initialCapacity, Comparer<T>.Default, isSynchronized)
        {
        }

        public PriorityQueue(IComparer<T> comparer, bool isSynchronized = false)
            : this(DefaultInitialCapacity, comparer, isSynchronized)
        {
        }

        public PriorityQueue(IEnumerable<T> enumerable, bool isSynchronized = false)
            : this(enumerable, Comparer<T>.Default, isSynchronized)
        {
        }

        public PriorityQueue(IEnumerable<T> enumerable, IComparer<T> comparer, bool isSynchronized = false)
            : this(new List<T>(enumerable), comparer, isSynchronized)
        {
            Heapify();
        }

        public PriorityQueue(int initialCapacity, IComparer<T> comparer, bool isSynchronized = false)
            : this(new List<T>(initialCapacity), comparer, isSynchronized)
        {
        }

        private PriorityQueue(List<T> queue, IComparer<T> comparer, bool isSynchronized)
        {
            IsSynchronized = isSynchronized;
            this.queue = queue ?? throw new ArgumentNullException(nameof(queue));
            Comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        }

        public int Count
        {
            get
            {
                return queue.Count;
            }
        }

        public IComparer<T> Comparer { get; }

        public bool IsSynchronized { get; }

        public object SyncRoot
        {
            get { return this; }
        }

        public void CopyTo(T[] array, int index)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    CopyToPrivate(array, index);
                    return;
                }
            }

            CopyToPrivate(array, index);
        }

        public bool TryAdd(T item)
        {
            Enqueue(item);
            return true;
        }

        public bool TryTake(out T item)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return TryTakePrivate(out item);
                }
            }

            return TryTakePrivate(out item);
        }

        public bool TryPeek(out T item)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return TryPeekPrivate(out item);
                }
            }

            return TryPeekPrivate(out item);
        }

        public void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    ClearPrivate();
                    return;
                }
            }

            ClearPrivate();
        }

        public bool Contains(T item)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return ContainsPrivate(item);
                }
            }

            return ContainsPrivate(item);
        }

        public T Dequeue()
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return DequeuePrivate();
                }
            }

            return DequeuePrivate();
        }

        public void Enqueue(T item)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    EnqueuePrivate(item);
                    return;
                }
            }

            EnqueuePrivate(item);
        }

        public void EnqueueRange(IEnumerable<T> items)
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    EnqueueRangePrivate(items);
                    return;
                }
            }

            EnqueueRangePrivate(items);
        }

        public T Peek()
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return PeekPrivate();
                }
            }

            return PeekPrivate();
        }

        public T[] ToArray()
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return ToArrayPrivate();
                }
            }

            return ToArrayPrivate();
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    return GetEnumeratorPrivate();
                }
            }

            return GetEnumeratorPrivate();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private void CopyToPrivate(T[] array, int index)
        {
            queue.CopyTo(array, index);
        }

        private bool TryTakePrivate(out T item)
        {
            if (queue.Count <= 0)
            {
                item = default;
                return false;
            }

            item = DequeuePrivate();
            return true;
        }

        private bool TryPeekPrivate(out T item)
        {
            if (queue.Count <= 0)
            {
                item = default;
                return false;
            }

            item = PeekPrivate();
            return true;
        }

        private void ClearPrivate()
        {
            queue.Clear();
        }

        private bool ContainsPrivate(T item)
        {
            return queue.Contains(item);
        }

        private T DequeuePrivate()
        {
            if (queue.Count <= 0)
            {
                throw new InvalidOperationException("No more elements");
            }

            T result = queue[0];
            queue[0] = queue[queue.Count - 1];
            queue.RemoveAt(queue.Count - 1);
            DownHeap(0);
            return result;
        }

        private void EnqueuePrivate(T item)
        {
            queue.Add(item);
            UpHeap(queue.Count - 1);
        }

        private void EnqueueRangePrivate(IEnumerable<T> items)
        {
            queue.AddRange(items);
            Heapify();
        }

        private T PeekPrivate()
        {
            if (queue.Count <= 0)
            {
                throw new InvalidOperationException("No more elements");
            }

            return queue[0];
        }

        private T[] ToArrayPrivate()
        {
            return queue.ToArray();
        }

        private IEnumerator<T> GetEnumeratorPrivate()
        {
            return new List<T>(queue).GetEnumerator();
        }

        private void Heapify()
        {
            for (int index = GetParentIndex(Count); index >= 0; --index)
            {
                DownHeap(index);
            }
        }

        private void DownHeap(int itemIndex)
        {
            while (itemIndex < queue.Count)
            {
                int smallestChildIndex = GetSmallestChildIndex(itemIndex);

                if (smallestChildIndex == itemIndex)
                {
                    break;
                }

                T item = queue[itemIndex];

                queue[itemIndex] = queue[smallestChildIndex];
                itemIndex = smallestChildIndex;
                queue[itemIndex] = item;
            }
        }

        private void UpHeap(int itemIndex)
        {
            while (itemIndex > 0)
            {
                int parentIndex = GetParentIndex(itemIndex);
                T parent = queue[parentIndex];

                T item = queue[itemIndex];

                if (Comparer.Compare(item, parent) >= 0)
                {
                    break;
                }

                queue[itemIndex] = parent;
                itemIndex = parentIndex;
                queue[itemIndex] = item;
            }
        }

        private int GetSmallestChildIndex(int parentIndex)
        {
            int leftChildIndex = (parentIndex * 2) + 1;
            int rightChildIndex = leftChildIndex + 1;
            int smallestChildIndex = parentIndex;

            if (leftChildIndex < queue.Count
                && Comparer.Compare(queue[smallestChildIndex], queue[leftChildIndex]) > 0)
            {
                smallestChildIndex = leftChildIndex;
            }

            if (rightChildIndex < queue.Count
                && Comparer.Compare(queue[smallestChildIndex], queue[rightChildIndex]) > 0)
            {
                smallestChildIndex = rightChildIndex;
            }

            return smallestChildIndex;
        }

        private int GetParentIndex(int childIndex)
        {
            return (childIndex - 1) / 2;
        }
    }
}
