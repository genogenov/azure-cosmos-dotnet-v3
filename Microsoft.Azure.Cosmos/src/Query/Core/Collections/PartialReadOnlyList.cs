//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Collections
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    internal sealed class PartialReadOnlyList<T> : IReadOnlyList<T>
    {
        private readonly IReadOnlyList<T> list;
        private readonly int startIndex;

        public PartialReadOnlyList(
            IReadOnlyList<T> list,
            int count)
            : this(list, 0, count)
        {
        }

        public PartialReadOnlyList(
            IReadOnlyList<T> list,
            int startIndex,
            int count)
        {
            if (list == null)
            {
                throw new ArgumentNullException("list");
            }

            if (count <= 0 || count > list.Count)
            {
                throw new ArgumentOutOfRangeException("count");
            }

            if (startIndex < 0 || (startIndex + count) > list.Count)
            {
                throw new ArgumentOutOfRangeException("startIndex");
            }

            this.list = list;
            this.startIndex = startIndex;
            Count = count;
        }

        public T this[int index]
        {
            get
            {
                if (index < 0 || index >= Count)
                {
                    throw new ArgumentOutOfRangeException("index");
                }

                return list[checked(startIndex + index)];
            }
        }

        public int Count { get; }

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; ++i)
            {
                yield return list[i + startIndex];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
