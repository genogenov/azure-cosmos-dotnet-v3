// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Json
{
    using System;

    internal abstract class JsonMemoryWriter
    {
        protected byte[] buffer;

        protected JsonMemoryWriter(int initialCapacity = 256)
        {
            buffer = new byte[initialCapacity];
        }

        public int Position { get; set; }

        public Span<byte> Cursor => buffer.AsSpan().Slice(Position);

        public ReadOnlyMemory<byte> BufferAsMemory => buffer.AsMemory();

        public Span<byte> BufferAsSpan => buffer.AsSpan();

        public void Write(ReadOnlySpan<byte> value)
        {
            EnsureRemainingBufferSpace(value.Length);
            value.CopyTo(Cursor);
            Position += value.Length;
        }

        public void EnsureRemainingBufferSpace(int size)
        {
            if (Position + size >= buffer.Length)
            {
                Resize(Position + size);
            }
        }

        private void Resize(int minNewSize)
        {
            if (minNewSize < 0)
            {
                throw new ArgumentOutOfRangeException();
            }

            long newLength = minNewSize * 2;
            newLength = Math.Min(newLength, int.MaxValue);
            Array.Resize(ref buffer, (int)newLength);
        }
    }
}
