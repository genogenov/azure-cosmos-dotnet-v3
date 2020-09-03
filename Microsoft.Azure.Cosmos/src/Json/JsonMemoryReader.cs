// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Json
{
    using System;
    using System.Runtime.CompilerServices;

    internal abstract class JsonMemoryReader
    {
        protected readonly ReadOnlyMemory<byte> buffer;
        protected int position;

        protected JsonMemoryReader(ReadOnlyMemory<byte> buffer)
        {
            this.buffer = buffer;
        }

        public bool IsEof => position >= buffer.Length;

        public int Position => position;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte Read()
        {
            byte value = position < buffer.Length ? buffer.Span[position] : (byte)0;
            position++;
            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte Peek() => position < buffer.Length ? buffer.Span[position] : (byte)0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyMemory<byte> GetBufferedRawJsonToken() => buffer.Slice(position);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyMemory<byte> GetBufferedRawJsonToken(
            int startPosition) => buffer.Slice(startPosition);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyMemory<byte> GetBufferedRawJsonToken(
            int startPosition,
            int endPosition) => buffer.Slice(startPosition, endPosition - startPosition);
    }
}