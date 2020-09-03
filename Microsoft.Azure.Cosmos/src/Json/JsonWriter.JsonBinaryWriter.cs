//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Json
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using Microsoft.Azure.Cosmos.Core.Utf8;

    /// <summary>
    /// Partial class for the JsonWriter that has a private JsonTextWriter below.
    /// </summary>
#if INTERNAL
    public
#else
    internal
#endif
    abstract partial class JsonWriter : IJsonWriter
    {
        /// <summary>
        /// Concrete implementation of <see cref="JsonWriter"/> that knows how to serialize to binary encoding.
        /// </summary>
        private sealed class JsonBinaryWriter : JsonWriter
        {
            private const int MaxStackAllocSize = 4 * 1024;

            /// <summary>
            /// Writer used to write fully materialized context to the internal stream.
            /// </summary>
            private readonly JsonBinaryMemoryWriter binaryWriter;

            /// <summary>
            /// With binary encoding all the json elements are length prefixed,
            /// unfortunately the caller of this class only provides what tokens to write.
            /// This means that whenever a user call WriteObject/ArrayStart we don't know the length of said object or array
            /// until WriteObject/ArrayEnd is invoked.
            /// To get around this we reserve some space for the length and write to it when the user supplies the end token.
            /// This stack remembers for each nesting level where it begins and how many items it has.
            /// </summary>
            private readonly Stack<BeginOffsetAndCount> bufferedContexts;

            /// <summary>
            /// With binary encoding json elements like arrays and object are prefixed with a length in bytes and optionally a count.
            /// This flag just determines whether you want to serialize the count, since it's optional and up to the user to make the
            /// tradeoff between O(1) .Count() operation as the cost of additional storage.
            /// </summary>
            private readonly bool serializeCount;

            /// <summary>
            /// When a user writes an open array or object we reserve this much space for the type marker + length + count
            /// And correct it later when they write a close array or object.
            /// </summary>
            private readonly int reservationSize;

            /// <summary>
            /// The string dictionary used for user string encoding.
            /// </summary>
            private readonly JsonStringDictionary jsonStringDictionary;

            /// <summary>
            /// Initializes a new instance of the JsonBinaryWriter class.
            /// </summary>
            /// <param name="jsonStringDictionary">The JSON string dictionary used for user string encoding.</param>
            /// <param name="initialCapacity">The initial capacity to avoid intermediary allocations.</param>
            /// <param name="serializeCount">Whether to serialize the count for object and array typemarkers.</param>
            public JsonBinaryWriter(
                JsonStringDictionary jsonStringDictionary = null,
                int initialCapacity = 256,
                bool serializeCount = false)
            {
                binaryWriter = new JsonBinaryMemoryWriter(initialCapacity);
                bufferedContexts = new Stack<BeginOffsetAndCount>();
                this.serializeCount = serializeCount;
                reservationSize = JsonBinaryEncoding.TypeMarkerLength + JsonBinaryEncoding.OneByteLength + (this.serializeCount ? JsonBinaryEncoding.OneByteCount : 0);

                // Write the serialization format as the very first byte
                byte binaryTypeMarker = (byte)JsonSerializationFormat.Binary;
                binaryWriter.Write(binaryTypeMarker);

                // Push on the outermost context
                bufferedContexts.Push(new BeginOffsetAndCount(CurrentLength));
                this.jsonStringDictionary = jsonStringDictionary;
            }

            /// <inheritdoc />
            public override JsonSerializationFormat SerializationFormat
            {
                get
                {
                    return JsonSerializationFormat.Binary;
                }
            }

            /// <inheritdoc />
            public override long CurrentLength
            {
                get
                {
                    return binaryWriter.Position;
                }
            }

            /// <inheritdoc />
            public override void WriteObjectStart()
            {
                WriterArrayOrObjectStart(isArray: false);
            }

            /// <inheritdoc />
            public override void WriteObjectEnd()
            {
                WriteArrayOrObjectEnd(isArray: false);
            }

            /// <inheritdoc />
            public override void WriteArrayStart()
            {
                WriterArrayOrObjectStart(isArray: true);
            }

            /// <inheritdoc />
            public override void WriteArrayEnd()
            {
                WriteArrayOrObjectEnd(isArray: true);
            }

            /// <inheritdoc />
            public override void WriteFieldName(Utf8Span fieldName)
            {
                WriteFieldNameOrString(isFieldName: true, fieldName);
            }

            /// <inheritdoc />
            public override void WriteStringValue(Utf8Span value)
            {
                WriteFieldNameOrString(isFieldName: false, value);
            }

            /// <inheritdoc />
            public override void WriteNumber64Value(Number64 value)
            {
                if (value.IsInteger)
                {
                    WriteIntegerInternal(Number64.ToLong(value));
                }
                else
                {
                    WriteDoubleInternal(Number64.ToDouble(value));
                }

                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteBoolValue(bool value)
            {
                JsonObjectState.RegisterToken(value ? JsonTokenType.True : JsonTokenType.False);
                binaryWriter.Write(value ? JsonBinaryEncoding.TypeMarker.True : JsonBinaryEncoding.TypeMarker.False);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteNullValue()
            {
                JsonObjectState.RegisterToken(JsonTokenType.Null);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Null);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteInt8Value(sbyte value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int8);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Int8);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteInt16Value(short value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int16);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Int16);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteInt32Value(int value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int32);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Int32);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteInt64Value(long value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int64);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Int64);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteFloat32Value(float value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Float32);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Float32);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteFloat64Value(double value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Float64);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Float64);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteUInt32Value(uint value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.UInt32);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.UInt32);
                binaryWriter.Write(value);
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteGuidValue(Guid value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Guid);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Guid);
                binaryWriter.Write(value.ToByteArray());
                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override void WriteBinaryValue(ReadOnlySpan<byte> value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Binary);

                long length = value.Length;
                if ((length & ~0xFF) == 0)
                {
                    binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Binary1ByteLength);
                    binaryWriter.Write((byte)length);
                }
                else if ((length & ~0xFFFF) == 0)
                {
                    binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Binary2ByteLength);
                    binaryWriter.Write((ushort)length);
                }
                else if ((length & ~0xFFFFFFFFL) == 0)
                {
                    binaryWriter.Write(JsonBinaryEncoding.TypeMarker.Binary4ByteLength);
                    binaryWriter.Write((ulong)length);
                }
                else
                {
                    throw new ArgumentOutOfRangeException("Binary length was too large.");
                }

                binaryWriter.Write(value);

                bufferedContexts.Peek().Count++;
            }

            /// <inheritdoc />
            public override ReadOnlyMemory<byte> GetResult()
            {
                if (bufferedContexts.Count > 1)
                {
                    throw new JsonNotCompleteException();
                }

                if (binaryWriter.Position == 1)
                {
                    // We haven't written anything but the type marker, so just return an empty buffer.
                    return ReadOnlyMemory<byte>.Empty;
                }

                return binaryWriter.BufferAsMemory.Slice(
                    0,
                    binaryWriter.Position);
            }

            /// <inheritdoc />
            public override void WriteRawJsonToken(
                JsonTokenType jsonTokenType,
                ReadOnlySpan<byte> rawJsonToken)
            {
                if (rawJsonToken == null)
                {
                    throw new ArgumentNullException(nameof(rawJsonToken));
                }

                switch (jsonTokenType)
                {
                    // Supported JsonTokenTypes
                    case JsonTokenType.String:
                    case JsonTokenType.Number:
                    case JsonTokenType.True:
                    case JsonTokenType.False:
                    case JsonTokenType.Null:
                    case JsonTokenType.FieldName:
                    case JsonTokenType.Int8:
                    case JsonTokenType.Int16:
                    case JsonTokenType.Int32:
                    case JsonTokenType.UInt32:
                    case JsonTokenType.Int64:
                    case JsonTokenType.Float32:
                    case JsonTokenType.Float64:
                    case JsonTokenType.Guid:
                    case JsonTokenType.Binary:
                        break;
                    default:
                        throw new ArgumentException($"{nameof(JsonBinaryWriter)}.{nameof(WriteRawJsonToken)} can not write a {nameof(JsonTokenType)}: {jsonTokenType}");
                }

                JsonObjectState.RegisterToken(jsonTokenType);
                binaryWriter.Write(rawJsonToken);
                bufferedContexts.Peek().Count++;
            }

            private void WriterArrayOrObjectStart(bool isArray)
            {
                JsonObjectState.RegisterToken(isArray ? JsonTokenType.BeginArray : JsonTokenType.BeginObject);

                // Save the start index
                bufferedContexts.Push(new BeginOffsetAndCount(CurrentLength));

                // Assume 1-byte value length; as such, we need to reserve up 3 bytes (1 byte type marker, 1 byte length, 1 byte count).
                // We'll adjust this as needed when writing the end of the array/object.
                binaryWriter.Write((byte)0);
                binaryWriter.Write((byte)0);
                if (serializeCount)
                {
                    binaryWriter.Write((byte)0);
                }
            }

            private void WriteArrayOrObjectEnd(bool isArray)
            {
                JsonObjectState.RegisterToken(isArray ? JsonTokenType.EndArray : JsonTokenType.EndObject);
                BeginOffsetAndCount nestedContext = bufferedContexts.Pop();

                // Do some math
                int typeMarkerIndex = (int)nestedContext.Offset;
                int payloadIndex = typeMarkerIndex + reservationSize;
                int originalCursor = (int)CurrentLength;
                int payloadLength = originalCursor - payloadIndex;
                int count = (int)nestedContext.Count;

                // Figure out what the typemarker and length should be and do any corrections needed
                if (count == 0)
                {
                    // Empty object

                    // Move the cursor back
                    binaryWriter.Position = typeMarkerIndex;

                    // Write the type marker
                    binaryWriter.Write(
                        isArray ? JsonBinaryEncoding.TypeMarker.EmptyArray : JsonBinaryEncoding.TypeMarker.EmptyObject);
                }
                else if (count == 1)
                {
                    // Single-property object

                    // Move the buffer back but leave one byte for the typemarker
                    Span<byte> buffer = binaryWriter.BufferAsSpan;
                    buffer.Slice(payloadIndex).CopyTo(buffer.Slice(typeMarkerIndex + JsonBinaryEncoding.TypeMarkerLength));

                    // Move the cursor back
                    binaryWriter.Position = typeMarkerIndex;

                    // Write the type marker
                    binaryWriter.Write(
                        isArray ? JsonBinaryEncoding.TypeMarker.SingleItemArray : JsonBinaryEncoding.TypeMarker.SinglePropertyObject);

                    // Move the cursor forward
                    binaryWriter.Position = typeMarkerIndex + JsonBinaryEncoding.TypeMarkerLength + payloadLength;
                }
                else
                {
                    // Need to figure out how many bytes to encode the length and the count
                    if (payloadLength <= byte.MaxValue)
                    {
                        // 1 byte length - don't need to move the buffer
                        int bytesToWrite = JsonBinaryEncoding.TypeMarkerLength
                            + JsonBinaryEncoding.OneByteLength
                            + (serializeCount ? JsonBinaryEncoding.OneByteCount : 0);

                        // Move the cursor back
                        binaryWriter.Position = typeMarkerIndex;

                        // Write the type marker
                        if (serializeCount)
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array1ByteLengthAndCount : JsonBinaryEncoding.TypeMarker.Object1ByteLengthAndCount);
                            binaryWriter.Write((byte)payloadLength);
                            binaryWriter.Write((byte)count);
                        }
                        else
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array1ByteLength : JsonBinaryEncoding.TypeMarker.Object1ByteLength);
                            binaryWriter.Write((byte)payloadLength);
                        }

                        // Move the cursor forward
                        binaryWriter.Position = typeMarkerIndex + bytesToWrite + payloadLength;
                    }
                    else if (payloadLength <= ushort.MaxValue)
                    {
                        // 2 byte length - make space for the extra byte length (and extra byte count)
                        binaryWriter.Write((byte)0);
                        if (serializeCount)
                        {
                            binaryWriter.Write((byte)0);
                        }

                        // Move the buffer forward
                        Span<byte> buffer = binaryWriter.BufferAsSpan;
                        int bytesToWrite = JsonBinaryEncoding.TypeMarkerLength
                            + JsonBinaryEncoding.TwoByteLength
                            + (serializeCount ? JsonBinaryEncoding.TwoByteCount : 0);
                        Span<byte> payload = buffer.Slice(payloadIndex, payloadLength);
                        Span<byte> newPayloadStart = buffer.Slice(typeMarkerIndex + bytesToWrite);
                        payload.CopyTo(newPayloadStart);

                        // Move the cursor back
                        binaryWriter.Position = typeMarkerIndex;

                        // Write the type marker
                        if (serializeCount)
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array2ByteLengthAndCount : JsonBinaryEncoding.TypeMarker.Object2ByteLengthAndCount);
                            binaryWriter.Write((ushort)payloadLength);
                            binaryWriter.Write((ushort)count);
                        }
                        else
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array2ByteLength : JsonBinaryEncoding.TypeMarker.Object2ByteLength);
                            binaryWriter.Write((ushort)payloadLength);
                        }

                        // Move the cursor forward
                        binaryWriter.Position = typeMarkerIndex + bytesToWrite + payloadLength;
                    }
                    else
                    {
                        // (payloadLength <= uint.MaxValue)

                        // 4 byte length - make space for an extra 2 byte length (and 2 byte count)
                        binaryWriter.Write((ushort)0);
                        if (serializeCount)
                        {
                            binaryWriter.Write((ushort)0);
                        }

                        // Move the buffer forward
                        Span<byte> buffer = binaryWriter.BufferAsSpan;
                        int bytesToWrite = JsonBinaryEncoding.TypeMarkerLength
                            + JsonBinaryEncoding.FourByteLength
                            + (serializeCount ? JsonBinaryEncoding.FourByteCount : 0);
                        Span<byte> payload = buffer.Slice(payloadIndex, payloadLength);
                        Span<byte> newPayloadStart = buffer.Slice(typeMarkerIndex + bytesToWrite);
                        payload.CopyTo(newPayloadStart);

                        // Move the cursor back
                        binaryWriter.Position = typeMarkerIndex;

                        // Write the type marker
                        if (serializeCount)
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array4ByteLengthAndCount : JsonBinaryEncoding.TypeMarker.Object4ByteLengthAndCount);
                            binaryWriter.Write((uint)payloadLength);
                            binaryWriter.Write((uint)count);
                        }
                        else
                        {
                            binaryWriter.Write(
                                isArray ? JsonBinaryEncoding.TypeMarker.Array4ByteLength : JsonBinaryEncoding.TypeMarker.Object4ByteLength);
                            binaryWriter.Write((uint)payloadLength);
                        }

                        // Move the cursor forward
                        binaryWriter.Position = typeMarkerIndex + bytesToWrite + payloadLength;
                    }
                }

                bufferedContexts.Peek().Count++;
            }

            private void WriteFieldNameOrString(bool isFieldName, Utf8Span utf8Span)
            {
                // String dictionary encoding is currently performed only for field names. 
                // This would be changed later, so that the writer can control which strings need to be encoded.
                JsonObjectState.RegisterToken(isFieldName ? JsonTokenType.FieldName : JsonTokenType.String);
                if (JsonBinaryEncoding.TryGetEncodedStringTypeMarker(
                    utf8Span,
                    JsonObjectState.CurrentTokenType == JsonTokenType.FieldName ? jsonStringDictionary : null,
                    out JsonBinaryEncoding.MultiByteTypeMarker multiByteTypeMarker))
                {
                    switch (multiByteTypeMarker.Length)
                    {
                        case 1:
                            binaryWriter.Write(multiByteTypeMarker.One);
                            break;

                        case 2:
                            binaryWriter.Write(multiByteTypeMarker.One);
                            binaryWriter.Write(multiByteTypeMarker.Two);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException($"Unable to serialize a {nameof(JsonBinaryEncoding.MultiByteTypeMarker)} of length: {multiByteTypeMarker.Length}");
                    }
                }
                else
                {
                    // See if the string length can be encoded into a single type marker
                    byte typeMarker = JsonBinaryEncoding.TypeMarker.GetEncodedStringLengthTypeMarker(utf8Span.Length);
                    if (JsonBinaryEncoding.TypeMarker.IsValid(typeMarker))
                    {
                        binaryWriter.Write(typeMarker);
                    }
                    else
                    {
                        // Just write the type marker and the corresponding length
                        if (utf8Span.Length < byte.MaxValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.String1ByteLength);
                            binaryWriter.Write((byte)utf8Span.Length);
                        }
                        else if (utf8Span.Length < ushort.MaxValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.String2ByteLength);
                            binaryWriter.Write((ushort)utf8Span.Length);
                        }
                        else
                        {
                            // (utf8String.Length < uint.MaxValue)
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.String4ByteLength);
                            binaryWriter.Write((uint)utf8Span.Length);
                        }
                    }

                    // Finally write the string itself.
                    binaryWriter.Write(utf8Span.Span);
                }

                if (!isFieldName)
                {
                    // If we just wrote a string then increment the count (we don't increment for field names, since we need to wait for the corresponding property value).
                    bufferedContexts.Peek().Count++;
                }
            }

            private void WriteIntegerInternal(long value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Number);
                if (JsonBinaryEncoding.TypeMarker.IsEncodedNumberLiteral(value))
                {
                    binaryWriter.Write((byte)(JsonBinaryEncoding.TypeMarker.LiteralIntMin + value));
                }
                else
                {
                    if (value >= 0)
                    {
                        // Non-negative Number
                        if (value <= byte.MaxValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberUInt8);
                            binaryWriter.Write((byte)value);
                        }
                        else if (value <= short.MaxValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt16);
                            binaryWriter.Write((short)value);
                        }
                        else if (value <= int.MaxValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt32);
                            binaryWriter.Write((int)value);
                        }
                        else
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt64);
                            binaryWriter.Write(value);
                        }
                    }
                    else
                    {
                        // Negative Number
                        if (value < int.MinValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt64);
                            binaryWriter.Write(value);
                        }
                        else if (value < short.MinValue)
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt32);
                            binaryWriter.Write((int)value);
                        }
                        else
                        {
                            binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberInt16);
                            binaryWriter.Write((short)value);
                        }
                    }
                }
            }

            private void WriteDoubleInternal(double value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Number);
                binaryWriter.Write(JsonBinaryEncoding.TypeMarker.NumberDouble);
                binaryWriter.Write(value);
            }

            private sealed class BeginOffsetAndCount
            {
                public BeginOffsetAndCount(long offset)
                {
                    Offset = offset;
                    Count = 0;
                }

                public long Offset { get; }

                public long Count { get; set; }
            }

            private sealed class JsonBinaryMemoryWriter : JsonMemoryWriter
            {
                public JsonBinaryMemoryWriter(int initialCapacity = 256)
                    : base(initialCapacity)
                {
                }

                public void Write(byte value)
                {
                    EnsureRemainingBufferSpace(sizeof(byte));
                    buffer[Position] = value;
                    Position++;
                }

                public void Write(sbyte value)
                {
                    Write((byte)value);
                }

                public void Write(short value)
                {
                    EnsureRemainingBufferSpace(sizeof(short));
                    BinaryPrimitives.WriteInt16LittleEndian(Cursor, value);
                    Position += sizeof(short);
                }

                public void Write(ushort value)
                {
                    EnsureRemainingBufferSpace(sizeof(ushort));
                    BinaryPrimitives.WriteUInt16LittleEndian(Cursor, value);
                    Position += sizeof(ushort);
                }

                public void Write(int value)
                {
                    EnsureRemainingBufferSpace(sizeof(int));
                    BinaryPrimitives.WriteInt32LittleEndian(Cursor, value);
                    Position += sizeof(int);
                }

                public void Write(uint value)
                {
                    EnsureRemainingBufferSpace(sizeof(uint));
                    BinaryPrimitives.WriteUInt32LittleEndian(Cursor, value);
                    Position += sizeof(uint);
                }

                public void Write(long value)
                {
                    EnsureRemainingBufferSpace(sizeof(long));
                    BinaryPrimitives.WriteInt64LittleEndian(Cursor, value);
                    Position += sizeof(long);
                }

                public void Write(float value)
                {
                    EnsureRemainingBufferSpace(sizeof(float));
                    MemoryMarshal.Write<float>(Cursor, ref value);
                    Position += sizeof(float);
                }

                public void Write(double value)
                {
                    EnsureRemainingBufferSpace(sizeof(double));
                    MemoryMarshal.Write<double>(Cursor, ref value);
                    Position += sizeof(double);
                }

                public void Write(Guid value)
                {
                    int sizeOfGuid = Marshal.SizeOf(Guid.Empty);
                    EnsureRemainingBufferSpace(sizeOfGuid);
                    MemoryMarshal.Write<Guid>(Cursor, ref value);
                    Position += sizeOfGuid;
                }
            }
        }
    }
}