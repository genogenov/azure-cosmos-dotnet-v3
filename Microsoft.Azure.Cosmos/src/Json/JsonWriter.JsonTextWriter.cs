//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Json
{
    using System;
    using System.Buffers;
    using System.Buffers.Text;
    using System.Globalization;
    using System.Numerics;
    using System.Runtime.CompilerServices;
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
        /// This class is used to build a JSON string.
        /// It supports our defined IJsonWriter interface.
        /// It keeps an stack to keep track of scope, and provides error checking using that.
        /// It has few other variables for error checking
        /// The user can also provide initial size to reserve string buffer, that will help reduce cost of reallocation.
        /// It provides error checking based on JSON grammar. It provides escaping for nine characters specified in JSON.
        /// </summary>
        private sealed class JsonTextWriter : JsonWriter
        {
            private const byte ValueSeperatorToken = (byte)':';
            private const byte MemberSeperatorToken = (byte)',';
            private const byte ObjectStartToken = (byte)'{';
            private const byte ObjectEndToken = (byte)'}';
            private const byte ArrayStartToken = (byte)'[';
            private const byte ArrayEndToken = (byte)']';
            private const byte PropertyStartToken = (byte)'"';
            private const byte PropertyEndToken = (byte)'"';
            private const byte StringStartToken = (byte)'"';
            private const byte StringEndToken = (byte)'"';

            private const byte Int8TokenPrefix = (byte)'I';
            private const byte Int16TokenPrefix = (byte)'H';
            private const byte Int32TokenPrefix = (byte)'L';
            private const byte UnsignedTokenPrefix = (byte)'U';
            private const byte FloatTokenPrefix = (byte)'S';
            private const byte DoubleTokenPrefix = (byte)'D';
            private const byte GuidTokenPrefix = (byte)'G';
            private const byte BinaryTokenPrefix = (byte)'B';

            private const byte DoubleQuote = (byte)'"';
            private const byte ReverseSolidus = (byte)'\\';
            private const byte Space = (byte)' ';

            private static readonly ReadOnlyMemory<byte> NotANumber = new byte[]
            {
                (byte)'N', (byte)'a', (byte)'N'
            };
            private static readonly ReadOnlyMemory<byte> PositiveInfinity = new byte[]
            {
                (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y'
            };
            private static readonly ReadOnlyMemory<byte> NegativeInfinity = new byte[]
            {
                (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y'
            };
            private static readonly ReadOnlyMemory<byte> TrueString = new byte[]
            {
                (byte)'t', (byte)'r', (byte)'u', (byte)'e'
            };
            private static readonly ReadOnlyMemory<byte> FalseString = new byte[]
            {
                (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e'
            };
            private static readonly ReadOnlyMemory<byte> NullString = new byte[]
            {
                (byte)'n', (byte)'u', (byte)'l', (byte)'l'
            };

            private static readonly Vector<byte> DoubleQuoteVector = new Vector<byte>(DoubleQuote);
            private static readonly Vector<byte> ReverseSolidusVector = new Vector<byte>(ReverseSolidus);
            private static readonly Vector<byte> SpaceVector = new Vector<byte>(Space);

            private readonly JsonTextMemoryWriter jsonTextMemoryWriter;

            /// <summary>
            /// Whether we are writing the first value of an array or object
            /// </summary>
            private bool firstValue;

            /// <summary>
            /// Initializes a new instance of the JsonTextWriter class.
            /// </summary>
            public JsonTextWriter(int initialCapacity = 256)
            {
                firstValue = true;
                jsonTextMemoryWriter = new JsonTextMemoryWriter(initialCapacity);
            }

            /// <inheritdoc />
            public override JsonSerializationFormat SerializationFormat
            {
                get
                {
                    return JsonSerializationFormat.Text;
                }
            }

            /// <inheritdoc />
            public override long CurrentLength
            {
                get
                {
                    return jsonTextMemoryWriter.Position;
                }
            }

            /// <inheritdoc />
            public override void WriteObjectStart()
            {
                JsonObjectState.RegisterToken(JsonTokenType.BeginObject);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(ObjectStartToken);
                firstValue = true;
            }

            /// <inheritdoc />
            public override void WriteObjectEnd()
            {
                JsonObjectState.RegisterToken(JsonTokenType.EndObject);
                jsonTextMemoryWriter.Write(ObjectEndToken);

                // We reset firstValue here because we'll need a separator before the next value
                firstValue = false;
            }

            /// <inheritdoc />
            public override void WriteArrayStart()
            {
                JsonObjectState.RegisterToken(JsonTokenType.BeginArray);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(ArrayStartToken);
                firstValue = true;
            }

            /// <inheritdoc />
            public override void WriteArrayEnd()
            {
                JsonObjectState.RegisterToken(JsonTokenType.EndArray);
                jsonTextMemoryWriter.Write(ArrayEndToken);

                // We reset firstValue here because we'll need a separator before the next value
                firstValue = false;
            }

            /// <inheritdoc />
            public override void WriteFieldName(Utf8Span fieldName)
            {
                JsonObjectState.RegisterToken(JsonTokenType.FieldName);
                PrefixMemberSeparator();

                // no separator after property name
                firstValue = true;

                jsonTextMemoryWriter.Write(PropertyStartToken);

                WriteEscapedString(fieldName);

                jsonTextMemoryWriter.Write(PropertyEndToken);

                jsonTextMemoryWriter.Write(ValueSeperatorToken);
            }

            /// <inheritdoc />
            public override void WriteStringValue(Utf8Span value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.String);
                PrefixMemberSeparator();

                jsonTextMemoryWriter.Write(StringStartToken);

                WriteEscapedString(value);

                jsonTextMemoryWriter.Write(StringEndToken);
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
            }

            /// <inheritdoc />
            public override void WriteBoolValue(bool value)
            {
                JsonObjectState.RegisterToken(value ? JsonTokenType.True : JsonTokenType.False);
                PrefixMemberSeparator();

                if (value)
                {
                    jsonTextMemoryWriter.Write(TrueString.Span);
                }
                else
                {
                    jsonTextMemoryWriter.Write(FalseString.Span);
                }
            }

            /// <inheritdoc />
            public override void WriteNullValue()
            {
                JsonObjectState.RegisterToken(JsonTokenType.Null);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(NullString.Span);
            }

            /// <inheritdoc />
            public override void WriteInt8Value(sbyte value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int8);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(Int8TokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteInt16Value(short value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int16);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(Int16TokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteInt32Value(int value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int32);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(Int32TokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteInt64Value(long value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Int64);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(Int32TokenPrefix);
                jsonTextMemoryWriter.Write(Int32TokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteFloat32Value(float value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Float32);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(FloatTokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteFloat64Value(double value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Float64);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(DoubleTokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteUInt32Value(uint value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.UInt32);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(UnsignedTokenPrefix);
                jsonTextMemoryWriter.Write(Int32TokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteGuidValue(Guid value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Guid);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(GuidTokenPrefix);
                jsonTextMemoryWriter.Write(value);
            }

            /// <inheritdoc />
            public override void WriteBinaryValue(ReadOnlySpan<byte> value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Binary);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(BinaryTokenPrefix);
                jsonTextMemoryWriter.WriteBinaryAsBase64(value);
            }

            /// <inheritdoc />
            public override ReadOnlyMemory<byte> GetResult()
            {
                return jsonTextMemoryWriter.BufferAsMemory.Slice(
                    0,
                    jsonTextMemoryWriter.Position);
            }

            /// <inheritdoc />
            public override void WriteRawJsonToken(
                JsonTokenType jsonTokenType,
                ReadOnlySpan<byte> rawJsonToken)
            {
                switch (jsonTokenType)
                {
                    case JsonTokenType.String:
                    case JsonTokenType.Number:
                    case JsonTokenType.True:
                    case JsonTokenType.False:
                    case JsonTokenType.Null:
                    case JsonTokenType.FieldName:
                    case JsonTokenType.Int8:
                    case JsonTokenType.Int16:
                    case JsonTokenType.Int32:
                    case JsonTokenType.Int64:
                    case JsonTokenType.UInt32:
                    case JsonTokenType.Float32:
                    case JsonTokenType.Float64:
                    case JsonTokenType.Guid:
                    case JsonTokenType.Binary:
                        // Supported Tokens
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($"Unknown token type: {jsonTokenType}.");
                }

                if (rawJsonToken.IsEmpty)
                {
                    throw new ArgumentException($"Expected non empty {nameof(rawJsonToken)}.");
                }

                JsonObjectState.RegisterToken(jsonTokenType);
                PrefixMemberSeparator();

                // No separator after property name
                if (jsonTokenType == JsonTokenType.FieldName)
                {
                    firstValue = true;
                    jsonTextMemoryWriter.Write(rawJsonToken);
                    jsonTextMemoryWriter.Write(ValueSeperatorToken);
                }
                else
                {
                    jsonTextMemoryWriter.Write(rawJsonToken);
                }
            }

            private void WriteIntegerInternal(long value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Number);
                PrefixMemberSeparator();
                jsonTextMemoryWriter.Write(value);
            }

            private void WriteDoubleInternal(double value)
            {
                JsonObjectState.RegisterToken(JsonTokenType.Number);
                PrefixMemberSeparator();
                if (double.IsNaN(value))
                {
                    jsonTextMemoryWriter.Write(StringStartToken);
                    jsonTextMemoryWriter.Write(NotANumber.Span);
                    jsonTextMemoryWriter.Write(StringEndToken);
                }
                else if (double.IsNegativeInfinity(value))
                {
                    jsonTextMemoryWriter.Write(StringStartToken);
                    jsonTextMemoryWriter.Write(NegativeInfinity.Span);
                    jsonTextMemoryWriter.Write(StringEndToken);
                }
                else if (double.IsPositiveInfinity(value))
                {
                    jsonTextMemoryWriter.Write(StringStartToken);
                    jsonTextMemoryWriter.Write(PositiveInfinity.Span);
                    jsonTextMemoryWriter.Write(StringEndToken);
                }
                else
                {
                    jsonTextMemoryWriter.Write(value);
                }
            }

            private void PrefixMemberSeparator()
            {
                if (!firstValue)
                {
                    jsonTextMemoryWriter.Write(MemberSeperatorToken);
                }

                firstValue = false;
            }

            private void WriteEscapedString(Utf8Span unescapedString)
            {
                while (!unescapedString.IsEmpty)
                {
                    int? indexOfFirstCharacterThatNeedsEscaping = JsonTextWriter.IndexOfCharacterThatNeedsEscaping(unescapedString);
                    if (!indexOfFirstCharacterThatNeedsEscaping.HasValue)
                    {
                        // No escaping needed;
                        indexOfFirstCharacterThatNeedsEscaping = unescapedString.Length;
                    }

                    // Write as much of the string as possible
                    jsonTextMemoryWriter.Write(
                        unescapedString.Span.Slice(
                            start: 0,
                            length: indexOfFirstCharacterThatNeedsEscaping.Value));
                    unescapedString = Utf8Span.UnsafeFromUtf8BytesNoValidation(unescapedString.Span.Slice(start: indexOfFirstCharacterThatNeedsEscaping.Value));

                    // Escape the next character if it exists
                    if (!unescapedString.IsEmpty)
                    {
                        byte character = unescapedString.Span[0];
                        unescapedString = Utf8Span.UnsafeFromUtf8BytesNoValidation(unescapedString.Span.Slice(start: 1));

                        switch (character)
                        {
                            case (byte)'\\':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'\\');
                                break;

                            case (byte)'"':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'"');
                                break;

                            case (byte)'/':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'/');
                                break;

                            case (byte)'\b':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'b');
                                break;

                            case (byte)'\f':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'f');
                                break;

                            case (byte)'\n':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'n');
                                break;

                            case (byte)'\r':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'r');
                                break;

                            case (byte)'\t':
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'t');
                                break;

                            default:
                                char wideCharToEscape = (char)character;
                                // We got a control character (U+0000 through U+001F).
                                jsonTextMemoryWriter.Write((byte)'\\');
                                jsonTextMemoryWriter.Write((byte)'u');
                                jsonTextMemoryWriter.Write(GetHexDigit((wideCharToEscape >> 12) & 0xF));
                                jsonTextMemoryWriter.Write(GetHexDigit((wideCharToEscape >> 8) & 0xF));
                                jsonTextMemoryWriter.Write(GetHexDigit((wideCharToEscape >> 4) & 0xF));
                                jsonTextMemoryWriter.Write(GetHexDigit((wideCharToEscape >> 0) & 0xF));
                                break;
                        }
                    }
                }
            }

            private static unsafe int? IndexOfCharacterThatNeedsEscaping(Utf8Span utf8Span)
            {
                int vectorCount = Vector<byte>.Count;
                int index = 0;

                // If we can benefit from SIMD scan, use that approach
                if (Vector.IsHardwareAccelerated)
                {
                    // Ensure we stop the SIMD scan before the length of the vector would
                    // go past the end of the array
#pragma warning disable IDE0047 // Remove unnecessary parentheses
                    int lastVectorMultiple = (utf8Span.Length / vectorCount) * vectorCount;
#pragma warning restore IDE0047 // Remove unnecessary parentheses

                    for (; index < lastVectorMultiple; index += vectorCount)
                    {
                        Vector<byte> vector;
                        unsafe
                        {
                            fixed (byte* spanPtr = utf8Span.Span)
                            {
                                vector = Unsafe.Read<Vector<byte>>(spanPtr + index);
                            }
                        }

                        if (JsonTextWriter.HasCharacterThatNeedsEscaping(vector))
                        {
                            // The Vector contained a character that needed escaping
                            // Loop to find the exact character and index
                            for (; index < utf8Span.Length; ++index)
                            {
                                byte c = utf8Span.Span[index];

                                if (JsonTextWriter.NeedsEscaping(c))
                                {
                                    return index;
                                }
                            }
                        }
                    }
                }

                // Unless the scan ended on a vectorCount multiple,
                // still need to check the last few characters
                for (; index < utf8Span.Length; ++index)
                {
                    byte c = utf8Span.Span[index];

                    if (JsonTextWriter.NeedsEscaping(c))
                    {
                        return index;
                    }
                }

                return null;
            }

            private static bool HasCharacterThatNeedsEscaping(Vector<byte> vector)
            {
                return Vector.EqualsAny(vector, JsonTextWriter.ReverseSolidusVector) ||
                    Vector.EqualsAny(vector, JsonTextWriter.DoubleQuoteVector) ||
                    Vector.LessThanAny(vector, JsonTextWriter.SpaceVector);
            }

            private static bool NeedsEscaping(byte value)
            {
                return (value == ReverseSolidus) || (value == DoubleQuote) || (value < Space);
            }

            private static byte GetHexDigit(int value)
            {
                return (byte)((value < 10) ? '0' + value : 'A' + value - 10);
            }

            private sealed class JsonTextMemoryWriter : JsonMemoryWriter
            {
                private static readonly StandardFormat floatFormat = new StandardFormat(
                    symbol: 'R');

                private static readonly StandardFormat doubleFormat = new StandardFormat(
                    symbol: 'R');

                public JsonTextMemoryWriter(int initialCapacity = 256)
                    : base(initialCapacity)
                {
                }

                public void Write(bool value)
                {
                    const int MaxBoolLength = 5;
                    EnsureRemainingBufferSpace(MaxBoolLength);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(bool).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(byte value)
                {
                    EnsureRemainingBufferSpace(1);
                    buffer[Position] = value;
                    Position++;
                }

                public void Write(sbyte value)
                {
                    const int MaxInt8Length = 4;
                    EnsureRemainingBufferSpace(MaxInt8Length);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(sbyte).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(short value)
                {
                    const int MaxInt16Length = 6;
                    EnsureRemainingBufferSpace(MaxInt16Length);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(short).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(int value)
                {
                    const int MaxInt32Length = 11;
                    EnsureRemainingBufferSpace(MaxInt32Length);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(int).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(uint value)
                {
                    const int MaxInt32Length = 11;
                    EnsureRemainingBufferSpace(MaxInt32Length);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(int).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(long value)
                {
                    const int MaxInt64Length = 20;
                    EnsureRemainingBufferSpace(MaxInt64Length);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(long).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void Write(float value)
                {
                    const int MaxNumberLength = 32;
                    EnsureRemainingBufferSpace(MaxNumberLength);
                    // Can't use Utf8Formatter until we bump to core 3.0, since they don't support float.ToString("G9")
                    // Also for the 2.0 shim they are creating an intermediary string anyways
                    string floatString = value.ToString("R", CultureInfo.InvariantCulture);
                    for (int index = 0; index < floatString.Length; index++)
                    {
                        // we can cast to byte, since it's all ascii
                        buffer[Position] = (byte)floatString[index];
                        Position++;
                    }
                }

                public void Write(double value)
                {
                    const int MaxNumberLength = 32;
                    EnsureRemainingBufferSpace(MaxNumberLength);
                    // Can't use Utf8Formatter until we bump to core 3.0, since they don't support float.ToString("R")
                    // Also for the 2.0 shim they are creating an intermediary string anyways
                    string doubleString = value.ToString("R", CultureInfo.InvariantCulture);
                    for (int index = 0; index < doubleString.Length; index++)
                    {
                        // we can cast to byte, since it's all ascii
                        buffer[Position] = (byte)doubleString[index];
                        Position++;
                    }
                }

                public void Write(Guid value)
                {
                    const int GuidLength = 38;
                    EnsureRemainingBufferSpace(GuidLength);
                    if (!Utf8Formatter.TryFormat(value, Cursor, out int bytesWritten))
                    {
                        throw new InvalidOperationException($"Failed to {nameof(this.Write)}({typeof(double).FullName}{value})");
                    }

                    Position += bytesWritten;
                }

                public void WriteBinaryAsBase64(ReadOnlySpan<byte> binary)
                {
                    EnsureRemainingBufferSpace(Base64.GetMaxEncodedToUtf8Length(binary.Length));
                    Base64.EncodeToUtf8(binary, Cursor, out int bytesConsumed, out int bytesWritten);

                    Position += bytesWritten;
                }
            }
        }
    }
}