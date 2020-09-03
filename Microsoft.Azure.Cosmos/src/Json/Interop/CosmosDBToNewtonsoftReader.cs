//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Json.Interop
{
    using System;
    using Newtonsoft.Json;

    /// <summary>
    /// Wrapper class that implements a Newtonsoft JsonReader,
    /// but forwards all the calls to a CosmosDB JSON reader.
    /// </summary>
#if INTERNAL
    public
#else
    internal
#endif
    sealed class CosmosDBToNewtonsoftReader : Newtonsoft.Json.JsonReader
    {
        /// <summary>
        /// Singleton boxed value for null.
        /// </summary>
        private static readonly object Null = null;

        /// <summary>
        /// Singleton boxed value for false.
        /// </summary>
        private static readonly object False = false;

        /// <summary>
        /// Singleton boxed value for true.
        /// </summary>
        private static readonly object True = true;

        /// <summary>
        /// The CosmosDB JSON Reader that will be used for implementation.
        /// </summary>
        private readonly IJsonReader jsonReader;

        /// <summary>
        /// Initializes a new instance of the NewtonsoftReader class.
        /// </summary>
        /// <param name="jsonReader">The reader to interop with.</param>
        public CosmosDBToNewtonsoftReader(IJsonReader jsonReader)
        {
            this.jsonReader = jsonReader ?? throw new ArgumentNullException(nameof(jsonReader));
        }

        /// <summary>
        /// Reads the next token from the reader.
        /// </summary>
        /// <returns>True if a token was read, else false.</returns>
        public override bool Read()
        {
            bool read = jsonReader.Read();
            if (!read)
            {
                SetToken(JsonToken.None);
                return false;
            }

            JsonTokenType jsonTokenType = jsonReader.CurrentTokenType;
            JsonToken newtonsoftToken;
            object value;
            switch (jsonTokenType)
            {
                case JsonTokenType.BeginArray:
                    newtonsoftToken = JsonToken.StartArray;
                    value = CosmosDBToNewtonsoftReader.Null;
                    break;

                case JsonTokenType.EndArray:
                    newtonsoftToken = JsonToken.EndArray;
                    value = CosmosDBToNewtonsoftReader.Null;
                    break;

                case JsonTokenType.BeginObject:
                    newtonsoftToken = JsonToken.StartObject;
                    value = CosmosDBToNewtonsoftReader.Null;
                    break;

                case JsonTokenType.EndObject:
                    newtonsoftToken = JsonToken.EndObject;
                    value = CosmosDBToNewtonsoftReader.Null;
                    break;

                case JsonTokenType.String:
                    newtonsoftToken = JsonToken.String;
                    value = jsonReader.GetStringValue();
                    break;

                case JsonTokenType.Number:
                    Number64 number64Value = jsonReader.GetNumberValue();
                    if (number64Value.IsInteger)
                    {
                        value = Number64.ToLong(number64Value);
                        newtonsoftToken = JsonToken.Integer;
                    }
                    else
                    {
                        value = Number64.ToDouble(number64Value);
                        newtonsoftToken = JsonToken.Float;
                    }
                    break;

                case JsonTokenType.True:
                    newtonsoftToken = JsonToken.Boolean;
                    value = CosmosDBToNewtonsoftReader.True;
                    break;

                case JsonTokenType.False:
                    newtonsoftToken = JsonToken.Boolean;
                    value = CosmosDBToNewtonsoftReader.False;
                    break;

                case JsonTokenType.Null:
                    newtonsoftToken = JsonToken.Null;
                    value = CosmosDBToNewtonsoftReader.Null;
                    break;

                case JsonTokenType.FieldName:
                    newtonsoftToken = JsonToken.PropertyName;
                    value = jsonReader.GetStringValue();
                    break;

                case JsonTokenType.Int8:
                    newtonsoftToken = JsonToken.Integer;
                    value = jsonReader.GetInt8Value();
                    break;

                case JsonTokenType.Int16:
                    newtonsoftToken = JsonToken.Integer;
                    value = jsonReader.GetInt16Value();
                    break;

                case JsonTokenType.Int32:
                    newtonsoftToken = JsonToken.Integer;
                    value = jsonReader.GetInt32Value();
                    break;

                case JsonTokenType.Int64:
                    newtonsoftToken = JsonToken.Integer;
                    value = jsonReader.GetInt64Value();
                    break;

                case JsonTokenType.UInt32:
                    newtonsoftToken = JsonToken.Integer;
                    value = jsonReader.GetUInt32Value();
                    break;

                case JsonTokenType.Float32:
                    newtonsoftToken = JsonToken.Float;
                    value = jsonReader.GetFloat32Value();
                    break;

                case JsonTokenType.Float64:
                    newtonsoftToken = JsonToken.Float;
                    value = jsonReader.GetFloat64Value();
                    break;

                case JsonTokenType.Guid:
                    newtonsoftToken = JsonToken.String;
                    value = jsonReader.GetGuidValue().ToString();
                    break;

                case JsonTokenType.Binary:
                    newtonsoftToken = JsonToken.Bytes;
                    value = jsonReader.GetBinaryValue().ToArray();
                    break;

                default:
                    throw new ArgumentException($"Unexpected jsonTokenType: {jsonTokenType}");
            }

            SetToken(newtonsoftToken, value);
            return read;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="byte"/>[].
        /// </summary>
        /// <returns>A <see cref="byte"/>[] or <c>null</c> if the next JSON token is null. This method will return <c>null</c> at the end of an array.</returns>
        public override byte[] ReadAsBytes()
        {
            Read();
            if (!jsonReader.TryGetBufferedRawJsonToken(out ReadOnlyMemory<byte> bufferedRawJsonToken))
            {
                throw new Exception("Failed to get the bytes.");
            }

            byte[] value = bufferedRawJsonToken.ToArray();
            SetToken(JsonToken.Bytes, value);
            return value;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="Nullable{T}"/> of <see cref="DateTime"/>.
        /// </summary>
        /// <returns>A <see cref="Nullable{T}"/> of <see cref="DateTime"/>. This method will return <c>null</c> at the end of an array.</returns>
        public override DateTime? ReadAsDateTime()
        {
            Read();
            if (jsonReader.CurrentTokenType == JsonTokenType.EndArray)
            {
                return null;
            }

            string stringValue = jsonReader.GetStringValue();
            DateTime dateTime = DateTime.Parse(stringValue);
            SetToken(JsonToken.Date, dateTime);

            return dateTime;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="Nullable{T}"/> of <see cref="DateTimeOffset"/>.
        /// </summary>
        /// <returns>A <see cref="Nullable{T}"/> of <see cref="DateTimeOffset"/>. This method will return <c>null</c> at the end of an array.</returns>
        public override DateTimeOffset? ReadAsDateTimeOffset()
        {
            Read();
            if (jsonReader.CurrentTokenType == JsonTokenType.EndArray)
            {
                return null;
            }

            string stringValue = jsonReader.GetStringValue();
            DateTimeOffset dateTimeOffset = DateTimeOffset.Parse(stringValue);
            SetToken(JsonToken.Date, dateTimeOffset);

            return dateTimeOffset;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="Nullable{T}"/> of <see cref="decimal"/>.
        /// </summary>
        /// <returns>A <see cref="Nullable{T}"/> of <see cref="decimal"/>. This method will return <c>null</c> at the end of an array.</returns>
        public override decimal? ReadAsDecimal()
        {
            decimal? value = (decimal?)ReadNumberValue();
            if (value != null)
            {
                SetToken(JsonToken.Float, value);
            }

            return value;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="Nullable{T}"/> of <see cref="int"/>.
        /// </summary>
        /// <returns>A <see cref="Nullable{T}"/> of <see cref="int"/>. This method will return <c>null</c> at the end of an array.</returns>
        public override int? ReadAsInt32()
        {
            int? value = (int?)ReadNumberValue();
            if (value != null)
            {
                SetToken(JsonToken.Integer, value);
            }

            return value;
        }

        /// <summary>
        /// Reads the next JSON token from the source as a <see cref="string"/>.
        /// </summary>
        /// <returns>A <see cref="string"/>. This method will return <c>null</c> at the end of an array.</returns>
        public override string ReadAsString()
        {
            Read();
            if (jsonReader.CurrentTokenType == JsonTokenType.EndArray)
            {
                return null;
            }

            string stringValue = jsonReader.GetStringValue();
            SetToken(JsonToken.String, stringValue);

            return stringValue;
        }

        /// <summary>
        /// Reads the next number token but returns null at the end of an array.
        /// </summary>
        /// <returns>The next number token but returns null at the end of an array.</returns>
        private double? ReadNumberValue()
        {
            Read();
            if (jsonReader.CurrentTokenType == JsonTokenType.EndArray)
            {
                return null;
            }

            Number64 value = jsonReader.GetNumberValue();
            double doubleValue = Number64.ToDouble(value);
            return doubleValue;
        }
    }
}
