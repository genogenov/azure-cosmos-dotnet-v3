//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Json
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using RMResources = Documents.RMResources;

    /// <summary>
    /// This class maintains the current state of a JSON object/value while it is being read or written.
    /// </summary>
#if INTERNAL
    public
#else
    internal
#endif
    sealed class JsonObjectState
    {
        /// <summary>
        /// This constant defines the maximum nesting depth that the parser supports.
        /// The JSON spec states that this is an implementation dependent thing, so we're just picking a value for now.
        /// FWIW .Net chose 100
        /// Note: This value needs to be a multiple of 8 and must be less than 2^15 (see asserts in the constructor)
        /// </summary>
        private const int JsonMaxNestingDepth = 256;

        /// <summary>
        /// Flag for determining whether to throw exceptions that connote a context at the end or not started / complete.
        /// </summary>
        private readonly bool readMode;

        /// <summary>
        /// Stores a bitmap for whether we are in an array or object context at a particular level (0 => array, 1 => object).
        /// </summary>
        private readonly byte[] nestingStackBitmap;

        /// <summary>
        /// The current nesting stack index.
        /// </summary>
        private int nestingStackIndex;

        /// <summary>
        /// The current JsonObjectContext.
        /// </summary>
        private JsonObjectContext currentContext;

        /// <summary>
        /// Initializes a new instance of the JsonObjectState class.
        /// </summary>
        /// <param name="readMode">Flag for determining whether to throw exceptions that correspond to a JsonReader or JsonWriter.</param>
        public JsonObjectState(bool readMode)
        {
            Debug.Assert(JsonMaxNestingDepth % 8 == 0, "JsonMaxNestingDepth must be multiple of 8");
            Debug.Assert(JsonMaxNestingDepth < (1 << 15), "JsonMaxNestingDepth must be less than 2^15");

            this.readMode = readMode;
            nestingStackBitmap = new byte[JsonMaxNestingDepth / 8];
            nestingStackIndex = -1;
            CurrentTokenType = JsonTokenType.NotStarted;
            currentContext = JsonObjectContext.None;
        }

        /// <summary>
        /// JsonObjectContext enum
        /// </summary>
        private enum JsonObjectContext
        {
            /// <summary>
            /// Context at the start of the object state.
            /// </summary>
            None,

            /// <summary>
            /// Context when state is in an array.
            /// </summary>
            Array,

            /// <summary>
            /// Context when state is in an object.
            /// </summary>
            Object,
        }

        /// <summary>
        /// Gets the current depth (level of nesting).
        /// </summary>
        public int CurrentDepth
        {
            get
            {
                return nestingStackIndex + 1;
            }
        }

        /// <summary>
        /// Gets the current JsonTokenType.
        /// </summary>
        public JsonTokenType CurrentTokenType { get; private set; }

        /// <summary>
        /// Gets a value indicating whether a property is expected.
        /// </summary>
        public bool IsPropertyExpected
        {
            get
            {
                return (CurrentTokenType != JsonTokenType.FieldName) && (currentContext == JsonObjectContext.Object);
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current context is an array.
        /// </summary>
        public bool InArrayContext
        {
            get
            {
                return currentContext == JsonObjectContext.Array;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current context in an object.
        /// </summary>
        public bool InObjectContext
        {
            get
            {
                return currentContext == JsonObjectContext.Object;
            }
        }

        /// <summary>
        /// Gets the current JsonObjectContext
        /// </summary>
        private JsonObjectContext RetrieveCurrentContext
        {
            get
            {
                if (nestingStackIndex < 0)
                {
                    return JsonObjectContext.None;
                }

                return (nestingStackBitmap[nestingStackIndex / 8] & Mask) == 0 ? JsonObjectContext.Array : JsonObjectContext.Object;
            }
        }

        /// <summary>
        /// Gets a mask to use to get the current context from the nesting stack
        /// </summary>
        private byte Mask
        {
            get
            {
                return (byte)(1 << (nestingStackIndex % 8));
            }
        }

        /// <summary>
        /// Registers a JsonTokenType.
        /// </summary>
        /// <param name="jsonTokenType">The JsonTokenType to register.</param>
        public void RegisterToken(JsonTokenType jsonTokenType)
        {
            switch (jsonTokenType)
            {
                case JsonTokenType.String:
                case JsonTokenType.Number:
                case JsonTokenType.True:
                case JsonTokenType.False:
                case JsonTokenType.Null:
                case JsonTokenType.Float32:
                case JsonTokenType.Float64:
                case JsonTokenType.Int8:
                case JsonTokenType.Int16:
                case JsonTokenType.Int32:
                case JsonTokenType.Int64:
                case JsonTokenType.UInt32:
                case JsonTokenType.Binary:
                case JsonTokenType.Guid:
                    RegisterValue(jsonTokenType);
                    break;
                case JsonTokenType.BeginArray:
                    RegisterBeginArray();
                    break;
                case JsonTokenType.EndArray:
                    RegisterEndArray();
                    break;
                case JsonTokenType.BeginObject:
                    RegisterBeginObject();
                    break;
                case JsonTokenType.EndObject:
                    RegisterEndObject();
                    break;
                case JsonTokenType.FieldName:
                    RegisterFieldName();
                    break;
                default:
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, RMResources.UnexpectedJsonTokenType, jsonTokenType));
            }
        }

        /// <summary>
        /// Pushes a JsonObjectContext onto the nesting stack.
        /// </summary>
        /// <param name="isArray">Whether the JsonObjectContext is an array.</param>
        private void Push(bool isArray)
        {
            if (nestingStackIndex + 1 >= JsonMaxNestingDepth)
            {
                throw new InvalidOperationException(RMResources.JsonMaxNestingExceeded);
            }

            nestingStackIndex++;

            if (isArray)
            {
                nestingStackBitmap[nestingStackIndex / 8] &= (byte)~Mask;
                currentContext = JsonObjectContext.Array;
            }
            else
            {
                nestingStackBitmap[nestingStackIndex / 8] |= Mask;
                currentContext = JsonObjectContext.Object;
            }
        }

        /// <summary>
        /// Registers any json token type.
        /// </summary>
        /// <param name="jsonTokenType">The jsonTokenType to register</param>
        private void RegisterValue(JsonTokenType jsonTokenType)
        {
            if ((currentContext == JsonObjectContext.Object) && (CurrentTokenType != JsonTokenType.FieldName))
            {
                throw new JsonMissingPropertyException();
            }

            if ((currentContext == JsonObjectContext.None) && (CurrentTokenType != JsonTokenType.NotStarted))
            {
                throw new JsonPropertyArrayOrObjectNotStartedException();
            }

            CurrentTokenType = jsonTokenType;
        }

        /// <summary>
        /// Registers a beginning of a json array ('[')
        /// </summary>
        private void RegisterBeginArray()
        {
            // An array start is also a value
            RegisterValue(JsonTokenType.BeginArray);
            Push(true);
        }

        /// <summary>
        /// Registers the end of a json array (']')
        /// </summary>
        private void RegisterEndArray()
        {
            if (currentContext != JsonObjectContext.Array)
            {
                if (readMode)
                {
                    throw new JsonUnexpectedEndArrayException();
                }
                else
                {
                    throw new JsonArrayNotStartedException();
                }
            }

            nestingStackIndex--;
            CurrentTokenType = JsonTokenType.EndArray;
            currentContext = RetrieveCurrentContext;
        }

        /// <summary>
        /// Registers a beginning of a json object ('{')
        /// </summary>
        private void RegisterBeginObject()
        {
            // An object start is also a value
            RegisterValue(JsonTokenType.BeginObject);
            Push(false);
        }

        /// <summary>
        /// Registers a end of a json object ('}')
        /// </summary>
        private void RegisterEndObject()
        {
            if (currentContext != JsonObjectContext.Object)
            {
                if (readMode)
                {
                    throw new JsonUnexpectedEndObjectException();
                }
                else
                {
                    throw new JsonObjectNotStartedException();
                }
            }

            // check if we have a property name but not a value
            if (CurrentTokenType == JsonTokenType.FieldName)
            {
                if (readMode)
                {
                    throw new JsonUnexpectedEndObjectException();
                }
                else
                {
                    throw new JsonNotCompleteException();
                }
            }

            nestingStackIndex--;
            CurrentTokenType = JsonTokenType.EndObject;
            currentContext = RetrieveCurrentContext;
        }

        /// <summary>
        /// Register a Json FieldName
        /// </summary>
        private void RegisterFieldName()
        {
            if (currentContext != JsonObjectContext.Object)
            {
                throw new JsonObjectNotStartedException();
            }

            if (CurrentTokenType == JsonTokenType.FieldName)
            {
                throw new JsonPropertyAlreadyAddedException();
            }

            CurrentTokenType = JsonTokenType.FieldName;
        }
    }
}