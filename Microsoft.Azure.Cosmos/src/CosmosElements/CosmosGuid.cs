﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.CosmosElements
{
#nullable enable

    using System;
    using Microsoft.Azure.Cosmos.Json;
    using Microsoft.Azure.Cosmos.Query.Core.Monads;

#if INTERNAL
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable SA1600 // Elements should be documented
#pragma warning disable SA1601 // Partial elements should be documented
    public
#else
    internal
#endif
    abstract partial class CosmosGuid : CosmosElement, IEquatable<CosmosGuid>, IComparable<CosmosGuid>
    {
        private const uint HashSeed = 527095639;

        protected CosmosGuid()
            : base()
        {
        }

        public abstract Guid Value { get; }

        public override void Accept(ICosmosElementVisitor cosmosElementVisitor) => cosmosElementVisitor.Visit(this);

        public override TResult Accept<TResult>(ICosmosElementVisitor<TResult> cosmosElementVisitor) => cosmosElementVisitor.Visit(this);

        public override TResult Accept<TArg, TResult>(ICosmosElementVisitor<TArg, TResult> cosmosElementVisitor, TArg input) => cosmosElementVisitor.Visit(this, input);

        public override bool Equals(CosmosElement cosmosElement) => cosmosElement is CosmosGuid cosmosGuid && Equals(cosmosGuid);

        public bool Equals(CosmosGuid cosmosGuid) => Value == cosmosGuid.Value;

        public override int GetHashCode()
        {
            uint hash = HashSeed;
            hash = MurmurHash3.Hash32(Value, hash);
            return (int)hash;
        }

        public int CompareTo(CosmosGuid cosmosGuid) => Value.CompareTo(cosmosGuid.Value);

        public static CosmosGuid Create(
            IJsonNavigator jsonNavigator,
            IJsonNavigatorNode jsonNavigatorNode)
        {
            return new LazyCosmosGuid(jsonNavigator, jsonNavigatorNode);
        }

        public static CosmosGuid Create(Guid value) => new EagerCosmosGuid(value);

        public override void WriteTo(IJsonWriter jsonWriter) => jsonWriter.WriteGuidValue(Value);

        public static new CosmosGuid CreateFromBuffer(ReadOnlyMemory<byte> buffer) => CosmosElement.CreateFromBuffer<CosmosGuid>(buffer);

        public static new CosmosGuid Parse(string json) => CosmosElement.Parse<CosmosGuid>(json);

        public static bool TryCreateFromBuffer(
            ReadOnlyMemory<byte> buffer,
            out CosmosGuid cosmosGuid) => CosmosElement.TryCreateFromBuffer<CosmosGuid>(buffer, out cosmosGuid);

        public static bool TryParse(
            string json,
            out CosmosGuid cosmosGuid) => CosmosElement.TryParse<CosmosGuid>(json, out cosmosGuid);

        public static new class Monadic
        {
            public static TryCatch<CosmosGuid> CreateFromBuffer(ReadOnlyMemory<byte> buffer) => CosmosElement.Monadic.CreateFromBuffer<CosmosGuid>(buffer);

            public static TryCatch<CosmosGuid> Parse(string json) => CosmosElement.Monadic.Parse<CosmosGuid>(json);
        }
    }
#if INTERNAL
#pragma warning restore SA1601 // Partial elements should be documented
#pragma warning restore SA1600 // Elements should be documented
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
}
