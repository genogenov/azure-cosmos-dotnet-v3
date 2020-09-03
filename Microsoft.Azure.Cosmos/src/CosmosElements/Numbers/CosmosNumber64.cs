//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

// This is auto-generated code. Modify: CosmosNumberCodeGenerator.tt: 45

namespace Microsoft.Azure.Cosmos.CosmosElements.Numbers
{
#nullable enable

    using System;
    using Microsoft.Azure.Cosmos.Json;

#if INTERNAL
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable SA1600 // Elements should be documented
#pragma warning disable SA1601 // Partial elements should be documented
    public
#else
    internal
#endif
    abstract partial class CosmosNumber64 : CosmosNumber, IEquatable<CosmosNumber64>, IComparable<CosmosNumber64>
    {
        protected CosmosNumber64()
            : base()
        {
        }

        public override Number64 Value => GetValue();

        public abstract Number64 GetValue();

        public override void Accept(ICosmosNumberVisitor cosmosNumberVisitor) => cosmosNumberVisitor.Visit(this);

        public override TResult Accept<TResult>(ICosmosNumberVisitor<TResult> cosmosNumberVisitor) => cosmosNumberVisitor.Visit(this);

        public override TOutput Accept<TArg, TOutput>(ICosmosNumberVisitor<TArg, TOutput> cosmosNumberVisitor, TArg input) => cosmosNumberVisitor.Visit(this, input);

        public override bool Equals(CosmosNumber cosmosNumber) => cosmosNumber is CosmosNumber64 cosmosNumber64 && Equals(cosmosNumber64);

        public bool Equals(CosmosNumber64 cosmosNumber64) => GetValue() == cosmosNumber64.GetValue();

        public override int GetHashCode() => (int)MurmurHash3.Hash32(Number64.ToDoubleEx(GetValue()), 1943952435);

        public int CompareTo(CosmosNumber64 cosmosNumber64) => GetValue().CompareTo(cosmosNumber64.GetValue());

        public override void WriteTo(IJsonWriter jsonWriter) => jsonWriter.WriteNumber64Value(GetValue());

        public static CosmosNumber64 Create(
            IJsonNavigator jsonNavigator,
            IJsonNavigatorNode jsonNavigatorNode) => new LazyCosmosNumber64(jsonNavigator, jsonNavigatorNode);

        public static CosmosNumber64 Create(Number64 number) => new EagerCosmosNumber64(number);
    }
}
