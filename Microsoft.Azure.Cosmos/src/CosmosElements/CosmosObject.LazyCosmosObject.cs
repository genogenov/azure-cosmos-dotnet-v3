﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.CosmosElements
{
#nullable enable

    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Azure.Cosmos.Json;

#if INTERNAL
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable SA1601 // Partial elements should be documented
    public
#else
    internal
#endif
    abstract partial class CosmosObject : CosmosElement, IReadOnlyDictionary<string, CosmosElement>, IEquatable<CosmosObject>, IComparable<CosmosObject>
    {
        private class LazyCosmosObject : CosmosObject
        {
            private readonly IJsonNavigator jsonNavigator;
            private readonly IJsonNavigatorNode jsonNavigatorNode;
            private readonly ConcurrentDictionary<string, CosmosElement> cachedElements;
            private readonly Lazy<int> lazyCount;

            public LazyCosmosObject(IJsonNavigator jsonNavigator, IJsonNavigatorNode jsonNavigatorNode)
            {
                JsonNodeType type = jsonNavigator.GetNodeType(jsonNavigatorNode);
                if (type != JsonNodeType.Object)
                {
                    throw new ArgumentOutOfRangeException($"{nameof(jsonNavigatorNode)} must be a {JsonNodeType.Object} node. Got {type} instead.");
                }

                this.jsonNavigator = jsonNavigator;
                this.jsonNavigatorNode = jsonNavigatorNode;
                cachedElements = new ConcurrentDictionary<string, CosmosElement>();
                lazyCount = new Lazy<int>(() => this.jsonNavigator.GetObjectPropertyCount(this.jsonNavigatorNode));
            }

            // This method is not efficient (but that's the expectation of IEnumerable ... A user should call ToList() if they are calling multiple times).
            public override IEnumerable<string> Keys =>
                jsonNavigator
                .GetObjectProperties(jsonNavigatorNode)
                .Select((objectProperty) => jsonNavigator.GetStringValue(objectProperty.NameNode));

            // This method is not efficient (but that's the expectation of IEnumerable ... A user should call ToList() if they are calling multiple times).
            public override IEnumerable<CosmosElement> Values =>
                jsonNavigator
                .GetObjectProperties(jsonNavigatorNode)
                .Select((objectProperty) => CosmosElement.Dispatch(jsonNavigator, objectProperty.ValueNode));

            public override int Count => lazyCount.Value;

            public override CosmosElement this[string key]
            {
                get
                {
                    if (!TryGetValue(key, out CosmosElement value))
                    {
                        throw new KeyNotFoundException($"Failed to find key: {key}");
                    }

                    return value;
                }
            }

            public override bool ContainsKey(string key) => jsonNavigator.TryGetObjectProperty(
                jsonNavigatorNode,
                key,
                out _);

            public override IEnumerator<KeyValuePair<string, CosmosElement>> GetEnumerator() =>
                jsonNavigator
                .GetObjectProperties(jsonNavigatorNode)
                .Select(
                    (objectProperty) =>
                    new KeyValuePair<string, CosmosElement>(
                        jsonNavigator.GetStringValue(objectProperty.NameNode),
                        CosmosElement.Dispatch(jsonNavigator, objectProperty.ValueNode)))
                .GetEnumerator();

            public override bool TryGetValue(string key, out CosmosElement value)
            {
                if (cachedElements.TryGetValue(
                    key,
                    out CosmosElement cosmosElemet))
                {
                    value = cosmosElemet;
                    return true;
                }

                if (jsonNavigator.TryGetObjectProperty(
                    jsonNavigatorNode,
                    key,
                    out ObjectProperty objectProperty))
                {
                    value = CosmosElement.Dispatch(jsonNavigator, objectProperty.ValueNode);
                    cachedElements[key] = value;

                    return true;
                }

#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
                value = default; // Dictionary.TryGetValue does not use nullable references.
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
                return false;
            }

            public override void WriteTo(IJsonWriter jsonWriter) => jsonNavigator.WriteTo(jsonNavigatorNode, jsonWriter);

            public override IJsonReader CreateReader() => jsonNavigator.CreateReader(jsonNavigatorNode);
        }
    }
#if INTERNAL
#pragma warning restore SA1601 // Partial elements should be documented
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
}