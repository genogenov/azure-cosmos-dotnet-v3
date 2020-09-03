//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Fluent
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;

    /// <summary>
    /// Indexing Policy fluent definition.
    /// </summary>
    /// <seealso cref="IndexingPolicy"/>
    public class IndexingPolicyDefinition<T>
    {
        private readonly IndexingPolicy indexingPolicy = new IndexingPolicy();
        private readonly T parent;
        private readonly Action<IndexingPolicy> attachCallback;
        private PathsDefinition<IndexingPolicyDefinition<T>> includedPathsBuilder;
        private PathsDefinition<IndexingPolicyDefinition<T>> excludedPathsBuilder;

        /// <summary>
        /// Creates an instance for unit-testing
        /// </summary>
        public IndexingPolicyDefinition()
        {
        }

        internal IndexingPolicyDefinition(
            T parent,
            Action<IndexingPolicy> attachCallback)
        {
            this.parent = parent;
            this.attachCallback = attachCallback;
        }

        /// <summary>
        /// Defines the <see cref="Container"/>'s <see cref="Cosmos.IndexingMode"/>.
        /// </summary>
        /// <param name="indexingMode">An <see cref="Cosmos.IndexingMode"/></param>
        /// <returns>An instance of <see cref="IndexingPolicyDefinition{T}"/>.</returns>
        /// <remarks>
        /// If multiple calls are made to this method within the same <see cref="IndexingPolicyDefinition{T}"/>, the last one will apply.
        /// </remarks>
        public IndexingPolicyDefinition<T> WithIndexingMode(IndexingMode indexingMode)
        {
            indexingPolicy.IndexingMode = indexingMode;
            return this;
        }

        /// <summary>
        /// Defines the <see cref="Container"/>'s automatic indexing.
        /// </summary>
        /// <param name="enabled">Defines whether Automatic Indexing is enabled or not.</param>
        /// <returns>An instance of <see cref="IndexingPolicyDefinition{T}"/>.</returns>
        public IndexingPolicyDefinition<T> WithAutomaticIndexing(bool enabled)
        {
            indexingPolicy.Automatic = enabled;
            return this;
        }

        /// <summary>
        /// Defines the <see cref="Container"/>'s <see cref="IndexingPolicy.IncludedPaths"/>.
        /// </summary>
        /// <returns>An instance of <see cref="PathsDefinition{T}"/>.</returns>
        public PathsDefinition<IndexingPolicyDefinition<T>> WithIncludedPaths()
        {
            if (includedPathsBuilder == null)
            {
                includedPathsBuilder = new PathsDefinition<IndexingPolicyDefinition<T>>(
                    this,
                    (paths) => AddIncludedPaths(paths));
            }

            return includedPathsBuilder;
        }

        /// <summary>
        /// Defines the <see cref="Container"/>'s <see cref="IndexingPolicy.ExcludedPaths"/>.
        /// </summary>
        /// <returns>An instance of <see cref="PathsDefinition{T}"/>.</returns>
        public PathsDefinition<IndexingPolicyDefinition<T>> WithExcludedPaths()
        {
            if (excludedPathsBuilder == null)
            {
                excludedPathsBuilder = new PathsDefinition<IndexingPolicyDefinition<T>>(
                    this,
                    (paths) => AddExcludedPaths(paths));
            }

            return excludedPathsBuilder;
        }

        /// <summary>
        /// Defines a Composite Index in the current <see cref="Container"/>'s definition.
        /// </summary>
        /// <returns>An instance of <see cref="CompositeIndexDefinition{T}"/>.</returns>
        public CompositeIndexDefinition<IndexingPolicyDefinition<T>> WithCompositeIndex()
        {
            return new CompositeIndexDefinition<IndexingPolicyDefinition<T>>(
                this,
                (compositePaths) => AddCompositePaths(compositePaths));
        }

        /// <summary>
        /// Defines a <see cref="Cosmos.SpatialIndex"/> in the current <see cref="Container"/>'s definition.
        /// </summary>
        /// <returns>An instance of <see cref="SpatialIndexDefinition{T}"/>.</returns>
        public SpatialIndexDefinition<IndexingPolicyDefinition<T>> WithSpatialIndex()
        {
            return new SpatialIndexDefinition<IndexingPolicyDefinition<T>>(
                this,
                (spatialIndex) => AddSpatialPath(spatialIndex));
        }

        /// <summary>
        /// Applies the current definition to the parent.
        /// </summary>
        /// <returns>An instance of the parent.</returns>
        public T Attach()
        {
            attachCallback(indexingPolicy);
            return parent;
        }

        private void AddCompositePaths(Collection<CompositePath> compositePaths)
        {
            indexingPolicy.CompositeIndexes.Add(compositePaths);
        }

        private void AddSpatialPath(SpatialPath spatialSpec)
        {
            indexingPolicy.SpatialIndexes.Add(spatialSpec);
        }

        private void AddIncludedPaths(IEnumerable<string> paths)
        {
            foreach (string path in paths)
            {
                indexingPolicy.IncludedPaths.Add(new IncludedPath() { Path = path });
            }
        }

        private void AddExcludedPaths(IEnumerable<string> paths)
        {
            foreach (string path in paths)
            {
                indexingPolicy.ExcludedPaths.Add(new ExcludedPath() { Path = path });
            }
        }
    }
}
