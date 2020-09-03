﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.SqlObjects
{
    using System;
    using Microsoft.Azure.Cosmos.SqlObjects.Visitors;

#if INTERNAL
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable SA1600 // Elements should be documented
    public
#else
    internal
#endif
    sealed class SqlQuery : SqlObject
    {
        private SqlQuery(
            SqlSelectClause selectClause,
            SqlFromClause fromClause,
            SqlWhereClause whereClause,
            SqlGroupByClause groupByClause,
            SqlOrderbyClause orderbyClause,
            SqlOffsetLimitClause offsetLimitClause)
        {
            SelectClause = selectClause ?? throw new ArgumentNullException(nameof(selectClause));
            FromClause = fromClause;
            WhereClause = whereClause;
            GroupByClause = groupByClause;
            OrderbyClause = orderbyClause;
            OffsetLimitClause = offsetLimitClause;
        }

        public SqlSelectClause SelectClause { get; }

        public SqlFromClause FromClause { get; }

        public SqlWhereClause WhereClause { get; }

        public SqlGroupByClause GroupByClause { get; }

        public SqlOrderbyClause OrderbyClause { get; }

        public SqlOffsetLimitClause OffsetLimitClause { get; }

        public override void Accept(SqlObjectVisitor visitor) => visitor.Visit(this);

        public override TResult Accept<TResult>(SqlObjectVisitor<TResult> visitor) => visitor.Visit(this);

        public override TResult Accept<T, TResult>(SqlObjectVisitor<T, TResult> visitor, T input) => visitor.Visit(this, input);

        public static SqlQuery Create(
            SqlSelectClause selectClause,
            SqlFromClause fromClause,
            SqlWhereClause whereClause,
            SqlGroupByClause groupByClause,
            SqlOrderbyClause orderByClause,
            SqlOffsetLimitClause offsetLimitClause) => new SqlQuery(
                selectClause,
                fromClause,
                whereClause,
                groupByClause,
                orderByClause,
                offsetLimitClause);
    }
}
