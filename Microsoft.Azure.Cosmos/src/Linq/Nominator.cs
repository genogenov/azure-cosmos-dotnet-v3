//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Linq
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary> 
    /// Performs bottom-up analysis to determine which nodes can possibly 
    /// be part of an evaluated sub-tree. 
    /// </summary>
    internal static class Nominator
    {
        public static HashSet<Expression> Nominate(Expression expression, Func<Expression, bool> fnCanBeEvaluated)
        {
            NominatorVisitor visitor = new NominatorVisitor(fnCanBeEvaluated);
            return visitor.Nominate(expression);
        }

        private sealed class NominatorVisitor : ExpressionVisitor
        {
            private readonly Func<Expression, bool> fnCanBeEvaluated;
            private HashSet<Expression> candidates;
            private bool canBeEvaluated;

            public NominatorVisitor(Func<Expression, bool> fnCanBeEvaluated)
            {
                this.fnCanBeEvaluated = fnCanBeEvaluated;
            }

            public HashSet<Expression> Nominate(Expression expression)
            {
                candidates = new HashSet<Expression>();
                Visit(expression);
                return candidates;
            }

            public override Expression Visit(Expression expression)
            {
                if (expression != null)
                {
                    bool lastCanBeEvaluated = canBeEvaluated;
                    canBeEvaluated = true;
                    base.Visit(expression);
                    if (canBeEvaluated)
                    {
                        canBeEvaluated = fnCanBeEvaluated(expression);
                        if (canBeEvaluated)
                        {
                            candidates.Add(expression);
                        }
                    }
                    canBeEvaluated &= lastCanBeEvaluated;
                }
                return expression;
            }
        }
    }
}
