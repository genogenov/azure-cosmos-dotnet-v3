// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Monads
{
    using System;

    internal readonly struct Either<TLeft, TRight>
    {
        private readonly TLeft left;
        private readonly TRight right;

        private Either(TLeft left, TRight right, bool isLeft)
        {
            this.left = left;
            this.right = right;
            IsLeft = isLeft;
        }

        public bool IsLeft { get; }

        public bool IsRight
        {
            get
            {
                return !IsLeft;
            }
        }

        public void Match(Action<TLeft> onLeft, Action<TRight> onRight)
        {
            if (IsLeft)
            {
                onLeft(left);
            }
            else
            {
                onRight(right);
            }
        }

        public TResult Match<TResult>(Func<TLeft, TResult> onLeft, Func<TRight, TResult> onRight)
        {
            TResult result;
            if (IsLeft)
            {
                result = onLeft(left);
            }
            else
            {
                result = onRight(right);
            }

            return result;
        }

        public TLeft FromLeft(TLeft defaultValue)
        {
            TLeft result;
            if (IsLeft)
            {
                result = left;
            }
            else
            {
                result = defaultValue;
            }

            return result;
        }

        public TRight FromRight(TRight defaultValue)
        {
            TRight result;
            if (IsRight)
            {
                result = right;
            }
            else
            {
                result = defaultValue;
            }

            return result;
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is Either<TLeft, TRight> other)
            {
                return Equals(other);
            }

            return false;
        }

        public bool Equals(Either<TLeft, TRight> other)
        {
            if (IsLeft != other.IsLeft)
            {
                return false;
            }

            bool memberEquals;
            if (IsLeft)
            {
                TLeft left1 = left;
                TLeft left2 = other.left;
                memberEquals = left1.Equals(left2);
            }
            else
            {
                TRight right1 = right;
                TRight right2 = other.right;
                memberEquals = right1.Equals(right2);
            }

            return memberEquals;
        }

        public override int GetHashCode()
        {
            int hashCode = 0;
            hashCode ^= IsLeft.GetHashCode();
            if (IsLeft)
            {
                hashCode ^= left.GetHashCode();
            }
            else
            {
                hashCode ^= right.GetHashCode();
            }

            return hashCode;
        }

        public static implicit operator Either<TLeft, TRight>(TLeft left)
        {
            return new Either<TLeft, TRight>(
                left: left,
                right: default,
                isLeft: true);
        }

        public static implicit operator Either<TLeft, TRight>(TRight right)
        {
            return new Either<TLeft, TRight>(
                left: default,
                right: right,
                isLeft: false);
        }
    }
}
