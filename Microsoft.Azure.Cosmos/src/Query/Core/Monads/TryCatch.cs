﻿// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Monads
{
    using System;
    using System.Threading.Tasks;

#if INTERNAL
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable SA1600 // Elements should be documented
    public
#else
    internal
#endif
    readonly struct TryCatch
    {
        private readonly TryCatch<Void> voidTryCatch;

        private TryCatch(TryCatch<Void> voidTryCatch)
        {
            this.voidTryCatch = voidTryCatch;
        }

        public Exception Exception => voidTryCatch.Exception;

        public bool Succeeded => voidTryCatch.Succeeded;

        public bool Failed => voidTryCatch.Failed;

        public void Match(
            Action onSuccess,
            Action<Exception> onError)
        {
            voidTryCatch.Match(
                onSuccess: (dummy) => { onSuccess(); },
                onError: onError);
        }

        public TryCatch Try(
            Action onSuccess)
        {
            return new TryCatch(voidTryCatch.Try(onSuccess: (dummy) => { onSuccess(); }));
        }

        public TryCatch<T> Try<T>(
            Func<T> onSuccess)
        {
            return voidTryCatch.Try<T>(onSuccess: (dummy) => { return onSuccess(); });
        }

        public Task<TryCatch<T>> TryAsync<T>(
            Func<Task<T>> onSuccess)
        {
            return voidTryCatch.TryAsync<T>(onSuccess: (dummy) => { return onSuccess(); });
        }

        public TryCatch Catch(
            Action<Exception> onError)
        {
            return new TryCatch(voidTryCatch.Catch(onError));
        }

        public async Task<TryCatch> CatchAsync(
            Func<Exception, Task> onError)
        {
            return new TryCatch(await voidTryCatch.CatchAsync(onError));
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is TryCatch other)
            {
                return Equals(other);
            }

            return false;
        }

        public bool Equals(TryCatch other)
        {
            return voidTryCatch.Equals(other.voidTryCatch);
        }

        public override int GetHashCode()
        {
            return voidTryCatch.GetHashCode();
        }

        public static TryCatch FromResult()
        {
            return new TryCatch(TryCatch<Void>.FromResult(default));
        }

        public static TryCatch FromException(Exception exception)
        {
            return new TryCatch(TryCatch<Void>.FromException(exception));
        }

        /// <summary>
        /// Represents a void return type.
        /// </summary>
        private readonly struct Void
        {
        }
    }
}
