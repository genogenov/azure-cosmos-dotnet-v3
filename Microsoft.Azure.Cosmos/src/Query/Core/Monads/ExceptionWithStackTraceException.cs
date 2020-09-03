// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.Monads
{
    using System;
    using System.Diagnostics;

    internal sealed class ExceptionWithStackTraceException : Exception
    {
        private static readonly string EndOfInnerExceptionString = "--- End of inner exception stack trace ---";
        private readonly StackTrace stackTrace;

        public ExceptionWithStackTraceException(StackTrace stackTrace)
            : this(message: null, innerException: null, stackTrace: stackTrace)
        {
        }

        public ExceptionWithStackTraceException(string message, StackTrace stackTrace)
            : this(message: message, innerException: null, stackTrace: stackTrace)
        {
        }

        public ExceptionWithStackTraceException(
            string message,
            Exception innerException,
            StackTrace stackTrace)
            : base(
                  message: message,
                  innerException: innerException)
        {
            if (stackTrace == null)
            {
                throw new ArgumentNullException(nameof(stackTrace));
            }

            this.stackTrace = stackTrace;
        }

        public override string StackTrace => stackTrace.ToString();

        public override string ToString()
        {
            // core2.x does not honor the StackTrace property in .ToString() (it uses the private internal stack trace).
            // core3.x uses the property as it should
            // For now just copying and pasting the 2.x implementation (this can be removed in 3.x)
            string s;

            if ((Message == null) || (Message.Length <= 0))
            {
                s = GetClassName();
            }
            else
            {
                s = GetClassName() + ": " + Message;
            }

            if (InnerException != null)
            {
                s = s
                    + " ---> "
                    + InnerException.ToString()
                    + Environment.NewLine
                    + "   "
                    + EndOfInnerExceptionString;

            }

            s += Environment.NewLine + StackTrace;
            return s;
        }

        private string GetClassName()
        {
            return GetType().ToString();
        }
    }
}
