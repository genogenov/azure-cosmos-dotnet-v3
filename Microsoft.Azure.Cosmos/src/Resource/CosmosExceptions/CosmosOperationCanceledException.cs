//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections;

    /// <summary>
    /// The exception that is thrown in a thread upon cancellation of an operation that
    ///  the thread was executing. This extends the OperationCanceledException to include the
    ///  diagnostics of the operation that was canceled.
    /// </summary>
    public class CosmosOperationCanceledException : OperationCanceledException
    {
        private readonly OperationCanceledException originalException;

        internal CosmosOperationCanceledException(
            OperationCanceledException originalException,
            CosmosDiagnosticsContext diagnosticsContext)
            : this(
                originalException,
                diagnosticsContext?.Diagnostics)
        {
        }

        /// <summary>
        /// Create an instance of CosmosOperationCanceledException
        /// </summary>
        /// <param name="originalException">The original operation canceled exception</param>
        /// <param name="diagnostics"></param>
        public CosmosOperationCanceledException(
            OperationCanceledException originalException,
            CosmosDiagnostics diagnostics)
            : base(originalException.CancellationToken)
        {
            if (originalException == null)
            {
                throw new ArgumentNullException(nameof(originalException));
            }

            if (diagnostics == null)
            {
                throw new ArgumentNullException(nameof(diagnostics));
            }

            this.originalException = originalException;
            Diagnostics = diagnostics;
        }

        /// <inheritdoc/>
        public override string Source
        {
            get => originalException.Source;
            set => originalException.Source = value;
        }

        /// <inheritdoc/>
        public override string Message => originalException.Message;

        /// <inheritdoc/>
        public override string StackTrace => originalException.StackTrace;

        /// <inheritdoc/>
        public override IDictionary Data => originalException.Data;

        /// <summary>
        /// Gets the diagnostics for the request
        /// </summary>
        public CosmosDiagnostics Diagnostics { get; }

        /// <inheritdoc/>
        public override string HelpLink
        {
            get => originalException.HelpLink;
            set => originalException.HelpLink = value;
        }

        /// <inheritdoc/>
        public override Exception GetBaseException()
        {
            return originalException.GetBaseException();
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{originalException.ToString()} {Environment.NewLine}CosmosDiagnostics: {Diagnostics.ToString()}";
        }
    }
}
