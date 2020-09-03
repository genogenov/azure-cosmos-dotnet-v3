// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Pagination
{
    internal sealed class DocumentContainerState : State
    {
        public DocumentContainerState(long resourceIdentifier)
        {
            ResourceIdentifer = resourceIdentifier;
        }

        public long ResourceIdentifer { get; }
    }
}
