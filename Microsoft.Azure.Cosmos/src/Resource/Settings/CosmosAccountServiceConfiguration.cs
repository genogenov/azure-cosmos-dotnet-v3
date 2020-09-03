﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;

    internal class CosmosAccountServiceConfiguration : IServiceConfigurationReader
    {
        private Func<Task<AccountProperties>> accountPropertiesTaskFunc { get; }

        internal AccountProperties AccountProperties { get; private set; }

        public CosmosAccountServiceConfiguration(Func<Task<AccountProperties>> accountPropertiesTaskFunc)
        {
            if (accountPropertiesTaskFunc == null)
            {
                throw new ArgumentNullException(nameof(accountPropertiesTaskFunc));
            }

            this.accountPropertiesTaskFunc = accountPropertiesTaskFunc;
        }

        public IDictionary<string, object> QueryEngineConfiguration => AccountProperties.QueryEngineConfiguration;

        public string DatabaseAccountId => throw new NotImplementedException();

        public Uri DatabaseAccountApiEndpoint => throw new NotImplementedException();

        public ReplicationPolicy UserReplicationPolicy => AccountProperties.ReplicationPolicy;

        public ReplicationPolicy SystemReplicationPolicy => AccountProperties.SystemReplicationPolicy;

        public Documents.ConsistencyLevel DefaultConsistencyLevel => (Documents.ConsistencyLevel)AccountProperties.Consistency.DefaultConsistencyLevel;

        public ReadPolicy ReadPolicy => AccountProperties.ReadPolicy;

        public string PrimaryMasterKey => throw new NotImplementedException();

        public string SecondaryMasterKey => throw new NotImplementedException();

        public string PrimaryReadonlyMasterKey => throw new NotImplementedException();

        public string SecondaryReadonlyMasterKey => throw new NotImplementedException();

        public string ResourceSeedKey => throw new NotImplementedException();

        public bool EnableAuthorization => true;

        public string SubscriptionId => throw new NotImplementedException();

        public async Task InitializeAsync()
        {
            if (AccountProperties == null)
            {
                AccountProperties = await accountPropertiesTaskFunc();
            }
        }
    }
}
