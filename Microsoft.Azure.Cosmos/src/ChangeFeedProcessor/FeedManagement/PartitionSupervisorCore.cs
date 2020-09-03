//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.ChangeFeed.FeedManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.ChangeFeed.Exceptions;
    using Microsoft.Azure.Cosmos.ChangeFeed.FeedProcessing;
    using Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement;
    using Microsoft.Azure.Cosmos.ChangeFeed.Utils;

    internal sealed class PartitionSupervisorCore<T> : PartitionSupervisor
    {
        private readonly DocumentServiceLease lease;
        private readonly ChangeFeedObserver<T> observer;
        private readonly FeedProcessor processor;
        private readonly LeaseRenewer renewer;
        private readonly CancellationTokenSource renewerCancellation = new CancellationTokenSource();
        private CancellationTokenSource processorCancellation;

        public PartitionSupervisorCore(DocumentServiceLease lease, ChangeFeedObserver<T> observer, FeedProcessor processor, LeaseRenewer renewer)
        {
            this.lease = lease;
            this.observer = observer;
            this.processor = processor;
            this.renewer = renewer;
        }

        public override async Task RunAsync(CancellationToken shutdownToken)
        {
            ChangeFeedObserverContextCore<T> context = new ChangeFeedObserverContextCore<T>(lease.CurrentLeaseToken);
            await observer.OpenAsync(context).ConfigureAwait(false);

            processorCancellation = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

            Task processorTask = processor.RunAsync(processorCancellation.Token);
            processorTask.ContinueWith(_ => renewerCancellation.Cancel()).LogException();

            Task renewerTask = renewer.RunAsync(renewerCancellation.Token);
            renewerTask.ContinueWith(_ => processorCancellation.Cancel()).LogException();

            ChangeFeedObserverCloseReason closeReason = shutdownToken.IsCancellationRequested ?
                ChangeFeedObserverCloseReason.Shutdown :
                ChangeFeedObserverCloseReason.Unknown;

            try
            {
                await Task.WhenAll(processorTask, renewerTask).ConfigureAwait(false);
            }
            catch (LeaseLostException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseLost;
                throw;
            }
            catch (FeedSplitException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseGone;
                throw;
            }
            catch (FeedNotFoundException)
            {
                closeReason = ChangeFeedObserverCloseReason.ResourceGone;
                throw;
            }
            catch (FeedReadSessionNotAvailableException)
            {
                closeReason = ChangeFeedObserverCloseReason.ReadSessionNotAvailable;
                throw;
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                closeReason = ChangeFeedObserverCloseReason.Shutdown;
            }
            catch (ObserverException)
            {
                closeReason = ChangeFeedObserverCloseReason.ObserverError;
                throw;
            }
            catch (Exception) when (processorTask.IsFaulted)
            {
                closeReason = ChangeFeedObserverCloseReason.Unknown;
                throw;
            }
            finally
            {
                await observer.CloseAsync(context, closeReason).ConfigureAwait(false);
            }
        }

        public override void Dispose()
        {
            processorCancellation?.Dispose();
            renewerCancellation.Dispose();
        }
    }
}