//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query.Core.ComparableTask
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Query.Core.Collections;
    using Microsoft.Azure.Documents;

    internal sealed class ComparableTaskScheduler : IDisposable
    {
        private const int MinimumBatchSize = 1;
        private readonly AsyncCollection<IComparableTask> taskQueue;
        private readonly ConcurrentDictionary<IComparableTask, Task> delayedTasks;
        private readonly CancellationTokenSource tokenSource;
        private readonly SemaphoreSlim canRunTaskSemaphoreSlim;
        private readonly Task schedulerTask;
        private volatile bool isStopped;

        public ComparableTaskScheduler()
            : this(Environment.ProcessorCount)
        {
        }

        public ComparableTaskScheduler(int maximumConcurrencyLevel)
            : this(Enumerable.Empty<IComparableTask>(), maximumConcurrencyLevel)
        {
        }

        public ComparableTaskScheduler(IEnumerable<IComparableTask> tasks, int maximumConcurrencyLevel)
        {
            taskQueue = new AsyncCollection<IComparableTask>(new PriorityQueue<IComparableTask>(tasks, true));
            delayedTasks = new ConcurrentDictionary<IComparableTask, Task>();
            MaximumConcurrencyLevel = maximumConcurrencyLevel;
            tokenSource = new CancellationTokenSource();
            canRunTaskSemaphoreSlim = new SemaphoreSlim(maximumConcurrencyLevel);
            schedulerTask = ScheduleAsync();
        }

        public int MaximumConcurrencyLevel { get; private set; }

        public int CurrentRunningTaskCount => MaximumConcurrencyLevel - Math.Max(0, canRunTaskSemaphoreSlim.CurrentCount);

        public bool IsStopped => isStopped;

        private CancellationToken CancellationToken => tokenSource.Token;

        public void IncreaseMaximumConcurrencyLevel(int delta)
        {
            if (delta <= 0)
            {
                throw new ArgumentOutOfRangeException("delta must be a positive number.");
            }

            canRunTaskSemaphoreSlim.Release(delta);
            MaximumConcurrencyLevel += delta;
        }

        public void Dispose()
        {
            Stop();

            canRunTaskSemaphoreSlim.Dispose();
            tokenSource.Dispose();
        }

        public void Stop()
        {
            isStopped = true;
            tokenSource.Cancel();
            delayedTasks.Clear();
        }

        public bool TryQueueTask(IComparableTask comparableTask, TimeSpan delay = default)
        {
            if (comparableTask == null)
            {
                throw new ArgumentNullException("task");
            }

            if (isStopped)
            {
                return false;
            }

            Task newTask = new Task<Task>(() => QueueDelayedTaskAsync(comparableTask, delay), CancellationToken);

            if (delayedTasks.TryAdd(comparableTask, newTask))
            {
                newTask.Start();
                return true;
            }

            return false;
        }

        private async Task QueueDelayedTaskAsync(IComparableTask comparableTask, TimeSpan delay)
        {
            if (delayedTasks.TryRemove(comparableTask, out Task task) && !task.IsCanceled)
            {
                if (delay > default(TimeSpan))
                {
                    await Task.Delay(delay, CancellationToken);
                }

                if (taskQueue.TryPeek(out IComparableTask firstComparableTask) && (comparableTask.CompareTo(firstComparableTask) <= 0))
                {
                    await ExecuteComparableTaskAsync(comparableTask);
                }
                else
                {
                    await taskQueue.AddAsync(comparableTask, CancellationToken);
                }
            }
        }

        private async Task ScheduleAsync()
        {
            while (!isStopped)
            {
                await ExecuteComparableTaskAsync(await taskQueue.TakeAsync(CancellationToken));
            }
        }

        private async Task ExecuteComparableTaskAsync(IComparableTask comparableTask)
        {
            await canRunTaskSemaphoreSlim.WaitAsync(CancellationToken);

#pragma warning disable 4014
            // Schedule execution on current .NET task scheduler.
            // Compute gateway uses custom task scheduler to track tenant resource utilization.
            // Task.Run() switches to default task scheduler for entire sub-tree of tasks making compute gateway incapable of tracking resource usage accurately.
            // Task.Factory.StartNew() allows specifying task scheduler to use.
            Task.Factory
                .StartNewOnCurrentTaskSchedulerAsync(
                    function: () => comparableTask
                    .StartAsync(CancellationToken)
                    .ContinueWith((antecendent) =>
                    {
                        // Observing the exception.
                        Exception exception = antecendent.Exception;
                        Extensions.TraceException(exception);

                        // Semaphore.Release can also throw an exception.
                        try
                        {
                            canRunTaskSemaphoreSlim.Release();
                        }
                        catch (Exception releaseException)
                        {
                            Extensions.TraceException(releaseException);
                        }
                    }, TaskScheduler.Current),
                    cancellationToken: CancellationToken)
                .ContinueWith((antecendent) =>
                {
                    // StartNew can have a task cancelled exception
                    Exception exception = antecendent.Exception;
                    Extensions.TraceException(exception);
                });
#pragma warning restore 4014
        }
    }
}