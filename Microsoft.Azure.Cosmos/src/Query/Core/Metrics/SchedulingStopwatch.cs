﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Query.Core.Metrics
{
    using System.Diagnostics;

    /// <summary>
    /// This class keeps track of scheduling metrics for a single process using a stopwatch interface.
    /// Internally this class is composed of Stopwatches keeping track of scheduling metrics.
    /// The main metrics are turnaround, response, run, and wait time.
    /// However this class only handles behavior; if you want the data / results, then you will have to call on the 
    /// </summary>
    internal sealed class SchedulingStopwatch
    {
        /// <summary>
        /// Stopwatch used to measure turnaround time.
        /// </summary>
        private readonly Stopwatch turnaroundTimeStopwatch;

        /// <summary>
        /// Stopwatch used to measure response time.
        /// </summary>
        private readonly Stopwatch responseTimeStopwatch;

        /// <summary>
        /// Stopwatch used to measure runtime.
        /// </summary>
        private readonly Stopwatch runTimeStopwatch;

        /// <summary>
        /// Number of times the process was preempted.
        /// </summary>
        private long numPreemptions;

        /// <summary>
        /// Whether or not the process got a response yet.
        /// </summary>
        private bool responded;

        /// <summary>
        /// Initializes a new instance of the SchedulingStopwatch class.
        /// </summary>
        public SchedulingStopwatch()
        {
            turnaroundTimeStopwatch = new Stopwatch();
            responseTimeStopwatch = new Stopwatch();
            runTimeStopwatch = new Stopwatch();
        }

        /// <summary>
        /// Gets the SchedulingMetricsTimeSpan, which is a readonly snapshot of the SchedulingMetrics.
        /// </summary>
        /// <returns>the SchedulingMetricsResult.</returns>
        public SchedulingTimeSpan Elapsed
        {
            get
            {
                return new SchedulingTimeSpan(
                    turnaroundTimeStopwatch.Elapsed,
                    responseTimeStopwatch.Elapsed,
                    runTimeStopwatch.Elapsed,
                    turnaroundTimeStopwatch.Elapsed - runTimeStopwatch.Elapsed,
                    numPreemptions);
            }
        }

        /// <summary>
        /// Tells the SchedulingStopwatch know that the process is in a state where it is ready to be worked on,
        /// which in turn starts the stopwatch for for response time and turnaround time.
        /// </summary>
        public void Ready()
        {
            turnaroundTimeStopwatch.Start();
            responseTimeStopwatch.Start();
        }

        /// <summary>
        /// Starts or resumes the stopwatch for runtime meaning that the process in the run state for the first time
        /// or was preempted and now back in the run state.
        /// </summary>
        public void Start()
        {
            if (!runTimeStopwatch.IsRunning)
            {
                if (!responded)
                {
                    // This is the first time the process got a response, so the response time stopwatch needs to stop.
                    responseTimeStopwatch.Stop();
                    responded = true;
                }

                runTimeStopwatch.Start();
            }
        }

        public void Stop()
        {
            if (runTimeStopwatch.IsRunning)
            {
                runTimeStopwatch.Stop();
                numPreemptions++;
            }
        }

        /// <summary>
        /// Stops all the internal stopwatches.
        /// This is mainly useful for marking the end of a process to get an accurate turnaround time.
        /// It is undefined behavior to start a stopwatch that has been terminated.
        /// </summary>
        public void Terminate()
        {
            turnaroundTimeStopwatch.Stop();
            responseTimeStopwatch.Stop();
        }

        /// <summary>
        /// Returns a string version of this SchedulingStopwatch
        /// </summary>
        /// <returns>String version of the SchedulingStopwatch.</returns>
        public override string ToString()
        {
            // Just passing on to the SchedulingTimeSpan ToString function.
            return Elapsed.ToString();
        }
    }
}
