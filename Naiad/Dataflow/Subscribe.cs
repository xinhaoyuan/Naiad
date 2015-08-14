/*
 * Naiad ver. 0.5
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Diagnostics;


namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Represents an observable "output" of a Naiad computation, and provides a means
    /// of synchronizing with the computation.
    /// </summary>
    public interface Subscription : IDisposable
    {
        /// <summary>
        /// Blocks the caller until this subscription has processed all inputs up to and
        /// including the given epoch.
        /// </summary>
        /// <param name="time">The epoch.</param>
        /// <remarks>
        /// To synchronize on all subscriptions in a computation at a particular epoch, use the <see cref="Computation.Sync"/> method.
        /// To block until the entire computation has terminated, use the <see cref="Computation.Join"/> method.
        /// </remarks>
        /// <seealso cref="Computation.Sync"/>
        /// <seealso cref="Computation.Join"/>
        void Sync(DataTimestamp time);
    }

    /// <summary>
    /// Extension methods
    /// </summary>
    public static class SubscribeExtensionMethods
    {
        /// <summary>
        /// Subscribes to a stream with no callback.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream)
        {
            return stream.Subscribe(x => { });
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied by one worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, new Placement.SingleVertex(0, 0), stream.Context, (j, t, l) => action(l));
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied at each worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<int, IEnumerable<R>> action)
        {
            return stream.Subscribe((j, t, l) => action(j, l));
        }

        /// <summary>
        /// Subscribes to a stream with a callback parameterized by worker id, epoch, and records.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id, epoch id, and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<int, Epoch, IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, stream.Context, action);
        }

        /// <summary>
        /// Subscribes to a stream with callbacks for record receipt, epoch completion notification, and stream completion notification.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="onRecv">receipt callback</param>
        /// <param name="onNotify">notification callback</param>
        /// <param name="onComplete">completion callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<Message<R, Epoch>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, stream.Context, onRecv, onNotify, onComplete);
        }
    }
}

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Manages several subscribe vertices, and allows another thread to block until all have completed a specified epoch
    /// </summary>
    internal class Subscription<R> : IDisposable, Subscription
    {
        private readonly Dictionary<DataTimestamp, CountdownEvent> Countdowns;        
        private int LocalVertexCount;

        private DataTimestamp CompleteThrough;

        private bool disposed = false;
        internal bool Disposed { get { return this.disposed; } }

        public void Dispose()
        {
            disposed = true;
        }

        internal readonly InputStage[] SourceInputs;


        /// <summary>
        /// Called by vertices, indicates the receipt of an OnNotify(time)
        /// </summary>
        /// <param name="time">Time that has completed for the vertex</param>
        internal void Signal(Epoch time)
        {
            lock (this.Countdowns)
            {
                // if this is the first mention of time.t, create a new countdown
                if (!this.Countdowns.ContainsKey(time.DataTimestamp))
                    this.Countdowns[time.DataTimestamp] = new CountdownEvent(this.LocalVertexCount);

                if (this.Countdowns[time.DataTimestamp].CurrentCount > 0)
                    this.Countdowns[time.DataTimestamp].Signal();
                else
                    Console.Error.WriteLine("Too many Signal({0})", time.DataTimestamp);

                // if the last signal, clean up a bit
                if (this.Countdowns[time.DataTimestamp].CurrentCount == 0)
                {
                    this.CompleteThrough = time.DataTimestamp.Meet(CompleteThrough); // bump completethrough int
                    this.Countdowns.Remove(time.DataTimestamp); // remove countdown object
                }
            }
        }

        /// <summary>
        /// Blocks the caller until this subscription has completed the given epoch.
        /// </summary>
        /// <param name="dataTimestamp">Time to wait until locally complete</param>
        public void Sync(DataTimestamp dataTimestamp)
        {
            CountdownEvent countdown;
            lock (this.Countdowns)
            {
                // if we have already completed it, don't wait
                if (dataTimestamp.CouldResultIn(this.CompleteThrough))
                    return;

                // if we haven't heard about it, create a new countdown
                if (!this.Countdowns.ContainsKey(dataTimestamp))
                    this.Countdowns[dataTimestamp] = new CountdownEvent(this.LocalVertexCount);

                countdown = this.Countdowns[dataTimestamp];
            }

            // having released the lock, wait.
            countdown.Wait();
        }

        internal Subscription(Stream<R, Epoch> input, Placement placement, TimeContext<Epoch> context, Action<Message<R, Epoch>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == context.Context.Manager.InternalComputation.Controller.Configuration.ProcessID)
                    this.LocalVertexCount++;

            var stage = new Stage<SubscribeStreamingVertex<R>, Epoch>(placement, context, Stage.OperatorType.Default, (i, v) => new SubscribeStreamingVertex<R>(i, v, this, onRecv, onNotify, onComplete), "SubscribeStreaming");

            stage.NewInput(input, (message, vertex) => vertex.OnReceive(message), null);

            this.Countdowns = new Dictionary<DataTimestamp, CountdownEvent>();
            this.CompleteThrough = new DataTimestamp(0);

            // important for reachability to be defined for the next test
            stage.InternalComputation.Reachability.UpdateReachabilityPartialOrder(stage.InternalComputation);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalComputation.Inputs.Where(i => stage.InternalComputation.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalComputation.Register(this);
        }

        internal Subscription(Stream<R, Epoch> input, Placement placement, TimeContext<Epoch> context, Action<int, Epoch, IEnumerable<R>> action)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == context.Context.Manager.InternalComputation.Controller.Configuration.ProcessID)
                    this.LocalVertexCount++;

            var stage = new Stage<SubscribeBufferingVertex<R>, Epoch>(placement, context, Stage.OperatorType.Default, (i, v) => new SubscribeBufferingVertex<R>(i, v, this, action), "SubscribeBuffering");

            stage.NewInput(input, (message, vertex) => vertex.OnReceive(message), null);

            this.Countdowns = new Dictionary<DataTimestamp, CountdownEvent>();
            this.CompleteThrough = new DataTimestamp(0);

            // important for reachability to be defined for the next test
            stage.InternalComputation.Reachability.UpdateReachabilityPartialOrder(stage.InternalComputation);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalComputation.Inputs.Where(i => stage.InternalComputation.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalComputation.Register(this);
        }
    }

    /// <summary>
    /// Individual subscription vertex, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeStreamingVertex<R> : SinkVertex<R, Epoch>
    {
        Action<Message<R, Epoch>, int> OnRecv;
        Action<Epoch, int> OnNotifyAction;
        Action<int> OnCompleted;

        Subscription<R> Parent;

        DataTimestamp completeThrough;

        protected override void OnShutdown()
        {
            this.OnCompleted(this.Scheduler.Index);
            base.OnShutdown();
        }

        public override void OnReceive(Message<R, Epoch> record)
        {
            this.OnRecv(record, this.Scheduler.Index);
            this.NotifyAt(record.time);
        }

        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnNotify for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnNotify(Epoch time)
        {
            if (time.DataTimestamp.CouldResultIn(completeThrough))
            {
                Logging.Progress("???");
                return;
            }

            // test to see if inputs supplied data for this epoch, or terminated instead
            List<KeyValuePair<int, int>> maxEpochList = new List<KeyValuePair<int, int>>();
            foreach (var input in this.Parent.SourceInputs.OrderByDescending(i => i.InputId))
                maxEpochList.Add(new KeyValuePair<int, int>(input.InputId, input.MaximumValidEpoch));
            var maxEpoch = new DataTimestamp(maxEpochList.ToArray());
            var validEpoch = time.DataTimestamp.CouldResultIn(maxEpoch);
            
            if (validEpoch)
                this.OnNotifyAction(time, this.Scheduler.Index);

            this.Parent.Signal(time);
            
            if (!this.Parent.Disposed && validEpoch)
            {
                // we actually want to be notified for <anytime could result from [time]>.
                completeThrough = completeThrough.Meet(time.DataTimestamp);
                this.NotifyAt(new Epoch(completeThrough));
            }
        }

        public SubscribeStreamingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<Message<R, Epoch>, int> onrecv, Action<Epoch, int> onnotify, Action<int> oncomplete)
            : base(index, stage)
        {
            this.Parent = parent;

            this.OnRecv = onrecv;
            this.OnNotifyAction = onnotify;
            this.OnCompleted = oncomplete;
            this.completeThrough = new DataTimestamp(0);
            this.NotifyAt(new Epoch(completeThrough));
        }
    }

    /// <summary>
    /// Individual subscription vertex, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeBufferingVertex<R> : SinkBufferingVertex<R, Epoch>
    {
        Action<int, Epoch, IEnumerable<R>> Action;        // (vertexid, epoch, data) => ()
        Subscription<R> Parent;
        DataTimestamp completeThrough;

        /// <summary>
        /// Called when a message is received.
        /// </summary>
        /// <param name="message">The message.</param>
        public override void OnReceive(Message<R, Epoch> message)
        {
            Logging.Debug("get message {0} at {1}", message.payload, message.time);
            this.Input.OnReceive(message, new ReturnAddress());
            Logging.Debug("got message {0} at {1}", message.payload, message.time);
            //if (!message.time.DataTimestamp.CouldResultIn(completeThrough))
            //{
            //    completeThrough = completeThrough.Meet(message.time.DataTimestamp);
            //    this.NotifyAt(new Epoch(completeThrough.Meet(message.time.DataTimestamp)));
            //}
        }

        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnNotify for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnNotify(Epoch time)
        {
            List<KeyValuePair<int, int>> maxEpochList = new List<KeyValuePair<int, int>>();
            foreach (var input in this.Parent.SourceInputs.OrderByDescending(i => i.InputId))
                maxEpochList.Add(new KeyValuePair<int, int>(input.InputId, input.MaximumValidEpoch));
            Logging.Debug("?????? {0} {1}", this.Parent.SourceInputs.Length, String.Join(";", maxEpochList));
            var maxEpoch = new DataTimestamp(maxEpochList.ToArray());
            var validEpoch = time.DataTimestamp.CouldResultIn(maxEpoch);

            if (validEpoch)
            {
                Logging.Debug("valid epoch {0}, maxEpoch = {1}", time.DataTimestamp, maxEpoch);
                Action(this.VertexId, time, Input.GetRecordsAt(time));
            }
            else
            {
                Logging.Debug("invalid epoch {0}, maxEpoch = {1}", time.DataTimestamp, maxEpoch);
            }
            
            this.Parent.Signal(time);
        }

        public SubscribeBufferingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<int, Epoch, IEnumerable<R>> action)
            : base(index, stage, null)
        {
            this.Parent = parent;
            this.Action = action;
            this.Input = new VertexInputBuffer<R, Epoch>(this);
            this.completeThrough = new DataTimestamp(0);
            // this.NotifyAt(new Epoch(completeThrough));
        }
    }
}
