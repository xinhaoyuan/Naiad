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
using System.Diagnostics;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Timestamp that represent structural information in the dataflow graph. 
    /// It contains a total order under could-result-in.
    /// </summary>
    public struct StructuralTimestamp
    {
        public Util.SArray<int> items;
        public StructuralTimestamp(int size)
        {
            items = new Util.SArray<int>(size);
        }
        public bool CouldResultIn(StructuralTimestamp other)
        {
            for (int i = 0; i < Length && i < other.Length; ++i)
            {
                if (items[i] > other.items[i])
                    return false;
                else if (items[i] < other.items[i])
                    return true;
            }
            return Length <= other.Length;
        }
        public bool Equals(StructuralTimestamp other)
        {
            if (Length != other.Length) return false;
            for (int i = 0; i < Length; ++i)
            {
                if (items[i] != other.items[i]) return false;
            }
            return true;
        }
        public StructuralTimestamp Join(StructuralTimestamp other)
        {
            if (CouldResultIn(other)) return other;
            else return this;
        }
        public StructuralTimestamp Meet(StructuralTimestamp other)
        {
            if (CouldResultIn(other)) return this;
            else return other;
        }
        public override string ToString()
        {
            return items.ToString();
        }
        public override int GetHashCode()
        {
            int result = 0;
            foreach (var ele in items.Enumerate())
                result ^= ele.GetHashCode();
            return result;
        }
        public int this[int index]
        {
            get { return items[index]; }
            set { items[index] = value; }
        }
        public int Length { get { return items.Length; } }
        public int Iteration { get { return items[items.Length - 1]; } }
        public StructuralTimestamp WithIteration(int v)
        {
            StructuralTimestamp sts = new StructuralTimestamp(items.Length);
            for (int i = 0; i < items.Length - 1; ++i)
                sts.items[i] = items[i];
            sts.items[items.Length - 1] = v;
            return sts;
        }
        public StructuralTimestamp WithIterationDelta(int d)
        {
            return WithIteration(items[items.Length - 1] + d);
        }
        public StructuralTimestamp RemoveIteration()
        {
            StructuralTimestamp sts = new StructuralTimestamp(items.Length - 1);
            for (int i = 0; i < items.Length - 1; ++i)
                sts.items[i] = items[i];
            return sts;
        }
        public StructuralTimestamp AddIteration(int v)
        {
            StructuralTimestamp sts = new StructuralTimestamp(items.Length + 1);
            for (int i = 0; i < items.Length; ++i)
                sts.items[i] = items[i];
            sts.items[items.Length] = v;
            return sts;
        }
        public IEnumerable<int> Enumerate()
        {
            return items.Enumerate();
        }
        public StructuralTimestamp(int[] structTimestamp)
        {
            items = new Util.SArray<int>(structTimestamp.Length);
            for (int i = 0; i < structTimestamp.Length; ++i)
            {
                items[i] = structTimestamp[i];
            }
        }
    }
    // Define this instead of KeyValuePair for Naiad serializer to properly serialize time structure
    public struct DataTimestampItem
    {
        public int Source;
        public int Epoch;
        public DataTimestampItem(int source, int epoch)
        {
            Source = source;
            Epoch = epoch;
        }
        public DataTimestampItem(KeyValuePair<int, int> kv)
        {
            Source = kv.Key;
            Epoch = kv.Value;
        }
    }
    /// <summary>
    /// Timestamp that represents external concurrency of data.
    /// It contains a vector clock with a partial order.
    /// </summary>
    public struct DataTimestamp
    {
        public Util.SArray<DataTimestampItem> items;
        public DataTimestamp(int size)
        {
            items = new Util.SArray<DataTimestampItem>(size);
        }
        public bool CouldResultIn(DataTimestamp other)
        {
            int i, j = 0;
            for (i = 0; i < Length; ++i)
            {
                while (j < other.Length && items[i].Source > other.items[j].Source)
                    ++j;
                // Key mismatch
                if (j == other.Length || items[i].Source < other.items[j].Source)
                return false;
                // Key match
                else if (items[i].Epoch > other.items[j].Epoch)
                    return false;
            }
            return true;
        }
        public bool Equals(DataTimestamp other)
        {
            if (items.Length != other.items.Length) return false;
            for (int i = 0; i < items.Length; ++i)
            {
                if (items[i].Source != other.items[i].Source) return false;
                if (items[i].Epoch != other.items[i].Epoch) return false;
            }
            return true;
        }
        public DataTimestamp Join(DataTimestamp other)
        {
            Dictionary<int, int> dict = Enumerate().ToDictionary(kv => kv.Source, kv => kv.Epoch);
            foreach (var ts in other.Enumerate())
            {
                if ((!dict.ContainsKey(ts.Source) || dict[ts.Source] < ts.Epoch))
                {
                    dict[ts.Source] = ts.Epoch;
                }
            }
            DataTimestamp ret = new DataTimestamp(dict.Count);
            int i = 0;
            foreach (var kv in dict.OrderBy(kv => kv.Key))
            {
                ret.items[i] = new DataTimestampItem(kv);
                ++i;
            }
            return ret;
        }
        public DataTimestamp Meet(DataTimestamp other)
        {
            int i = 0, j = 0;
            List<DataTimestampItem> list = new List<DataTimestampItem>();
            while (i < Length && j < other.Length)
            {
                if (items[i].Source == other.items[j].Source)
                {
                    if (items[i].Epoch < other.items[j].Epoch)
                    {
                        list.Add(items[i]);
                    }
                    else
                    {
                        list.Add(other.items[j]);
                    }
                    ++i; ++j;
                }
                else if (items[i].Source < other.items[j].Source)
                {
                    ++i;
                }
                else
                {
                    ++j;
                }
            }
            DataTimestamp ret = new DataTimestamp(list.Count);
            i = 0;
            foreach (var kv in list)
            {
                ret.items[i] = kv;
                ++i;
            }
            return ret;
        }
        public override string ToString()
        {
            return items.ToString();
        }
        public override int GetHashCode()
        {
            int result = 0;
            foreach (var ele in Enumerate())
                result ^= ele.GetHashCode();
            return result;
        }
        public DataTimestampItem this[int index]
        {
            get { return items[index]; }
            set { items[index] = value; }
        }
        public int Length { get { return items.Length; } }
        public IEnumerable<DataTimestampItem> Enumerate()
        {
            return items.Enumerate();
        }
        public DataTimestamp(int source, int epoch)
        {
            items = new Util.SArray<DataTimestampItem>(1);
            items[0] = new DataTimestampItem(source, epoch);
        }
        public DataTimestamp(DataTimestampItem[] kvItems)
        {
            items = new Util.SArray<DataTimestampItem>(kvItems.Length);
            for (int i = 0; i < kvItems.Length; ++i)
            {
                items[i] = kvItems[i];
            }
        }
    }
    /// <summary>
    /// Represents a logical timestamp in a timely dataflow computation. All messages in a
    /// timely dataflow computation are labeled with a logical timestamp.
    /// </summary>
    /// <remarks>
    /// This interface and its concrete implementations <see cref="Dataflow.Epoch"/> and <see cref="Dataflow.IterationIn{TTime}"/> are the typed equivalent of the <see cref="Pointstamp.Timestamp"/> field,
    /// corresponding to a sequence of integers.
    /// </remarks>
    /// <seealso cref="Microsoft.Research.Naiad.Dataflow.Epoch"/>
    /// <seealso cref="Microsoft.Research.Naiad.Dataflow.IterationIn{TTime}"/>
    /// <typeparam name="TTime">The concrete type of the timestamp.</typeparam>
    public interface Time<TTime> : IEquatable<TTime>, IComparable<TTime>
    {
        /// <summary>
        /// Returns a hashcode for this time.
        /// </summary>
        /// <returns>Returns a hashcode for this time.</returns>
        int GetHashCode();

        /// <summary>
        /// Returns <c>true</c> if and only if this time is less than or equal to the <paramref name="other"/>
        /// time, according to the partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        bool LessThan(TTime other);

        /// <summary>
        /// Computes the least upper bound of this and <paramref name="other"/>, according to the
        /// partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>The least upper bound of the two times.</returns>
        TTime Join(TTime other);

        /// <summary>
        /// Computes the greatest lower bound of this and <paramref name="other"/>, according to the
        /// partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>The greatest lower bound of the two times.</returns>
        TTime Meet(TTime other);

        /// <summary>
        /// The number of structural coordinates in timestamps of this type.
        /// </summary>
        int StructuralDepth { get; }
        int DataConcurrency { get; } 

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from a typed timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        void Populate(ref DataTimestamp dataTimstamp, ref StructuralTimestamp structTimestamp);
        TTime InitializeFrom(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp);
    }
}

namespace Microsoft.Research.Naiad.Dataflow
{
 
    /// <summary>
    /// Represents a non-varying logical time.
    /// </summary>
    public struct Empty : Time<Empty>
    {
        /// <summary>
        /// A dummy value, for compatibility with the current serializer.
        /// </summary>
        public int zero;

        /// <summary>
        /// Returns an empty time.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>An empty time.</returns>
        public Empty Join(Empty other) { return this; }

        /// <summary>
        /// Returns an empty time.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>An empty time.</returns>
        public Empty Meet(Empty other) { return this; }

        /// <summary>
        /// The number of structural coordinates in timestamps of this type (i.e. zero).
        /// </summary>
        public int StructuralDepth { get { return 0; } }
        /// <summary>
        /// The number of data coordinates in timestamps of this type (i.e. zero).
        /// </summary>
        public int DataConcurrency { get { return 0; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> with an empty timestamp.
        /// </summary>
        /// <param name="dataTimestamp"></param>
        /// <param name="structTimestamp"></param>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp) { }
        public Empty InitializeFrom(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp) { return this;  }

        /// <summary>
        /// GetHashCode override
        /// </summary>
        /// <returns>zero</returns>
        public override int GetHashCode()
        {
            return 0;
        }

        /// <summary>
        /// Returns <c>true</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c>.</returns>
        public bool Equals(Empty other) { return true; }

        /// <summary>
        /// Returns <c>0</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>0</returns>
        public int CompareTo(Empty other) { return 0; }

        /// <summary>
        /// Returns <c>true</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c></returns>
        public bool LessThan(Empty other) { return true; }
    }

    /// <summary>
    /// Represents the logical timestamp in a streaming context.
    /// </summary>
    public struct Epoch : Time<Epoch>
    {
        /// <summary>
        /// The vector clock sorted by source
        /// </summary>
        public DataTimestamp DataTimestamp;

        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is less than or equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        public bool LessThan(Epoch other)
        {
            return DataTimestamp.CouldResultIn(other.DataTimestamp);
        }
        
        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is equal to <c>other</c>.</returns>
        public bool Equals(Epoch other)
        {
            return DataTimestamp.Equals(other.DataTimestamp);
        }

        /// <summary>
        /// Compares this epoch with the <paramref name="other"/> epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(Epoch other) 
        {
            if (LessThan(other)) return -1;
            if (other.LessThan(this)) return 1;
            return 0;
        }

        /// <summary>
        /// Returns the later of this and the <paramref name="other"/> epochs.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>The later of this and the <paramref name="other"/> epochs.</returns>
        public Epoch Join(Epoch other)
        {
            return new Epoch(DataTimestamp.Join(other.DataTimestamp));
        }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> epochs.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> epochs.</returns>
        public Epoch Meet(Epoch other)
        {
            return new Epoch(DataTimestamp.Meet(other.DataTimestamp));
        }

        /// <summary>
        /// Returns a string representation of this epoch.
        /// </summary>
        /// <returns>A string representation of this epoch.</returns>
        public override string ToString()
        {
            return DataTimestamp.ToString();
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return DataTimestamp.GetHashCode();
        }

        /// <summary>
        /// The number of structural coordinates in timestamps of this type (i.e. zero).
        /// </summary>
        public int StructuralDepth { get { return 0; } }
        /// <summary>
        /// The number of data coordinates in timestamps of this type
        /// </summary>
        public int DataConcurrency { get { return DataTimestamp.Length; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this epoch.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated (i.e. one).</returns>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp)
        {
            for (int i = 0; i < DataConcurrency; ++i)
                dataTimestamp[i] = this.DataTimestamp[i];
        }

        public Epoch InitializeFrom(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp)
        {
            // XXX This may be not right
            DataTimestamp = dataTimestamp;
            return this;
        }

        public Epoch(DataTimestampItem[] dataTimestamp)
        {
            this.DataTimestamp = new DataTimestamp(dataTimestamp.Length);
            for (int i = 0; i < dataTimestamp.Length; ++i)
                this.DataTimestamp[i] = dataTimestamp[i];
        }

        public Epoch(DataTimestampItem singleSourceTimestamp)
        {
            this.DataTimestamp = new DataTimestamp(1);
            this.DataTimestamp[0] = singleSourceTimestamp;
        }

        public Epoch(DataTimestamp dataTimestamp)
        {
            this.DataTimestamp = dataTimestamp;
        }
    }

    /// <summary>
    /// Represents the logical timestamp containing a loop counter nested within another logical <typeparamref name="TTime"/> context.
    /// </summary>
    /// <typeparam name="TTime">The type of the outer timestamp.</typeparam>
    public struct IterationIn<TTime> : Time<IterationIn<TTime>>
        where TTime : Time<TTime>
    {
        public DataTimestamp DataTimestamp;
        public StructuralTimestamp StructTimestamp;

        /// <summary>
        /// Compares this timestamp with the <paramref name="other"/> timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(IterationIn<TTime> other)
        {
            // XXX
            return 0;
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this timestamp is equal to the <paramref name="other"/>
        /// timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is equal to <c>other</c>.</returns>
        public bool Equals(IterationIn<TTime> other)
        {
            return DataTimestamp.Equals(other.DataTimestamp) && StructTimestamp.Equals(other.StructTimestamp);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this timestamp is less than or equal to the <paramref name="other"/>
        /// timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        public bool LessThan(IterationIn<TTime> other)
        {
            DataTimestamp selfDTS = default(DataTimestamp), otherDTS = default(DataTimestamp);
            StructuralTimestamp selfSTS = default(StructuralTimestamp), otherSTS = default(StructuralTimestamp);
            Populate(ref selfDTS, ref selfSTS);
            other.Populate(ref otherDTS, ref otherSTS);
            return selfDTS.CouldResultIn(otherDTS) && selfSTS.CouldResultIn(otherSTS);
        }

        /// <summary>
        /// Returns a string representation of this timestamp.
        /// </summary>
        /// <returns>A string representation of this timestamp.</returns>
        public override string ToString()
        {
            return String.Format("[{0}, {1}]", DataTimestamp, StructTimestamp);
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return DataTimestamp.GetHashCode() ^ StructTimestamp.GetHashCode();
        }

        /// <summary>
        /// The number of integer coordinates in timestamps of this type.
        /// </summary>
        public int StructuralDepth { get { return default(TTime).StructuralDepth + 1; } }
        public int DataConcurrency { get { return DataTimestamp.Length; } }
        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated.</returns>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp)
        {
            for (int i = 0; i < dataTimestamp.Length; ++i)
                dataTimestamp[i] = this.DataTimestamp[i];
            for (int i = 0; i < structTimestamp.Length; ++i)
                structTimestamp[i] = this.StructTimestamp[i];
        }


        public IterationIn<TTime> InitializeFrom(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp)
        {
            DataTimestamp = dataTimestamp;
            StructTimestamp = new StructuralTimestamp(StructuralDepth);
            for (int i = 0; i < StructTimestamp.Length; ++i) {
                StructTimestamp[i] = structTimestamp[i];
            }
            return this;
        }

        public TTime GetOuterTime()
        {
            return default(TTime).InitializeFrom(DataTimestamp, StructTimestamp);
        }

        /// <summary>
        /// Returns the later of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns>The later of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Join(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(
                DataTimestamp.Join(other.DataTimestamp), 
                StructTimestamp.Join(other.StructTimestamp));
        }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamps.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Meet(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(
                DataTimestamp.Meet(other.DataTimestamp),
                StructTimestamp.Meet(other.StructTimestamp));
        }

        public IterationIn(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp) 
        {
            this.DataTimestamp = new DataTimestamp(dataTimestamp.Length);
            this.StructTimestamp = new StructuralTimestamp(structTimestamp.Length);
            for (int i = 0; i < dataTimestamp.Length; ++i)
                this.DataTimestamp[i] = dataTimestamp[i];
            for (int i = 0; i < structTimestamp.Length; ++i)
                this.StructTimestamp[i] = structTimestamp[i];
        }

        public IterationIn(TTime outTime, int iteration)
        {
            DataTimestamp = new DataTimestamp(outTime.DataConcurrency);
            StructTimestamp = new StructuralTimestamp(outTime.StructuralDepth + 1);
            outTime.Populate(ref DataTimestamp, ref StructTimestamp);
            StructTimestamp[outTime.StructuralDepth] = iteration;
        }
    }
}
