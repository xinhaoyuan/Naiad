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

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Timestamp that represent structural information in the dataflow graph. It containsa a total order under could-result-in.
    /// </summary>
    public struct StructuralTimestamp
    {
        private Util.SArray<int> items;
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
        public IEnumerable<int> Enumerate()
        {
            return items.Enumerate();
        }
    }
    /// <summary>
    /// Timestamp that represents external concurrency of data. It contains a vector clock with a partial order.
    /// </summary>
    public struct DataTimestamp
    {
        private Util.SArray<KeyValuePair<int, int>> items;
        public DataTimestamp(int size)
        {
            items = new Util.SArray<KeyValuePair<int, int>>(size);
        }
        public bool CouldResultIn(DataTimestamp other)
        {
            int i, j = 0;
            for (i = 0; i < Length; ++i)
            {
                while (items[i].Key != other.items[j].Key && j < other.Length)
                    ++j;
                // Key mismatch
                if (j == other.Length) return false;
                if (items[i].Value > other.items[j].Value) return false;
                ++j;
            }
            return true;
        }
        public bool Equals(DataTimestamp other)
        {
            if (Length != other.Length) return false;
            for (int i = 0; i < Length; ++i)
            {
                if (items[i].Key != other.items[i].Key) return false;
                if (items[i].Value != other.items[i].Value) return false;
            }
            return true;
        }
        public DataTimestamp Join(DataTimestamp other)
        {
            Dictionary<int, int> dict = Enumerate().ToDictionary(kv => kv.Key, kv => kv.Value);
            foreach (var ts in other.Enumerate())
            {
                if (!dict.ContainsKey(ts.Key) || dict[ts.Key] < ts.Value)
                {
                    dict[ts.Key] = ts.Value;
                }
            }
            DataTimestamp ret = new DataTimestamp(dict.Count);
            int i = 0;
            foreach (var kv in dict)
            {
                ret.items[i] = kv;
                ++i;
            }
            return ret;
        }
        public DataTimestamp Meet(DataTimestamp other)
        {
            int i = 0, j = 0;
            List<KeyValuePair<int, int>> list = new List<KeyValuePair<int, int>>();
            while (i < Length && j < other.Length)
            {
                if (items[i].Key == other.items[j].Key)
                {
                    if (items[i].Value < other.items[j].Value)
                    {
                        list.Add(items[i]);
                    }
                    else
                    {
                        list.Add(other.items[j]);
                    }
                    ++i; ++j;
                }
                else if (items[i].Key < other.items[j].Key)
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
        public KeyValuePair<int, int> this[int index]
        {
            get { return items[index]; }
            set { items[index] = value; }
        }
        public int Length { get { return items.Length; } }
        public IEnumerable<KeyValuePair<int, int>> Enumerate()
        {
            return items.Enumerate();
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
        public int DataConcurrency { get { return 0; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> with an empty timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated (i.e. zero).</returns>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp) { }

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
        public DataTimestamp dataTimestamp;

        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is less than or equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        public bool LessThan(Epoch other)
        {
            return dataTimestamp.CouldResultIn(other.dataTimestamp);
        }
        
        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is equal to <c>other</c>.</returns>
        public bool Equals(Epoch other)
        {
            return dataTimestamp.Equals(other.dataTimestamp);
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
            return new Epoch(dataTimestamp.Join(other.dataTimestamp));
        }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> epochs.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> epochs.</returns>
        public Epoch Meet(Epoch other)
        {
            return new Epoch(dataTimestamp.Meet(other.dataTimestamp));
        }

        /// <summary>
        /// Returns a string representation of this epoch.
        /// </summary>
        /// <returns>A string representation of this epoch.</returns>
        public override string ToString()
        {
            return dataTimestamp.ToString();
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return dataTimestamp.GetHashCode();
        }

        /// <summary>
        /// The number of structural coordinates in timestamps of this type (i.e. zero).
        /// </summary>
        public int StructuralDepth { get { return 0; } }
        public int DataConcurrency { get { return dataTimestamp.Length; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this epoch.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated (i.e. one).</returns>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp)
        {
            for (int i = 0; i < DataConcurrency; ++i)
                dataTimestamp.items[i] = this.dataTimestamp.items[i];
        }

        public Epoch(KeyValuePair<int, int>[] dataTimestamp)
        {
            this.dataTimestamp = new DataTimestamp(dataTimestamp.Length);
            for (int i = 0; i < dataTimestamp.Length; ++i)
                this.dataTimestamp.items[i] = dataTimestamp[i];
        }

        public Epoch(DataTimestamp dataTimestamp)
        {
            this.dataTimestamp = dataTimestamp;
        }
    }

    /// <summary>
    /// Represents the logical timestamp containing a loop counter nested within another logical <typeparamref name="TTime"/> context.
    /// </summary>
    /// <typeparam name="TTime">The type of the outer timestamp.</typeparam>
    public struct IterationIn<TTime> : Time<IterationIn<TTime>>
        where TTime : Time<TTime>
    {
        DataTimestamp dataTimestamp;
        StructuralTimestamp structTimestamp;

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
            return dataTimestamp.Equals(other.dataTimestamp) && structTimestamp.Equals(other.structTimestamp);
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
            return String.Format("[{0}, {1}]", dataTimestamp, structTimestamp);
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return dataTimestamp.GetHashCode() ^ structTimestamp.GetHashCode();
        }

        /// <summary>
        /// The number of integer coordinates in timestamps of this type.
        /// </summary>
        public int StructuralDepth { get { return structTimestamp.Length; } }
        public int DataConcurrency { get { return dataTimestamp.Length; } }
        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated.</returns>
        public void Populate(ref DataTimestamp dataTimestamp, ref StructuralTimestamp structTimestamp)
        {
            for (int i = 0; i < dataTimestamp.Length; ++i)
                dataTimestamp.items[i] = this.dataTimestamp.items[i];
            for (int i = 0; i < structTimestamp.Length; ++i)
                structTimestamp.items[i] = this.structTimestamp.items[i];
        }

        /// <summary>
        /// Returns the later of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns>The later of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Join(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(
                dataTimestamp.Join(other.dataTimestamp), 
                structTimestamp.Join(other.structTimestamp));
        }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamps.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Meet(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(
                dataTimestamp.Meet(other.dataTimestamp),
                structTimestamp.Meet(other.structTimestamp));
        }

        public IterationIn(DataTimestamp dataTimestamp, StructuralTimestamp structTimestamp) 
        {
            this.dataTimestamp = new DataTimestamp(dataTimestamp.Length);
            this.structTimestamp = new StructuralTimestamp(structTimestamp.Length);
            for (int i = 0; i < dataTimestamp.Length; ++i)
                this.dataTimestamp.items[i] = dataTimestamp.items[i];
            for (int i = 0; i < structTimestamp.Length; ++i)
                this.structTimestamp.items[i] = structTimestamp.items[i];
        }

        public IterationIn(TTime outTime, int iteration)
        {
            dataTimestamp = new DataTimestamp(outTime.DataConcurrency);
            structTimestamp = new StructuralTimestamp(outTime.StructuralDepth + 1);
            outTime.Populate(ref dataTimestamp, ref structTimestamp);
            structTimestamp.items[outTime.StructuralDepth] = iteration;
        }
    }
}
