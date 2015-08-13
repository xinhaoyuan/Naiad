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
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    internal static class PointstampConstructor
    {
        public static Pointstamp ToPointstamp<T>(this T time, int graphObjectID) where T : Time<T>
        {
            var pointstamp = new Pointstamp(time.DataConcurrency, time.StructuralDepth);
            pointstamp.Location = graphObjectID;
            time.Populate(ref pointstamp.DataTimestamp, ref pointstamp.StructTimestamp);

            return pointstamp;
        }
    }


    /// <summary>
    /// Represents a combined dataflow graph location and timestamp,
    /// for use in progress tracking.
    /// </summary>
    /// <seealso cref="Computation.OnFrontierChange"/>
    public struct Pointstamp : IEquatable<Pointstamp>
    {
        /// <summary>
        /// Dataflow graph location
        /// </summary>
        public int Location;

        public StructuralTimestamp StructTimestamp;
        public DataTimestamp DataTimestamp;

        /// <summary>
        /// Returns a hashcode for this pointstamp.
        /// </summary>
        /// <returns>A hashcode for this pointstamp.</returns>
        public override int GetHashCode()
        {
            var result = Location;
            for (int i = 0; i < DataTimestamp.items.Length; ++i)
                result += DataTimestamp.items[i].Key + DataTimestamp.items[i].Value;
            for (int i = 0; i < StructTimestamp.items.Length; i++)
                result += StructTimestamp.items[i];

            return result;
        }

        /// <summary>
        /// Returns a string representation of this pointstamp.
        /// </summary>
        /// <returns>A string representation of this pointstamp.</returns>
        public override string ToString()
        {
            return String.Format("[location = {0}, data = <{1}>, struct= <{2}>]", Location, DataTimestamp.items, StructTimestamp.items);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this and the other pointstamps are equal.
        /// </summary>
        /// <param name="other">The other pointstamp.</param>
        /// <returns><c>true</c> if and only if this and the other pointstamps are equal.</returns>
        public bool Equals(Pointstamp other)
        {
            if (this.Location != other.Location)
                return false;

            if (this.StructTimestamp.items.Length != other.StructTimestamp.items.Length)
                return false;

            for (int i = 0; i < this.StructTimestamp.items.Length; i++)
                if (this.StructTimestamp.items[i] != other.StructTimestamp.items[i])
                    return false;

            if (this.DataTimestamp.items.Length != other.DataTimestamp.items.Length)
                return false;

            for (int i = 0; i < this.DataTimestamp.items.Length; i++)
            {
                if (this.DataTimestamp.items[i].Key != other.DataTimestamp.items[i].Key)
                    return false;
                if (this.DataTimestamp.items[i].Value != other.DataTimestamp.items[i].Value)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Constructs a Pointstamp copying from another
        /// </summary>
        /// <param name="that"></param>
        internal Pointstamp(Pointstamp that) 
        {
            this.Location = that.Location;
            this.DataTimestamp = that.DataTimestamp;
            this.StructTimestamp = that.StructTimestamp;
        }

        /// <summary>
        /// Constructs a new pointstamp from a location and int array
        /// </summary>
        /// <param name="location">dataflow graph location</param>
        /// <param name="indices">timestamp indices</param>
        internal Pointstamp(int location, KeyValuePair<int, int>[] dataTimestamp, int[] structTimestamp)
        {
            Location = location;
            DataTimestamp = new DataTimestamp(dataTimestamp.Length);
            for (int j = 0; j < dataTimestamp.Length; j++)
                DataTimestamp.items[j] = dataTimestamp[j];
            StructTimestamp = new StructuralTimestamp(structTimestamp.Length);
            for (int j = 0; j < structTimestamp.Length; j++)
                StructTimestamp.items[j] = structTimestamp[j];
        }

        internal Pointstamp(int dataTimestampSize, int sturctTimestampSize)
        {
            Location = default(int);
            DataTimestamp = new DataTimestamp(dataTimestampSize);
            StructTimestamp = new StructuralTimestamp(sturctTimestampSize);
        }
    }
}
