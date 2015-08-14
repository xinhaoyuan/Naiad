using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Util
{
    /// <summary>
    /// A fake array implementation to avoid heap allocation
    /// </summary>
    public struct SArray<T>
    {
        /// <summary>
        /// first coordinate
        /// </summary>
        public T a;

        /// <summary>
        /// second coordinate
        /// </summary>
        public T b;

        /// <summary>
        /// third coordinate
        /// </summary>
        public T c;

        /// <summary>
        /// fourth coordinate
        /// </summary>
        public T d;

        /// <summary>
        /// "length" of array
        /// </summary>
        public int Length;

        /// <summary>
        /// space for anything beyond four coordinates
        /// </summary>
        public T[] spillover;

        /// <summary>
        /// Returns the value at the given <paramref name="index"/>.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>The value at the given <paramref name="index"/>.</returns>
        public T this[int index]
        {
            get
            {
                if (index >= Length)
                    throw new IndexOutOfRangeException();

                switch (index)
                {
                    case 0: return a;
                    case 1: return b;
                    case 2: return c;
                    case 3: return d;
                    default: return spillover[index - 4];
                }
            }
            set
            {
                if (index >= Length)
                    throw new IndexOutOfRangeException();

                switch (index)
                {
                    case 0: a = value; break;
                    case 1: b = value; break;
                    case 2: c = value; break;
                    case 3: d = value; break;
                    default: spillover[index - 4] = value; break;
                }
            }
        }

        /// <summary>
        /// Constructs a FakeArray with the specified size.
        /// </summary>
        /// <param name="size">The size of this FakeArray.</param>
        public SArray(int size)
        {
            Length = size;
            a = b = c = d = default(T);
            if (Length > 4)
                spillover = new T[Length - 4];
            else
                spillover = null;
        }

        /// <summary>
        /// Returns a string representation of this array.
        /// </summary>
        /// <returns>A string representation of this array.</returns>
        public override string ToString()
        {
            if (Length == 0)
            {
                return "[]";
            }
            else
            {
                var result = new StringBuilder().AppendFormat("[{0}", this[0]);
                for (int i = 1; i < this.Length; i++)
                    result.AppendFormat(", {0}", this[i]);
                result.Append("]");
                return result.ToString();
            }
        }
        /// <summary>
        /// Convert this into Enumerable
        /// </summary>
        /// <returns>the IEnumerable</returns>
        public IEnumerable<T> Enumerate()
        {
            if (Length > 0) yield return a;
            if (Length > 1) yield return b;
            if (Length > 2) yield return c;
            if (Length > 3) yield return d;
            for (int i = 4; i < Length; ++i)
            {
                yield return spillover[i - 4];
            }
        }
    }
}
