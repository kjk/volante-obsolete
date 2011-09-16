using System;
using System.Collections;
using System.Collections.Generic;

namespace Volante
{
    ///<summary>
    /// Interface of objects set
    /// </summary>
    public interface ISet<T> : IPersistent, IResource, ICollection<T> where T : class,IPersistent
    {
        /// <summary>
        /// Check if the set contains all members from specified collection
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns><code>true</code> if all members of enumerator are present in the set</returns>
        bool ContainsAll(ICollection<T> c);

        /// <summary>
        /// Add all elements from specified collection to the set
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns><code>true</code> if at least one element was added to the set,
        /// <code>false</code> if now new elements were added</returns>
        bool AddAll(ICollection<T> c);

        /// <summary>
        /// Remove from the set all members from the specified enumerator
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns></returns>
        bool RemoveAll(ICollection<T> c);

        /// <summary>
        /// Copy all set members to an array
        /// </summary>
        /// <returns>array of object with set members</returns>
        T[] ToArray();
    }
}
