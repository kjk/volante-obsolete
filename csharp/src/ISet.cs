using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif

namespace Perst
{
    ///<summary>
    /// Interface of objects set
    /// </summary>
#if USE_GENERICS
    public interface ISet<T> : IPersistent, IResource, ICollection<T> where T:class,IPersistent
#else
    public interface ISet : IPersistent, IResource, ICollection
#endif
    {
#if !USE_GENERICS
        /// <summary>
        /// Check if set contains specified element
        /// </summary>
        /// <param name="o">checked element</param>
        /// <returns><code>true</code> if elementis in set</returns>
        bool Contains(IPersistent o);
#endif

        /// <summary>
        /// Check if the set contains all members from specified collection
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns><code>true</code> if all members of enumerator are present in the set</returns>
#if USE_GENERICS
        bool ContainsAll(ICollection<T> c);
#else
        bool ContainsAll(ICollection c);
#endif

#if !USE_GENERICS
        /// <summary>
        /// Add new element to the set
        /// </summary>
        /// <param name="o">element to be added</param>
        void Add(IPersistent o);
#endif

        /// <summary>
        /// Add all elements from specified collection to the set
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns><code>true</code> if at least one element was added to the set,
        /// <code>false</code> if now new elements were added</returns>
#if USE_GENERICS
        bool AddAll(ICollection<T> c);
#else
        bool AddAll(ICollection c);
#endif

#if !USE_GENERICS
        /// <summary> 
        /// Remove element from the set
        /// </summary>
        /// <param name="o">removed element</param>
        /// <returns><code>true</code> if element was successfully removed,
        /// <code>false</code> if there is not such element in the set</returns>
        bool Remove(IPersistent o);
#endif
    
        /// <summary>
        /// Remove from the set all members from the specified enumerator
        /// </summary>
        /// <param name="c">collection specifying members</param>
        /// <returns></returns>
#if USE_GENERICS
        bool RemoveAll(ICollection<T> c);
#else
        bool RemoveAll(ICollection c);
#endif

        /// <summary>
        /// Copy all set members to an array
        /// </summary>
        /// <returns>array of object with set members</returns>
#if USE_GENERICS
        T[] ToArray();
#else
        IPersistent[] ToArray();
#endif
        
        /// <summary>
        /// Copy all set members to an array of specified type
        /// </summary>
        /// <param name="elemType">type of array element</param>
        /// <returns>array of specified type with members of the set</returns>
        Array ToArray(Type elemType);

#if !USE_GENERICS
        /// <summary>
        /// Remove all set members
        /// </summary>
        void Clear();
#endif
    }
}
