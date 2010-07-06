using System;
using System.Collections;

namespace Perst
{
    ///<summary>
    /// Interface of objects set
    /// </summary>
    public interface ISet : IPersistent, IResource, ICollection
    {
        /// <summary>
        /// Check if set contains specified element
        /// </summary>
        /// <param name="o">checked element</param>
        /// <returns><code>true</code> if elementis in set</returns>
        bool Contains(IPersistent o);

        /// <summary>
        /// Check if the set contains all members from specified enumerator
        /// </summary>
        /// <param name="e">enumerator specifying members</param>
        /// <returns><code>true</code> if all members of enumerator are present in the set</returns>
        bool ContainsAll(IEnumerator e);

        /// <summary>
        /// Check if the set contains all members from specified collection
        /// </summary>
        /// <param name="e">collection specifying members</param>
        /// <returns><code>true</code> if all members of collection are present in the set</returns>
        bool ContainsAll(IEnumerable e);

        /// <summary>
        /// Add new element to the set
        /// </summary>
        /// <param name="o">element to be added</param>
        /// <returns><code>true</code> if element was successfully added, 
        /// <code>false</code> if element is already in the set</returns>
        bool Add(IPersistent o);

        /// <summary>
        /// Add all elements from specified enumerator to the set
        /// </summary>
        /// <param name="e">enumerator specifying members</param>
        /// <returns><code>true</code> if at least one element was added to the set,
        /// <code>false</code> if now new elements were added</returns>
        bool AddAll(IEnumerator e);

        /// <summary>
        /// Add all elements from specified collection to the set
        /// </summary>
        /// <param name="e">collection specifying members</param>
        /// <returns><code>true</code> if at least one element was added to the set,
        /// <code>false</code> if now new elements were added</returns>
        bool AddAll(IEnumerable e);

        /// <summary>
        /// Remove element from the set
        /// </summary>
        /// <param name="o">removed element</param>
        /// <returns><code>true</code> if element was successfully removed,
        /// <code>false</code> if there is not such element in the set</returns>
        bool Remove(IPersistent o);
    
        /// <summary>
        /// Remove from the set all members from the specified enumerator
        /// </summary>
        /// <param name="c">enumerator specifying members to be removed from the set</param>
        /// <returns></returns>
        bool RemoveAll(IEnumerator e);

        /// <summary>
        /// Remove from the set all members from the specified collection
        /// </summary>
        /// <param name="c">collection specifying members to be removed from the set</param>
        /// <returns></returns>
        bool RemoveAll(IEnumerable e);

        /// <summary>
        /// Copy all set members to an array
        /// </summary>
        /// <returns>array of object with set members</returns>
        IPersistent[] ToArray();
        
        /// <summary>
        /// Copy all set members to an array of specified type
        /// </summary>
        /// <param name="elemType">type of array element</param>
        /// <returns>array of specified type with members of the set</returns>
        Array ToArray(Type elemType);
    }
}
