namespace Volante 
{ 
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Range boundary kind
    /// </summary>
    public enum BoundaryKind {
        /// <summary>exclusive interval</summary>
        Exclusive = 0,
        /// <summary>inclusive interval</summary>
        Inclusive = 1,
        /// <summary>open interval</summary>
        None = -1
    }

    /// <summary>
    /// Interface of sorted collection.
    /// Sorted collections keeps in members in order specified by comparator.
    /// Members in the collections can be located using key or range of keys.
    /// The SortedCollection is efficient container of objects for in-memory databases.
    /// For databases which size is significatly larger than size of page pool, operation with SortedList
    /// can cause trashing and so very bad performance. Unlike other index structures SortedCollection
    /// doesn't store values of keys and so search in the collection requires fetching all of its members.
    /// </summary>
    public interface SortedCollection<K,V> : IPersistent, IResource, ICollection<V> where V:class,IPersistent
    { 
        /// <summary> Access element by key
        /// </summary>
        V this[K key] 
        {
            get;
        }       

        /// <summary> Access elements by key range
        /// </summary>
        V[] this[K low, K high] 
        {
            get;
        }       

        /// <summary>
        /// Get member with specified key.
        /// </summary>
        /// <param name="key"> specified key. It should match with type of the index and should be inclusive.</param>
        /// <returns> object with this value of the key or <code>null</code> if key not found</returns>
        /// <exception cref="Volante.StorageError">StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the collection with specified value of the key.  
        /// </exception>
        ///
        V Get(K key);

        /// <summary>
        /// Get members which key value belongs to the specified range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the collection.
        /// </summary>
        /// <param name="from"> inclusive low boundary</param>
        /// <param name="till"> inclusive high boundary</param>
        /// <returns> array of objects which keys belongs to the specified interval, ordered by key value</returns>
        ///
        V[] Get(K from, K till);

        /// <summary>
        /// Get members which key value belongs to the specified range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the collection.
        /// </summary>
        /// <param name="from"> low boundary</param>
        /// <param name="fromKind"> kind of low boundary</param>
        /// <param name="till"> high boundary</param>
        /// <param name="tillKind"> kind of high boundary</param>
        /// <returns> array of objects which keys belongs to the specified interval, ordered by key value</returns>
        V[] Get(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);

        /// <summary>
        /// Get number of objects in the collection
        /// </summary>
        /// <returns> number of objects in the collection</returns>
        ///
        int        Size();

        /// <summary>
        /// Get all objects in the index as array ordered by index key.
        /// </summary>
        /// <returns> array of objects in the index ordered by key value</returns>
        ///
        V[] ToArray();

        /// <summary> Get all objects in the index as array of specified type orderd by index key
        /// </summary>
        /// <param name="elemType">type of array element</param>
        /// <returns>array of objects in the index ordered by key value
        /// </returns>
        Array ToArray(Type elemType);

        /// <summary>
        /// Get iterator for traversing collection members  with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> inclusive low boundary</param>
        /// <param name="till"> inclusive high boundary</param>
        /// <returns> selection iterator</returns>
        ///
        IEnumerator<V> GetEnumerator(K from, K till);

        /// <summary>
        /// Get iterator for traversing collection members  with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> low boundary</param>
        /// <param name="fromKind"> kind of low boundary</param>
        /// <param name="till"> high boundary</param>
        /// <param name="tillKind"> kind of till boundary</param>
        /// <returns> selection iterator</returns>
        ///
        IEnumerator<V> GetEnumerator(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);

        /// <summary>
        /// Get enumerable set of collection members with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> inclusive low boundary</param>
        /// <param name="till"> inclusive high boundary</param>
        /// <returns>  enumerable set</returns>
        ///
        IEnumerable<V> Range(K from, K till);

        /// <summary>
        /// Get enumerable set of collection members with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> low boundary</param>
        /// <param name="fromKind"> kind of low boundary</param>
        /// <param name="till"> high boundary</param>
        /// <param name="tillKind"> kind of till boundary</param>
        /// <returns> enumerable set</returns>
        ///
        IEnumerable<V> Range(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);

        /// <summary>
        /// Get comparator used in this collection
        /// </summary>
        /// <returns> collection comparator</returns>
        ///
        PersistentComparator<K,V> GetComparator();
    }
}
