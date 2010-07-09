namespace NachoDB 
{ 
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif

    /// <summary>
    /// Range boundary kind
    /// </summary>
    public enum BoundaryKind { 
         Exclusive = 0,
         Inclusive = 1, 
         None = -1 // open interval
    }

    /// <summary>
    /// Interface of sorted collection.
    /// Sorted collections keeps in members in order specified by comparator.
    /// Members in the collections can be located using key or range of keys.
    /// The SortedCollection is efficient container of objects for in-memory databases.
    /// For databases which size is significatly larger than size of page pool, operation with SortedList
    /// can cause trashing and so very bad performance. Unlike other index structures SortedCollection
    /// doesn't store values of keys and so search in the collection requires fetching of its members.
    /// </summary>
#if USE_GENERICS
    public interface SortedCollection<K,V> : IPersistent, IResource, ICollection<V> where V:class,IPersistent
#else
    public interface SortedCollection : IPersistent, IResource, ICollection
#endif
    { 
        /// <summary> Access element by key
        /// </summary>
#if USE_GENERICS
        V this[K key] 
#else
        IPersistent this[object key] 
#endif
        {
            get;
        }       

        /// <summary> Access elements by key range
        /// </summary>
#if USE_GENERICS
        V[] this[K low, K high] 
#else
        IPersistent[] this[object low, object high] 
#endif
        {
            get;
        }       

        /// <summary>
        /// Get member with specified key.
        /// </summary>
        /// <param name="key"> specified key. It should match with type of the index and should be inclusive.</param>
        /// <returns> object with this value of the key or <code>null</code> if key nmot found</returns>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the collection with specified value of the key.  
        /// </exception>
        ///
#if USE_GENERICS
        V Get(K key);
#else
        IPersistent Get(object key);
#endif

        /// <summary>
        /// Get members which key value belongs to the specified range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the collection.
        /// </summary>
        /// <param name="from"> inclusive low boundary</param>
        /// <param name="till"> inclusive high boundary</param>
        /// <returns> array of objects which keys belongs to the specified interval, ordered by key value</returns>
        ///
#if USE_GENERICS
        V[] Get(K from, K till);
#else
        IPersistent[] Get(object from, object till);
#endif

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
#if USE_GENERICS
        V[] Get(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);
#else
        IPersistent[] Get(object from, BoundaryKind fromKind, object till, BoundaryKind tillKind);
#endif

#if !USE_GENERICS
        /// <summary>
        /// Add new member to collection
        /// </summary>
        /// <param name="obj"> new member</param>
        void Add(IPersistent obj);
#endif
 
#if !USE_GENERICS
       /// <summary>
        /// Check if collections contains specified member
        /// </summary>
        /// <returns> <code>true</code> if specified member belongs to the collection</returns>
        ///
        bool       Contains(IPersistent member);
#endif

#if !USE_GENERICS
        /// <summary>
        /// Remove member from collection
        /// </summary>
        /// <param name="obj"> member to be removed</param>
        /// <returns><code>true</code> if member was successfully removed or <code>false</code> if member is not found</returns>
        ///
        bool       Remove(IPersistent obj);
#endif

        /// <summary>
        /// Get number of objects in the collection
        /// </summary>
        /// <returns> number of objects in the collection</returns>
        ///
        int        Size();
    
#if !USE_GENERICS
        /// <summary>
        /// Remove all objects from the collection
        /// </summary>
        ///
        void       Clear();
#endif

        /// <summary>
        /// Get all objects in the index as array ordered by index key.
        /// </summary>
        /// <returns> array of objects in the index ordered by key value</returns>
        ///
#if USE_GENERICS
        V[] ToArray();
#else
        IPersistent[] ToArray();
#endif

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
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(K from, K till);
#else
        IEnumerator GetEnumerator(object from, object till);
#endif

        /// <summary>
        /// Get iterator for traversing collection members  with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> low boundary</param>
        /// <param name="fromKind"> kind of low boundary</param>
        /// <param name="till"> high boundary</param>
        /// <param name="tillKind"> kind of till boundary</param>
        /// <returns> selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);
#else
        IEnumerator GetEnumerator(object from, BoundaryKind fromKind, object till, BoundaryKind tillKind);
#endif

        /// <summary>
        /// Get enumerable set of collection members with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> inclusive low boundary</param>
        /// <param name="till"> inclusive high boundary</param>
        /// <returns>  enumerable set</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(K from, K till);
#else
        IEnumerable Range(object from, object till);
#endif

        /// <summary>
        /// Get enumerable set of collection members with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> low boundary</param>
        /// <param name="fromKind"> kind of low boundary</param>
        /// <param name="till"> high boundary</param>
        /// <param name="tillKind"> kind of till boundary</param>
        /// <returns> enumerable set</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind);
#else
        IEnumerable Range(object from, BoundaryKind fromKind, object till, BoundaryKind tillKind);
#endif

        /// <summary>
        /// Get comparator used in this collection
        /// </summary>
        /// <returns> collection comparator</returns>
        ///
#if USE_GENERICS
        PersistentComparator<K,V> GetComparator();
#else
        PersistentComparator GetComparator();
#endif
    }
}
