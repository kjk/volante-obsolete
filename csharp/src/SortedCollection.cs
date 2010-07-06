namespace Perst 
{ 
    using System;
    using System.Collections;

    /// <summary>
    /// Interface of sorted collection.
    /// Sorted collections keeps in members in order specified by comparator.
    /// Members in the collections can be located using key or range of keys.
    /// The SortedCollection is efficient container of objects for in-memory databases.
    /// For databases which size is significatly larger than size of page pool, operation with SortedList
    /// can cause trashing and so very bad performance. Unlike other index structures SortedCollection
    /// doesn't store values of keys and so search in the collection requires fetching of its members.
    /// </summary>
    public interface SortedCollection : IPersistent, IResource, IEnumerable, ICollection
    { 
        /// <summary> Access element by key
        /// </summary>
        IPersistent this[object key] 
        {
            get;
        }       

        /// <summary> Access elements by key range
        /// </summary>
        IPersistent[] this[object low, object high] 
        {
            get;
        }       

        /// <summary>
        /// Get member with specified key.
        /// </summary>
        /// <param name="key"> specified key. It should match with type of the index and should be inclusive.</param>
        /// <returns> object with this value of the key or <code>null</code> if key nmot found</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the collection with specified value of the key.  
        /// </exception>
        ///
        IPersistent get(Object key);

        /// <summary>
        /// Get members which key value belongs to the specified range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the collection.
        /// </summary>
        /// <param name="from"> low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive. </param>
        /// <param name="till"> high boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive. </param>
        /// <returns> array of objects which keys belongs to the specified interval, ordered by key value</returns>
        ///
        IPersistent[] get(Object from, Object till);

        /// <summary>
        /// Add new member to collection
        /// </summary>
        /// <param name="obj"> new member</param>
        /// <returns> <code>true</code> if object is successfully added in the index, 
        /// <code>false</code> if collection was declared as unique and there is already member with such value
        /// of the key in the collection. </returns>
        ///
        bool       add(IPersistent obj);

        /// <summary>
        /// Check if collections contains specified member
        /// </summary>
        /// <returns> <code>true</code> if specified member belongs to the collection</returns>
        ///
        bool       contains(IPersistent member);

        /// <summary>
        /// Remove member from collection
        /// </summary>
        /// <param name="obj"> member to be removed</param>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the collection</exception>
        ///
        void       remove(IPersistent obj);

        /// <summary>
        /// Get number of objects in the collection
        /// </summary>
        /// <returns> number of objects in the collection</returns>
        ///
        int        size();
    
        /// <summary>
        /// Remove all objects from the collection
        /// </summary>
        ///
        void       clear();
        /// <summary>
        /// Get all objects in the index as array ordered by index key.
        /// </summary>
        /// <returns> array of objects in the index ordered by key value</returns>
        ///
        IPersistent[] ToArray();

        /// <summary> Get all objects in the index as array of specified type orderd by index key
        /// </summary>
        /// <param name="elemType">type of array element</param>
        /// <returns>array of objects in the index ordered by key value
        /// </returns>
        Array ToArray(Type elemType);

        /// <summary>
        /// Get iterator for traversing collection members  with key belonging to the specified range. 
        /// </summary>
        /// <param name="from"> low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till"> high boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive. </param>
        /// <returns> selection iterator</returns>
        ///
        IEnumerator GetEnumerator(Key from, Key till);

        /// <summary>
        /// Get comparator used in this collection
        /// </summary>
        /// <returns> collection comparator</returns>
        ///
        PersistentComparator getComparator();
    }
}
