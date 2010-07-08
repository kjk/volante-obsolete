namespace NachoDB
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#endif
    using System.Collections;

    public enum IterationOrder 
    {
        AscentOrder, 
        DescentOrder
    };
    
    /// <summary> Interface of object index.
    /// Index is used to provide fast access to the object by key. 
    /// Object in the index are stored ordered by key value. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects which key belongs to the specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, java.util.Date or peristent object type.
    /// </summary>
#if USE_GENERICS
    public interface GenericIndex {}

    public interface GenericIndex<K,V> : IPersistent, IResource, ICollection<V>, GenericIndex where V:class,IPersistent
#else
    public interface GenericIndex : IPersistent, IResource, ICollection
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
            set;
        }       

        /// <summary> Get objects which key value belongs to the specified range.
        /// </summary>
#if USE_GENERICS
        V[] this[K from, K till] 
#else
        IPersistent[] this[object from, object till] 
#endif
        {
            get;
        }       

        /// <summary> Get object by key (exact match)     
        /// </summary>
        /// <param name="key">wrapper of the specified key. It should match with type of the index and should be inclusive.
        /// </param>
        /// <returns>object with this value of the key or <code>null</code> if key nmot found
        /// </returns>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the index with specified value of the key.
        /// 
        /// </exception>
#if USE_GENERICS
        V Get(Key key);
#else
        IPersistent Get(Key key);
#endif

        /// <summary> Get object by key (exact match)     
        /// </summary>
        /// <param name="key">specified key value. It should match with type of the index and should be inclusive.
        /// </param>
        /// <returns>object with this value of the key or <code>null</code> if key nmot found
        /// </returns>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the index with specified value of the key.
        /// 
        /// </exception>
#if USE_GENERICS
        V Get(K key);
#else
        IPersistent Get(object key);
#endif

        /// <summary> Get objects which key value belongs to the specified range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the index.
        /// </summary>
        /// <param name="from">low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive. 
        /// </param>
        /// <param name="till">high boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive. 
        /// </param>
        /// <returns>array of objects which keys belongs to the specified interval, ordered by key value
        /// 
        /// </returns>
#if USE_GENERICS
        V[] Get(Key from, Key till);
#else
        IPersistent[] Get(Key from, Key till);
#endif

        /// <summary> Get objects which key value belongs to the specified inclusive range.
        /// Either from boundary, either till boundary either both of them can be <code>null</code>.
        /// In last case the method returns all objects from the index.
        /// </summary>
        /// <param name="from">Inclusive low boundary. If <code>null</code> then low boundary is not specified.
        /// </param>
        /// <param name="till">Inclusive high boundary. If <code>null</code> then high boundary is not specified.
        /// </param>
        /// <returns>array of objects which keys belongs to the specified interval, ordered by key value
        /// 
        /// </returns>
#if USE_GENERICS
        V[] Get(K from, K till);
#else
        IPersistent[] Get(object from, object till);
#endif

        /// <summary> Get objects which key starts with specifid prefix.
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>array of objects which key starts with specifid prefix, ordered by key value 
        /// </returns>
#if USE_GENERICS
        V[] GetPrefix(string prefix);
#else
        IPersistent[] GetPrefix(string prefix);
#endif

        /// <summary> 
        /// Locate all objects which key is prefix of specified word.
        /// </summary>
        /// <param name="word">string which prefixes are located in index</param>
        /// <returns>array of objects which key is prefix of specified word, ordered by key value
        /// </returns>
#if USE_GENERICS
        V[] PrefixSearch(string word);
#else
        IPersistent[] PrefixSearch(string word);
#endif

        /// <summary> Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// </returns>
        int Size();

#if !USE_GENERICS
        /// <summary> Remove all objects from the index
        /// </summary>
        void  Clear();
#endif

        /// <summary> Get all objects in the index as array orderd by index key
        /// </summary>
        /// <returns>array of objects in the index ordered by key value
        /// </returns>
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
        /// Get iterator for traversing objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(Key from, Key till, IterationOrder order);
#else
        IEnumerator GetEnumerator(Key from, Key till, IterationOrder order);
#endif

        /// <summary>
        /// Get iterator for traversing objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(K from, K till, IterationOrder order);
#else
        IEnumerator GetEnumerator(object from, object till, IterationOrder order);
#endif

        /// <summary>
        /// Get iterator for traversing objects in ascent order belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(Key from, Key till);
#else
        IEnumerator GetEnumerator(Key from, Key till);
#endif

        /// <summary>
        /// Get iterator for traversing objects in ascent order belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(K from, K till);
#else
        IEnumerator GetEnumerator(object from, object till);
#endif

        /// <summary>
        /// Get iterator for traversing objects in ascent order which key starts with specified prefix. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>selection iterator</returns>
        ///
#if USE_GENERICS
        IEnumerator<V> GetEnumerator(string prefix);
#else
        IEnumerator GetEnumerator(string prefix);
#endif

        /// <summary>
        /// Get enumerable collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(Key from, Key till, IterationOrder order);
#else
        IEnumerable Range(Key from, Key till, IterationOrder order);
#endif

        /// <summary>
        /// Get enumerable ascent ordered collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(Key from, Key till);
#else
        IEnumerable Range(Key from, Key till);
#endif

        /// <summary>
        /// Get enumerable collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Inclusive low boundary. If <code>null</code> then low boundary is not specified.</param>
        /// <param name="till">Inclusive high boundary. If <code>null</code> then high boundary is not specified.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(K from, K till, IterationOrder order);
#else
        IEnumerable Range(object from, object till, IterationOrder order);
#endif

        /// <summary>
        /// Get enumerable collection of objects in descending order
        /// </summary>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Reverse();
#else
        IEnumerable Reverse();
#endif
        
        /// <summary>
        /// Get enumerable ascent ordered collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Inclusive low boundary. If <code>null</code> then low boundary is not specified.</param>
        /// <param name="till">Inclusive high boundary. If <code>null</code> then high boundary is not specified.</param>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> Range(K from, K till);
#else
        IEnumerable Range(object from, object till);
#endif

        /// <summary>
        /// Get enumerable ascent ordered collection of objects in the index which key starts with specified prefix. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>enumerable collection</returns>
        ///
#if USE_GENERICS
        IEnumerable<V> StartsWith(string prefix);
#else
        IEnumerable StartsWith(string prefix);
#endif

        /// <summary>
        /// Get iterator for traversing all entries in the index 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <returns>entry iterator</returns>
        ///
        IDictionaryEnumerator GetDictionaryEnumerator();
        
        /// <summary>
        /// Get iterator for traversing entries in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>AscanrOrder</code> or <code>DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
        IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order);

        /// <summary>
        /// Get type of index key
        /// </summary>
        /// <returns>type of index key</returns>
        Type KeyType {get;}
    }
}
