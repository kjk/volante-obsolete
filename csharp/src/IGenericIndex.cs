namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public enum IterationOrder
    {
        AscentOrder,
        DescentOrder
    }

    /// <summary> Interface of object index.
    /// Index is used to provide fast access to the object by key. 
    /// Object in the index are stored ordered by key value. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects whose key belongs to the specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, DateTime or peristent object type.
    /// </summary>
    public interface IGenericIndex { }

    public interface IGenericIndex<K, V> : IPersistent, IResource, ICollection<V>, IGenericIndex where V : class,IPersistent
    {
        /// <summary> Access element by key
        /// </summary>
        V this[K key]
        {
            get;
            set;
        }

        /// <summary> Get objects which key value belongs to the specified range.
        /// </summary>
        V[] this[K from, K till]
        {
            get;
        }

        /// <summary> Get object by key (exact match)
        /// </summary>
        /// <param name="key">wrapper of the specified key. It should match with type of the index and should be inclusive.
        /// </param>
        /// <returns>object with this value of the key or <code>null</code> if key not found
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the index with specified value of the key.
        /// 
        /// </exception>
        V Get(Key key);

        /// <summary> Get object by key (exact match)     
        /// </summary>
        /// <param name="key">specified key value. It should match with type of the index and should be inclusive.
        /// </param>
        /// <returns>object with this value of the key or <code>null</code> if key not found
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the index with specified value of the key.
        /// 
        /// </exception>
        V Get(K key);

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
        V[] Get(Key from, Key till);

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
        V[] Get(K from, K till);

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
        IEnumerator<V> GetEnumerator(Key from, Key till, IterationOrder order);

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
        IEnumerator<V> GetEnumerator(K from, K till, IterationOrder order);

        /// <summary>
        /// Get iterator for traversing objects in ascent order belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <returns>selection iterator</returns>
        ///
        IEnumerator<V> GetEnumerator(Key from, Key till);

        /// <summary>
        /// Get iterator for traversing objects in ascent order belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <returns>selection iterator</returns>
        IEnumerator<V> GetEnumerator(K from, K till);

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
        IEnumerable<V> Range(Key from, Key till, IterationOrder order);

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
        IEnumerable<V> Range(Key from, Key till);

        /// <summary>
        /// Get enumerable collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Inclusive low boundary. If <code>null</code> then low boundary is not specified.</param>
        /// <param name="till">Inclusive high boundary. If <code>null</code> then high boundary is not specified.</param>
        /// <param name="order"><code>IterationOrder.AscentOrder</code> or <code>IterationOrder.DescentOrder</code></param>
        /// <returns>enumerable collection</returns>
        ///
        IEnumerable<V> Range(K from, K till, IterationOrder order);

        /// <summary>
        /// Get enumerable ascent ordered collection of objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Inclusive low boundary. If <code>null</code> then low boundary is not specified.</param>
        /// <param name="till">Inclusive high boundary. If <code>null</code> then high boundary is not specified.</param>
        /// <returns>enumerable collection</returns>
        ///
        IEnumerable<V> Range(K from, K till);

        /// <summary>
        /// Get enumerable collection of objects in descending order
        /// </summary>
        /// <returns>enumerable collection</returns>
        ///
        IEnumerable<V> Reverse();

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

        /// <summary> Get objects whose key starts with specifid prefix.
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>array of objects which key starts with specifid prefix, ordered by key value 
        /// </returns>
        V[] GetPrefix(string prefix);

        /// <summary>Get iterator for traversing objects in ascending order whose key starts with specified prefix. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>selection iterator</returns>
        ///
        IEnumerator<V> GetEnumerator(string prefix);

        /// <summary>
        /// Get enumerable in ascending ordered of objects whose key starts with specified prefix. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        /// <returns>enumerable collection</returns>
        IEnumerable<V> StartsWith(string prefix);

        /// <summary> 
        /// Locate all objects whose key is prefix of a specified word.
        /// </summary>
        /// <param name="word">string whose prefixes are located in index</param>
        /// <returns>array of objects whose key is prefix of specified word, ordered by key value
        /// </returns>
        V[] PrefixSearch(string word);

        /// <summary> Get all objects in the index as array orderd by index key
        /// </summary>
        /// <returns>array of objects in the index ordered by key value
        /// </returns>
        V[] ToArray();

        /// <summary>
        /// Get type of index key
        /// </summary>
        /// <returns>type of index key</returns>
        Type KeyType { get; }
    }
}
