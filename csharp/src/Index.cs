namespace Perst
{
    using System;
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
    public interface Index : IPersistent, IResource, IEnumerable
    {
        /// <summary> Get object by key (exact match)     
        /// </summary>
        /// <param name="key">specified key. It should match with type of the index and should be inclusive.
        /// </param>
        /// <returns>object with this value of the key or <code>null</code> if key nmot found
        /// </returns>
        /// <exception cref="StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) exception if there are more than 
        /// one objects in the index with specified value of the key.
        /// 
        /// </exception>
        IPersistent get(Key key);
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
        IPersistent[] get(Key from, Key till);
        /// <summary> Put new object in the index. 
        /// </summary>
        /// <param name="key">object key
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// 
        /// </returns>
        bool put(Key key, IPersistent obj);
        /// <summary> Associate new value with the key. If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="key">object key
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        void set(Key key, IPersistent obj);
        /// <summary> Remove object with specified key from the tree.
        /// </summary>
        /// <param name="key">value of the key of removed object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
        void  remove(Key key, IPersistent obj);
        /// <summary> Remove key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <exception cref="StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        void  remove(Key key);
        /// <summary> Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// 
        /// </returns>
        int size();
        /// <summary> Remove all objects from the index
        /// </summary>
        void  clear();
        /// <summary> Get all objects in the index as array orderd by index key
        /// </summary>
        /// <returns>array of objects in the index ordered by key value
        /// 
        /// </returns>
        IPersistent[] toArray();

        /// <summary>
        /// Get iterator for traversing objects in the index with key belonging to the specified range. 
        /// You should not update/remove or add members to the index during iteration
        /// </summary>
        /// <param name="from">Low boundary. If <code>null</code> then low boundary is not specified.
        /// Low boundary can be inclusive or exclusive.</param>
        /// <param name="till">High boundary. If <code>null</code> then high boundary is not specified.
        /// High boundary can be inclusive or exclusive.</param>
        /// <param name="order"><code>AscanrOrder</code> or <code>DescentOrder</code></param>
        /// <returns>selection iterator</returns>
        ///
        IEnumerator GetEnumerator(Key from, Key till, IterationOrder order);
    }
}