namespace Perst
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
	    
    /// <summary> Interface of object index.
    /// Index is used to provide fast access to the object by key. 
    /// Object in the index are stored ordered by key value. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects which key belongs to the specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, java.util.Date or peristent object type.
    /// </summary>
#if USE_GENERICS
    public interface Index<K,V> : GenericIndex<K,V> where V:class,IPersistent
#else
    public interface Index : GenericIndex
#endif
    {    
        /// <summary> Put new object in the index. 
        /// </summary>
        /// <param name="key">object key wrapper
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// </returns>
#if USE_GENERICS
        bool Put(Key key, V obj);
#else
        bool Put(Key key, IPersistent obj);
#endif

        /// <summary> Put new object in the index. 
        /// </summary>
        /// <param name="key">object key value
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// 
        /// </returns>
#if USE_GENERICS
        bool Put(K key, V obj);
#else
        bool Put(object key, IPersistent obj);
#endif

        /// <summary> Associate new value with the key. If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="key">object key wrapper
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
#if USE_GENERICS
        V Set(Key key, V obj);
#else
        IPersistent Set(Key key, IPersistent obj);
#endif

        /// <summary> Associate new value with the key. If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="key">object key value
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
#if USE_GENERICS
        V Set(K key, V obj);
#else
        IPersistent Set(object key, IPersistent obj);
#endif

        /// <summary> Remove object with specified key from the tree.
        /// </summary>
        /// <param name="key">wrapper of the value of the key of removed object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
#if USE_GENERICS
        void  Remove(Key key, V obj);
#else
        void  Remove(Key key, IPersistent obj);
#endif

        /// <summary> Remove object with specified key from the tree.
        /// </summary>
        /// <param name="key">value of the key of removed object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
#if USE_GENERICS
        void  Remove(K key, V obj);
#else
        void  Remove(object key, IPersistent obj);
#endif

        /// <summary> Remove key from the unique index.
        /// </summary>
        /// <param name="key">wrapper of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
#if USE_GENERICS
        V  Remove(Key key);
#else
        IPersistent  Remove(Key key);
#endif

        /// <summary> Remove key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOV_UNIQUE) if index is not unique.
        /// 
        /// </exception>
#if USE_GENERICS
        V RemoveKey(K key);
#else
        IPersistent  Remove(object key);
#endif
    }
}