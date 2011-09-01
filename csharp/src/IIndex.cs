namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary> Interface of object index.
    /// Index is used to provide fast access to the object by key. 
    /// Objects in the index are stored ordered by key value. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects whose key belongs to a specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, DateTime or peristent object type.
    /// </summary>
    public interface IIndex<K, V> : IGenericIndex<K, V> where V : class,IPersistent
    {
        /// <summary>Put new object in the index. 
        /// </summary>
        /// <param name="key">object key wrapper
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// </returns>
        bool Put(Key key, V obj);

        /// <summary>Put new object in the index. 
        /// </summary>
        /// <param name="key">object key value
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// 
        /// </returns>
        bool Put(K key, V obj);

        /// <summary>Associate new value with the key. If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="key">object key wrapper
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
        V Set(Key key, V obj);

        /// <summary>Associate new value with the key. If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="key">object key value
        /// </param>
        /// <param name="obj">object associated with this key. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
        V Set(K key, V obj);

        /// <summary>Remove object with specified key from the tree.
        /// </summary>
        /// <param name="key">wrapper of the value of the key of removed object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
        void Remove(Key key, V obj);

        /// <summary>Remove object with specified key from the tree.
        /// </summary>
        /// <param name="key">value of the key of removed object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
        void Remove(K key, V obj);

        /// <summary>Remove key from the unique index.
        /// </summary>
        /// <param name="key">wrapper of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        V Remove(Key key);

        /// <summary>Remove key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or DatabaseException(DatabaseException.ErrorCode.KEY_NOV_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        V RemoveKey(K key);
    }
}
