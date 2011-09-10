namespace Volante
{
    using System;
    using System.Collections.Generic;
    using System.Collections;
    using System.Reflection;

    /// <summary> Interface of indexed field. 
    /// Index is used to provide fast access to the object by the value of indexed field. 
    /// Objects in the index are stored ordered by the value of indexed field. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects whose key belongs to a specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, DateTime or peristent object type.
    /// </summary>
    public interface IFieldIndex<K, V> : IGenericIndex<K, V> where V : class,IPersistent
    {

        /// <summary> Put new object in the index. 
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field. 
        /// Object can be not yet persistent, in this case its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// 
        /// </returns>
        bool Put(V obj);

        /// <summary>
        /// Associate new object with the key specified by object field value. 
        /// If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field. 
        /// Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
        V Set(V obj);

        /// <summary>
        /// Assign to the integer indexed field unique auto-icremented value and 
        /// insert object in the index. 
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field
        /// of integer (<code>int</code> or <code>long</code>) type.
        /// This field is assigned unique value (which will not be reused while 
        /// this index exists) and object is marked as modified.
        /// Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <exception cref="Volante.DatabaseException"><code>DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE)</code> 
        /// is thrown when indexed field has type other than <code>int</code> or <code>long</code></exception>
        void Append(V obj);

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">wrapper of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        V Remove(Key key);

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        V RemoveKey(K key);

        /// <summary>
        /// Get class object objects which can be inserted in this index
        /// </summary>
        /// <returns>class specified in IDatabase.CreateFielIndex method</returns>
        Type IndexedClass { get; }

        /// <summary>
        /// Get key field
        /// </summary>
        /// <returns>field info for key field</returns>
        MemberInfo KeyField { get; }
    }

    /// <summary> Interface of multifield index. 
    /// </summary>
    public interface IMultiFieldIndex<V> : IFieldIndex<object[], V> where V : class,IPersistent
    {
        /// <summary>
        /// Get fields used as a key
        /// </summary>
        /// <returns>array of index key fields</returns>
        MemberInfo[] KeyFields { get; }
    }
}