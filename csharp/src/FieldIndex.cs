namespace Perst
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
    using System.Reflection;
	
    /// <summary> Interface of indexed field. 
    /// Index is used to provide fast access to the object by the value of indexed field. 
    /// Objects in the index are stored ordered by the value of indexed field. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects which key belongs to the specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, DateTime or peristent object type.
    /// </summary>
#if USE_GENERICS
    public interface FieldIndex<K,V> : GenericIndex<K,V> where V:class,IPersistent
#else
    public interface FieldIndex : GenericIndex
#endif
    {
#if !USE_GENERICS
        /// <summary> 
        /// Check if index contains specified object
        /// </summary>
        /// <param name="obj">object to be searched in the index. Object should contain indexed field. 
        /// </param>
        /// <returns><code>true</code> if object is present in the index, <code>false</code> otherwise
        /// </returns>
        bool Contains(IPersistent obj);
#endif

        /// <summary> Put new object in the index. 
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field. 
        /// Object can be not yet persistent, in this case its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns><code>true</code> if object is successfully inserted in the index, 
        /// <code>false</code> if index was declared as unique and there is already object with such value
        /// of the key in the index. 
        /// 
        /// </returns>
#if USE_GENERICS
        bool Put(V obj);
#else
        bool Put(IPersistent obj);
#endif

        /// <summary>
        /// Associate new object with the key specified by object field value. 
        /// If there is already object with such key in the index, 
        /// then it will be removed from the index and new value associated with this key.
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field. 
        /// Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <returns>object previously associated with this key, <code>null</code> if there was no such object
        /// </returns>
#if USE_GENERICS
        V Set(V obj);
#else
        IPersistent Set(IPersistent obj);
#endif

        /// <summary>
        /// Assign to the integer indexed field unique autoicremented value and 
        /// insert object in the index. 
        /// </summary>
        /// <param name="obj">object to be inserted in index. Object should contain indexed field
        /// of integer (<code>int</code> or <code>long</code>) type.
        /// This field is assigned unique value (which will not be reused while 
        /// this index exists) and object is marked as modified.
        /// Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <exception cref="Perst.StorageError"><code>StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE)</code> 
        /// is thrown when indexed field has type other than <code>int</code> or <code>long</code></exception>
#if USE_GENERICS
        void Append(V obj);
#else
        void Append(IPersistent obj);
#endif

#if !USE_GENERICS
        /// <summary> Remove object from the index
        /// </summary>
        /// <param name="obj">object removed from the index. Object should contain indexed field. 
        /// </param>
        /// <returns><code>true</code> if member was successfully removed or <code>false</code> if member is not found</returns>
        bool Remove(IPersistent obj);
#endif

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">wrapper of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
#if USE_GENERICS
        V Remove(Key key);
#else
        IPersistent Remove(Key key);
#endif

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
#if USE_GENERICS
        V RemoveKey(K key);
#else
        IPersistent Remove(object key);
#endif

        /// <summary>
        /// Get class obejct objects which can be inserted in this index
        /// </summary>
        /// <returns>class specified in Storage.createFielIndex method</returns>
        Type IndexedClass{get;}

        /// <summary>
        /// Get key field
        /// </summary>
        /// <returns>field info for key field</returns>
        MemberInfo KeyField{get;}
    }

    /// <summary> Interface of multifield index. 
    /// </summary>
#if USE_GENERICS
    public interface MultiFieldIndex<V> : FieldIndex<object[],V> where V:class,IPersistent
#else
    public interface MultiFieldIndex : FieldIndex
#endif
    {
        /// <summary>
        /// Get fields used as a key
        /// </summary>
        /// <returns>array of index key fields</returns>
        MemberInfo[] KeyFields{get;}
    }
}