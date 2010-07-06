namespace Perst
{
    using System;
    using System.Collections;
    using System.Reflection;
	
    /// <summary> Interface of indexed field. 
    /// Index is used to provide fast access to the object by the value of indexed field. 
    /// Objects in the index are stored ordered by the value of indexed field. 
    /// It is possible to select object using exact value of the key or 
    /// select set of objects which key belongs to the specified interval 
    /// (each boundary can be specified or unspecified and can be inclusive or exclusive)
    /// Key should be of scalar, String, DateTime or peristent object type.
    /// </summary>
    public interface FieldIndex : GenericIndex
    {
        /// <summary> 
        /// Check if index contains specified object
        /// </summary>
        /// <param name="obj">object to be searched in the index. Object should contain indexed field. 
        /// </param>
        /// <returns><code>true</code> if object is present in the index, <code>false</code> otherwise
        /// </returns>
        bool Contains(IPersistent obj);

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
        bool Put(IPersistent obj);

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
        IPersistent Set(IPersistent obj);

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
        void Append(IPersistent obj);


        /// <summary> Remove object from the index
        /// </summary>
        /// <param name="obj">object removed from the index. Object should contain indexed field. 
        /// </param>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index
        /// 
        /// </exception>
        void  Remove(IPersistent obj);

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">wrapper of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        IPersistent Remove(Key key);

        /// <summary> Remove object with specified key from the unique index.
        /// </summary>
        /// <param name="key">value of removed key
        /// </param>
        /// <returns>removed object</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such key in the index,
        /// or StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE) if index is not unique.
        /// 
        /// </exception>
        IPersistent Remove(object key);

        /// <summary>
        /// Get class obejct objects which can be inserted in this index
        /// </summary>
        /// <returns>class specified in Storage.createFielIndex method</returns>
        Type IndexedClass{get;}

        /// <summary>
        /// Get fields used as a key
        /// </summary>
        /// <returns>array of index key fields</returns>
        MemberInfo[] KeyFields{get;}
   }
}