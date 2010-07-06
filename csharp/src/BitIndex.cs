namespace Perst
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif

    /// <summary>
    /// Interface of bit index.
    /// Bit index allows to effiicently search object with specified 
    /// set of properties. Each object has associated mask of 32 bites. 
    /// Meaning of bits is application dependent. Usually each bit stands for
    /// some binary or boolean property, for example "sex", but it is possible to 
    /// use group of bits to represent enumerations with more possible values.
    /// </summary>
#if USE_GENERICS
    public interface BitIndex<T> : IPersistent, IResource, ICollection<T> where T:class,IPersistent
#else
    public interface BitIndex : IPersistent, IResource, ICollection 
#endif
    { 
        /// <summary>
        /// Get properties of specified object
        /// </summary>
        /// <param name="obj">object which properties are requested</param>
        /// <returns>bit mask associated with this objects</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such object in the index
        /// </exception>
#if USE_GENERICS
        int Get(T obj);
#else 
        int Get(IPersistent obj);
#endif

        /// <summary>
        /// Put new object in the index. If such objct already exists in index, then its
        /// mask will be rewritten 
        /// </summary>
        /// <param name="obj">object to be placed in the index. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <param name="mask">bit mask associated with this objects</param>
#if USE_GENERICS
        void Put(T obj, int mask);
#else
        void Put(IPersistent obj, int mask);
#endif


        /// <summary> Access object bitmask
        /// </summary>
#if USE_GENERICS
        int this[T obj] 
#else
        int this[IPersistent obj] 
#endif
        {
            get;
            set;
        }       

#if !USE_GENERICS
        /// <summary>
        /// Remove object from the index 
        /// </summary>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <returns><code>true</code> if member was successfully removed or <code>false</code> if member is not found</returns>
        bool Remove(IPersistent obj);
#endif

        /// <summary> Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// 
        /// </returns>
        int Size();

#if !USE_GENERICS
        /// <summary> Remove all objects from the index
        /// </summary>
        void Clear();
#endif

        /// <summary>
        /// Get enumerator for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerator</returns>
#if USE_GENERICS
        IEnumerator<T> GetEnumerator(int setBits, int clearBits);
#else
        IEnumerator GetEnumerator(int setBits, int clearBits);
#endif

        /// <summary>
        /// Get enumerable collection for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerable collection</returns>
#if USE_GENERICS
        IEnumerable<T> Select(int setBits, int clearBits);
#else 
        IEnumerable Select(int setBits, int clearBits);
#endif
    }
}

