namespace Perst
{
    using System;
    using System.Collections;

    /// <summary>
    /// Interface of bit index.
    /// Bit index allows to effiicently search object with specified 
    /// set of properties. Each object has associated mask of 32 bites. 
    /// Meaning of bits is application dependent. Usually each bit stands for
    /// some binary or boolean property, for example "sex", but it is possible to 
    /// use group of bits to represent enumerations with more possible values.
    /// </summary>
    public interface BitIndex : IPersistent, IResource, IEnumerable, ICollection 
    { 
        /// <summary>
        /// Get properties of specified object
        /// </summary>
        /// <param name="obj">object which properties are requested</param>
        /// <returns>bit mask associated with this objects</returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such object in the index
        /// </exception>
        int Get(IPersistent obj);

        /// <summary>
        /// Put new object in the index. If such objct already exists in index, then its
        /// mask will be rewritten 
        /// </summary>
        /// <param name="obj">object to be placed in the index. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        /// <param name="mask">bit mask associated with this objects</param>
        void Put(IPersistent obj, int mask);


        /// <summary> Access object bitmask
        /// </summary>
        int this[IPersistent obj] 
        {
            get;
            set;
        }       


        /// <summary>
        /// Remove object from the index 
        /// </summary>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.KEY_NOT_FOUND) exception if there is no such object in the index
        /// </exception>
        void Remove(IPersistent obj);

        /// <summary> Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// 
        /// </returns>
        int Size();

        /// <summary> Remove all objects from the index
        /// </summary>
        void Clear();

        /// <summary>
        /// Get enumerator for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerator</returns>
        IEnumerator GetEnumerator(int setBits, int clearBits);

        /// <summary>
        /// Get enumerable collection for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerable collection</returns>
        IEnumerable Select(int setBits, int clearBits);
    }
}

