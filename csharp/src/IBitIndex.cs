#if WITH_OLD_BTREE
namespace Volante
{
    using System;
    using System.Collections.Generic;
    using System.Collections;

    /// <summary>
    /// Interface of bit index.
    /// Bit index allows to efficiently search object with specified 
    /// set of properties. Each object has associated mask of 32 bites. 
    /// Meaning of bits is application dependent. Usually each bit stands for
    /// some binary or boolean property, for example "sex", but it is possible to 
    /// use group of bits to represent enumerations with more possible values.
    /// </summary>
    public interface IBitIndex<T> : IPersistent, IResource, ICollection<T> where T : class,IPersistent
    {
        /// <summary>
        /// Get properties of specified object
        /// </summary>
        /// <param name="obj">object which properties are requested</param>
        /// <returns>bit mask associated with this objects</returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND) exception if there is no such object in the index
        /// </exception>
        int Get(T obj);

        /// <summary>
        /// Put new object in the index. If such objct already exists in index, then its
        /// mask will be rewritten 
        /// </summary>
        /// <param name="obj">object to be placed in the index. Object can be not yet peristent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        /// <param name="mask">bit mask associated with this objects</param>
        void Put(T obj, int mask);

        /// <summary> Access object bitmask
        /// </summary>
        int this[T obj]
        {
            get;
            set;
        }

        /// <summary>
        /// Get enumerator for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerator</returns>
        IEnumerator<T> GetEnumerator(int setBits, int clearBits);

        /// <summary>
        /// Get enumerable collection for selecting objects with specified properties.
        /// </summary>
        /// <param name="setBits">bitmask specifying bits which should be set (1)</param>
        /// <param name="clearBits">bitmask specifying bits which should be cleared (0)</param>
        /// <returns>enumerable collection</returns>
        IEnumerable<T> Select(int setBits, int clearBits);
    }
}
#endif
