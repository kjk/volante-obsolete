namespace NachoDB
{
    /// <summary> Base class for persistent comparator used in SortedCollection class
    /// </summary>
#if USE_GENERICS
    public abstract class PersistentComparator<K,V> : Persistent where V:class,IPersistent
#else
    public abstract class PersistentComparator : Persistent 
#endif
	{
        /// <summary> 
        /// Compare two members of collection
        /// </summary>
        /// <param name="m1"> first members</param>
        /// <param name="m2"> second members</param>
        /// <returns>negative number if m1 &lt; m2, zero if m1 == m2 and positive number if m1 &gt; m2</returns>
#if USE_GENERICS
        public abstract int CompareMembers(V m1, V m2);
#else
        public abstract int CompareMembers(IPersistent m1, IPersistent m2);
#endif

        /// <summary>
        /// Compare member with specified search key
        /// </summary>
        /// <param name="mbr"> collection member</param>
        /// <param name="key"> search key</param>
        /// <returns>negative number if mbr &lt; key, zero if mbr == key and positive number if mbr &gt; key</returns>
#if USE_GENERICS
        public abstract int CompareMemberWithKey(V mbr, K key);
#else
        public abstract int CompareMemberWithKey(IPersistent mbr, object key);
#endif
    }
}
