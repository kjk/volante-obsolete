namespace Perst.Impl
{
    using System;
    using Perst;

#if USE_GENERICS
    public class DefaultPersistentComparator<K,V> : PersistentComparator<K,V> where V:class,IPersistent,IComparable<V>,IComparable<K> 
    { 
        public override int CompareMembers(V m1, V m2) {
            return m1.CompareTo(m2);
        }
        
        public override int CompareMemberWithKey(V mbr, K key) { 
            return mbr.CompareTo(key);
        }
    }
#else
    public class DefaultPersistentComparator : PersistentComparator { 
        public override int CompareMembers(IPersistent m1, IPersistent m2) {
            return ((IComparable)m1).CompareTo(m2);
        }
        
        public override int CompareMemberWithKey(IPersistent mbr, object key) { 
            return ((IComparable)mbr).CompareTo(key);
        }
    }
#endif
}