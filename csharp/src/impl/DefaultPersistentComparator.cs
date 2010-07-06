namespace Perst.Impl
{
    using System;
    using Perst;

    public class DefaultPersistentComparator : PersistentComparator { 
        public override int CompareMembers(IPersistent m1, IPersistent m2) {
            return ((IComparable)m1).CompareTo(m2);
        }
        
        public override int CompareMemberWithKey(IPersistent mbr, object key) { 
            return ((IComparable)mbr).CompareTo(key);
        }
    }
}