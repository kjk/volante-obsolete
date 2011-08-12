namespace Volante.Impl
{
    using System;
    using Volante;

    public class DefaultPersistentComparator<K, V> : PersistentComparator<K, V> where V : class,IPersistent, IComparable<V>, IComparable<K>
    {
        public override int CompareMembers(V m1, V m2)
        {
            return m1.CompareTo(m2);
        }

        public override int CompareMemberWithKey(V mbr, K key)
        {
            return mbr.CompareTo(key);
        }
    }
}
