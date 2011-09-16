namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Volante;

    class Ttree<K, V> : PersistentCollection<V>, ISortedCollection<K, V> where V : class, IPersistent
    {
        private PersistentComparator<K, V> comparator;
        private bool unique;
        private TtreePage<K, V> root;
        private int nMembers;

        private Ttree() { }

        public override int Count
        {
            get
            {
                return nMembers;
            }
        }

        public V this[K key]
        {
            get
            {
                return Get(key);
            }
        }

        public V[] this[K low, K high]
        {
            get
            {
                return Get(low, high);
            }
        }

        internal Ttree(PersistentComparator<K, V> comparator, bool unique)
        {
            this.comparator = comparator;
            this.unique = unique;
        }

        public PersistentComparator<K, V> GetComparator()
        {
            return comparator;
        }

        public override bool RecursiveLoading()
        {
            return false;
        }

        public V Get(K key)
        {
            if (null == root)
                return null;
            List<V> list = new List<V>();
            root.find(comparator, key, BoundaryKind.Inclusive, key, BoundaryKind.Inclusive, list);
            if (list.Count > 1)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
            if (list.Count > 0)
                return list[0];
            return null;
        }

        public V[] Get(K from, K till)
        {
            return Get(from, BoundaryKind.Inclusive, till, BoundaryKind.Inclusive);
        }

        public V[] Get(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind)
        {
            List<V> list = new List<V>();
            if (root != null)
                root.find(comparator, from, fromKind, till, tillKind, list);
            return list.ToArray();
        }

        public override void Add(V obj)
        {
            TtreePage<K, V> newRoot = root;
            if (root == null)
            {
                newRoot = new TtreePage<K, V>(obj);
            }
            else
            {
                if (root.insert(comparator, obj, unique, ref newRoot) == TtreePage<K, V>.NOT_UNIQUE)
                {
                    return;
                }
            }
            Modify();
            root = newRoot;
            nMembers += 1;
        }

        public override bool Contains(V member)
        {
            if (null == root)
                return false;
            return root.contains(comparator, member);
        }

        public override bool Remove(V obj)
        {
            if (root == null)
                return false; // TODO: shouldn't that be an exception too?

            TtreePage<K, V> newRoot = root;
            if (root.remove(comparator, obj, ref newRoot) == TtreePage<K, V>.NOT_FOUND)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }
            Modify();
            root = newRoot;
            nMembers -= 1;
            return true;
        }

        public override void Clear()
        {
            if (null == root)
                return;
            root.prune();
            Modify();
            root = null;
            nMembers = 0;
        }

        public override void Deallocate()
        {
            if (root != null)
                root.prune();
            base.Deallocate();
        }

        public V[] ToArray()
        {
            V[] arr = new V[nMembers];
            if (root != null)
                root.toArray(arr, 0);
            return arr;
        }

        class TtreeEnumerator : IEnumerator<V>, IEnumerable<V>
        {
            int i;
            List<V> list;
            //Ttree<K,V>    tree;

            //internal TtreeEnumerator(Ttree<K,V> tree, List<V> list) 
            internal TtreeEnumerator(List<V> list)
            {
                //this.tree = tree;
                this.list = list;
                i = -1;
            }

            public IEnumerator<V> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Reset()
            {
                i = -1;
            }

            public V Current
            {
                get
                {
                    if (i < 0 || i >= list.Count)
                        throw new InvalidOperationException();

                    return list[i];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (i + 1 < list.Count)
                {
                    i++;
                    return true;
                }
                i++;
                return false;
            }
        }

        public override IEnumerator<V> GetEnumerator()
        {
            return GetEnumerator(default(K), BoundaryKind.None, default(K), BoundaryKind.None);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(K from, K till)
        {
            return Range(from, BoundaryKind.Inclusive, till, BoundaryKind.Inclusive).GetEnumerator();
        }

        public IEnumerable<V> Range(K from, K till)
        {
            return Range(from, BoundaryKind.Inclusive, till, BoundaryKind.Inclusive);
        }

        public IEnumerator<V> GetEnumerator(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind)
        {
            return Range(from, fromKind, till, tillKind).GetEnumerator();
        }

        public IEnumerable<V> Range(K from, BoundaryKind fromKind, K till, BoundaryKind tillKind)
        {
            List<V> list = new List<V>();
            if (root != null)
                root.find(comparator, from, fromKind, till, tillKind, list);
            //return new TtreeEnumerator(this, list);
            return new TtreeEnumerator(list);
        }
    }
}
