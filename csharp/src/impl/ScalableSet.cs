using System;
using System.Collections;
using System.Collections.Generic;

namespace Volante.Impl
{
    class ScalableSet<T> : PersistentCollection<T>, ISet<T> where T : class,IPersistent
    {
        ILink<T> link;
        ISet<T> pset;
        const int BTREE_THRESHOLD = 128;

        internal ScalableSet(DatabaseImpl db, int initialSize)
            : base(db)
        {
            if (initialSize <= BTREE_THRESHOLD)
                link = db.CreateLink<T>(initialSize);
            else
                pset = db.CreateBtreeSet<T>();
        }

        ScalableSet() { }

        public override int Count
        {
            get
            {
                return link != null ? link.Count : pset.Count;
            }
        }

        public override void Clear()
        {
            if (link != null)
            {
                link.Clear();
                Modify();
            }
            else
            {
                pset.Clear();
            }
        }

        public override bool Contains(T o)
        {
            return link != null ? link.Contains(o) : pset.Contains(o);
        }

        public T[] ToArray()
        {
            return link != null ? link.ToArray() : pset.ToArray();
        }

        public Array ToArray(Type elemType)
        {
            return link != null ? link.ToArray(elemType) : pset.ToArray(elemType);
        }

        public override IEnumerator<T> GetEnumerator()
        {
            return link != null ? link.GetEnumerator() : pset.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override void Add(T o)
        {
            if (link == null)
            {
                pset.Add(o);
                return;
            }

            if (link.IndexOf(o) >= 0)
                return;

            if (link.Count <= BTREE_THRESHOLD)
            {
                Modify();
                link.Add(o);
                return;
            }

            pset = ((DatabaseImpl)Database).CreateBtreeSet<T>();
            for (int i = 0, n = link.Count; i < n; i++)
            {
                pset.Add(link[i]);
            }
            link = null;
            Modify();
            pset.Add(o);
        }

        public override bool Remove(T o)
        {
            if (link == null)
            {
                return pset.Remove(o);
            }

            int i = link.IndexOf(o);
            if (i < 0)
                return false;

            link.Remove(i);
            Modify();
            return true;
        }

        public bool ContainsAll(ICollection<T> c)
        {
            foreach (T o in c)
            {
                if (!Contains(o))
                    return false;
            }
            return true;
        }

        public bool AddAll(ICollection<T> c)
        {
            bool modified = false;
            foreach (T o in c)
            {
                if (!Contains(o))
                {
                    modified = true;
                    Add(o);
                }
            }
            return modified;
        }

        public bool RemoveAll(ICollection<T> c)
        {
            bool modified = false;
            foreach (T o in c)
            {
                modified |= Remove(o);
            }
            return modified;
        }

        public override bool Equals(object o)
        {
            if (o == this)
                return true;

            ISet<T> s = o as ISet<T>;
            if (s == null)
                return false;

            if (s.Count != Count)
                return false;

            return ContainsAll(s);
        }

        public override int GetHashCode()
        {
            int h = 0;
            foreach (IPersistent o in this)
            {
                h += o.Oid;
            }
            return h;
        }

        public override void Deallocate()
        {
            if (pset != null)
                pset.Deallocate();

            base.Deallocate();
        }
    }
}
