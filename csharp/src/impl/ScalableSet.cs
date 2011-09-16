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
                if (pset != null)
                    return pset.Count;
                else
                    return link.Count;
            }
        }

        public override void Clear()
        {
            if (pset != null)
                pset.Clear();
            else
            {
                link.Clear();
                Modify();
            }
        }

        public override bool Contains(T o)
        {
            if (pset != null)
                return pset.Contains(o);
            else
                return link.Contains(o);
        }

        public T[] ToArray()
        {
            if (pset != null)
                return pset.ToArray();
            else
                return link.ToArray();
        }

        public override IEnumerator<T> GetEnumerator()
        {
            if (pset != null)
                return pset.GetEnumerator();
            else
                return link.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override void Add(T o)
        {
            if (pset != null)
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
            if (pset != null)
                return pset.Remove(o);

            int i = link.IndexOf(o);
            if (i < 0)
                return false;

            link.RemoveAt(i);
            Modify();
            return true;
        }

        public bool ContainsAll(ICollection<T> c)
        {
            if (pset != null)
                return pset.ContainsAll(c);

            foreach (T o in c)
            {
                if (!Contains(o))
                    return false;
            }
            return true;
        }

        public bool AddAll(ICollection<T> c)
        {
            if (pset != null)
                return pset.AddAll(c);

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
            if (pset != null)
                return pset.RemoveAll(c);

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
            if (pset != null)
                return pset.GetHashCode();

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
