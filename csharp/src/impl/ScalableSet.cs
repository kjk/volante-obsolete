using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif

namespace NachoDB.Impl
{
#if USE_GENERICS
    class ScalableSet<T> : PersistentCollection<T>, ISet<T> where T:class,IPersistent
    { 
        Link<T> link;
        ISet<T> pset;
#else
    class ScalableSet : PersistentCollection, ISet 
    { 
        Link link;
        ISet pset;
#endif
        const int BTREE_THRESHOLD = 128;

        internal ScalableSet(StorageImpl storage, int initialSize) 
            : base(storage)
        {
#if USE_GENERICS
            if (initialSize <= BTREE_THRESHOLD) 
            { 
                link = storage.CreateLink<T>(initialSize);
            } 
            else 
            { 
                pset = storage.CreateSet<T>();
            }
#else
            if (initialSize <= BTREE_THRESHOLD) 
            { 
                link = storage.CreateLink(initialSize);
            } 
            else 
            { 
                pset = storage.CreateSet();
            }
#endif
        }

        ScalableSet() {}

        public override int Count 
        { 
            get 
            {
                return link != null ? link.Count : pset.Count;
            }
        }

#if USE_GENERICS
        public override void Clear() 
#else
        public void Clear() 
#endif
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

#if USE_GENERICS
        public override bool Contains(T o) 
#else
        public bool Contains(IPersistent o) 
#endif
        {
            return link != null ? link.Contains(o) : pset.Contains(o);
        }
    
#if USE_GENERICS
        public T[] ToArray() 
#else
        public IPersistent[] ToArray() 
#endif
        { 
            return link != null ? link.ToArray() : pset.ToArray();
        }

        public Array ToArray(Type elemType) 
        { 
            return link != null ? link.ToArray(elemType) : pset.ToArray(elemType);
        }

#if USE_GENERICS
        public override IEnumerator<T> GetEnumerator() 
#else
        public override IEnumerator GetEnumerator() 
#endif
        { 
            return link != null ? link.GetEnumerator() : pset.GetEnumerator();
        }

#if USE_GENERICS
        public override void Add(T o) 
#else
        public void Add(IPersistent o) 
#endif
        { 
            if (link != null) 
            { 
                if (link.IndexOf(o) >= 0) 
                { 
                    return;
                }
                if (link.Count == BTREE_THRESHOLD) 
                { 
#if USE_GENERICS
                    pset = Storage.CreateSet<T>();
#else
                    pset = Storage.CreateSet();
#endif
                    for (int i = 0, n = link.Count; i < n; i++) 
                    { 
#if USE_GENERICS
                        pset.Add(link[i]);
#else
                        pset.Add(link.GetRaw(i));
#endif
                    }
                    link = null;
                    Modify();
                    pset.Add(o);
                } 
                else 
                { 
                    Modify();
                    link.Add(o);
                }
            } 
            else 
            { 
                pset.Add(o);
            }
        }

#if USE_GENERICS
        public override bool Remove(T o) 
#else
        public bool Remove(IPersistent o) 
#endif
        { 
            if (link != null) 
            {  
                int i = link.IndexOf(o);        
                if (i < 0) 
                { 
                    return false;
                }
                link.Remove(i);
                Modify();
                return true;
            } 
            else 
            { 
                return pset.Remove(o);
            }
        }
    
    
#if USE_GENERICS
        public bool ContainsAll(ICollection<T> c) 
        { 
            foreach (T o in c) 
            {
                if (!Contains(o)) 
                {
                    return false;
                }
            }
            return true;
        }
#else
        public bool ContainsAll(ICollection c)  
        { 
            foreach (IPersistent o in c) 
            {
                if (!Contains(o)) 
                {
                    return false;
                }
            }
            return true;
        }
#endif

    
#if USE_GENERICS
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
#else
        public bool AddAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                if (!Contains(o)) 
                {
                    modified = true;
                    Add(o);
                }
            }
            return modified;
        }
#endif

 
#if USE_GENERICS
        public bool RemoveAll(ICollection<T> c) 
        {
            bool modified = false;
            foreach (T o in c) 
            {
                modified |= Remove(o);
            }
            return modified;
        }
#else
        public bool RemoveAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= Remove(o);
            }
            return modified;
        }
#endif

        public override  bool Equals(object o) 
        {
            if (o == this) 
            {
                return true;
            }
#if USE_GENERICS
            ISet<T> s = o as ISet<T>;
#else
            ISet s = o as ISet;
#endif
            if (s == null) 
            {
                return false;
            }
            if (s.Count != Count) 
            {
                return false;
            }
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
            { 
                pset.Deallocate();
            }
            base.Deallocate();
        }
    }
}
