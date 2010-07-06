using System;
using System.Collections;

namespace Perst.Impl
{
    class ScalableSet : PersistentResource, ISet 
    { 
        Link link;
        ISet pset;

        const int BTREE_THRESHOLD = 128;

        internal ScalableSet(StorageImpl storage, int initialSize) 
            : base(storage)
        {
            if (initialSize <= BTREE_THRESHOLD) 
            { 
                link = storage.CreateLink(initialSize);
            } 
            else 
            { 
                pset = storage.CreateSet();
            }
        }

        ScalableSet() {}

        public int Count 
        { 
            get 
            {
                return link != null ? link.Count : pset.Count;
            }
        }

        public bool IsSynchronized 
        {
            get 
            {
                return true;
            }
        }

        public object SyncRoot 
        {
            get 
            {
                return this;
            }
        }

        public void CopyTo(Array dst, int i) 
        {
            foreach (object o in this) 
            { 
                dst.SetValue(o, i++);
            }
        }

        public void Clear() 
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

        public bool Contains(IPersistent o) 
        {
            return link != null ? link.Contains(o) : pset.Contains(o);
        }
    
        public IPersistent[] ToArray() 
        { 
            return link != null ? link.ToArray() : pset.ToArray();
        }

        public Array ToArray(Type elemType) 
        { 
            return link != null ? link.ToArray(elemType) : pset.ToArray(elemType);
        }

        public IEnumerator GetEnumerator() 
        { 
            return link != null ? link.GetEnumerator() : pset.GetEnumerator();
        }

        public bool Add(IPersistent o) 
        { 
            if (link != null) 
            { 
                if (link.IndexOf(o) >= 0) 
                { 
                    return false;
                }
                if (link.Count == BTREE_THRESHOLD) 
                { 
                    pset = Storage.CreateSet();
                    for (int i = 0, n = link.Count; i < n; i++) 
                    { 
                        pset.Add(link.GetRaw(i));
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
                return true;
            } 
            else 
            { 
                return pset.Add(o);
            }
        }

        public bool Remove(IPersistent o) 
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

    
        public bool AddAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= Add(o);
            }
            return modified;
        }

 
        public bool RemoveAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= Remove(o);
            }
            return modified;
        }

        public override  bool Equals(object o) 
        {
            if (o == this) 
            {
                return true;
            }
            if (!(o is ICollection)) 
            {
                return false;
            }
            ICollection c = (ICollection) o;
            if (c.Count != Count) 
            {
                return false;
            }
            return ContainsAll(c);
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
