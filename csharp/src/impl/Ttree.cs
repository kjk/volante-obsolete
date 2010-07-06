namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;

    class Ttree:PersistentResource, SortedCollection 
    {
        private PersistentComparator comparator;
        private bool                 unique;
        private TtreePage            root;
        private int                  nMembers;
    
        private Ttree() {} 

        public int Count 
        { 
            get 
            {
                return nMembers;
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

        public IPersistent this[object key] 
        {
            get 
            {
                return Get(key);
            }
        } 
       
        public IPersistent[] this[object low, object high] 
        {
            get
            {
                return Get(low, high);
            }
        }       

        
        internal Ttree(PersistentComparator comparator, bool unique) 
        { 
            this.comparator = comparator;
            this.unique = unique;
        }

        public PersistentComparator GetComparator() 
        { 
            return comparator;
        }

        public override bool RecursiveLoading() 
        {
            return false;
        }

        public IPersistent Get(object key) 
        { 
            if (root != null) 
            { 
                ArrayList list = new ArrayList();
                root.find(comparator, key, key, list);
                if (list.Count > 1) 
                { 
                    throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
                } 
                else if (list.Count == 0) 
                { 
                    return null;
                } 
                else 
                { 
                    return (IPersistent)list[0];
                }
            }
            return null;
        }

            

        public IPersistent[] Get(object from, object till) 
        { 
            ArrayList list = new ArrayList();
            if (root != null) 
            { 
                
                root.find(comparator, from, till, list);
            }
            return (IPersistent[])list.ToArray(typeof(IPersistent));
        }


        public bool Add(IPersistent obj) 
        { 
            TtreePage newRoot = root;
            if (root == null) 
            { 
                newRoot = new TtreePage(obj);
            } 
            else 
            { 
                if (root.insert(comparator, obj, unique, ref newRoot) == TtreePage.NOT_UNIQUE) 
                { 
                    return false;
                }
            }
            Modify();
            root = newRoot;
            nMembers += 1;
            return true;
        }
                
                
        public bool Contains(IPersistent member) 
        {
            return (root != null) ? root.contains(comparator, member) : false;
        }        

        public void Remove(IPersistent obj) 
        {
            if (root == null) 
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            TtreePage newRoot = root;
            if (root.remove(comparator, obj, ref newRoot) == TtreePage.NOT_FOUND) 
            {             
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            Modify();
            root = newRoot;
            nMembers -= 1;        
        }

        public int Size() 
        {
            return nMembers;
        }
    
        public void Clear() 
        {
            if (root != null) 
            { 
                root.prune();
                Modify();
                root = null;
                nMembers = 0;
            }
        }
 
        public IPersistent[] ToArray() 
        {
            IPersistent[] arr = new IPersistent[nMembers];
            if (root != null) 
            { 
                root.toArray(arr, 0);
            }
            return arr;
        }

        public virtual Array ToArray(Type elemType)
        {
            Array arr = Array.CreateInstance(elemType, nMembers);
            if (root != null)
            {
                root.toArray((IPersistent[])arr, 0);
            }
            return arr;
        }

 
        class TtreeEnumerator : IEnumerator 
        { 
            int           i;
            ArrayList     list;
            Ttree         tree;

            internal TtreeEnumerator(Ttree tree, ArrayList list) 
            { 
                this.tree = tree;
                this.list = list;
                i = -1;
            }
        
            public void Reset() 
            {
                i = -1;
            }
                
            public object Current
            {
                get 
                {
                    if (i < 0 || i >= list.Count) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return list[i];
                }
            }

            public bool MoveNext() 
            {
                if (i+1 < list.Count) 
                { 
                    i += 1;
                    return true;
                }
                return false;
            }
        }
        

        public IEnumerator GetEnumerator()
        {
            return GetEnumerator(null, null);
        }

        public IEnumerator GetEnumerator(object from, object till) 
        {
            ArrayList list = new ArrayList();
            if (root != null) 
            { 
                root.find(comparator, from, till, list);
            }            
            return new TtreeEnumerator(this, list);
        }
    }

}
