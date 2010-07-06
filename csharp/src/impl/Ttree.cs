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
                return get(key);
            }
        } 
       
        public IPersistent[] this[object low, object high] 
        {
            get
            {
                return get(low, high);
            }
        }       

        
        internal Ttree(PersistentComparator comparator, bool unique) 
        { 
            this.comparator = comparator;
            this.unique = unique;
        }

        public PersistentComparator getComparator() 
        { 
            return comparator;
        }

        public override bool recursiveLoading() 
        {
            return false;
        }

        public IPersistent get(Object key) 
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

            

        public IPersistent[] get(Object from, Object till) 
        { 
            ArrayList list = new ArrayList();
            if (root != null) 
            { 
                
                root.find(comparator, from, till, list);
            }
            return (IPersistent[])list.ToArray(typeof(IPersistent));
        }


        public bool add(IPersistent obj) 
        { 
            TtreePage newRoot;
            if (root == null) 
            { 
                newRoot = new TtreePage(obj);
            } 
            else 
            { 
                TtreePage.PageReference pgRef = new TtreePage.PageReference(root);
                if (root.insert(comparator, obj, unique, pgRef) == TtreePage.NOT_UNIQUE) 
                { 
                    return false;
                }
                newRoot = pgRef.pg;
            }
            modify();
            root = newRoot;
            nMembers += 1;
            return true;
        }
                
                
        public bool contains(IPersistent member) 
        {
            return (root != null) ? root.contains(comparator, member) : false;
        }        

        public void remove(IPersistent obj) 
        {
            if (root == null) 
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            TtreePage.PageReference pgRef = new TtreePage.PageReference(root);
            if (root.remove(comparator, obj, pgRef) == TtreePage.NOT_FOUND) 
            {             
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            modify();
            root = pgRef.pg;
            nMembers -= 1;        
        }

        public int size() 
        {
            return nMembers;
        }
    
        public void clear() 
        {
            if (root != null) 
            { 
                root.prune();
                modify();
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

        public IEnumerator GetEnumerator(Key from, Key till) 
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
