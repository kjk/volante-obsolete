namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    class Rtree:PersistentResource, SpatialIndex
    {
        private int       height;
        private int       n;
        private RtreePage root;

        internal Rtree() {}

        public int Count 
        { 
            get 
            {
                return n;
            }
        }

        public void put(Rectangle r, IPersistent obj) 
        {
            if (!obj.isPersistent()) 
            { 
                ((StorageImpl)Storage).storeObject(obj);
            }
            if (root == null) 
            { 
                root = new RtreePage(obj, r);
                height = 1;
            } 
            else 
            { 
                RtreePage p = root.insert(r, obj, height); 
                if (p != null) 
                {
                    root = new RtreePage(root, p);
                    height += 1;
                }
            }
            n += 1;
            modify();
        }
    
        public int size() 
        { 
            return n;
        }

        public void remove(Rectangle r, IPersistent obj) 
        {
            if (root == null) 
            { 
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            ArrayList reinsertList = new ArrayList();
            int reinsertLevel = root.remove(r, obj, height, reinsertList);
            if (reinsertLevel < 0) 
            { 
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }        
            for (int i = reinsertList.Count; --i >= 0;) 
            {
                RtreePage p = (RtreePage)reinsertList[i];
                for (int j = 0, pn = p.n; j < pn; j++) 
                { 
                    RtreePage q = root.insert(p.b[j].r, p.b[j].p, height - reinsertLevel); 
                    if (q != null) 
                    { 
                        // root splitted
                        root = new RtreePage(root, q);
                        height += 1;
                    }
                }
                reinsertLevel -= 1;
                p.deallocate();
            }
            if (root.n == 1 && height > 1) 
            { 
                RtreePage newRoot = (RtreePage)root.b[0].p;
                root.deallocate();
                root = newRoot;
                height -= 1;
            }
            n -= 1;
            modify();
        }
    
        public IPersistent[] get(Rectangle r) 
        {
            ArrayList result = new ArrayList();
            if (root != null) 
            { 
                root.find(r, result, height);
            }
            return (IPersistent[])result.ToArray(typeof(IPersistent));
        }


        public void clear() 
        {
            if (root != null) 
            { 
                root.purge(height);
                root = null;
            }
            height = 0;
            n = 0;
            modify();
        }

        public override void deallocate() 
        {
            clear();
            base.deallocate();
        }
    }
}
    
