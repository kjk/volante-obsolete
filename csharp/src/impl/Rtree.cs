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

        public void Put(Rectangle r, IPersistent obj) 
        {
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
            Modify();
        }
    
        public int Size() 
        { 
            return n;
        }

        public void Remove(Rectangle r, IPersistent obj) 
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
                p.Deallocate();
            }
            if (root.n == 1 && height > 1) 
            { 
                RtreePage newRoot = (RtreePage)root.b[0].p;
                root.Deallocate();
                root = newRoot;
                height -= 1;
            }
            n -= 1;
            Modify();
        }
    
        public IPersistent[] Get(Rectangle r) 
        {
            ArrayList result = new ArrayList();
            if (root != null) 
            { 
                root.find(r, result, height);
            }
            return (IPersistent[])result.ToArray(typeof(IPersistent));
        }


        public void Clear() 
        {
            if (root != null) 
            { 
                root.purge(height);
                root = null;
            }
            height = 0;
            n = 0;
            Modify();
        }

        public override void Deallocate() 
        {
            Clear();
            base.Deallocate();
        }

        public Rectangle WrappingRectangle
        {
            get 
            {
                return (root != null) 
                    ? root.cover() 
                    : new Rectangle(int.MaxValue, int.MaxValue, int.MinValue, int.MinValue);
            }
        }

    }
}
    
