using System;
#if USE_GENERICS
using System.Collections.Generic;
#endif
using System.Collections;
using Perst;

namespace Perst.Impl
{
	
#if USE_GENERICS
    class Rtree<T>:PersistentCollection<T>, SpatialIndex<T> where T:class,IPersistent
#else
    class Rtree:PersistentCollection, SpatialIndex
#endif
    {
        private int         height;
        private int         n;
        private RtreePage   root;
        [NonSerialized()]
        private int         updateCounter;

        internal Rtree() {}

        public override int Count 
        { 
            get 
            {
                return n;
            }
        }

#if USE_GENERICS
        public void Put(Rectangle r, T obj) 
#else
        public void Put(Rectangle r, IPersistent obj) 
#endif
        {
            if (root == null) 
            { 
                root = new RtreePage(Storage, obj, r);
                height = 1;
            } 
            else 
            { 
                RtreePage p = root.insert(Storage, r, obj, height); 
                if (p != null) 
                {
                    root = new RtreePage(Storage, root, p);
                    height += 1;
                }
            }
            n += 1;
            updateCounter += 1;
            Modify();
        }
    
        public int Size() 
        { 
            return n;
        }

#if USE_GENERICS
        public void Remove(Rectangle r, T obj) 
#else
        public void Remove(Rectangle r, IPersistent obj) 
#endif
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
                    RtreePage q = root.insert(Storage, p.b[j], p.branch[j], height - reinsertLevel); 
                    if (q != null) 
                    { 
                        // root splitted
                        root = new RtreePage(Storage, root, q);
                        height += 1;
                    }
                }
                reinsertLevel -= 1;
                p.Deallocate();
            }
            if (root.n == 1 && height > 1) 
            { 
                RtreePage newRoot = (RtreePage)root.branch[0];
                root.Deallocate();
                root = newRoot;
                height -= 1;
            }
            n -= 1;
            updateCounter += 1;
            Modify();
        }
    
#if USE_GENERICS
        public T[] Get(Rectangle r) 
#else
        public IPersistent[] Get(Rectangle r) 
#endif
        {
            ArrayList result = new ArrayList();
            if (root != null) 
            { 
                root.find(r, result, height);
            }
#if USE_GENERICS
            return (T[])result.ToArray(typeof(T));
#else
            return (IPersistent[])result.ToArray(typeof(IPersistent));
#endif
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

#if USE_GENERICS
        public override void Clear() 
#else
        public void Clear() 
#endif
        {
            if (root != null) 
            { 
                root.purge(height);
                root = null;
            }
            height = 0;
            n = 0;
            updateCounter += 1;
            Modify();
        }

        public override void Deallocate() 
        {
            Clear();
            base.Deallocate();
        }

#if USE_GENERICS
        public IEnumerable<T> Overlaps(Rectangle r) 
#else
        public IEnumerable Overlaps(Rectangle r) 
#endif
        { 
            return new RtreeIterator(this, r);
        }

#if USE_GENERICS
        public override IEnumerator<T> GetEnumerator() 
#else
        public override IEnumerator GetEnumerator() 
#endif
        {
            return Overlaps(WrappingRectangle).GetEnumerator();
        }

        public IDictionaryEnumerator GetDictionaryEnumerator(Rectangle r) 
        { 
            return new RtreeEntryIterator(this, r);
        }

        public IDictionaryEnumerator GetDictionaryEnumerator() 
        { 
            return GetDictionaryEnumerator(WrappingRectangle);
        }

#if USE_GENERICS
        class RtreeIterator: IEnumerator<T>, IEnumerable<T>
        {
            internal RtreeIterator(Rtree<T> tree, Rectangle r) 
#else
        class RtreeIterator: IEnumerator, IEnumerable 
        {
            internal RtreeIterator(Rtree tree, Rectangle r) 
#endif
            { 
                counter = tree.updateCounter;
                height = tree.height;
                this.tree = tree;
                if (height == 0) 
                { 
                    return;
                }
                this.r = r;            
                pageStack = new RtreePage[height];
                posStack = new int[height];
                Reset();
            }

#if USE_GENERICS
            public IEnumerator<T> GetEnumerator()
#else
            public IEnumerator GetEnumerator()
#endif
            {
                return this;
            }

            public void Reset()
            {
                hasNext = gotoFirstItem(0, tree.root);
            }

            public void Dispose() {}

            public bool MoveNext() 
            {
                if (counter != tree.updateCounter) 
                { 
                    throw new InvalidOperationException("Tree was modified");
                }
                if (hasNext) 
                { 
                    page = pageStack[height-1];
                    pos = posStack[height-1];
                    if (!gotoNextItem(height-1)) 
                    { 
                        hasNext = false;
                    }
                    return true;
                } 
                else 
                { 
                    page = null;
                    return false;
                }
            }

#if USE_GENERICS
            public virtual T Current 
#else
            public virtual object Current 
#endif
            {
                get 
                {
                    if (page == null) 
                    { 
                        throw new InvalidOperationException();
                    }
#if USE_GENERICS
                    return (T)page.branch[pos];
#else
                    return page.branch[pos];
#endif
                }
            }

            private bool gotoFirstItem(int sp, RtreePage pg) 
            { 
                for (int i = 0, n = pg.n; i < n; i++) 
                { 
                    if (r.Intersects(pg.b[i])) 
                    { 
                        if (sp+1 == height || gotoFirstItem(sp+1, (RtreePage)pg.branch[i])) 
                        { 
                            pageStack[sp] = pg;
                            posStack[sp] = i;
                            return true;
                        }
                    }
                }
                return false;
            }
              
 
            private bool gotoNextItem(int sp) 
            {
                RtreePage pg = pageStack[sp];
                for (int i = posStack[sp], n = pg.n; ++i < n;) 
                { 
                    if (r.Intersects(pg.b[i])) 
                    { 
                        if (sp+1 == height || gotoFirstItem(sp+1, (RtreePage)pg.branch[i])) 
                        { 
                            pageStack[sp] = pg;
                            posStack[sp] = i;
                            return true;
                        }
                    }
                }
                pageStack[sp] = null;
                return (sp > 0) ? gotoNextItem(sp-1) : false;
            }
              
 
            protected RtreePage[] pageStack;
            protected int[]       posStack;
            protected int         counter;
            protected int         height;
            protected int         pos;
            protected bool        hasNext;
            protected RtreePage   page;
#if USE_GENERICS
            protected Rtree<T>    tree;
#else
            protected Rtree       tree;
#endif
            protected Rectangle   r;
        }

        class RtreeEntryIterator: RtreeIterator, IDictionaryEnumerator 
        {
#if USE_GENERICS
            internal RtreeEntryIterator(Rtree<T> tree, Rectangle r) 
#else
            internal RtreeEntryIterator(Rtree tree, Rectangle r) 
#endif
                : base(tree, r)
            {}

#if USE_GENERICS
            public new virtual object Current 
#else
            public override object Current 
#endif
            {
                get 
                {
                    return Entry;
                }
            }

            public DictionaryEntry Entry 
            {
                get 
                {
                    if (page == null) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return new DictionaryEntry(page.b[pos], page.branch[pos]);
                }
            }

            public object Key 
            {
                get 
                {
                    if (page == null) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return page.b[pos];
                }
            }

            public object Value 
            {
                get 
                {
                    if (page == null) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return page.branch[pos];
                }
            }
        }
    }
}
