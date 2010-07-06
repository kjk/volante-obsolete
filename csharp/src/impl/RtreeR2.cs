namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    class RtreeR2:PersistentResource, SpatialIndexR2
    {
        private int         height;
        private int         n;
        private RtreeR2Page root;
        [NonSerialized()]
        private int         updateCounter;

        internal RtreeR2() {}

        public int Count 
        { 
            get 
            {
                return n;
            }
        }

        public void Put(RectangleR2 r, IPersistent obj) 
        {
            if (root == null) 
            { 
                root = new RtreeR2Page(Storage, obj, r);
                height = 1;
            } 
            else 
            { 
                RtreeR2Page p = root.insert(Storage, r, obj, height); 
                if (p != null) 
                {
                    root = new RtreeR2Page(Storage, root, p);
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

        public void Remove(RectangleR2 r, IPersistent obj) 
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
                RtreeR2Page p = (RtreeR2Page)reinsertList[i];
                for (int j = 0, pn = p.n; j < pn; j++) 
                { 
                    RtreeR2Page q = root.insert(Storage, p.b[j], p.branch[j], height - reinsertLevel); 
                    if (q != null) 
                    { 
                        // root splitted
                        root = new RtreeR2Page(Storage, root, q);
                        height += 1;
                    }
                }
                reinsertLevel -= 1;
                p.Deallocate();
            }
            if (root.n == 1 && height > 1) 
            { 
                RtreeR2Page newRoot = (RtreeR2Page)root.branch[0];
                root.Deallocate();
                root = newRoot;
                height -= 1;
            }
            n -= 1;
            updateCounter += 1;
            Modify();
        }
    
        public IPersistent[] Get(RectangleR2 r) 
        {
            ArrayList result = new ArrayList();
            if (root != null) 
            { 
                root.find(r, result, height);
            }
            return (IPersistent[])result.ToArray(typeof(IPersistent));
        }

        public RectangleR2 WrappingRectangle
        {
            get 
            {
                return (root != null) 
                    ? root.cover()
                    : new RectangleR2(double.MaxValue, double.MaxValue, double.MinValue, double.MinValue);
            }
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
            updateCounter += 1;
            Modify();
        }

        public override void Deallocate() 
        {
            Clear();
            base.Deallocate();
        }

        public IEnumerable Overlaps(RectangleR2 r) 
        { 
            return new RtreeIterator(this, r);
        }

        class RtreeIterator : IEnumerator, IEnumerable 
        { 
            private RtreeR2Page[] pageStack;
            private int[]         posStack;
            private int           sp;
            private RectangleR2   r;
            private int           counter;
            private RtreeR2       tree;
            private bool          hasCurrent;
            private IPersistent   curr;

            internal RtreeIterator(RtreeR2 tree, RectangleR2 rect) 
            {             
                r = rect;
                this.tree = tree;
                Reset();
            }

            public IEnumerator GetEnumerator() 
            {
                return this;
            }

            public void Reset() 
            {
                pageStack = new RtreeR2Page[tree.height];
                posStack = new int[tree.height];
                RtreeR2Page pg = tree.root;
                hasCurrent = false;
                counter = tree.updateCounter;
                sp = 0;
                if (pg != null) 
                { 
                push:
                    while (true) 
                    { 
                        for (int i = 0; i < pg.n; i++) 
                        { 
                            if (r.Intersects(pg.b[i])) 
                            { 
                                posStack[sp] = i;
                                pageStack[sp] = pg;
                                if (++sp == pageStack.Length) 
                                { 
                                    return;
                                }
                                pg = (RtreeR2Page)pg.branch[i];
                                goto push;
                            }
                        }
                        popNext();
                        return;
                    }
                }
            }


            public bool MoveNext() 
            {
                if (counter != tree.updateCounter) 
                { 
                    throw new InvalidOperationException("B-Tree was modified");
                }
                if (sp > 0) { 
                    int i = posStack[sp-1];   
                    RtreeR2Page pg = pageStack[sp-1];
                    if (i < pg.n) 
                    { 
                        curr = pg.branch[i];
                        hasCurrent = true;
                        while (++i < pg.n) 
                        { 
                             if (r.Intersects(pg.b[i])) 
                             { 
                                 posStack[sp-1] = i;
                                 return true;
                             }
                        }
                        sp -= 1;
                        popNext();
                        return true;                      
                    }
                }
                hasCurrent = false;
                return false;
            }
        
            public virtual object Current
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return curr;
                }
            }

            void popNext() 
            { 
                pop:
                    while (sp != 0) 
                    { 
                        sp -= 1;
                        int i = posStack[sp];
                        RtreeR2Page pg = pageStack[sp];
                        while (++i < pg.n) 
                        { 
                            if (r.Intersects(pg.b[i])) 
                            {
                                posStack[sp] = i; 
                                sp += 1;
                            push:
                                while (true) 
                                { 
                                    pg = (RtreeR2Page)pg.branch[i];
                                    for (i = 0; i < pg.n; i++) 
                                    { 
                                        if (r.Intersects(pg.b[i])) 
                                        { 
                                            posStack[sp] = i;
                                            pageStack[sp] = pg;
                                            if (++sp == pageStack.Length) 
                                            { 
                                                return;
                                            }
                                            goto push;
                                        }
                                    }
                                    goto pop;
                                }
                            }
                        }
                    }
            }
        }
    }
}
