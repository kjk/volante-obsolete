namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Volante;

    class RtreeR2<T> : PersistentCollection<T>, ISpatialIndexR2<T> where T : class, IPersistent
    {
        private int height;
        private int n;
        private RtreeR2Page root;
        [NonSerialized()]
        private int updateCounter;

        internal RtreeR2() { }

        public override int Count
        {
            get
            {
                return n;
            }
        }

        public void Put(RectangleR2 r, T obj)
        {
            if (root == null)
            {
                root = new RtreeR2Page(Database, obj, r);
                height = 1;
            }
            else
            {
                RtreeR2Page p = root.insert(Database, r, obj, height);
                if (p != null)
                {
                    root = new RtreeR2Page(Database, root, p);
                    height += 1;
                }
            }
            n += 1;
            updateCounter += 1;
            Modify();
        }

        public void Remove(RectangleR2 r, T obj)
        {
            if (root == null)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }
            ArrayList reinsertList = new ArrayList();
            int reinsertLevel = root.remove(r, obj, height, reinsertList);

            if (reinsertLevel < 0)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }

            for (int i = reinsertList.Count; --i >= 0; )
            {
                RtreeR2Page p = (RtreeR2Page)reinsertList[i];
                for (int j = 0, pn = p.n; j < pn; j++)
                {
                    RtreeR2Page q = root.insert(Database, p.b[j], p.branch[j], height - reinsertLevel);
                    if (q != null)
                    {
                        // root splitted
                        root = new RtreeR2Page(Database, root, q);
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

        public T[] Get(RectangleR2 r)
        {
            ArrayList result = new ArrayList();
            if (root != null)
                root.find(r, result, height);
            return (T[])result.ToArray(typeof(T));
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

        public override void Clear()
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

        public IEnumerable<T> Overlaps(RectangleR2 r)
        {
            return new RtreeIterator(this, r);
        }

        public override IEnumerator<T> GetEnumerator()
        {
            return Overlaps(WrappingRectangle).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IDictionaryEnumerator GetDictionaryEnumerator(RectangleR2 r)
        {
            return new RtreeEntryIterator(this, r);
        }

        public IDictionaryEnumerator GetDictionaryEnumerator()
        {
            return GetDictionaryEnumerator(WrappingRectangle);
        }

        class RtreeIterator : IEnumerator<T>, IEnumerable<T>
        {
            internal RtreeIterator(RtreeR2<T> tree, RectangleR2 r)
            {
                counter = tree.updateCounter;
                height = tree.height;
                this.tree = tree;
                if (height == 0)
                    return;

                this.r = r;
                pageStack = new RtreeR2Page[height];
                posStack = new int[height];
                Reset();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Reset()
            {
                hasNext = gotoFirstItem(0, tree.root);
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (counter != tree.updateCounter)
                    throw new InvalidOperationException("Tree was modified");

                if (hasNext)
                {
                    page = pageStack[height - 1];
                    pos = posStack[height - 1];
                    if (!gotoNextItem(height - 1))
                        hasNext = false;
                    return true;
                }
                else
                {
                    page = null;
                    return false;
                }
            }

            public virtual T Current
            {
                get
                {
                    if (page == null)
                        throw new InvalidOperationException();
                    return (T)page.branch[pos];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            private bool gotoFirstItem(int sp, RtreeR2Page pg)
            {
                for (int i = 0, n = pg.n; i < n; i++)
                {
                    if (r.Intersects(pg.b[i]))
                    {
                        if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreeR2Page)pg.branch[i]))
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
                RtreeR2Page pg = pageStack[sp];
                for (int i = posStack[sp], n = pg.n; ++i < n; )
                {
                    if (r.Intersects(pg.b[i]))
                    {
                        if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreeR2Page)pg.branch[i]))
                        {
                            pageStack[sp] = pg;
                            posStack[sp] = i;
                            return true;
                        }
                    }
                }
                pageStack[sp] = null;
                return (sp > 0) ? gotoNextItem(sp - 1) : false;
            }

            protected RtreeR2Page[] pageStack;
            protected int[] posStack;
            protected int counter;
            protected int height;
            protected int pos;
            protected bool hasNext;
            protected RtreeR2Page page;
            protected RtreeR2<T> tree;
            protected RectangleR2 r;
        }

        class RtreeEntryIterator : RtreeIterator, IDictionaryEnumerator
        {
            internal RtreeEntryIterator(RtreeR2<T> tree, RectangleR2 r)
                : base(tree, r)
            { }

            public new virtual object Current
            {
                get
                {
                    return Entry;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public DictionaryEntry Entry
            {
                get
                {
                    if (page == null)
                        throw new InvalidOperationException();

                    return new DictionaryEntry(page.b[pos], page.branch[pos]);
                }
            }

            public object Key
            {
                get
                {
                    if (page == null)
                        throw new InvalidOperationException();

                    return page.b[pos];
                }
            }

            public object Value
            {
                get
                {
                    if (page == null)
                        throw new InvalidOperationException();

                    return page.branch[pos];
                }
            }
        }
    }
}

