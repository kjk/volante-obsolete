using System;
using System.Collections;
using System.Collections.Generic;
using Volante;

namespace Volante.Impl
{

    class Rtree<T> : PersistentCollection<T>, ISpatialIndex<T> where T : class, IPersistent
    {
        private int height;
        private int n;
        private RtreePage root;
        [NonSerialized()]
        private int updateCounter;

        internal Rtree() { }

        public override int Count
        {
            get
            {
                return n;
            }
        }

        public void Put(Rectangle r, T obj)
        {
            if (root == null)
            {
                root = new RtreePage(Database, obj, r);
                height = 1;
            }
            else
            {
                RtreePage p = root.insert(Database, r, obj, height);
                if (p != null)
                {
                    root = new RtreePage(Database, root, p);
                    height += 1;
                }
            }
            n += 1;
            updateCounter += 1;
            Modify();
        }

        public void Remove(Rectangle r, T obj)
        {
            if (root == null)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);

            ArrayList reinsertList = new ArrayList();
            int reinsertLevel = root.remove(r, obj, height, reinsertList);
            if (reinsertLevel < 0)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);

            for (int i = reinsertList.Count; --i >= 0; )
            {
                RtreePage p = (RtreePage)reinsertList[i];
                for (int j = 0, pn = p.n; j < pn; j++)
                {
                    RtreePage q = root.insert(Database, p.b[j], p.branch[j], height - reinsertLevel);
                    if (q != null)
                    {
                        // root splitted
                        root = new RtreePage(Database, root, q);
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

        public T[] Get(Rectangle r)
        {
            ArrayList result = new ArrayList();
            if (root != null)
                root.find(r, result, height);
            return (T[])result.ToArray(typeof(T));
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

        public IEnumerable<T> Overlaps(Rectangle r)
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

        public IDictionaryEnumerator GetDictionaryEnumerator(Rectangle r)
        {
            return new RtreeEntryIterator(this, r);
        }

        public IDictionaryEnumerator GetDictionaryEnumerator()
        {
            return GetDictionaryEnumerator(WrappingRectangle);
        }

        class RtreeIterator : IEnumerator<T>, IEnumerable<T>
        {
            internal RtreeIterator(Rtree<T> tree, Rectangle r)
            {
                counter = tree.updateCounter;
                height = tree.height;
                this.tree = tree;
                if (height == 0)
                    return;

                this.r = r;
                pageStack = new RtreePage[height];
                posStack = new int[height];
                Reset();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
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

            private bool gotoFirstItem(int sp, RtreePage pg)
            {
                for (int i = 0, n = pg.n; i < n; i++)
                {
                    if (r.Intersects(pg.b[i]))
                    {
                        if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreePage)pg.branch[i]))
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
                for (int i = posStack[sp], n = pg.n; ++i < n; )
                {
                    if (r.Intersects(pg.b[i]))
                    {
                        if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreePage)pg.branch[i]))
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

            protected RtreePage[] pageStack;
            protected int[] posStack;
            protected int counter;
            protected int height;
            protected int pos;
            protected bool hasNext;
            protected RtreePage page;
            protected Rtree<T> tree;
            protected Rectangle r;
        }

        class RtreeEntryIterator : RtreeIterator, IDictionaryEnumerator
        {
            internal RtreeEntryIterator(Rtree<T> tree, Rectangle r)
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
