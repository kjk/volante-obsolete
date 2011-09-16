using System;
using System.Collections;
using System.Collections.Generic;
using Volante;

namespace Volante.Impl
{
    class ThickIndex<K, V> : PersistentCollection<V>, IIndex<K, V> where V : class,IPersistent
    {
        private IIndex<K, IPersistent> index;
        private int nElems;

        const int BTREE_THRESHOLD = 128;

        internal ThickIndex(DatabaseImpl db)
            : base(db)
        {
            index = db.CreateIndex<K, IPersistent>(IndexType.Unique);
        }

        ThickIndex() { }

        public override int Count
        {
            get
            {
                return nElems;
            }
        }

        public V this[K key]
        {
            get
            {
                return Get(key);
            }
            set
            {
                Set(key, value);
            }
        }

        public V[] this[K from, K till]
        {
            get
            {
                return Get(from, till);
            }
        }

        public V Get(Key key)
        {
            IPersistent s = index.Get(key);
            if (s == null)
                return null;

            Relation<V, V> r = s as Relation<V, V>;
            if (r != null)
            {
                if (r.Count == 1)
                    return r[0];
            }
            throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
        }

        public V[] Get(Key from, Key till)
        {
            return extend(index.Get(from, till));
        }

        public V Get(K key)
        {
            return Get(KeyBuilder.getKeyFromObject(key));
        }

        public V[] Get(K from, K till)
        {
            return Get(KeyBuilder.getKeyFromObject(from), KeyBuilder.getKeyFromObject(till));
        }

        private V[] extend(IPersistent[] s)
        {
            List<V> list = new List<V>();
            for (int i = 0; i < s.Length; i++)
            {
                list.AddRange((ICollection<V>)s[i]);
            }
            return list.ToArray();
        }

        public V[] GetPrefix(string prefix)
        {
            return extend(index.GetPrefix(prefix));
        }

        public V[] PrefixSearch(string word)
        {
            return extend(index.PrefixSearch(word));
        }

        public override void Clear()
        {
            // TODO: not sure but the index might not own the objects in it,
            // so it cannot deallocate them
            //foreach (IPersistent o in this)
            //{
            //    o.Deallocate();
            //}
            index.Clear();
            nElems = 0;
            Modify();
        }

        public V[] ToArray()
        {
            return extend(index.ToArray());
        }

        class ExtendEnumerator : IEnumerator<V>, IEnumerable<V>
        {
            public void Dispose() { }

            public bool MoveNext()
            {
                if (reachedEnd)
                    return false;
                while (!inner.MoveNext())
                {
                    if (!outer.MoveNext())
                    {
                        reachedEnd = false;
                        return false;
                    }
                    inner = ((IEnumerable<V>)outer.Current).GetEnumerator();
                }
                return true;
            }

            public V Current
            {
                get
                {
                    if (reachedEnd)
                        throw new InvalidOperationException();
                    return inner.Current;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    if (reachedEnd)
                        throw new InvalidOperationException();
                    return Current;
                }
            }

            public void Reset()
            {
                reachedEnd = true;
                if (outer.MoveNext())
                {
                    reachedEnd = false;
                    inner = ((IEnumerable<V>)outer.Current).GetEnumerator();
                }
            }

            public IEnumerator<V> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
            }

            internal ExtendEnumerator(IEnumerator<IPersistent> enumerator)
            {
                outer = enumerator;
                Reset();
            }

            private IEnumerator<IPersistent> outer;
            private IEnumerator<V> inner;
            private bool reachedEnd;
        }

        class ExtendDictionaryEnumerator : IDictionaryEnumerator
        {
            public object Current
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
                    return new DictionaryEntry(key, inner.Current);
                }
            }

            public object Key
            {
                get
                {
                    if (reachedEnd)
                        throw new InvalidOperationException();
                    return key;
                }
            }

            public object Value
            {
                get
                {
                    return inner.Current;
                }
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (reachedEnd)
                    return false;

                while (!inner.MoveNext())
                {
                    if (!outer.MoveNext())
                    {
                        reachedEnd = true;
                        return false;
                    }

                    key = outer.Key;
                    inner = ((IEnumerable<V>)outer.Value).GetEnumerator();
                }
                return true;
            }

            public void Reset()
            {
                reachedEnd = true;
                if (outer.MoveNext())
                {
                    reachedEnd = false;
                    key = outer.Key;
                    inner = ((IEnumerable<V>)outer.Value).GetEnumerator();
                }
            }

            internal ExtendDictionaryEnumerator(IDictionaryEnumerator enumerator)
            {
                outer = enumerator;
                Reset();
            }

            private IDictionaryEnumerator outer;
            private IEnumerator<V> inner;
            private object key;
            private bool reachedEnd;
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator()
        {
            return new ExtendDictionaryEnumerator(index.GetDictionaryEnumerator());
        }

        public override IEnumerator<V> GetEnumerator()
        {
            return new ExtendEnumerator(index.GetEnumerator());
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(Key from, Key till, IterationOrder order)
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(K from, K till, IterationOrder order)
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(Key from, Key till)
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(K from, K till)
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(string prefix)
        {
            return StartsWith(prefix).GetEnumerator();
        }

        public virtual IEnumerable<V> Range(Key from, Key till, IterationOrder order)
        {
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

        public virtual IEnumerable<V> Reverse()
        {
            return new ExtendEnumerator(index.Reverse().GetEnumerator());
        }

        public virtual IEnumerable<V> Range(Key from, Key till)
        {
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }

        public IEnumerable<V> Range(K from, K till, IterationOrder order)
        {
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

        public IEnumerable<V> Range(K from, K till)
        {
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }

        public IEnumerable<V> StartsWith(string prefix)
        {
            return new ExtendEnumerator(index.GetEnumerator(prefix));
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order)
        {
            return new ExtendDictionaryEnumerator(index.GetDictionaryEnumerator(from, till, order));
        }

        public Type KeyType
        {
            get
            {
                return index.KeyType;
            }
        }

        public bool Put(Key key, V obj)
        {
            IPersistent s = index.Get(key);
            if (s == null)
            {
                Relation<V, V> r = Database.CreateRelation<V, V>(null);
                r.Add(obj);
                index.Put(key, r);
            }
            else if (s is Relation<V, V>)
            {
                Relation<V, V> r = (Relation<V, V>)s;
                if (r.Count == BTREE_THRESHOLD)
                {
                    ISet<V> ps = ((DatabaseImpl)Database).CreateBtreeSet<V>();
                    for (int i = 0; i < BTREE_THRESHOLD; i++)
                    {
                        ps.Add(r[i]);
                    }
                    ps.Add(obj);
                    index.Set(key, ps);
                    r.Deallocate();
                }
                else
                {
                    r.Add(obj);
                }
            }
            else
            {
                ((ISet<V>)s).Add(obj);
            }
            nElems += 1;
            Modify();
            return true;
        }

        public V Set(Key key, V obj)
        {
            IPersistent s = index.Get(key);
            if (s == null)
            {
                Relation<V, V> r = Database.CreateRelation<V, V>(null);
                r.Add(obj);
                index.Put(key, r);
                nElems += 1;
                Modify();
                return null;
            }
            else if (s is Relation<V, V>)
            {
                Relation<V, V> r = (Relation<V, V>)s;
                if (r.Count == 1)
                {
                    V prev = r[0];
                    r[0] = obj;
                    return prev;
                }
            }
            throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
        }

        public void Remove(Key key, V obj)
        {
            IPersistent s = index.Get(key);
            if (s is Relation<V, V>)
            {
                Relation<V, V> r = (Relation<V, V>)s;
                int i = r.IndexOf(obj);
                if (i >= 0)
                {
                    r.RemoveAt(i);
                    if (r.Count == 0)
                    {
                        index.Remove(key, r);
                        r.Deallocate();
                    }
                    nElems -= 1;
                    Modify();
                    return;
                }
            }
            else if (s is ISet<V>)
            {
                ISet<V> ps = (ISet<V>)s;
                if (ps.Remove(obj))
                {
                    if (ps.Count == 0)
                    {
                        index.Remove(key, ps);
                        ps.Deallocate();
                    }
                    nElems -= 1;
                    Modify();
                    return;
                }
            }
            throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
        }

        public V Remove(Key key)
        {
            throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
        }

        public bool Put(K key, V obj)
        {
            return Put(KeyBuilder.getKeyFromObject(key), obj);
        }

        public V Set(K key, V obj)
        {
            return Set(KeyBuilder.getKeyFromObject(key), obj);
        }

        public void Remove(K key, V obj)
        {
            Remove(KeyBuilder.getKeyFromObject(key), obj);
        }

        public V RemoveKey(K key)
        {
            throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
        }

        public override void Deallocate()
        {
            Clear();
            index.Deallocate();
            base.Deallocate();
        }
    }
}
