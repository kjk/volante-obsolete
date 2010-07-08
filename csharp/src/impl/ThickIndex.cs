using System;
#if USE_GENERICS
using System.Collections.Generic;
#endif
using System.Collections;
using NachoDB;

namespace NachoDB.Impl
{
#if USE_GENERICS
    class ThickIndex<K,V> : PersistentCollection<V>, Index<K,V> where V:class,IPersistent
#else
    class ThickIndex : PersistentCollection, Index 
#endif
    { 
#if USE_GENERICS
        private Index<K,IPersistent> index;
#else
        private Index index;
#endif
        private int   nElems;

        const int BTREE_THRESHOLD = 128;

#if USE_GENERICS
        internal ThickIndex(StorageImpl db) 
            : base(db)
        {
            index = db.CreateIndex<K,IPersistent>(true);
        }
#else
        internal ThickIndex(Type keyType, StorageImpl db) 
            : base(db)
        {
            index = db.CreateIndex(keyType, true);
        }
#endif
    
        ThickIndex() {}

        public override int Count 
        { 
            get 
            {
                return nElems;
            }
        }

#if USE_GENERICS
        public V this[K key] 
#else
        public IPersistent this[object key] 
#endif
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
    
#if USE_GENERICS
        public V[] this[K from, K till] 
#else
        public IPersistent[] this[object from, object till] 
#endif
        {
            get 
            {
                return Get(from, till);
            }
        }

#if USE_GENERICS
        public V Get(Key key) 
#else
        public IPersistent Get(Key key) 
#endif
        {
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                return null;
            }
#if USE_GENERICS
            Relation<V,V> r = s as Relation<V,V>;
#else
            Relation r = s as Relation;
#endif
            if (r != null)
            { 
                if (r.Count == 1) 
                { 
                    return r[0];
                }
            }
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }
                  
#if USE_GENERICS
        public V[] Get(Key from, Key till) 
#else
        public IPersistent[] Get(Key from, Key till) 
#endif
        {
            return extend(index.Get(from, till));
        }

#if USE_GENERICS
        public V Get(K key) 
#else
        public IPersistent Get(object key) 
#endif
        {
            return Get(KeyBuilder.getKeyFromObject(key));
        }
    
#if USE_GENERICS
        public V[] Get(K from, K till) 
#else
        public IPersistent[] Get(object from, object till) 
#endif
        {
            return Get(KeyBuilder.getKeyFromObject(from), KeyBuilder.getKeyFromObject(till));
        }

#if USE_GENERICS
        private V[] extend(IPersistent[] s) 
        { 
            List<V> list = new List<V>();
            for (int i = 0; i < s.Length; i++) 
            { 
                list.AddRange((ICollection<V>)s[i]);
            }
            return list.ToArray();
        }
#else
        private IPersistent[] extend(IPersistent[] s) 
        { 
            ArrayList list = new ArrayList();
            for (int i = 0; i < s.Length; i++) 
            { 
                list.AddRange((ICollection)s[i]);
            }
            return (IPersistent[])list.ToArray(typeof(IPersistent));
        }
#endif

                      
#if USE_GENERICS
        public V[] GetPrefix(string prefix) 
#else
        public IPersistent[] GetPrefix(string prefix) 
#endif
        { 
            return extend(index.GetPrefix(prefix));
        }
    
#if USE_GENERICS
        public V[] PrefixSearch(string word) 
#else
        public IPersistent[] PrefixSearch(string word) 
#endif
        { 
            return extend(index.PrefixSearch(word));
        }
           
        public int Size() 
        { 
            return nElems;
        }
    
#if USE_GENERICS
        public override void Clear() 
#else
        public void Clear() 
#endif
        { 
            foreach (IPersistent o in this) 
            { 
                o.Deallocate();
            }
            index.Clear();
            nElems = 0;
            Modify();
        }

#if USE_GENERICS
        public V[] ToArray() 
#else
        public IPersistent[] ToArray() 
#endif
        { 
            return extend(index.ToArray());
        }
        
        public Array ToArray(Type elemType) 
        { 
            ArrayList list = new ArrayList();
            foreach (ICollection c in index) 
            { 
                list.AddRange(c);
            }
            return list.ToArray(elemType);
        }

#if USE_GENERICS
        class ExtendEnumerator : IEnumerator<V>, IEnumerable<V>
#else
        class ExtendEnumerator : IEnumerator, IEnumerable
#endif
        {  
            public void Dispose() {}

            public bool MoveNext() 
            { 
                while (!inner.MoveNext()) 
                {                 
                    if (outer.MoveNext()) 
                    {
#if USE_GENERICS
                        inner = ((IEnumerable<V>)outer.Current).GetEnumerator();
#else
                        inner = ((IEnumerable)outer.Current).GetEnumerator();
#endif
                    } 
                    else 
                    { 
                        return false;
                    }
                }
                return true;
            }

#if USE_GENERICS
            public V Current 
#else
            public object Current 
#endif
            {
                get 
                {
                    return inner.Current;
                }
            }

            public void Reset() 
            {
#if !USE_GENERICS
                outer.Reset();
#endif
                if (outer.MoveNext()) 
                {
#if USE_GENERICS
                    inner = ((IEnumerable<V>)outer.Current).GetEnumerator();
#else
                    inner = ((IEnumerable)outer.Current).GetEnumerator();
#endif
                }
            }

#if USE_GENERICS
            public IEnumerator<V> GetEnumerator() 
#else
            public IEnumerator GetEnumerator() 
#endif
            {
                return this;
            }

#if USE_GENERICS
            internal ExtendEnumerator(IEnumerator<IPersistent> enumerator) 
#else
            internal ExtendEnumerator(IEnumerator enumerator) 
#endif
            { 
                outer = enumerator;
                Reset();
            }

#if USE_GENERICS
            private IEnumerator<IPersistent> outer;
            private IEnumerator<V>           inner;
#else
            private IEnumerator outer;
            private IEnumerator inner;
#endif
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

            public void Dispose() {}

            public bool MoveNext() 
            { 
                while (!inner.MoveNext()) 
                {                 
                    if (outer.MoveNext()) 
                    {
                        key = outer.Key;
#if USE_GENERICS
                        inner = ((IEnumerable<V>)outer.Value).GetEnumerator();
#else
                        inner = ((IEnumerable)outer.Value).GetEnumerator();
#endif
                    } 
                    else 
                    { 
                        return false;
                    }
                }
                return true;
            }

            public void Reset() 
            {
#if !USE_GENERICS
                outer.Reset();
#endif
                if (outer.MoveNext()) 
                {
                    key = outer.Key;
#if USE_GENERICS
                    inner = ((IEnumerable<V>)outer.Value).GetEnumerator();
#else
                    inner = ((IEnumerable)outer.Value).GetEnumerator();
#endif
                }
            }
       
            internal ExtendDictionaryEnumerator(IDictionaryEnumerator enumerator) 
            { 
                outer = enumerator;
                Reset();
            }

            private IDictionaryEnumerator outer;
#if USE_GENERICS
            private IEnumerator<V>        inner;
#else
            private IEnumerator           inner;
#endif
            private object                key;
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator() 
        { 
            return new ExtendDictionaryEnumerator(index.GetDictionaryEnumerator());
        }

#if USE_GENERICS
        public override IEnumerator<V> GetEnumerator() 
#else
        public override IEnumerator GetEnumerator() 
#endif
        { 
            return new ExtendEnumerator(index.GetEnumerator());
        }

#if USE_GENERICS
        public IEnumerator<V> GetEnumerator(Key from, Key till, IterationOrder order) 
#else
        public IEnumerator GetEnumerator(Key from, Key till, IterationOrder order) 
#endif
        {
            return Range(from, till, order).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<V> GetEnumerator(K from, K till, IterationOrder order) 
#else
        public IEnumerator GetEnumerator(object from, object till, IterationOrder order) 
#endif
        {
            return Range(from, till, order).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<V> GetEnumerator(Key from, Key till) 
#else
        public IEnumerator GetEnumerator(Key from, Key till) 
#endif
        {
            return Range(from, till).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<V> GetEnumerator(K from, K till) 
#else
        public IEnumerator GetEnumerator(object from, object till) 
#endif
        {
            return Range(from, till).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<V> GetEnumerator(string prefix) 
#else
        public IEnumerator GetEnumerator(string prefix) 
#endif
        {
            return StartsWith(prefix).GetEnumerator();
        }

#if USE_GENERICS
        public virtual IEnumerable<V> Range(Key from, Key till, IterationOrder order) 
#else
        public virtual IEnumerable Range(Key from, Key till, IterationOrder order) 
#endif
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

#if USE_GENERICS
        public virtual IEnumerable<V> Reverse() 
#else
        public virtual IEnumerable Reverse() 
#endif
        { 
            return new ExtendEnumerator(index.Reverse().GetEnumerator());
        }

#if USE_GENERICS
        public virtual IEnumerable<V> Range(Key from, Key till) 
#else
        public virtual IEnumerable Range(Key from, Key till) 
#endif
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }
            
#if USE_GENERICS
        public IEnumerable<V> Range(K from, K till, IterationOrder order) 
#else
        public IEnumerable Range(object from, object till, IterationOrder order) 
#endif
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

#if USE_GENERICS
        public IEnumerable<V> Range(K from, K till) 
#else
        public IEnumerable Range(object from, object till) 
#endif
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }
 
#if USE_GENERICS
        public IEnumerable<V> StartsWith(string prefix) 
#else
        public IEnumerable StartsWith(string prefix) 
#endif
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

#if USE_GENERICS
        public bool Put(Key key, V obj) 
        { 
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                Relation<V,V> r = Storage.CreateRelation<V,V>(null);
                r.Add(obj);
                index.Put(key, r);
            } 
            else if (s is Relation<V,V>) 
            { 
                Relation<V,V> r = (Relation<V,V>)s;
                if (r.Count == BTREE_THRESHOLD) 
                {
                    ISet<V> ps = Storage.CreateSet<V>();
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
#else
        public bool Put(Key key, IPersistent obj) 
        { 
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                Relation r = Storage.CreateRelation(null);
                r.Add(obj);
                index.Put(key, r);
            } 
            else if (s is Relation) 
            { 
                Relation r = (Relation)s;
                if (r.Count == BTREE_THRESHOLD) 
                {
                    ISet ps = Storage.CreateSet();
                    for (int i = 0; i < BTREE_THRESHOLD; i++) 
                    { 
                        ps.Add(r.GetRaw(i));
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
                ((ISet)s).Add(obj);
            }
            nElems += 1;
            Modify();
            return true;
        }
#endif


#if USE_GENERICS
        public V Set(Key key, V obj) 
        {
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                Relation<V,V> r = Storage.CreateRelation<V,V>(null);
                r.Add(obj);
                index.Put(key, r);
                nElems += 1;
                Modify();
                return null;
            } 
            else if (s is Relation<V,V>) 
            { 
                Relation<V,V> r = (Relation<V,V>)s;
                if (r.Count == 1) 
                {
                    V prev = r[0];
                    r[0] = obj;
                    return prev;
                } 
            }
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }
#else
        public IPersistent Set(Key key, IPersistent obj) 
        {
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                Relation r = Storage.CreateRelation(null);
                r.Add(obj);
                index[key] = r;
                nElems += 1;
                Modify();
                return null;
            } 
            else if (s is Relation) 
            { 
                Relation r = (Relation)s;
                if (r.Count == 1) 
                {
                    IPersistent prev = r[0];
                    r[0] = obj;
                    return prev;
                } 
            }
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }
#endif

#if USE_GENERICS
        public void Remove(Key key, V obj) 
        { 
            IPersistent s = index.Get(key);
            if (s is Relation<V,V>) 
            { 
                Relation<V,V> r = (Relation<V,V>)s;
                int i = r.IndexOf(obj);
                if (i >= 0) 
                { 
                    r.Remove(i);
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
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
        }
#else
        public void Remove(Key key, IPersistent obj) 
        { 
            IPersistent s = index[key];
            if (s is Relation) 
            { 
                Relation r = (Relation)s;
                int i = r.IndexOf(obj);
                if (i >= 0) 
                { 
                    r.Remove(i);
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
            else if (s is ISet) 
            { 
                ISet ps = (ISet)s;
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
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
        }
#endif

#if USE_GENERICS
        public V Remove(Key key) 
#else
        public IPersistent Remove(Key key) 
#endif
        {
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }

#if USE_GENERICS
        public bool Put(K key, V obj) 
#else
        public bool Put(object key, IPersistent obj) 
#endif
        {
            return Put(KeyBuilder.getKeyFromObject(key), obj);
        }

#if USE_GENERICS
        public V Set(K key, V obj) 
#else
        public IPersistent Set(object key, IPersistent obj) 
#endif
        {
            return Set(KeyBuilder.getKeyFromObject(key), obj);
        }

#if USE_GENERICS
        public void Remove(K key, V obj) 
#else
        public void Remove(object key, IPersistent obj) 
#endif
        {
            Remove(KeyBuilder.getKeyFromObject(key), obj);
        }

#if USE_GENERICS
        public V RemoveKey(K key) 
#else
        public IPersistent Remove(object key) 
#endif
        {
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }

        public override void Deallocate() 
        {
            Clear();
            index.Deallocate();
            base.Deallocate();
        }
    }
}
