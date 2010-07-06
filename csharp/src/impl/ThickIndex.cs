using System;
using System.Collections;
using Perst;

namespace Perst.Impl
{
    class ThickIndex : PersistentResource, Index 
    { 
        private Index index;
        private int   nElems;

        const int BTREE_THRESHOLD = 128;

        internal ThickIndex(Type keyType, StorageImpl db) 
            : base(db)
        {
            index = db.CreateIndex(keyType, true);
        }
    
        ThickIndex() {}

        public int Count 
        { 
            get 
            {
                return nElems;
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
            set 
            {
                Set(key, value);
            }
        } 
    
        public IPersistent Get(Key key) 
        {
            IPersistent s = index.Get(key);
            if (s == null) 
            { 
                return null;
            }
            if (s is Relation) 
            { 
                Relation r = (Relation)s;
                if (r.Count == 1) 
                { 
                    return r[0];
                }
            }
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }
                  
        public IPersistent[] Get(Key from, Key till) 
        {
            return extend(index.Get(from, till));
        }

        public IPersistent Get(object key) 
        {
            return Get(Btree.getKeyFromObject(key));
        }
    
        public IPersistent[] Get(object from, object till) 
        {
            return Get(Btree.getKeyFromObject(from), Btree.getKeyFromObject(till));
        }

        private IPersistent[] extend(IPersistent[] s) 
        { 
            ArrayList list = new ArrayList();
            for (int i = 0; i < s.Length; i++) 
            { 
                list.AddRange((ICollection)s[i]);
            }
            return (IPersistent[])list.ToArray(typeof(IPersistent));
        }

                      
        public IPersistent[] GetPrefix(string prefix) 
        { 
            return extend(index.GetPrefix(prefix));
        }
    
        public IPersistent[] PrefixSearch(string word) 
        { 
            return extend(index.PrefixSearch(word));
        }
           
        public int Size() 
        { 
            return nElems;
        }
    
        public void Clear() 
        { 
            foreach (IPersistent o in this) 
            { 
                o.Deallocate();
            }
            index.Clear();
            nElems = 0;
            Modify();
        }

        public IPersistent[] ToArray() 
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

        class ExtendEnumerator : IEnumerator, IEnumerable
        {  
            public bool MoveNext() 
            { 
                while (!inner.MoveNext()) 
                {                 
                    if (outer.MoveNext()) 
                    {
                        inner = ((IEnumerable)outer.Current).GetEnumerator();
                    } 
                    else 
                    { 
                        return false;
                    }
                }
                return true;
            }

            public object Current 
            {
                get 
                {
                    return inner.Current;
                }
            }

            public void Reset() 
            {
                outer.Reset();
                if (outer.MoveNext()) 
                {
                    inner = ((IEnumerable)outer.Current).GetEnumerator();
                }
            }

            public IEnumerator GetEnumerator() 
            {
                return this;
            }

            internal ExtendEnumerator(IEnumerator enumerator) 
            { 
                outer = enumerator;
                Reset();
            }

            private IEnumerator outer;
            private IEnumerator inner;
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

            public bool MoveNext() 
            { 
                while (!inner.MoveNext()) 
                {                 
                    if (outer.MoveNext()) 
                    {
                        key = outer.Key;
                        inner = ((IEnumerable)outer.Value).GetEnumerator();
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
                outer.Reset();
                if (outer.MoveNext()) 
                {
                    key = outer.Key;
                    inner = ((IEnumerable)outer.Value).GetEnumerator();
                }
            }
       
            internal ExtendDictionaryEnumerator(IDictionaryEnumerator enumerator) 
            { 
                outer = enumerator;
                Reset();
            }

            private IDictionaryEnumerator outer;
            private IEnumerator           inner;
            private object                key;
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator() 
        { 
            return new ExtendDictionaryEnumerator(index.GetDictionaryEnumerator());
        }

        public virtual IEnumerator GetEnumerator() 
        { 
            return new ExtendEnumerator(index.GetEnumerator());
        }


        public IEnumerator GetEnumerator(Key from, Key till, IterationOrder order) 
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator GetEnumerator(object from, object till, IterationOrder order) 
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator GetEnumerator(Key from, Key till) 
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator GetEnumerator(object from, object till) 
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator GetEnumerator(string prefix) 
        {
            return StartsWith(prefix).GetEnumerator();
        }

        public virtual IEnumerable Range(Key from, Key till, IterationOrder order) 
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

        public virtual IEnumerable Range(Key from, Key till) 
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }
            
        public IEnumerable Range(object from, object till, IterationOrder order) 
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till, order));
        }

        public IEnumerable Range(object from, object till) 
        { 
            return new ExtendEnumerator(index.GetEnumerator(from, till));
        }
 
        public IEnumerable StartsWith(string prefix) 
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

        public bool Put(Key key, IPersistent obj) 
        { 
            IPersistent s = index[key];
            if (s == null) 
            { 
                Relation r = Storage.CreateRelation(null);
                r.Add(obj);
                index[key] = r;
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

        public IPersistent Set(Key key, IPersistent obj) 
        {
            IPersistent s = index[key];
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

        public IPersistent Remove(Key key) 
        {
            throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
        }

        public bool Put(object key, IPersistent obj) 
        {
            return Put(Btree.getKeyFromObject(key), obj);
        }

        public IPersistent Set(object key, IPersistent obj) 
        {
            return Set(Btree.getKeyFromObject(key), obj);
        }

        public void Remove(object key, IPersistent obj) 
        {
            Remove(Btree.getKeyFromObject(key), obj);
        }

        public IPersistent Remove(object key) 
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
