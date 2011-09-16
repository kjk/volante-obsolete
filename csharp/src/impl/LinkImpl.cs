namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Volante;

    public class LinkImpl<T> : ILink<T> where T : class,IPersistent
    {
        private void Modify()
        {
            if (owner != null)
                owner.Modify();
        }

        public int Count
        {
            get
            {
                return used;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public void CopyTo(T[] dst, int i)
        {
            Pin();
            Array.Copy(arr, 0, dst, i, used);
        }

        public virtual int Size()
        {
            return used;
        }

        public virtual int Length
        {
            get
            {
                return used;
            }

            set
            {
                if (value < used)
                {
                    Array.Clear(arr, value, used - value);
                    Modify();
                }
                else
                {
                    reserveSpace(value - used);
                }
                used = value;
            }
        }

        public virtual T this[int i]
        {
            get
            {
                return Get(i);
            }

            set
            {
                Set(i, value);
            }
        }

        void EnsureValidIndex(int i)
        {
            if (i < 0 || i >= used)
                throw new IndexOutOfRangeException();
        }

        public virtual T Get(int i)
        {
            EnsureValidIndex(i);
            return loadElem(i);
        }

        public virtual IPersistent GetRaw(int i)
        {
            EnsureValidIndex(i);
            return arr[i];
        }

        public virtual void Set(int i, T obj)
        {
            EnsureValidIndex(i);
            arr[i] = obj;
            Modify();
        }

        public bool Remove(T obj)
        {
            int i = IndexOf(obj);
            if (i >= 0)
            {
                RemoveAt(i);
                return true;
            }
            return false;
        }

        public virtual void RemoveAt(int i)
        {
            EnsureValidIndex(i);
            used -= 1;
            Array.Copy(arr, i + 1, arr, i, used - i);
            arr[used] = null;
            Modify();
        }

        internal void reserveSpace(int len)
        {
            if (used + len > arr.Length)
            {
                int newLen = used + len > arr.Length * 2 ? used + len : arr.Length * 2;
                IPersistent[] newArr = new IPersistent[newLen];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
            Modify();
        }

        public virtual void Insert(int i, T obj)
        {
            EnsureValidIndex(i);
            reserveSpace(1);
            Array.Copy(arr, i, arr, i + 1, used - i);
            arr[i] = obj;
            used += 1;
        }

        public virtual void Add(T obj)
        {
            reserveSpace(1);
            arr[used++] = obj;
        }

        public virtual void AddAll(T[] a)
        {
            AddAll(a, 0, a.Length);
        }

        public virtual void AddAll(T[] a, int from, int length)
        {
            reserveSpace(length);
            Array.Copy(a, from, arr, used, length);
            used += length;
        }

        public virtual void AddAll(ILink<T> link)
        {
            int n = link.Length;
            reserveSpace(n);
            for (int i = 0; i < n; i++)
            {
                arr[used++] = link.GetRaw(i);
            }
        }

        public virtual Array ToRawArray()
        {
            //TODO: this seems like the right code, but changing it
            //breaks a lot of code in Btree (it uses ILink internally
            //for its implementation). Maybe they rely on having the
            //original array 
            //T[] arrUsed = new T[used];
            //Array.Copy(arr, arrUsed, used);
            //return arrUsed;
            return arr;
        }

        public virtual T[] ToArray()
        {
            T[] a = new T[used];
            for (int i = used; --i >= 0; )
            {
                a[i] = loadElem(i);
            }
            return a;
        }

        public virtual bool Contains(T obj)
        {
            return IndexOf(obj) >= 0;
        }

        int IndexOfByOid(int oid)
        {
            for (int i = 0;  i < used; i++)
            {
                IPersistent elem = arr[i];
                if (elem != null && elem.Oid == oid)
                    return i;
            }
            return -1;
        }

        int IndexOfByObj(T obj)
        {
            IPersistent po = (IPersistent)obj;
            for (int i = 0; i < used; i++)
            {
                IPersistent o = arr[i];
                if (o == obj)
                    return i;
                // TODO: compare by oid if o is PersistentStub ?
            }
            return -1;
        }

        public virtual int IndexOf(T obj)
        {
            int oid = obj.Oid;
            int idx;
            if (obj != null && oid != 0)
                idx = IndexOfByOid(oid);
            else
                idx = IndexOfByObj(obj);
            return idx;
        }

        public virtual bool ContainsElement(int i, T obj)
        {
            EnsureValidIndex(i);
            IPersistent elem = arr[i];
            T elTyped = elem as T;
            if (elTyped == obj)
                return true;
            if (null == elem)
                return false;
            return elem.Oid != 0 && elem.Oid == obj.Oid;
        }

        public virtual void Clear()
        {
            Array.Clear(arr, 0, used);
            used = 0;
            Modify();
        }

        class LinkEnumerator : IEnumerator<T>
        {
            public void Dispose() { }

            public bool HasMore()
            {
                return i + 1 < link.Length;
            }

            public bool ReachEnd()
            {
                return i == link.Length + 1;
            }

            public bool MoveNext()
            {
                if (HasMore())
                {
                    i += 1;
                    return true;
                }
                i = link.Length + 1;
                return false;
            }

            public T Current
            {
                get
                {
                    if (ReachEnd())
                        throw new InvalidOperationException();
                    return link[i];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Reset()
            {
                i = -1;
            }

            internal LinkEnumerator(ILink<T> link)
            {
                this.link = link;
                i = -1;
            }

            private int i;
            private ILink<T> link;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new LinkEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new LinkEnumerator(this);
        }

        public void Pin()
        {
            for (int i = 0, n = used; i < n; i++)
            {
                arr[i] = loadElem(i);
            }
        }

        public void Unpin()
        {
            for (int i = 0, n = used; i < n; i++)
            {
                IPersistent elem = arr[i];
                if (elem != null && !elem.IsRaw() && elem.IsPersistent())
                    arr[i] = new PersistentStub(elem.Database, elem.Oid);
            }
        }

        private T loadElem(int i)
        {
            IPersistent elem = arr[i];
            if (elem != null && elem.IsRaw())
                elem = ((DatabaseImpl)elem.Database).lookupObject(elem.Oid, null);
            return (T)elem;
        }

        public void SetOwner(IPersistent owner)
        {
            this.owner = owner;
        }

        internal LinkImpl()
        {
        }

        internal LinkImpl(int initSize)
        {
            arr = new IPersistent[initSize];
        }

        internal LinkImpl(IPersistent[] arr, IPersistent owner)
        {
            this.arr = arr;
            this.owner = owner;
            used = arr.Length;
        }

        IPersistent[] arr;
        int used;
        [NonSerialized()]
        IPersistent owner;
    }
}