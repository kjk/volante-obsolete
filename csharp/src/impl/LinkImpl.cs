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
            {
                owner.Modify();
            }
        }

        public int Count
        {
            get
            {
                return used;
            }
        }

        public bool IsSynchronized
        {
            get
            {
                return false;
            }
        }

        public object SyncRoot
        {
            get
            {
                return null;
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
                    Array.Clear(arr, value, used);
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

        public virtual T Get(int i)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            return loadElem(i);
        }

        public virtual IPersistent GetRaw(int i)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            return arr[i];
        }

        public virtual void Set(int i, T obj)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = obj;
            Modify();
        }

        public bool Remove(T obj)
        {
            int i = IndexOf(obj);
            if (i >= 0)
            {
                Remove(i);
                return true;
            }
            return false;
        }

        public virtual void RemoveAt(int i)
        {
            Remove(i);
        }

        public virtual void Remove(int i)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            used -= 1;
            Array.Copy(arr, i + 1, arr, i, used - i);
            arr[used] = null;
            Modify();
        }

        internal void reserveSpace(int len)
        {
            if (used + len > arr.Length)
            {
                IPersistent[] newArr = new IPersistent[used + len > arr.Length * 2 ? used + len : arr.Length * 2];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
            Modify();
        }

        public virtual void Insert(int i, T obj)
        {
            if (i < 0 || i > used)
            {
                throw new IndexOutOfRangeException();
            }
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
            for (int i = 0, j = used; i < n; i++, j++)
            {
                arr[j] = link.GetRaw(i);
            }
            used += n;
        }

        public virtual Array ToRawArray()
        {
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

        public virtual Array ToArray(Type elemType)
        {
            Array a = Array.CreateInstance(elemType, used);
            for (int i = used; --i >= 0; )
            {
                a.SetValue(loadElem(i), i);
            }
            return a;
        }

        public virtual bool Contains(T obj)
        {
            return IndexOf(obj) >= 0;
        }

        public virtual int IndexOf(T obj)
        {
            int oid;
            if (obj != null && (oid = obj.Oid) != 0)
            {
                for (int i = used; --i >= 0; )
                {
                    IPersistent elem = arr[i];
                    if (elem != null && elem.Oid == oid)
                    {
                        return i;
                    }
                }
            }
            else
            {
                for (int i = used; --i >= 0; )
                {
                    if ((T)arr[i] == obj)
                    {
                        return i;
                    }
                }
            }
            return -1;
        }

        public virtual bool ContainsElement(int i, T obj)
        {
            IPersistent elem = arr[i];
            return (T)elem == obj || (elem != null && elem.Oid != 0 && elem.Oid == obj.Oid);
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

            public bool MoveNext()
            {
                if (i + 1 < link.Length)
                {
                    i += 1;
                    return true;
                }
                return false;
            }

            public T Current
            {
                get
                {
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
                {
                    arr[i] = new PersistentStub(elem.Storage, elem.Oid);
                }
            }
        }

        private T loadElem(int i)
        {
            IPersistent elem = arr[i];
            if (elem != null && elem.IsRaw())
            {
                elem = ((StorageImpl)elem.Storage).lookupObject(elem.Oid, null);
            }
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