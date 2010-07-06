namespace Perst.Impl
{
    using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif
    using Perst;
	
#if USE_GENERICS
    public class LinkImpl<T> : Link<T> where T:class,IPersistent
#else
    public class LinkImpl : Link
#endif
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

#if USE_GENERICS
        public bool IsReadOnly 
        {
            get
            {
                return false;
            }
        }
#endif
 
#if USE_GENERICS
        public void CopyTo(T[] dst, int i) 
#else
        public void CopyTo(Array dst, int i) 
#endif
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

#if USE_GENERICS
        public virtual T this[int i] 
#else
        public virtual IPersistent this[int i] 
#endif
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
   
#if USE_GENERICS
        public virtual T Get(int i)
#else
        public virtual IPersistent Get(int i)
#endif
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
		
#if USE_GENERICS
        public virtual void Set(int i, T obj)
#else
        public virtual void Set(int i, IPersistent obj)
#endif
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = obj;
            Modify();
        }
		
#if USE_GENERICS
        public bool Remove(T obj) 
#else
        public bool Remove(IPersistent obj) 
#endif
        {
            int i = IndexOf(obj);
            if (i >= 0) 
            { 
                Remove(i);
                return true;
            }
            return false;
        }

#if USE_GENERICS
        public virtual void RemoveAt(int i)
        {
            Remove(i);
        }
#endif

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
                IPersistent[] newArr = new IPersistent[used + len > arr.Length * 2?used + len:arr.Length * 2];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
            Modify();
        }
		
#if USE_GENERICS
        public virtual void Insert(int i, T obj)
#else
        public virtual void Insert(int i, IPersistent obj)
#endif
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
		
#if USE_GENERICS
        public virtual void Add(T obj)
#else
        public virtual void Add(IPersistent obj)
#endif
        {
            reserveSpace(1);
            arr[used++] = obj;
        }
		
#if USE_GENERICS
        public virtual void AddAll(T[] a)
#else
        public virtual void AddAll(IPersistent[] a)
#endif
        {
            AddAll(a, 0, a.Length);
        }
		
#if USE_GENERICS
        public virtual void AddAll(T[] a, int from, int length)
#else
        public virtual void AddAll(IPersistent[] a, int from, int length)
#endif
        {
            reserveSpace(length);
            Array.Copy(a, from, arr, used, length);
            used += length;
        }
		
#if USE_GENERICS
        public virtual void AddAll(Link<T> link)
#else
        public virtual void AddAll(Link link)
#endif
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

#if USE_GENERICS
        public virtual T[] ToArray()
        {
            T[] a = new T[used];
#else
        public virtual IPersistent[] ToArray()
        {
            IPersistent[] a = new IPersistent[used];
#endif
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
		
#if USE_GENERICS
        public virtual bool Contains(T obj)
#else
        public virtual bool Contains(IPersistent obj)
#endif
        {
            return IndexOf(obj) >= 0;
        }
		
#if USE_GENERICS
        public virtual int IndexOf(T obj)
#else
        public virtual int IndexOf(IPersistent obj)
#endif
        {
            int oid;
            if (obj != null && (oid = obj.Oid) != 0) 
            { 
                for (int i = used; --i >= 0;) 
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
                for (int i = used; --i >= 0;) 
                {
                    if (arr[i] == obj) 
                    {
                        return i;
                    }
                }
            }
            return - 1;
        }
		
#if USE_GENERICS
        public virtual bool ContainsElement(int i, T obj) 
#else
        public virtual bool ContainsElement(int i, IPersistent obj) 
#endif
        {
            IPersistent elem = arr[i];
            return elem == obj || (elem != null && elem.Oid != 0 && elem.Oid == obj.Oid);
        }

        public virtual void Clear()
        {
            Array.Clear(arr, 0, used);
            used = 0;
            Modify();
        }
		
#if USE_GENERICS
        class LinkEnumerator : IEnumerator<T> { 
#else
        class LinkEnumerator : IEnumerator { 
#endif
            public void Dispose() {}

            public bool MoveNext() 
            {
                if (i+1 < link.Length) { 
                    i += 1;
                    return true;
                }
                return false;
            }

#if USE_GENERICS
            public T Current
#else
            public object Current
#endif
            {
                get 
                {
                    return link[i];
                }
            }

            public void Reset() 
            {
                i = -1;
            }

#if USE_GENERICS
            internal LinkEnumerator(Link<T> link) { 
#else
            internal LinkEnumerator(Link link) { 
#endif
                this.link = link;
                i = -1;
            }

            private int  i;
#if USE_GENERICS
            private Link<T> link;
#else
            private Link link;
#endif
        }      

#if USE_GENERICS
        public IEnumerator<T> GetEnumerator() 
#else
        public IEnumerator GetEnumerator() 
#endif
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

#if USE_GENERICS
        private T loadElem(int i)
#else
        private IPersistent loadElem(int i)
#endif
        {
            IPersistent elem = arr[i];
            if (elem != null && elem.IsRaw())
            {
                elem = ((StorageImpl) elem.Storage).lookupObject(elem.Oid, null);
            }
#if USE_GENERICS
            return (T)elem;
#else
            return elem;
#endif
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
        int           used;
        [NonSerialized()]
        IPersistent   owner;        
    }
}