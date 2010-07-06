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
    public class PArrayImpl<T> : PArray<T> where T:class,IPersistent
#else
    public class PArrayImpl : PArray
#endif
    {
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
            return new PersistentStub(storage, arr[i]);
        }
		
        public virtual int GetOid(int i)
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
            arr[i] = storage.MakePersistent(obj);
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
            arr[used] = 0;
        }
		
        internal void reserveSpace(int len)
        {
            if (used + len > arr.Length)
            {
                int[] newArr = new int[used + len > arr.Length * 2?used + len:arr.Length * 2];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
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
            arr[i] = storage.MakePersistent(obj);
            used += 1;
        }
		
#if USE_GENERICS
        public virtual void Add(T obj)
#else
        public virtual void Add(IPersistent obj)
#endif
        {
            reserveSpace(1);
            arr[used++] = storage.MakePersistent(obj);
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
            int i, j;
            reserveSpace(length);
            for (i = from, j = used; --length >= 0; i++, j++) 
            { 
                arr[j] = storage.MakePersistent(a[i]); 
            }
            used = j;
        }
		
#if USE_GENERICS
        public virtual void AddAll(Link<T> link)
        {
            int n = link.Length;
            reserveSpace(n);
            if (link is PArray<T>) 
            {
                PArray<T> src = (PArray<T>)link; 
                for (int i = 0, j = used; i < n; i++, j++)
                {
                    arr[j] = src.GetOid(i);
                }
            } else {
                for (int i = 0, j = used; i < n; i++, j++)
                {
                    arr[j] = storage.MakePersistent(link.GetRaw(i));
                }
            }
            used += n;
        }
#else
        public virtual void AddAll(Link link)
        {
            int n = link.Length;
            reserveSpace(n);
            if (link is PArray) 
            {
                PArray src = (PArray)link; 
                for (int i = 0, j = used; i < n; i++, j++)
                {
                    arr[j] = src.GetOid(i);
                }
            } else {
                for (int i = 0, j = used; i < n; i++, j++)
                {
                    arr[j] = storage.MakePersistent(link.GetRaw(i));
                }
            }
            used += n;
        }
#endif
		
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
            int oid = obj == null ? 0 : ((IPersistent)obj).Oid;
            for (int i = used; --i >= 0;) 
            {
                 if (arr[i] == oid) 
                 {
                     return i;
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
            int oid = arr[i];
            return (obj == null && oid == 0) || (obj != null && obj.Oid == oid);
        }

        public virtual void Clear()
        {
            Array.Clear(arr, 0, used);
            used = 0;
        }
		
#if USE_GENERICS
        class ArrayEnumerator : IEnumerator<T> { 
#else
        class ArrayEnumerator : IEnumerator { 
#endif
            public void Dispose() {}

            public bool MoveNext() 
            {
                if (i+1 < arr.Length) { 
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
                    return arr[i];
                }
            }

            public void Reset() 
            {
                i = -1;
            }

#if USE_GENERICS
            internal ArrayEnumerator(PArray<T> arr) { 
#else
            internal ArrayEnumerator(PArray arr) { 
#endif
                this.arr = arr;
                i = -1;
            }

            private int    i;
#if USE_GENERICS
            private PArray<T> arr;
#else
            private PArray arr;
#endif
        }      

#if USE_GENERICS
        public IEnumerator<T> GetEnumerator() 
#else
        public IEnumerator GetEnumerator() 
#endif
        { 
            return new ArrayEnumerator(this);
        }

        public void Pin() 
        { 
        }

        public void Unpin() 
        { 
        }

#if USE_GENERICS
        private T loadElem(int i)
        {
            return (T)storage.lookupObject(arr[i], null);
        }
#else
        private IPersistent loadElem(int i)
        {
            return storage.lookupObject(arr[i], null);
        }
#endif
		

        internal PArrayImpl()
        {
        }
		
        internal PArrayImpl(StorageImpl storage, int initSize)
        {
            this.storage = storage;
            arr = new int[initSize];
        }
		
        internal PArrayImpl(StorageImpl storage, int[] oids)
        {
            this.storage = storage;
            arr = oids;
            used = oids.Length;
        }
		
        internal int[]       arr;
        internal int         used;
        internal StorageImpl storage;
    }
}