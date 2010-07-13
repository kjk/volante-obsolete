namespace NachoDB.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using NachoDB;
	
    public class PArrayImpl<T> : PArray<T> where T:class,IPersistent
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
		
        public virtual void Set(int i, T obj)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = storage.MakePersistent(obj);
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
            arr[used] = 0;
            Modify();
        }
		
        internal void reserveSpace(int len)
        {
            if (used + len > arr.Length)
            {
                int[] newArr = new int[used + len > arr.Length * 2?used + len:arr.Length * 2];
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
            arr[i] = storage.MakePersistent(obj);
            used += 1;
        }
		
        public virtual void Add(T obj)
        {
            reserveSpace(1);
            arr[used++] = storage.MakePersistent(obj);
        }
		
        public virtual void AddAll(T[] a)
        {
            AddAll(a, 0, a.Length);
        }
		
        public virtual void AddAll(T[] a, int from, int length)
        {
            int i, j;
            reserveSpace(length);
            for (i = from, j = used; --length >= 0; i++, j++) 
            { 
                arr[j] = storage.MakePersistent(a[i]); 
            }
            used = j;
        }
		
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
		
        public virtual bool ContainsElement(int i, T obj) 
        {
            int oid = arr[i];
            return (obj == null && oid == 0) || (obj != null && obj.Oid == oid);
        }

        public virtual void Clear()
        {
            Array.Clear(arr, 0, used);
            used = 0;
            Modify();
        }
		
        class ArrayEnumerator : IEnumerator<T>
        {
            public void Dispose() {}

            public bool MoveNext() 
            {
                if (i+1 < arr.Length) { 
                    i += 1;
                    return true;
                }
                return false;
            }

            public T Current
            {
                get 
                {
                    return arr[i];
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

            internal ArrayEnumerator(PArray<T> arr)
            {
                this.arr = arr;
                i = -1;
            }

            private int    i;
            private PArray<T> arr;
        }      

        public IEnumerator<T> GetEnumerator() 
        { 
            return new ArrayEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Pin() 
        { 
        }

        public void Unpin() 
        { 
        }

        private T loadElem(int i)
        {
            return (T)storage.lookupObject(arr[i], null);
        }
		
        public void SetOwner(IPersistent owner)
        { 
             this.owner = owner;
        }

        internal PArrayImpl()
        {
        }
		
        internal PArrayImpl(StorageImpl storage, int initSize)
        {
            this.storage = storage;
            arr = new int[initSize];
        }
		
        internal PArrayImpl(StorageImpl storage, int[] oids, IPersistent owner)
        {
            this.storage = storage;
            this.owner = owner;
            arr = oids;
            used = oids.Length;
        }
		
        int[]       arr;
        int         used;
        StorageImpl storage;
        [NonSerialized()]
        IPersistent owner;        
    }
}
