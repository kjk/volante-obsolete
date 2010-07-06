namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    public class PArrayImpl : PArray
    {
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

        public virtual IPersistent this[int i] 
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
   
        public virtual IPersistent Get(int i)
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
		
        public virtual void Set(int i, IPersistent obj)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = storage.MakePersistent(obj);
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
		
        public virtual void Insert(int i, IPersistent obj)
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
		
        public virtual void Add(IPersistent obj)
        {
            reserveSpace(1);
            arr[used++] = storage.MakePersistent(obj);
        }
		
        public virtual void AddAll(IPersistent[] a)
        {
            AddAll(a, 0, a.Length);
        }
		
        public virtual void AddAll(IPersistent[] a, int from, int length)
        {
            int i, j;
            reserveSpace(length);
            for (i = from, j = used; --length >= 0; i++, j++) 
            { 
                arr[j] = storage.MakePersistent(a[i]); 
            }
            used = j;
        }
		
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
		
        public virtual Array ToRawArray()
        {
            return arr;
        }

        public virtual IPersistent[] ToArray()
        {
            IPersistent[] a = new IPersistent[used];
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
		
        public virtual bool Contains(IPersistent obj)
        {
            return IndexOf(obj) >= 0;
        }
		
        public virtual int IndexOf(IPersistent obj)
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
		
        public virtual bool ContainsElement(int i, IPersistent obj) 
        {
            int oid = arr[i];
            return (obj == null && oid == 0) || (obj != null && obj.Oid == oid);
        }

        public virtual void Clear()
        {
            Array.Clear(arr, 0, used);
            used = 0;
        }
		
        class ArrayEnumerator : IEnumerator { 
            public bool MoveNext() 
            {
                if (i+1 < arr.Length) { 
                    i += 1;
                    return true;
                }
                return false;
            }

            public object Current
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

            internal ArrayEnumerator(PArray arr) { 
                this.arr = arr;
                i = -1;
            }

            private int    i;
            private PArray arr;
        }      

        public IEnumerator GetEnumerator() 
        { 
            return new ArrayEnumerator(this);
        }

        public void Pin() 
        { 
        }

        public void Unpin() 
        { 
        }

        private IPersistent loadElem(int i)
        {
            return storage.lookupObject(arr[i], null);
        }
		

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