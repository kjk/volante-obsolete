namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    public class LinkImpl : Link
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
            return arr[i];
        }
		
        public virtual void Set(int i, IPersistent obj)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = obj;
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
        }
		
        internal void reserveSpace(int len)
        {
            if (used + len > arr.Length)
            {
                IPersistent[] newArr = new IPersistent[used + len > arr.Length * 2?used + len:arr.Length * 2];
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
            arr[i] = obj;
            used += 1;
        }
		
        public virtual void Add(IPersistent obj)
        {
            reserveSpace(1);
            arr[used++] = obj;
        }
		
        public virtual void AddAll(IPersistent[] a)
        {
            AddAll(a, 0, a.Length);
        }
		
        public virtual void AddAll(IPersistent[] a, int from, int length)
        {
            reserveSpace(length);
            Array.Copy(a, from, arr, used, length);
            used += length;
        }
		
        public virtual void AddAll(Link link)
        {
            int n = link.Length;
            reserveSpace(n);
            for (int i = 0, j = used; i < n; i++, j++)
            {
                arr[j] = link.GetRaw(i);
            }
            used += n;
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
            for (int i = used; --i >= 0; )
            {
                if (arr[i].Equals(obj))
                {
                    return i;
                }
            }
            return - 1;
        }
		
        public virtual void Clear()
        {
            for (int i = used; --i >= 0; )
            {
                arr[i] = null;
            }
            used = 0;
        }
		
        class LinkEnumerator : IEnumerator { 
            public bool MoveNext() 
            {
                if (i+1 < link.Length) { 
                    i += 1;
                    return true;
                }
                return false;
            }

            public object Current
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

            internal LinkEnumerator(Link link) { 
                this.link = link;
                i = -1;
            }

            private int  i;
            private Link link;
        }      

        public IEnumerator GetEnumerator() 
        { 
            return new LinkEnumerator(this);
        }

        private IPersistent loadElem(int i)
        {
            IPersistent elem = arr[i];
            if (elem.IsRaw())
            {
                arr[i] = elem = ((StorageImpl) elem.Storage).lookupObject(elem.Oid, null);
            }
            return elem;
        }
		
        internal LinkImpl()
        {
        }
		
        internal LinkImpl(int initSize)
        {
            arr = new IPersistent[initSize];
        }
		
        internal LinkImpl(IPersistent[] arr)
        {
            this.arr = arr;
            used = arr.Length;
        }
		
        internal IPersistent[] arr;
        internal int used;
    }
}