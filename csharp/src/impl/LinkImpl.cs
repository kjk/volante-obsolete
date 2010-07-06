namespace Perst.Impl
{
    using System;
    using Perst;
	
    public class LinkImpl : Link
    {
        public virtual int size()
        {
            return used;
        }
		
        public virtual IPersistent get(int i)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            return loadElem(i);
        }
		
        public virtual IPersistent getRaw(int i)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            return arr[i];
        }
		
        public virtual void  set(int i, IPersistent obj)
        {
            if (i < 0 || i >= used)
            {
                throw new IndexOutOfRangeException();
            }
            arr[i] = obj;
        }
		
        public virtual void  remove(int i)
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
		
        public virtual void  insert(int i, IPersistent obj)
        {
            if (i < 0 || i > used)
            {
                throw new IndexOutOfRangeException();
            }
            reserveSpace(1);
            Array.Copy(arr, i + 1, arr, i, used - i);
            arr[i] = obj;
            used += 1;
        }
		
        public virtual void  add(IPersistent obj)
        {
            reserveSpace(1);
            arr[used++] = obj;
        }
		
        public virtual void  addAll(IPersistent[] a)
        {
            addAll(a, 0, a.Length);
        }
		
        public virtual void  addAll(IPersistent[] a, int from, int length)
        {
            reserveSpace(length);
            Array.Copy(a, from, arr, used, length);
            used += length;
        }
		
        public virtual void  addAll(Link link)
        {
            int n = link.size();
            reserveSpace(n);
            for (int i = 0, j = used; i < n; i++, j++)
            {
                arr[j] = link.getRaw(i);
            }
            used += n;
        }
		
        public virtual IPersistent[] toArray()
        {
            IPersistent[] a = new IPersistent[used];
            for (int i = used; --i >= 0; )
            {
                a[i] = loadElem(i);
            }
            return a;
        }
		
        public virtual bool contains(IPersistent obj)
        {
            return indexOf(obj) >= 0;
        }
		
        public virtual int indexOf(IPersistent obj)
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
		
        public virtual void  clear()
        {
            for (int i = used; --i >= 0; )
            {
                arr[i] = null;
            }
            used = 0;
        }
		
        private IPersistent loadElem(int i)
        {
            IPersistent elem = arr[i];
            if (elem.isRaw())
            {
                arr[i] = elem = ((StorageImpl) elem.Storage).lookupObject(elem.Oid, null);
            }
            return elem;
        }
		
        internal LinkImpl()
        {
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