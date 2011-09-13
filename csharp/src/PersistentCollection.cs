using System;
using System.Collections;
using System.Collections.Generic;

namespace Volante
{
    /// <summary>
    /// Base class for all persistent collections
    /// </summary>
    public abstract class PersistentCollection<T> : PersistentResource, ICollection<T> where T : class,IPersistent
    {
        public PersistentCollection()
        {
        }

        public PersistentCollection(IDatabase db)
            : base(db)
        {
        }

        public abstract IEnumerator<T> GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public abstract int Count
        {
            get;
        }

        public virtual bool IsSynchronized
        {
            get
            {
                return true;
            }
        }

        public virtual object SyncRoot
        {
            get
            {
                return this;
            }
        }

        public virtual void CopyTo(T[] dst, int i)
        {
            foreach (object o in this)
            {
                dst.SetValue(o, i++);
            }
        }

        public virtual void Add(T obj)
        {
            throw new InvalidOperationException("Add is not supported");
        }

        public abstract void Clear();

        public virtual bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public virtual bool Contains(T obj)
        {
            foreach (T o in this)
            {
                if (o == obj)
                    return true;
            }
            return false;
        }

        public virtual bool Remove(T obj)
        {
            throw new InvalidOperationException("Remove is not supported");
        }
    }
}
