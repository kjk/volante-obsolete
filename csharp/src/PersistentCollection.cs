using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif

namespace NachoDB
{
    /// <summary>
    /// Base class for all persistent collections
    /// </summary>
#if USE_GENERICS
    public abstract class PersistentCollection<T> : PersistentResource, ICollection<T> where T:class,IPersistent
#else
    public abstract class PersistentCollection : PersistentResource, ICollection
#endif
    {
        public PersistentCollection()
        {
        }

        public PersistentCollection(Storage storage)
        : base(storage) 
        {
        }

#if USE_GENERICS
        public abstract IEnumerator<T> GetEnumerator();
#else
        public abstract IEnumerator GetEnumerator();
#endif
        
        
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

#if USE_GENERICS
        public virtual void CopyTo(T[] dst, int i) 
#else
        public virtual void CopyTo(Array dst, int i) 
#endif
        {
            foreach (object o in this) 
            { 
                dst.SetValue(o, i++);
            }
        }

#if USE_GENERICS
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
                {
                    return true;
                }
            }
            return false;
        }

        public virtual bool Remove(T obj) 
        {        
            throw new InvalidOperationException("Remove is not supported");
        }
#endif
    }
}
