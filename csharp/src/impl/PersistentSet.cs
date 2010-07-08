using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif
using NachoDB;

namespace NachoDB.Impl        
{

	
#if USE_GENERICS
    class PersistentSet<T> : Btree<T,T>, ISet<T> where T:class,IPersistent
#else
    class PersistentSet : Btree, ISet
#endif
    {
        public PersistentSet() 
        : base (ClassDescriptor.FieldType.tpObject, true)
        {
        }


#if USE_GENERICS
        public override bool Contains(T o) 
        {
            Key key = new Key(o);
            IEnumerator<T> e = GetEnumerator(key, key, IterationOrder.AscentOrder);
            return e.MoveNext();
        }
#else
        public bool Contains(IPersistent o) 
        {
            Key key = new Key(o);
            IEnumerator e = GetEnumerator(key, key, IterationOrder.AscentOrder);
            return e.MoveNext();
        }
#endif
    
#if USE_GENERICS
        public override void Add(T o) 
#else
        public void Add(IPersistent o) 
#endif
        { 
            if (!o.IsPersistent()) 
            { 
                ((StorageImpl)Storage).MakePersistent(o);
            }
            base.Put(new Key(o), o);
        }

#if USE_GENERICS
        public bool AddAll(ICollection<T> c) 
        {
            bool modified = false;
            foreach (T o in c) 
            {
                modified |= base.Put(new Key(o), o);
            }
            return modified;
        }
#else
        public bool AddAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= base.Put(new Key(o), o);
            }
            return modified;
        }
#endif


#if USE_GENERICS
        public override bool Remove(T o) 
#else
        public bool Remove(IPersistent o) 
#endif
        { 
            try 
            { 
                Remove(new Key(o), o);
            } 
            catch (StorageError x) 
            { 
                if (x.Code == StorageError.ErrorCode.KEY_NOT_FOUND) 
                { 
                    return false;
                }
                throw;
            }
            return true;
        }
    
#if USE_GENERICS
        public bool ContainsAll(ICollection<T> c) 
        { 
            foreach (T o in c) 
            { 
                if (!Contains(o)) 
                {
                    return false;
                }
            }
            return true;
        }
#else
        public bool ContainsAll(ICollection c) 
        { 
            foreach (IPersistent o in c) 
            { 
                if (!Contains(o)) 
                {
                    return false;
                }
            }
            return true;
        }
#endif
             
#if USE_GENERICS
        public bool RemoveAll(ICollection<T> c) 
        {
            bool modified = false;
            foreach (T o in c) 
            {
                modified |= Remove(o);
            }
            return modified;
        }
#else
        public bool RemoveAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= Remove(o);
            }
            return modified;
        }
#endif
        
        public override bool Equals(object o) 
        {
            if (o == this) 
            {
                return true;
            }
#if USE_GENERICS
            ISet<T> s = o as ISet<T>;
#else
            ISet s = o as ISet;
#endif
            if (s == null) 
            {
                return false;
            }
            if (Count != s.Count) 
            {
                return false;
            }
            return ContainsAll(s);
        }

        public override int GetHashCode() 
        {
            int h = 0;
            foreach (IPersistent o in this) 
            { 
                h += o.Oid;
            }
            return h;
        }
    }
}