namespace NachoDB.Impl        
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
    using NachoDB;
    
#if USE_GENERICS
    class AltPersistentSet<T> : AltBtree<T,T>, ISet<T> where T:class,IPersistent
#else
    class AltPersistentSet : AltBtree, ISet
#endif
    {
        public AltPersistentSet() 
        { 
            type = ClassDescriptor.FieldType.tpObject;
            unique = true;
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
            base.Put(new Key(o), o);
        }

#if USE_GENERICS
        public bool AddAll(ICollection<T> c) 
#else
        public bool AddAll(ICollection c) 
#endif
        {
            bool modified = false;
#if USE_GENERICS
            foreach (T o in c)
#else
            foreach (IPersistent o in c)
#endif
            {
                modified |= base.Put(new Key(o), o);
            }
            return modified;
        }


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
                throw x;
            }
            return true;
        }
    
#if USE_GENERICS
        public bool ContainsAll(ICollection<T> c) 
        { 
            foreach (T o in c)
#else
        public bool ContainsAll(ICollection c) 
        { 
            foreach (IPersistent o in c)
#endif
            { 
                if (!Contains(o)) 
                {
                    return false;
                }
            }
            return true;
        }


             
#if USE_GENERICS
        public bool RemoveAll(ICollection<T> c) 
#else
        public bool RemoveAll(ICollection c) 
#endif
        {
            bool modified = false;
#if USE_GENERICS
            foreach (T o in c)
#else
            foreach (IPersistent o in c)
#endif
            {
                modified |= Remove(o);
            }
            return modified;
        }

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
