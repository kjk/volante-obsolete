namespace Perst.Impl        
{
    using System;
    using System.Collections;
    using Perst;
	
    class PersistentSet : Btree, ISet
    {
        public PersistentSet() 
        { 
            type = ClassDescriptor.FieldType.tpObject;
            unique = true;
        }


        public bool Contains(IPersistent o) 
        {
            Key key = new Key(o);
            IEnumerator e = GetEnumerator(key, key, IterationOrder.AscentOrder);
            return e.MoveNext();
        }
    
        public bool Add(IPersistent o) 
        { 
            if (!o.IsPersistent()) 
            { 
                ((StorageImpl)Storage).storeObject(o);
            }
            return insert(new Key(o), o, false);
        }

        public bool AddAll(IEnumerator e) 
        {
            bool modified = false;
            while (e.MoveNext()) 
            {
                modified |= Add((IPersistent)e.Current);
            }
            return modified;
        }


        public bool AddAll(IEnumerable e) 
        {
            return AddAll(e.GetEnumerator());
        }

        public bool Remove(IPersistent o) 
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
    
        public bool ContainsAll(IEnumerator e) 
        { 
            while (e.MoveNext()) 
            { 
                if (!Contains((IPersistent)e.Current)) 
                {
                    return false;
                }
            }
            return true;
        }

        public bool ContainsAll(IEnumerable e) 
        {
            return ContainsAll(e.GetEnumerator());
        }

             
        public bool RemoveAll(IEnumerator e) 
        {
            bool modified = false;
            while (e.MoveNext())  
            {
                modified |= Remove((IPersistent)e.Current);
            }
            return modified;
        }

        public bool RemoveAll(IEnumerable e) 
        {
            return RemoveAll(e.GetEnumerator());
        }

        
        public override bool Equals(object o) 
        {
            if (o == this) 
            {
                return true;
            }
            ISet s = o as ISet;
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