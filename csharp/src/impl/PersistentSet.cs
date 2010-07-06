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
                ((StorageImpl)Storage).MakePersistent(o);
            }
            return base.Put(new Key(o), o);
        }

        public bool AddAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
            {
                modified |= Add(o);
            }
            return modified;
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
             
        public bool RemoveAll(ICollection c) 
        {
            bool modified = false;
            foreach (IPersistent o in c) 
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