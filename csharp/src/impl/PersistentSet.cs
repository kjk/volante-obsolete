#if !OMIT_BTREE
using System;
using System.Collections;
using System.Collections.Generic;
using Volante;

namespace Volante.Impl
{
    class PersistentSet<T> : Btree<T, T>, ISet<T> where T : class, IPersistent
    {
        public PersistentSet()
            : base(ClassDescriptor.FieldType.tpObject, true)
        {
        }

        public override bool Contains(T o)
        {
            Key key = new Key(o);
            IEnumerator<T> e = GetEnumerator(key, key, IterationOrder.AscentOrder);
            return e.MoveNext();
        }

        public override void Add(T o)
        {
            if (!o.IsPersistent())
            {
                ((StorageImpl)Storage).MakePersistent(o);
            }
            base.Put(new Key(o), o);
        }

        public bool AddAll(ICollection<T> c)
        {
            bool modified = false;
            foreach (T o in c)
            {
                modified |= base.Put(new Key(o), o);
            }
            return modified;
        }

        public override bool Remove(T o)
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

        public bool RemoveAll(ICollection<T> c)
        {
            bool modified = false;
            foreach (T o in c)
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
            ISet<T> s = o as ISet<T>;
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
#endif
