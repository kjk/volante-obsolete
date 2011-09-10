namespace Volante.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Collections;
    using System.Reflection;
    using System.Diagnostics;
    using Volante;

    class BtreeMultiFieldIndex<T> : Btree<object[], T>, IMultiFieldIndex<T> where T : class,IPersistent
    {
        internal String className;
        internal String[] fieldNames;
        [NonSerialized()]
        Type cls;
        [NonSerialized()]
        MemberInfo[] mbr;

        internal BtreeMultiFieldIndex()
        {
        }

        private void locateFields()
        {
            mbr = new MemberInfo[fieldNames.Length];
            for (int i = 0; i < fieldNames.Length; i++)
            {
                mbr[i] = cls.GetField(fieldNames[i], BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                if (mbr[i] == null)
                {
                    mbr[i] = cls.GetProperty(fieldNames[i], BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                    if (mbr[i] == null)
                        throw new DatabaseException(DatabaseException.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                }
            }
        }

        public Type IndexedClass
        {
            get
            {
                return cls;
            }
        }

        public MemberInfo KeyField
        {
            get
            {
                return mbr[0];
            }
        }

        public MemberInfo[] KeyFields
        {
            get
            {
                return mbr;
            }
        }

        public override void OnLoad()
        {
            cls = ClassDescriptor.lookup(Database, className);
            if (cls != typeof(T))
                throw new DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_VALUE_TYPE, cls);

            locateFields();
        }

        internal BtreeMultiFieldIndex(string[] fieldNames, bool unique)
        {
            this.cls = typeof(T);
            this.unique = unique;
            this.fieldNames = fieldNames;
            this.className = ClassDescriptor.getTypeName(cls);
            locateFields();
            type = ClassDescriptor.FieldType.tpRaw;
        }

        [Serializable]
        internal class CompoundKey : IComparable
        {
            internal object[] keys;

            public int CompareTo(object o)
            {
                CompoundKey c = (CompoundKey)o;
                int n = keys.Length < c.keys.Length ? keys.Length : c.keys.Length;
                for (int i = 0; i < n; i++)
                {
                    int diff = ((IComparable)keys[i]).CompareTo(c.keys[i]);
                    if (diff != 0)
                        return diff;
                }
                return keys.Length - c.keys.Length;
            }

            internal CompoundKey(object[] keys)
            {
                this.keys = keys;
            }
        }

        private Key convertKey(Key key)
        {
            if (key == null)
                return null;

            if (key.type != ClassDescriptor.FieldType.tpArrayOfObject)
                throw new DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);

            return new Key(new CompoundKey((System.Object[])key.oval), key.inclusion != 0);
        }

        private Key extractKey(IPersistent obj)
        {
            object[] keys = new object[mbr.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = mbr[i] is FieldInfo ? ((FieldInfo)mbr[i]).GetValue(obj) : ((PropertyInfo)mbr[i]).GetValue(obj, null);
            }
            return new Key(new CompoundKey(keys));
        }

        public bool Put(T obj)
        {
            return base.Put(extractKey(obj), obj);
        }

        public T Set(T obj)
        {
            return base.Set(extractKey(obj), obj);
        }

        public override bool Remove(T obj)
        {
            try
            {
                base.Remove(new BtreeKey(extractKey(obj), obj));
            }
            catch (DatabaseException x)
            {
                if (x.Code == DatabaseException.ErrorCode.KEY_NOT_FOUND)
                    return false;

                throw;
            }
            return true;
        }

        public override T Remove(Key key)
        {
            return base.Remove(convertKey(key));
        }


        public override bool Contains(T obj)
        {
            Key key = extractKey(obj);
            if (unique)
                return base.Get(key) == obj;

            T[] mbrs = GetNoKeyConvert(key, key);

            for (int i = 0; i < mbrs.Length; i++)
            {
                if (mbrs[i] == obj)
                    return true;
            }
            return false;
        }

        public void Append(T obj)
        {
            throw new DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE);
        }

        T[] GetNoKeyConvert(Key from, Key till)
        {
            ArrayList list = new ArrayList();
            if (root != null)
                root.find(from, till, height, list);
            return (T[])list.ToArray(cls);
        }

        public override T[] Get(Key from, Key till)
        {
            ArrayList list = new ArrayList();
            if (root != null)
                root.find(convertKey(from), convertKey(till), height, list);
            return (T[])list.ToArray(cls);
        }

        public override T[] ToArray()
        {
            T[] arr = new T[nElems];
            if (root != null)
                root.traverseForward(height, arr, 0);
            return arr;
        }

        public override T Get(Key key)
        {
            return base.Get(convertKey(key));
        }

        public override IEnumerable<T> Range(Key from, Key till, IterationOrder order)
        {
            return base.Range(convertKey(from), convertKey(till), order);
        }

        public override IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order)
        {
            return base.GetDictionaryEnumerator(convertKey(from), convertKey(till), order);
        }
    }
}