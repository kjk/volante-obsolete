namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Diagnostics;
    using Perst;
    
    class AltBtreeMultiFieldIndex:AltBtree, FieldIndex
    {
        internal String className;
        internal String[] fieldNames;
         [NonSerialized()]
        Type cls;
        [NonSerialized()]
        MemberInfo[] mbr;
 
        internal AltBtreeMultiFieldIndex()
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
                    { 
                        throw new StorageError(StorageError.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                    }
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

        public MemberInfo[] KeyFields 
        {
            get 
            { 
                return mbr;
            }
        }

        public override void OnLoad()
        {
            cls = ClassDescriptor.lookup(Storage, className);
            locateFields();
        }
        
        internal AltBtreeMultiFieldIndex(Type cls, string[] fieldNames, bool unique) 
        {
            this.cls = cls;
            this.unique = unique;
            this.fieldNames = fieldNames;
            this.className = cls.FullName;
            locateFields();
            type = ClassDescriptor.FieldType.tpRaw;        
        }
        
        [Serializable]
        internal class CompoundKey : IComparable
        {
            internal object[] keys;
            
            public int CompareTo(object o)
            {
                CompoundKey c = (CompoundKey) o;
                int n = keys.Length < c.keys.Length?keys.Length:c.keys.Length;
                for (int i = 0; i < n; i++)
                {
                    int diff = ((IComparable) keys[i]).CompareTo(c.keys[i]);
                    if (diff != 0)
                    {
                        return diff;
                    }
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
            {
                return null;
            }
            if (key.type != ClassDescriptor.FieldType.tpArrayOfObject)
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new Key(new CompoundKey((System.Object[]) key.oval), key.inclusion != 0);
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
        
        public bool Put(IPersistent obj) 
        {
            return base.Put(extractKey(obj), obj);
        }

        public IPersistent Set(IPersistent obj) 
        {
            return base.Set(extractKey(obj), obj);
        }

        public void Remove(IPersistent obj) 
        {
            base.Remove(new BtreeKey(extractKey(obj), obj));
        }
        
        public override IPersistent Remove(Key key) 
        {
            return base.Remove(convertKey(key));
        }       

        public override IPersistent Remove(object key) 
        {
            return base.Remove(convertKey(new Key(new object[]{key})));
        }       

        public bool Contains(IPersistent obj) 
        {
            Key key = extractKey(obj);
            if (unique) 
            { 
                return base.Get(key) != null;
            } 
            else 
            { 
                IPersistent[] mbrs = Get(key, key);
                for (int i = 0; i < mbrs.Length; i++) 
                { 
                    if (mbrs[i] == obj) 
                    { 
                        return true;
                    }
                }
                return false;
            }
        }

        public void Append(IPersistent obj) 
        {
            throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE);
        }

        public override IPersistent[] Get(Key from, Key till)
        {
            ArrayList list = new ArrayList();
            if (root != null)
            {
                root.find(convertKey(from), convertKey(till), height, list);
            }
            return (IPersistent[]) list.ToArray(cls);
        }

        public override IPersistent[] ToArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
            if (root != null) 
            { 
                root.traverseForward(height, arr, 0);
            }
            return arr;
        }

        public override IPersistent Get(Key key) 
        {
            return base.Get(convertKey(key));
        }
 
        public override IEnumerable Range(Key from, Key till, IterationOrder order) 
        { 
            return base.Range(convertKey(from), convertKey(till), order);
        }


        public override IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order) 
        {
            return base.GetDictionaryEnumerator(convertKey(from), convertKey(till), order);
        }
    }
}