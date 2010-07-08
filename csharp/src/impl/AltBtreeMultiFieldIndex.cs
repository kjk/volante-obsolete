namespace NachoDB.Impl
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#endif
    using System.Collections;
    using System.Reflection;
    using System.Diagnostics;
    using NachoDB;
    
#if USE_GENERICS
    class AltBtreeMultiFieldIndex<T>:AltBtree<object[],T>, MultiFieldIndex<T> where T:class,IPersistent
#else
    class AltBtreeMultiFieldIndex:AltBtree, MultiFieldIndex
#endif
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
            cls = ClassDescriptor.lookup(Storage, className);
#if USE_GENERICS
            if (cls != typeof(T)) 
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_VALUE_TYPE, cls);
            }
#endif
            locateFields();
        }
        
#if USE_GENERICS
        internal AltBtreeMultiFieldIndex(string[] fieldNames, bool unique) 
        {
            this.cls = typeof(T);
#else
        internal AltBtreeMultiFieldIndex(Type cls, string[] fieldNames, bool unique) 
        {
            this.cls = cls;
#endif
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
        
#if USE_GENERICS
        public bool Put(T obj) 
#else
        public bool Put(IPersistent obj) 
#endif
        {
            return base.Put(extractKey(obj), obj);
        }

#if USE_GENERICS
        public T Set(T obj) 
#else
        public IPersistent Set(IPersistent obj) 
#endif
        {
            return base.Set(extractKey(obj), obj);
        }

#if USE_GENERICS
        public override bool Remove(T obj) 
#else
        public bool Remove(IPersistent obj) 
#endif
        {
            try 
            { 
                base.Remove(new BtreeKey(extractKey(obj), obj));        
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
        public override T Remove(Key key) 
#else
        public override IPersistent Remove(Key key) 
#endif
        {
            return base.Remove(convertKey(key));
        }       

#if !USE_GENERICS
        public override IPersistent Remove(object key) 
        {
            return base.Remove(convertKey(new Key(new object[]{key})));
        }       
#endif

#if USE_GENERICS
        public override bool Contains(T obj) 
#else
        public bool Contains(IPersistent obj) 
#endif
        {
            Key key = extractKey(obj);
            if (unique) 
            { 
                return base.Get(key) != null;
            } 
            else 
            { 
#if USE_GENERICS
                T[] mbrs = Get(key, key);
#else
                IPersistent[] mbrs = Get(key, key);
#endif

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

#if USE_GENERICS
        public void Append(T obj) 
#else
        public void Append(IPersistent obj) 
#endif
        {
            throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE);
        }

#if USE_GENERICS
        public override T[] Get(Key from, Key till)
#else
        public override IPersistent[] Get(Key from, Key till)
#endif
        {
            ArrayList list = new ArrayList();
            if (root != null)
            {
                root.find(convertKey(from), convertKey(till), height, list);
            }
#if USE_GENERICS
            return (T[]) list.ToArray(cls);
#else
            return (IPersistent[]) list.ToArray(cls);
#endif
        }

#if USE_GENERICS
        public override T[] ToArray() 
        {
            T[] arr = new T[nElems];
#else
        public override IPersistent[] ToArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
#endif
            if (root != null) 
            { 
                root.traverseForward(height, arr, 0);
            }
            return arr;
        }

#if USE_GENERICS
        public override T Get(Key key) 
#else
        public override IPersistent Get(Key key) 
#endif
        {
            return base.Get(convertKey(key));
        }
 
#if USE_GENERICS
        public override IEnumerable<T> Range(Key from, Key till, IterationOrder order) 
#else
        public override IEnumerable Range(Key from, Key till, IterationOrder order) 
#endif
        { 
            return base.Range(convertKey(from), convertKey(till), order);
        }

        public override IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order) 
        {
            return base.GetDictionaryEnumerator(convertKey(from), convertKey(till), order);
        }
    }
}