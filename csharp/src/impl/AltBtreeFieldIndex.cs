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
    class AltBtreeFieldIndex<K,V> : AltBtree<K,V>, FieldIndex<K,V> where V:class, IPersistent
#else
    class AltBtreeFieldIndex : AltBtree, FieldIndex
#endif
    {
        internal String className;
        internal String fieldName;
        internal long   autoincCount;
        [NonSerialized()]
        Type cls;
        [NonSerialized()]
        MemberInfo mbr;
        [NonSerialized()]
        Type mbrType;
 
        internal AltBtreeFieldIndex()
        {
        }

        private void lookupField(String name) 
        {
            FieldInfo fld = cls.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            if (fld == null) 
            { 
                PropertyInfo prop = cls.GetProperty(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                if (prop == null)  
                {
                    throw new StorageError(StorageError.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
                }
                mbrType = prop.PropertyType;
                mbr = prop;
            } 
            else 
            {
                mbrType = fld.FieldType;
                mbr = fld;
            }
#if USE_GENERICS
            if (mbrType != typeof(K)) { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE, mbrType);
            }    
#endif
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
                return mbr;
            }
        }

        public override void OnLoad()
        {
            cls = ClassDescriptor.lookup(Storage, className);
#if USE_GENERICS
            if (cls != typeof(V)) 
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_VALUE_TYPE, mbrType);
            }
#endif
            lookupField(fieldName);
        }
        
#if USE_GENERICS
        internal AltBtreeFieldIndex(String fieldName, bool unique) 
        {
            this.cls = typeof(K);
#else
        internal AltBtreeFieldIndex(Type cls, String fieldName, bool unique) 
        {
            this.cls = cls;
#endif
            this.unique = unique;
            this.fieldName = fieldName;
            this.className = ClassDescriptor.getTypeName(cls);
            lookupField(fieldName);
            type = checkType(mbrType);
        }
        
        private Key extractKey(IPersistent obj) 
        { 
            Object val = mbr is FieldInfo ? ((FieldInfo)mbr).GetValue(obj) : ((PropertyInfo)mbr).GetValue(obj, null);
            Key key = null;
            switch (type) 
            {
                case ClassDescriptor.FieldType.tpBoolean:
                    key = new Key((bool)val);
                    break;
                case ClassDescriptor.FieldType.tpByte:
                    key = new Key((byte)val);
                    break;
                case ClassDescriptor.FieldType.tpSByte:
                    key = new Key((sbyte)val);
                    break;
                case ClassDescriptor.FieldType.tpShort:
                    key = new Key((short)val);
                    break;
                case ClassDescriptor.FieldType.tpUShort:
                    key = new Key((ushort)val);
                    break;
                case ClassDescriptor.FieldType.tpChar:
                    key = new Key((char)val);
                    break;
                case ClassDescriptor.FieldType.tpInt:
                    key = new Key((int)val);
                    break;            
                case ClassDescriptor.FieldType.tpUInt:
                    key = new Key((uint)val);
                    break;            
                case ClassDescriptor.FieldType.tpObject:
                    key = new Key((IPersistent)val);
                    break;
                case ClassDescriptor.FieldType.tpOid:
                    key = new Key(ClassDescriptor.FieldType.tpOid, (int)val);
                    break;
                case ClassDescriptor.FieldType.tpLong:
                    key = new Key((long)val);
                    break;            
                case ClassDescriptor.FieldType.tpULong:
                    key = new Key((ulong)val);
                    break;            
                case ClassDescriptor.FieldType.tpDate:
                    key = new Key((DateTime)val);
                    break;
                case ClassDescriptor.FieldType.tpFloat:
                    key = new Key((float)val);
                    break;
                case ClassDescriptor.FieldType.tpDouble:
                    key = new Key((double)val);
                    break;
                case ClassDescriptor.FieldType.tpDecimal:
                    key = new Key((decimal)val);
                    break;
                case ClassDescriptor.FieldType.tpGuid:
                    key = new Key((Guid)val);
                    break;
                case ClassDescriptor.FieldType.tpString:
                    key = new Key(((string)val).ToCharArray());
                    break;
                case ClassDescriptor.FieldType.tpEnum:
                    key = new Key((Enum)val);
                    break;
                default:
                    Debug.Assert(false, "Invalid type");
                    break;
            }
            return key;
        }
        
        
#if USE_GENERICS
        public bool Put(V obj) 
#else
        public bool Put(IPersistent obj) 
#endif
        {
            return base.Put(extractKey(obj), obj);
        }

#if USE_GENERICS
        public V Set(V obj) 
#else
        public IPersistent Set(IPersistent obj) 
#endif
        {
            return base.Set(extractKey(obj), obj);
        }

#if USE_GENERICS
        public override bool Remove(V obj) 
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
        public override bool Contains(V obj) 
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
                V[] mbrs = Get(key, key);
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
        public void Append(V obj) 
#else
        public void Append(IPersistent obj) 
#endif
        {
            lock(this) 
            { 
                Key key;
                object val; 
                switch (type) 
                {
                    case ClassDescriptor.FieldType.tpInt:
                        key = new Key((int)autoincCount);
                        val = (int)autoincCount;
                        break;            
                    case ClassDescriptor.FieldType.tpLong:
                        key = new Key(autoincCount);
                        val = autoincCount;
                        break;            
                    default:
                        throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE, mbrType);
                }
                if (mbr is FieldInfo) 
                { 
                    ((FieldInfo)mbr).SetValue(obj, val);
                } 
                else 
                {
                    ((PropertyInfo)mbr).SetValue(obj, val, null);
                }              
                autoincCount += 1;
                obj.Modify();
                base.insert(key, obj, false);
            }
        }

#if !USE_GENERICS
        public override IPersistent[] Get(Key from, Key till)
        {
            ArrayList list = new ArrayList();
            if (root != null)
            {
                root.find(checkKey(from), checkKey(till), height, list);
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
#endif
    }
}