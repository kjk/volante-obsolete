namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Diagnostics;
    using Perst;
	
    class BtreeFieldIndex:Btree, FieldIndex
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
 
        internal BtreeFieldIndex()
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
                return new MemberInfo[]{mbr};
            }
        }

        public override void OnLoad()
        {
            cls = ClassDescriptor.lookup(Storage, className);
            lookupField(fieldName);
        }
		
        internal BtreeFieldIndex(Type cls, String fieldName, bool unique) {
            this.cls = cls;
            this.unique = unique;
            this.fieldName = fieldName;
            this.className = cls.FullName;
            lookupField(fieldName);
            type = checkType(mbrType);
        }

        protected override object unpackEnum(int val) 
        {
            return Enum.ToObject(mbrType, val);
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
                    key = new Key((String)val);
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
 
        public bool Put(IPersistent obj) 
        {
            return base.insert(extractKey(obj), obj, false);
        }

        public void Set(IPersistent obj) 
        {
            base.insert(extractKey(obj), obj, true);
        }

        public void Remove(IPersistent obj) 
        {
            base.remove(new BtreeKey(extractKey(obj), obj.Oid));
        }
        
        public bool Contains(IPersistent obj) 
        {
            Key key = extractKey(obj);
            if (unique) { 
                return base.Get(key) != null;
            } else { 
                IPersistent[] mbrs = Get(key, key);
                for (int i = 0; i < mbrs.Length; i++) { 
                    if (mbrs[i] == obj) { 
                        return true;
                    }
                }
                return false;
            }
        }

        public void Append(IPersistent obj) {
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

        public override IPersistent[] Get(Key from, Key till)
        {
            if ((from != null && from.type != type) || (till != null && till.type != type))
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            ArrayList list = new ArrayList();
            if (root != 0)
            {
                BtreePage.find((StorageImpl) Storage, root, from, till, this, height, list);
            }
            return (IPersistent[]) list.ToArray(cls);
        }

        public override IPersistent[] ToArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
            if (root != 0) { 
                BtreePage.traverseForward((StorageImpl)Storage, root, type, height, arr, 0);
            }
            return arr;
        }
    }
}