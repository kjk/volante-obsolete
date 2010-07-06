namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using Perst;
	
    class BtreeFieldIndex:Btree, FieldIndex
    {
        internal String className;
        internal String fieldName;
        [NonSerialized()]
        Type cls;
        [NonSerialized()]
        FieldInfo fld;
 
        internal BtreeFieldIndex()
        {
        }

        public override void onLoad()
        {
            cls = ClassDescriptor.lookup(className);
            fld = cls.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);            
            if (fld == null) { 
                throw new StorageError(StorageError.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
            }
        }
		
        internal BtreeFieldIndex(Type cls, String fieldName, bool unique) {
            fld = cls.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);            
            if (fld == null) { 
                throw new StorageError(StorageError.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
            }
            type = ClassDescriptor.getTypeCode(fld.FieldType);
            if ((int)type >= (int)ClassDescriptor.FieldType.tpLink) { 
                throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE, fld.FieldType);
            }
            this.cls = cls;
            this.unique = unique;
            this.fieldName = fieldName;
            this.className = cls.FullName;
        }

        private Key extractKey(IPersistent obj) { 
            Object value = fld.GetValue(obj);
            Key key = null;
            switch (type) {
              case ClassDescriptor.FieldType.tpBoolean:
                key = new Key((bool)value);
                break;
              case ClassDescriptor.FieldType.tpByte:
                key = new Key((byte)value);
                break;
              case ClassDescriptor.FieldType.tpSByte:
                key = new Key((sbyte)value);
                break;
              case ClassDescriptor.FieldType.tpShort:
                key = new Key((short)value);
                break;
              case ClassDescriptor.FieldType.tpUShort:
                key = new Key((ushort)value);
                break;
              case ClassDescriptor.FieldType.tpChar:
                key = new Key((char)value);
                break;
              case ClassDescriptor.FieldType.tpInt:
                key = new Key((int)value);
                break;            
              case ClassDescriptor.FieldType.tpUInt:
                key = new Key((uint)value);
                break;            
              case ClassDescriptor.FieldType.tpObject:
                key = new Key((IPersistent)value);
                break;
              case ClassDescriptor.FieldType.tpLong:
                key = new Key((long)value);
                break;            
              case ClassDescriptor.FieldType.tpULong:
                key = new Key((ulong)value);
                break;            
              case ClassDescriptor.FieldType.tpDate:
                key = new Key((DateTime)value);
                break;
              case ClassDescriptor.FieldType.tpFloat:
                key = new Key((float)value);
                break;
              case ClassDescriptor.FieldType.tpDouble:
                key = new Key((double)value);
                break;
              case ClassDescriptor.FieldType.tpString:
                key = new Key((String)value);
                break;
              default:
                Assert.failed("Invalid type");
                break;
            }
            return key;
        }
 
        public bool put(IPersistent obj) 
        {
            return base.insert(extractKey(obj), obj, false);
        }

        public void  remove(IPersistent obj) 
        {
            base.remove(new BtreeKey(extractKey(obj), obj.Oid));
        }
        
        public virtual IPersistent[] get(Key from, Key till)
        {
            if ((from != null && from.type != type) || (till != null && till.type != type))
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            ArrayList list = new ArrayList();
            if (root != 0)
            {
                BtreePage.find((StorageImpl) Storage, root, from, till, type, height, list);
            }
            return (IPersistent[]) list.ToArray(cls);
        }

        public IPersistent[] toArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
            if (root != 0) { 
                BtreePage.traverseForward((StorageImpl)Storage, root, type, height, arr, 0);
            }
            return arr;
        }
    }
}