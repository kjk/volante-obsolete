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
            Object val = fld.GetValue(obj);
            Key key = null;
            switch (type) {
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
              case ClassDescriptor.FieldType.tpString:
                key = new Key((String)val);
                break;
             case ClassDescriptor.FieldType.tpEnum:
                key = new Key((Enum)val);
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