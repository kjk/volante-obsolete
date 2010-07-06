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
        transient Type cls;
        [NonSerialized()]
        transient FieldInfo fld;
 
        internal BtreeFieldIndex()
        {
            cls = ClassDescriptor.lookup(className);
            fld = cls.GetField(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, fieldName);            
            if (fld == null) { 
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName, x);
            }
        }
		
        internal BtreeFieldIndex(Type cls, String fieldName, bool unique) {
            fld = cls.GetField(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, fieldName);            
            if (fld == null) { 
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName, x);
            }
            type = ClassDescriptor.getTypeCode(fld.FieldType);
            if ((int)type >= (int)ClassDescriptor.tpLink) { 
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
            }
            this.cls = cls;
            this.unique = unique;
            this.fieldName = fieldName;
            this.className = cls.getName();
        }

        private Key extractKey(IPersistent obj) { 
            Object value = fld.GetValue(obj);
            Key key = null;
            switch (type) {
              case ClassDescriptor.tpBoolean:
                key = new Key((bool)value);
                break;
              case ClassDescriptor.tpByte:
                key = new Key((byte)value);
                break;
              case ClassDescriptor.tpSByte:
                key = new Key((sbyte)value);
                break;
              case ClassDescriptor.tpShort:
                key = new Key((short)value);
                break;
              case ClassDescriptor.tpUShort:
                key = new Key((ushort)value);
                break;
              case ClassDescriptor.tpChar:
                key = new Key((char)value);
                break;
              case ClassDescriptor.tpInt:
                key = new Key((int)value);
                break;            
              case ClassDescriptor.tpUInt:
                key = new Key((uint)value);
                break;            
              case ClassDescriptor.tpObject:
                key = new Key((IPersistent)value);
                break;
              case ClassDescriptor.tpLong:
                key = new Key((long)value);
                break;            
              case ClassDescriptor.tpULong:
                key = new Key((ulong)value);
                break;            
              case ClassDescriptor.tpDate:
                key = new Key((DateTime)value);
                break;
              case ClassDescriptor.tpFloat:
                key = new Key((float)value);
                break;
              case ClassDescriptor.tpDouble:
                key = new Key((double)value);
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        }
 
        public boolean put(IPersistent obj) 
        {
            return base.insert(extractKey(obj), obj, false);
        }

        public void  remove(IPersistent obj) 
        {
            base.remove(new BtreeKey(extractKey(obj), obj.getOid()));
        }
        
        public IPersistent[] toArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
            if (root != 0) { 
                BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
            }
            return arr;
        }
    }
}