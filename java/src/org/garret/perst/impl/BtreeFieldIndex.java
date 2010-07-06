package org.garret.perst.impl;
import  org.garret.perst.*;

import java.lang.reflect.*;
import java.util.Date;

class BtreeFieldIndex extends Btree implements FieldIndex { 
    String className;
    String fieldName;
    transient Class cls;
    transient Field fld;

    BtreeFieldIndex() {
        try { 
            cls = Class.forName(className);
            fld = cls.getDeclaredField(fieldName);            
            fld.setAccessible(true);
        } catch (Exception x) { 
             throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName, x);
        }                   
    }

    BtreeFieldIndex(Class cls, String fieldName, boolean unique) {
        try { 
            fld = cls.getDeclaredField(fieldName);            
            fld.setAccessible(true);
        } catch (Exception x) { 
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName, x);
        }        
        type = ClassDescriptor.getTypeCode(fld.getType());
        if (type >= ClassDescriptor.tpLink) { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
        }
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = cls.getName();
    }

    private Key extractKey(IPersistent obj) { 
        try { 
            Field f = fld;
            Key key = null;
            switch (type) {
              case ClassDescriptor.tpBoolean:
                key = new Key(f.getBoolean(obj));
                break;
              case ClassDescriptor.tpByte:
                key = new Key(f.getByte(obj));
                break;
              case ClassDescriptor.tpShort:
                key = new Key(f.getShort(obj));
                break;
              case ClassDescriptor.tpChar:
                key = new Key(f.getChar(obj));
                break;
              case ClassDescriptor.tpInt:
                key = new Key(f.getInt(obj));
                break;            
              case ClassDescriptor.tpObject:
                key = new Key((IPersistent)f.get(obj));
                break;
              case ClassDescriptor.tpLong:
                key = new Key(f.getLong(obj));
                break;            
              case ClassDescriptor.tpDate:
                key = new Key((Date)f.get(obj));
                break;
              case ClassDescriptor.tpFloat:
                key = new Key(f.getFloat(obj));
                break;
              case ClassDescriptor.tpDouble:
                key = new Key(f.getDouble(obj));
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            

    public boolean put(IPersistent obj) {
        return super.insert(extractKey(obj), obj, false);
    }

    public void  remove(IPersistent obj) {
        super.remove(new BtreeKey(extractKey(obj), obj.getOid()));
    }
        
    public IPersistent[] toArray() {
        IPersistent[] arr = (IPersistent[])Array.newInstance(cls, nElems);
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        return arr;
    }
}

