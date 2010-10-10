package org.nachodb.impl;
import  org.nachodb.*;

import java.lang.reflect.*;
import java.util.*;
import java.io.Serializable;

class AltBtreeMultiFieldIndex extends AltBtree implements FieldIndex { 
    String   className;
    String[] fieldName;

    transient Class   cls;
    transient Field[] fld;

    AltBtreeMultiFieldIndex() {}
    
    AltBtreeMultiFieldIndex(Class cls, String[] fieldName, boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;        
        this.className = cls.getName();
        locateFields();
        type = ClassDescriptor.tpRaw;        
    }

    private final void locateFields() 
    {
        fld = new Field[fieldName.length];
        for (int i = 0; i < fieldName.length; i++) {
            Class scope = cls;
            try { 
                do { 
                    try { 
                        fld[i] = scope.getDeclaredField(fieldName[i]);                
                        fld[i].setAccessible(true);
                        break;
                    } catch (NoSuchFieldException x) { 
                        scope = scope.getSuperclass();
                    }
                } while (scope != null);
            } catch (Exception x) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, className + "." + fieldName[i], x);
            }
            if (fld[i] == null) { 
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName[i]);
            }
        }
    }

    public Class getIndexedClass() { 
        return cls;
    }

    public Field[] getKeyFields() { 
        return fld;
    }

    public void onLoad()
    {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }

    static class CompoundKey implements Comparable, Serializable {
        Object[] keys;

        public int compareTo(Object o) { 
            CompoundKey c = (CompoundKey)o;
            int n = keys.length < c.keys.length ? keys.length : c.keys.length; 
            for (int i = 0; i < n; i++) { 
                int diff = ((Comparable)keys[i]).compareTo(c.keys[i]);
                if (diff != 0) { 
                    return diff;
                }
            }
            return keys.length - c.keys.length;
        }

        CompoundKey(Object[] keys) { 
            this.keys = keys;
        }
    }
                
    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new Key(new CompoundKey((Object[])key.oval), key.inclusion != 0);
    }
            
    private Key extractKey(IPersistent obj) {
        Object[] keys = new Object[fld.length];
        try { 
            for (int i = 0; i < keys.length; i++) { 
                keys[i] = fld[i].get(obj);
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        return new Key(new CompoundKey(keys));
    }

    public boolean put(IPersistent obj) {
        return super.put(extractKey(obj), obj);
    }

    public IPersistent set(IPersistent obj) {
        return super.set(extractKey(obj), obj);
    }

    public void remove(IPersistent obj) {
        super.remove(new BtreeKey(extractKey(obj), obj));
    }

    public IPersistent remove(Key key) {
        return super.remove(convertKey(key));
    }
    
    public boolean contains(IPersistent obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i] == obj) { 
                    return true;
                }
            }
            return false;
        }
    }

    public void append(IPersistent obj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    public IPersistent[] get(Key from, Key till) {
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(convertKey(from), convertKey(till), height, list);
        }
        return (IPersistent[])list.toArray((Object[])Array.newInstance(cls, list.size()));
    }

    public IPersistent[] toPersistentArray() {
        IPersistent[] arr = (IPersistent[])Array.newInstance(cls, nElems);
        if (root != null) { 
            root.traverseForward(height, arr, 0);
        }
        return arr;
    }

    public IPersistent get(Key key) {
        return super.get(convertKey(key));
    }

    public Iterator iterator(Key from, Key till, int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    public Iterator entryIterator(Key from, Key till, int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }
}

