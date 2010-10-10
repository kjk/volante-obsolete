package org.nachodb.impl;
import  org.nachodb.*;
import  java.util.*;

class PersistentSet<T extends IPersistent> extends Btree<T> implements IPersistentSet<T> { 
    PersistentSet() { 
        type = ClassDescriptor.tpObject;
        unique = true;
    }

    public boolean isEmpty() { 
        return nElems == 0;
    }

    public boolean contains(Object o) {
        if (o instanceof IPersistent) { 
            Key key = new Key((IPersistent)o);
            Iterator i = iterator(key, key, ASCENT_ORDER);
            return i.hasNext();
        }
        return false;
    }
    
    public Object[] toArray() { 
        return toPersistentArray();
    }

    public <E> E[] toArray(E[] arr) { 
        return (E[])super.toArray((T[])arr);
    }

    public boolean add(T obj) { 
        if (!obj.isPersistent()) { 
            ((StorageImpl)getStorage()).makePersistent(obj);
        }
        return put(new Key(obj), obj);
    }

    public boolean remove(Object o) { 
        T obj = (T)o;
        try { 
            remove(new Key(obj), obj);
        } catch (StorageError x) { 
            if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) { 
                return false;
            }
            throw x;
        }
        return true;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Set)) {
            return false;
        }
        Collection c = (Collection) o;
        if (c.size() != size()) {
            return false;
        }
        return containsAll(c);
    }

    public int hashCode() {
        int h = 0;
        Iterator i = iterator();
        while (i.hasNext()) {
            h += ((IPersistent)i.next()).getOid();
        }
        return h;
    }
}
