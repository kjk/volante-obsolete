package org.nachodb.impl;
import  org.nachodb.*;
import  java.util.*;

class PersistentSet extends Btree implements IPersistentSet { 
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

    public Object[] toArray(Object a[]) { 
        return toPersistentArray((IPersistent[])a);
    }

    public boolean add(Object o) { 
        IPersistent obj = (IPersistent)o;
        if (!obj.isPersistent()) { 
            ((StorageImpl)getStorage()).makePersistent(obj);
        }
        return put(new Key(obj), obj);
    }

    public boolean remove(Object o) { 
        IPersistent obj = (IPersistent)o;
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
    
    public boolean containsAll(Collection c) { 
        Iterator i = c.iterator();
        while (i.hasNext()) { 
            if (!contains(i.next()))
                return false;
        }
        return true;
    }

    
    public boolean addAll(Collection c) {
        boolean modified = false;
        Iterator i = c.iterator();
        while (i.hasNext()) {
            modified |= add(i.next());
        }
        return modified;
    }

    public boolean retainAll(Collection c) {
        ArrayList toBeRemoved = new ArrayList();
        Iterator i = iterator();
        while (i.hasNext()) {
            Object o = i.next();
            if (!c.contains(o)) {
                toBeRemoved.add(o);
            }
        }
        int n = toBeRemoved.size();
        for (int j = 0; j < n; j++) { 
            remove(toBeRemoved.get(j));
        }
        return n != 0;         
    }

    public boolean removeAll(Collection c) {
        boolean modified = false;
        Iterator i = c.iterator();
        while (i.hasNext()) {
            modified |= remove(i.next());
        }
        return modified;
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
