package org.garret.perst;

/**
 * Base class for all persistent capable objects
 */
public class Persistent implements IPersistent { 
    public void load() {
        if (storage != null) { 
            storage.loadObject(this);
        }
    }
    
    public final boolean isRaw() { 
        return (state & RAW) != 0;
    } 
    
    public final boolean isModified() { 
        return (state & DIRTY) != 0;
    } 
    
    public final boolean isPersistent() { 
        return oid != 0;
    }
    
    public void makePersistent(Storage storage) { 
        if (oid == 0) { 
            storage.storeObject(this);
        }
    }

    public void store() {
        if ((state & RAW) != 0) { 
            throw new StorageError(StorageError.ACCESS_TO_STUB);
        }
        if (storage != null) { 
            storage.storeObject(this);
            state &= ~DIRTY;
        }
    }
  
    public void modify() { 
        if ((state & DIRTY) == 0 && storage != null) { 
            if ((state & RAW) != 0) { 
                throw new StorageError(StorageError.ACCESS_TO_STUB);
            }
            storage.modifyObject(this);
            state |= DIRTY;
        }
    }



    public final int getOid() {
        return oid;
    }

    public void deallocate() { 
        if (storage != null) { 
            storage.deallocateObject(this);
            storage = null;
        }
    }

    public boolean recursiveLoading() {
        return true;
    }
    
    public final Storage getStorage() {
        return storage;
    }
    
    public boolean equals(Object o) { 
        return o instanceof Persistent && ((Persistent)o).getOid() == oid;
    }

    public int hashCode() {
        return oid;
    }

    public void onLoad() {
    }

    transient Storage storage;
    transient int     oid;
    transient int     state;

    static final int RAW   = 1;
    static final int DIRTY = 2;
}





