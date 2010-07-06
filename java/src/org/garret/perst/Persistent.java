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
        return raw;
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
        if (raw) { 
            throw new StorageError(StorageError.ACCESS_TO_STUB);
        }
        if (storage != null) { 
            storage.storeObject(this);
        }
    }
  
    public final int getOid() {
        return oid;
    }

    public void deallocate() { 
        storage.deallocateObject(this);
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
    transient boolean raw;
}





