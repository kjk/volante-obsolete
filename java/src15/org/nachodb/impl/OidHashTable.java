package org.garret.perst.impl;
import  org.garret.perst.IPersistent;

public interface OidHashTable { 
    boolean     remove(int oid);
    void        put(int oid, IPersistent obj);
    IPersistent get(int oid);
    void        flush();
    void        invalidate();
    int         size();
    void        setDirty(int oid);
    void        clearDirty(int oid);
}
