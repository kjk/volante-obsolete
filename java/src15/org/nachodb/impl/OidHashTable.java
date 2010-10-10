package org.nachodb.impl;
import  org.nachodb.IPersistent;

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
