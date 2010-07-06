package org.garret.perst.impl;
import  org.garret.perst.IPersistent;

public interface OidHashTable { 
    boolean     remove(int oid);
    void        put(int oid, IPersistent obj);
    IPersistent get(int oid);
    void        clear();
    int         size();
}
