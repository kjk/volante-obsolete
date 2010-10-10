package org.nachodb.impl;
import  java.lang.ref.*;

public class SoftHashTable extends WeakHashTable { 
    public SoftHashTable(int initialCapacity) {
        super(initialCapacity);
    }
    
    protected Reference createReference(Object obj) { 
        return new SoftReference(obj);
    }
}    
