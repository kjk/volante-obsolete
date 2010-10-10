package org.nachodb.impl;

class Page extends LRU implements Comparable {
    Page collisionChain;
    int  accessCount;
    int  writeQueueIndex;
    int  state;
    long offs;
    byte data[];

    static final int psDirty = 0x01;// page has been modified
    static final int psRaw   = 0x02;// page is loaded from the disk
    static final int psWait  = 0x04;// other thread(s) wait load operation completion

    static final int pageBits = 12;
    static final int pageSize = 1 << pageBits;

    public int compareTo(Object o) 
    { 
        long po = ((Page)o).offs;
        return offs < po ? -1 : offs == po ? 0 : 1;
    }

    Page() 
    { 
        data = new byte[pageSize];
    }
}

