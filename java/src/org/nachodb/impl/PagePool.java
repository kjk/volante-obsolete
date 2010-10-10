package org.nachodb.impl;
import  org.nachodb.*;

class PagePool { 
    LRU     lru;
    Page    freePages;
    Page    hashTable[];
    int     poolSize;
    boolean autoExtended;
    IFile   file;

    int     nDirtyPages;
    Page    dirtyPages[];
    
    boolean flushing;

    static final int INFINITE_POOL_INITIAL_SIZE = 8;

    PagePool(int poolSize) { 
        if (poolSize == 0) { 
            autoExtended = true;
            poolSize = INFINITE_POOL_INITIAL_SIZE;
        }            
        this.poolSize = poolSize;
    }

    final Page find(long addr, int state) {     
        //Assert.that((addr & (Page.pageSize-1)) == 0);
        Page pg;
        int pageNo = (int)(addr >>> Page.pageBits);
        int hashCode = pageNo % poolSize;

        synchronized (this) {           
            for (pg = hashTable[hashCode]; pg != null; pg = pg.collisionChain) 
            { 
                if (pg.offs == addr) {
                    if (pg.accessCount++ == 0) { 
                        pg.unlink();
                    }
                    break;
                }
            }
            if (pg == null) { 
                pg = freePages;
                if (pg != null) { 
                    freePages = (Page)pg.next;
                } else if (autoExtended) { 
                    if (pageNo >= poolSize) {
                        int newPoolSize = pageNo >= poolSize*2 ? pageNo+1 : poolSize*2;
                        Page[] newHashTable = new Page[newPoolSize];
                        System.arraycopy(hashTable, 0, newHashTable, 0, hashTable.length);
                        hashTable = newHashTable;
                        poolSize = newPoolSize;
                    }
                    pg = new Page();
                    hashCode = pageNo;
                } else { 
                    Assert.that("unfixed page available", lru.prev != lru);
                    pg = (Page)lru.prev;
                    pg.unlink();
                    synchronized (pg) { 
                        if ((pg.state & Page.psDirty) != 0) { 
                            pg.state = 0;
                            file.write(pg.offs, pg.data);
                            if (!flushing) { 
                                dirtyPages[pg.writeQueueIndex] = dirtyPages[--nDirtyPages];
                                dirtyPages[pg.writeQueueIndex].writeQueueIndex = pg.writeQueueIndex;
                            }
                        }
                    }
                    int h = (int)(pg.offs >> Page.pageBits) % poolSize;
                    Page curr = hashTable[h], prev = null;
                    while (curr != pg) { 
                        prev = curr;
                        curr = curr.collisionChain;
                    }
                    if (prev == null) { 
                        hashTable[h] = pg.collisionChain;
                    } else { 
                        prev.collisionChain = pg.collisionChain;
                    }
                }
                pg.accessCount = 1;
                pg.offs = addr;
                pg.state = Page.psRaw;
                pg.collisionChain = hashTable[hashCode];
                hashTable[hashCode] = pg;
            }
            if ((pg.state & Page.psDirty) == 0 && (state & Page.psDirty) != 0)
            {
                Assert.that(!flushing);
                if (nDirtyPages >= dirtyPages.length) {                     
                    Page[] newDirtyPages = new Page[nDirtyPages*2];
                    System.arraycopy(dirtyPages, 0, newDirtyPages, 0, dirtyPages.length);
                    dirtyPages = newDirtyPages;
                }
                dirtyPages[nDirtyPages] = pg;
                pg.writeQueueIndex = nDirtyPages++;
                pg.state |= Page.psDirty;
            }
            if ((pg.state & Page.psRaw) != 0) {
                if (file.read(pg.offs, pg.data) < Page.pageSize) {
                    for (int i = 0; i < Page.pageSize; i++) { 
                        pg.data[i] = 0;
                    }
                }
                pg.state &= ~Page.psRaw;
            }           
        }
        return pg;
    }


    final void copy(long dst, long src, long size) 
    {
        int dstOffs = (int)dst & (Page.pageSize-1);
        int srcOffs = (int)src & (Page.pageSize-1);
        dst -= dstOffs;
        src -= srcOffs;
        Page dstPage = find(dst, Page.psDirty);
        Page srcPage = find(src, 0);
        do { 
            if (dstOffs == Page.pageSize) { 
                unfix(dstPage);
                dst += Page.pageSize;
                dstPage = find(dst, Page.psDirty);
                dstOffs = 0;
            }
            if (srcOffs == Page.pageSize) { 
                unfix(srcPage);
                src += Page.pageSize;
                srcPage = find(src, 0);
                srcOffs = 0;
            }
            long len = size;
            if (len > Page.pageSize - srcOffs) { 
                len = Page.pageSize - srcOffs; 
            }
            if (len > Page.pageSize - dstOffs) { 
                len = Page.pageSize - dstOffs; 
            }
            System.arraycopy(srcPage.data, srcOffs, dstPage.data, dstOffs, (int)len);
            srcOffs += len;
            dstOffs += len;
            size -= len;
        } while (size != 0);
        unfix(dstPage);
        unfix(srcPage);
    }

    final void write(long dstPos, byte[] src) 
    {
        Assert.that((dstPos & (Page.pageSize-1)) == 0);
        Assert.that((src.length & (Page.pageSize-1)) == 0);
        for (int i = 0; i < src.length;) { 
            Page pg = find(dstPos, Page.psDirty);
            byte[] dst = pg.data;
            for (int j = 0; j < Page.pageSize; j++) { 
                dst[j] = src[i++];
            }
            unfix(pg);
            dstPos += Page.pageSize;
        }
    }

    final void open(IFile f) 
    {
        file = f;
        hashTable = new Page[poolSize];
        dirtyPages = new Page[poolSize];
        nDirtyPages = 0;
        lru = new LRU();
        freePages = null;
        if (!autoExtended) { 
            for (int i = poolSize; --i >= 0; ) { 
                Page pg = new Page();
                pg.next = freePages;
                freePages = pg;
            }
        }
    }

    final synchronized void close() {
        file.close();
        hashTable = null;
        dirtyPages = null;
        lru = null;
        freePages = null;
    }

    final synchronized void unfix(Page pg) { 
        Assert.that(pg.accessCount > 0);
        if (--pg.accessCount == 0) { 
            lru.link(pg);
        }
    }

    final synchronized void modify(Page pg) { 
        Assert.that(pg.accessCount > 0);
        if ((pg.state & Page.psDirty) == 0) { 
            Assert.that(!flushing);
            pg.state |= Page.psDirty;
            if (nDirtyPages >= dirtyPages.length) {                     
                Page[] newDirtyPages = new Page[nDirtyPages*2];
                System.arraycopy(dirtyPages, 0, newDirtyPages, 0, dirtyPages.length);
                dirtyPages = newDirtyPages;
            }
            dirtyPages[nDirtyPages] = pg;
            pg.writeQueueIndex = nDirtyPages++;
        }
    }
    
    final Page getPage(long addr) { 
        return find(addr, 0);
    }
    
    final Page putPage(long addr) { 
        return find(addr, Page.psDirty);
    }
    
    final byte[] get(long pos) { 
        Assert.that(pos != 0);
        int offs = (int)pos & (Page.pageSize-1);
        Page pg = find(pos - offs, 0);
        int size = ObjectHeader.getSize(pg.data, offs);
        Assert.that(size >= ObjectHeader.sizeof);
        byte[] obj = new byte[size];
        int dst = 0;
        while (size > Page.pageSize - offs) { 
            System.arraycopy(pg.data, offs, obj, dst, Page.pageSize - offs);
            unfix(pg);
            size -= Page.pageSize - offs;
            pos += Page.pageSize - offs;
            dst += Page.pageSize - offs;
            pg = find(pos, 0);
            offs = 0;
        }
        System.arraycopy(pg.data, offs, obj, dst, size);
        unfix(pg);
        return obj;
    }

    final void put(long pos, byte[] obj) { 
        put(pos, obj, obj.length);
    }

    final void put(long pos, byte[] obj, int size) { 
        int offs = (int)pos & (Page.pageSize-1);
        Page pg = find(pos - offs, Page.psDirty);
        int src = 0;
        while (size > Page.pageSize - offs) { 
            System.arraycopy(obj, src, pg.data, offs, Page.pageSize - offs);
            unfix(pg);
            size -= Page.pageSize - offs;
            pos += Page.pageSize - offs;
            src += Page.pageSize - offs;
            pg = find(pos, Page.psDirty);
            offs = 0;
        }
        System.arraycopy(obj, src, pg.data, offs, size);
        unfix(pg);
    }

    void flush() { 
        synchronized (this) { 
            flushing = true;
            java.util.Arrays.sort(dirtyPages, 0, nDirtyPages); 
        }
        for (int i = 0; i < nDirtyPages; i++) { 
            Page pg = dirtyPages[i];
            synchronized (pg) { 
                if ((pg.state & Page.psDirty) != 0) { 
                    file.write(pg.offs, pg.data);
                    pg.state &= ~Page.psDirty;
                }
            }
        }           
        file.sync();
        nDirtyPages = 0;
        flushing = false;
    }
}







