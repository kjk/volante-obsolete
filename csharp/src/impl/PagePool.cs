namespace Perst.Impl        
{
    using System;
    using Perst;
	
    class PagePool
    {
        internal LRU lru;
        internal Page freePages;
        internal Page[] hashTable;
        internal int poolSize;
        internal long fileSize;
        internal FileIO file;
		
        internal int nDirtyPages;
        internal Page[] dirtyPages;
		
        internal bool flushing;
		
        internal PagePool(int poolSize)
        {
            this.poolSize = poolSize;
        }
		
        internal Page find(long addr, int state)
        {
            Assert.that((addr & (Page.pageSize - 1)) == 0);
            Page pg;
            int hashCode = (int)(addr >> Page.pageBits) % poolSize;
			
            lock(this)
            {
                for (pg = hashTable[hashCode]; pg != null; pg = pg.collisionChain)
                {
                    if (pg.offs == addr)
                    {
                        if (pg.accessCount++ == 0)
                        {
                            pg.unlink();
                        }
                        break;
                    }
                }
                if (pg == null)
                {
                    pg = freePages;
                    if (pg != null)
                    {
                        freePages = (Page) pg.next;
                    }
                    else
                    {
                        Assert.that("unfixed page available", lru.prev != lru);
                        pg = (Page) lru.prev;
                        pg.unlink();
                        lock(pg)
                        {
                            if ((pg.state & Page.psDirty) != 0)
                            {
                                pg.state = 0;
                                file.write(pg.offs, pg.data);
                                if (!flushing)
                                {
                                    dirtyPages[pg.writeQueueIndex] = dirtyPages[--nDirtyPages];
                                    dirtyPages[pg.writeQueueIndex].writeQueueIndex = pg.writeQueueIndex;
                                }
                                if (pg.offs >= fileSize)
                                {
                                    fileSize = pg.offs + Page.pageSize;
                                }
                            }
                        }
                        int h = (int) (pg.offs >> Page.pageBits) % poolSize;
                        Page curr = hashTable[h], prev = null;
                        while (curr != pg)
                        {
                            prev = curr;
                            curr = curr.collisionChain;
                        }
                        if (prev == null)
                        {
                            hashTable[h] = pg.collisionChain;
                        }
                        else
                        {
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
                    dirtyPages[nDirtyPages] = pg;
                    pg.writeQueueIndex = nDirtyPages++;
                    pg.state |= Page.psDirty;
                }
            }
            lock(pg)
            {
                if ((pg.state & Page.psRaw) != 0)
                {
                    if (file.read(pg.offs, pg.data) < Page.pageSize)
                    {
                        for (int i = 0; i < Page.pageSize; i++)
                        {
                            pg.data[i] = 0;
                        }
                    }
                    pg.state &= ~ Page.psRaw;
                }
            }
            return pg;
        }
		
		
        internal void  copy(long dst, long src, long size)
        {
            int dstOffs = (int) dst & (Page.pageSize - 1);
            int srcOffs = (int) src & (Page.pageSize - 1);
            dst -= dstOffs;
            src -= srcOffs;
            Page dstPage = find(dst, Page.psDirty);
            Page srcPage = find(src, 0);
            do 
            {
                if (dstOffs == Page.pageSize)
                {
                    unfix(dstPage);
                    dst += Page.pageSize;
                    dstPage = find(dst, Page.psDirty);
                    dstOffs = 0;
                }
                if (srcOffs == Page.pageSize)
                {
                    unfix(srcPage);
                    src += Page.pageSize;
                    srcPage = find(src, 0);
                    srcOffs = 0;
                }
                long len = size;
                if (len > Page.pageSize - srcOffs)
                {
                    len = Page.pageSize - srcOffs;
                }
                if (len > Page.pageSize - dstOffs)
                {
                    len = Page.pageSize - dstOffs;
                }
                Array.Copy(srcPage.data, srcOffs, dstPage.data, dstOffs, (int) len);
                srcOffs = (int) (srcOffs + len);
                dstOffs = (int) (dstOffs + len);
                size -= len;
            }
            while (size != 0);
            unfix(dstPage);
            unfix(srcPage);
        }
		
        internal void  open(FileIO f, long size)
        {
            file = f;
            fileSize = size;
            hashTable = new Page[poolSize];
            dirtyPages = new Page[poolSize];
            nDirtyPages = 0;
            lru = new LRU();
            freePages = null;
            for (int i = poolSize; --i >= 0; )
            {
                Page pg = new Page();
                pg.next = freePages;
                freePages = pg;
            }
        }
		
        internal void  close()
        {
            lock(this)
            {
                file.close();
                hashTable = null;
                dirtyPages = null;
                lru = null;
                freePages = null;
            }
        }
		
        internal void  unfix(Page pg)
        {
            lock(this)
            {
                Assert.that(pg.accessCount > 0);
                if (--pg.accessCount == 0)
                {
                    lru.link(pg);
                }
            }
        }
		
        internal void  modify(Page pg)
        {
            lock(this)
            {
                Assert.that(pg.accessCount > 0);
                if ((pg.state & Page.psDirty) == 0)
                {
                    Assert.that(!flushing);
                    pg.state |= Page.psDirty;
                    dirtyPages[nDirtyPages] = pg;
                    pg.writeQueueIndex = nDirtyPages++;
                }
            }
        }
		
        internal Page getPage(long addr)
        {
            return find(addr, 0);
        }
		
        internal Page putPage(long addr)
        {
            return find(addr, Page.psDirty);
        }
		
        internal byte[] get(long pos)
        {
            Assert.that(pos != 0);
            int offs = (int) pos & (Page.pageSize - 1);
            Page pg = find(pos - offs, 0);
            int size = ObjectHeader.getSize(pg.data, offs);
            Assert.that(size >= ObjectHeader.Sizeof);
            byte[] obj = new byte[size];
            int dst = 0;
            while (size > Page.pageSize - offs)
            {
                Array.Copy(pg.data, offs, obj, dst, Page.pageSize - offs);
                unfix(pg);
                size -= Page.pageSize - offs;
                pos += Page.pageSize - offs;
                dst += Page.pageSize - offs;
                pg = find(pos, 0);
                offs = 0;
            }
            Array.Copy(pg.data, offs,obj, dst, size);
            unfix(pg);
            return obj;
        }
		
        internal void  put(long pos, byte[] obj)
        {
            put(pos, obj, obj.Length);
        }
		
        internal void  put(long pos, byte[] obj, int size)
        {
            int offs = (int) pos & (Page.pageSize - 1);
            Page pg = find(pos - offs, Page.psDirty);
            int src = 0;
            while (size > Page.pageSize - offs)
            {
                Array.Copy(obj, src, pg.data, offs, Page.pageSize - offs);
                unfix(pg);
                size -= Page.pageSize - offs;
                pos += Page.pageSize - offs;
                src += Page.pageSize - offs;
                pg = find(pos, Page.psDirty);
                offs = 0;
            }
            Array.Copy(obj, src, pg.data, offs, size);
            unfix(pg);
        }
		
        internal virtual void  flush()
        {
            long maxOffs;
            lock(this)
            {
                flushing = true;
                Array.Sort(dirtyPages, 0, nDirtyPages);
                maxOffs = fileSize;
            }
            for (int i = 0; i < nDirtyPages; i++)
            {
                Page pg = dirtyPages[i];
                lock(pg)
                {
                    if ((pg.state & Page.psDirty) != 0)
                    {
                        file.write(pg.offs, pg.data);
                        pg.state &= ~ Page.psDirty;
                        if (pg.offs >= maxOffs)
                        {
                            maxOffs = pg.offs + Page.pageSize;
                        }
                    }
                }
            }
            file.sync();
            nDirtyPages = 0;
            flushing = false;
            if (maxOffs > fileSize)
            {
                fileSize = maxOffs;
            }
        }
    }
}