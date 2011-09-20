namespace Volante.Impl
{
    using System;
    using Volante;
    using System.Diagnostics;

    class PagePool
    {
        internal LRU lru;
        internal Page freePages;
        internal Page[] hashTable;
        internal int poolSize;
        internal bool autoExtended;
        internal IFile file;

        internal int nDirtyPages;
        internal Page[] dirtyPages;

        internal bool flushing;

        const int INFINITE_POOL_INITIAL_SIZE = 8;

        internal PagePool(int poolSize)
        {
            if (poolSize == 0)
            {
                autoExtended = true;
                poolSize = INFINITE_POOL_INITIAL_SIZE;
            }
            this.poolSize = poolSize;
        }

        internal Page find(long addr, int state)
        {
            Debug.Assert((addr & (Page.pageSize - 1)) == 0);
            Page pg;
            int pageNo = (int)((ulong)addr >> Page.pageBits);
            int hashCode = pageNo % poolSize;

            lock (this)
            {
                int nCollisions = 0;
                for (pg = hashTable[hashCode]; pg != null; pg = pg.collisionChain)
                {
                    if (pg.offs == addr)
                    {
                        if (pg.accessCount++ == 0)
                            pg.unlink();
                        break;
                    }
                    nCollisions += 1;
                }
                if (pg == null)
                {
                    pg = freePages;
                    if (pg != null)
                        freePages = (Page)pg.next;
                    else if (autoExtended)
                    {
                        if (pageNo >= poolSize)
                        {
                            int newPoolSize = pageNo >= poolSize * 2 ? pageNo + 1 : poolSize * 2;
                            Page[] newHashTable = new Page[newPoolSize];
                            Array.Copy(hashTable, 0, newHashTable, 0, hashTable.Length);
                            hashTable = newHashTable;
                            poolSize = newPoolSize;
                        }
                        pg = new Page();
                        hashCode = pageNo;
                    }
                    else
                    {
                        Debug.Assert(lru.prev != lru, "unfixed page available");
                        pg = (Page)lru.prev;
                        pg.unlink();
                        lock (pg)
                        {
                            if ((pg.state & Page.psDirty) != 0)
                            {
                                pg.state = 0;
                                file.Write(pg.offs, pg.data);
                                if (!flushing)
                                {
                                    dirtyPages[pg.writeQueueIndex] = dirtyPages[--nDirtyPages];
                                    dirtyPages[pg.writeQueueIndex].writeQueueIndex = pg.writeQueueIndex;
                                }
                            }
                        }
                        int h = (int)(pg.offs >> Page.pageBits) % poolSize;
                        Page curr = hashTable[h], prev = null;
                        while (curr != pg)
                        {
                            prev = curr;
                            curr = curr.collisionChain;
                        }

                        if (prev == null)
                            hashTable[h] = pg.collisionChain;
                        else
                            prev.collisionChain = pg.collisionChain;
                    }
                    pg.accessCount = 1;
                    pg.offs = addr;
                    pg.state = Page.psRaw;
                    pg.collisionChain = hashTable[hashCode];
                    hashTable[hashCode] = pg;
                }
                if ((pg.state & Page.psDirty) == 0 && (state & Page.psDirty) != 0)
                {
                    Debug.Assert(!flushing);
                    if (nDirtyPages >= dirtyPages.Length)
                    {
                        Page[] newDirtyPages = new Page[nDirtyPages * 2];
                        Array.Copy(dirtyPages, 0, newDirtyPages, 0, dirtyPages.Length);
                        dirtyPages = newDirtyPages;
                    }
                    dirtyPages[nDirtyPages] = pg;
                    pg.writeQueueIndex = nDirtyPages++;
                    pg.state |= Page.psDirty;
                }

                if ((pg.state & Page.psRaw) != 0)
                {
                    if (file.Read(pg.offs, pg.data) < Page.pageSize)
                        Array.Clear(pg.data, 0, Page.pageSize);

                    pg.state &= ~Page.psRaw;
                }
            }
            return pg;
        }

        internal void copy(long dst, long src, long size)
        {
            int dstOffs = (int)dst & (Page.pageSize - 1);
            int srcOffs = (int)src & (Page.pageSize - 1);
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
                    len = Page.pageSize - srcOffs;

                if (len > Page.pageSize - dstOffs)
                    len = Page.pageSize - dstOffs;

                Array.Copy(srcPage.data, srcOffs, dstPage.data, dstOffs, (int)len);
                srcOffs = (int)(srcOffs + len);
                dstOffs = (int)(dstOffs + len);
                size -= len;
            }
            while (size != 0);
            unfix(dstPage);
            unfix(srcPage);
        }

        internal void write(long dstPos, byte[] src)
        {
            Debug.Assert((dstPos & (Page.pageSize - 1)) == 0);
            Debug.Assert((src.Length & (Page.pageSize - 1)) == 0);
            for (int i = 0; i < src.Length; )
            {
                Page pg = find(dstPos, Page.psDirty);
                byte[] dst = pg.data;
                for (int j = 0; j < Page.pageSize; j++)
                {
                    dst[j] = src[i++];
                }
                unfix(pg);
                dstPos += Page.pageSize;
            }
        }

        internal void open(IFile f)
        {
            file = f;
            hashTable = new Page[poolSize];
            dirtyPages = new Page[poolSize];
            nDirtyPages = 0;
            lru = new LRU();
            freePages = null;
            if (autoExtended)
                return;

            for (int i = poolSize; --i >= 0; )
            {
                Page pg = new Page();
                pg.next = freePages;
                freePages = pg;
            }
        }

        internal void close()
        {
            lock (this)
            {
                file.Close();
                hashTable = null;
                dirtyPages = null;
                lru = null;
                freePages = null;
            }
        }

        internal void unfix(Page pg)
        {
            lock (this)
            {
                Debug.Assert(pg.accessCount > 0);
                if (--pg.accessCount == 0)
                {
                    lru.link(pg);
                }
            }
        }

        internal void modify(Page pg)
        {
            lock (this)
            {
                Debug.Assert(pg.accessCount > 0);
                if ((pg.state & Page.psDirty) == 0)
                {
                    Debug.Assert(!flushing);
                    pg.state |= Page.psDirty;
                    if (nDirtyPages >= dirtyPages.Length)
                    {
                        Page[] newDirtyPages = new Page[nDirtyPages * 2];
                        Array.Copy(dirtyPages, 0, newDirtyPages, 0, dirtyPages.Length);
                        dirtyPages = newDirtyPages;
                    }
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
            Debug.Assert(pos != 0);
            int offs = (int)pos & (Page.pageSize - 1);
            Page pg = find(pos - offs, 0);
            int size = ObjectHeader.getSize(pg.data, offs);
            Debug.Assert(size >= ObjectHeader.Sizeof);
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
            Array.Copy(pg.data, offs, obj, dst, size);
            unfix(pg);
            return obj;
        }

        internal void put(long pos, byte[] obj)
        {
            put(pos, obj, obj.Length);
        }

        internal void put(long pos, byte[] obj, int size)
        {
            int offs = (int)pos & (Page.pageSize - 1);
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

#if CF
        class PageComparator : System.Collections.IComparer 
        {
            public int Compare(object o1, object o2) 
            {
                long delta = ((Page)o1).offs - ((Page)o2).offs;
                return delta < 0 ? -1 : delta == 0 ? 0 : 1;
            }
        }
        static PageComparator pageComparator = new PageComparator();
#endif

        internal virtual void flush()
        {
            lock (this)
            {
                flushing = true;
#if CF
                Array.Sort(dirtyPages, 0, nDirtyPages, pageComparator);
#else
                Array.Sort(dirtyPages, 0, nDirtyPages);
#endif
            }
            for (int i = 0; i < nDirtyPages; i++)
            {
                Page pg = dirtyPages[i];
                lock (pg)
                {
                    if ((pg.state & Page.psDirty) != 0)
                    {
                        file.Write(pg.offs, pg.data);
                        pg.state &= ~Page.psDirty;
                    }
                }
            }
            file.Sync();
            nDirtyPages = 0;
            flushing = false;
        }
    }
}