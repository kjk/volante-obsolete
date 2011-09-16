namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using System.Diagnostics;
    using System.Text;
    using Volante;

    public class DatabaseImpl : IDatabase
    {
        public const int DEFAULT_PAGE_POOL_SIZE = 4 * 1024 * 1024;

#if CF
        static DatabaseImpl() 
        {
            assemblies = new System.Collections.ArrayList();
        }
        public DatabaseImpl(Assembly callingAssembly) 
        {
            assemblies.Add(callingAssembly);
            assemblies.Add(Assembly.GetExecutingAssembly());
        }
#endif

        public IPersistent Root
        {
            get
            {
                lock (this)
                {
                    ensureOpened();
                    int rootOid = header.root[1 - currIndex].rootObject;
                    return (rootOid == 0) ? null : lookupObject(rootOid, null);
                }
            }

            set
            {
                lock (this)
                {
                    ensureOpened();
                    if (!value.IsPersistent())
                        storeObject0(value);

                    header.root[1 - currIndex].rootObject = value.Oid;
                    modified = true;
                }
            }
        }

        /// <summary> Initialial database index size - increasing it reduce number of inde reallocation but increase
        /// initial database size. Should be set before openning connection.
        /// </summary>
        const int dbDefaultInitIndexSize = 1024;

        /// <summary> Initial capacity of object hash
        /// </summary>
        const int dbDefaultObjectCacheInitSize = 1319;

        /// <summary> Database extension quantum. Memory is allocated by scanning bitmap. If there is no
        /// large enough hole, then database is extended by the value of dbDefaultExtensionQuantum 
        /// This parameter should not be smaller than dbFirstUserId
        /// </summary>
        static long dbDefaultExtensionQuantum = 1024 * 1024;

        const int dbDatabaseOffsetBits = 32; // up to 4 gigabyte
        const int dbLargeDatabaseOffsetBits = 40; // up to 1 terabyte

        const int dbAllocationQuantumBits = 5;
        const int dbAllocationQuantum = 1 << dbAllocationQuantumBits;
        const int dbBitmapSegmentBits = Page.pageBits + 3 + dbAllocationQuantumBits;
        const int dbBitmapSegmentSize = 1 << dbBitmapSegmentBits;
        const int dbBitmapPages = 1 << (dbDatabaseOffsetBits - dbBitmapSegmentBits);
        const int dbLargeBitmapPages = 1 << (dbLargeDatabaseOffsetBits - dbBitmapSegmentBits);
        const int dbHandlesPerPageBits = Page.pageBits - 3;
        const int dbHandlesPerPage = 1 << dbHandlesPerPageBits;
        const int dbDirtyPageBitmapSize = 1 << (32 - dbHandlesPerPageBits - 3);

        const int dbInvalidId = 0;
        const int dbBitmapId = 1;
        const int dbFirstUserId = dbBitmapId + dbBitmapPages;

        internal const int dbPageObjectFlag = 1;
        internal const int dbModifiedFlag = 2;
        internal const int dbFreeHandleFlag = 4;
        internal const int dbFlagsMask = 7;
        internal const int dbFlagsBits = 3;

        internal void ensureOpened()
        {
            if (!opened)
                throw new DatabaseException(DatabaseException.ErrorCode.DATABASE_NOT_OPENED);
        }

        int getBitmapPageId(int i)
        {
            return i < dbBitmapPages ? dbBitmapId + i : header.root[1 - currIndex].bitmapExtent + i;
        }

        internal long getPos(int oid)
        {
            lock (objectCache)
            {
                if (oid == 0 || oid >= currIndexSize)
                    throw new DatabaseException(DatabaseException.ErrorCode.INVALID_OID);
                Page pg = pool.getPage(header.root[1 - currIndex].index + ((long)(oid >> dbHandlesPerPageBits) << Page.pageBits));
                long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage - 1)) << 3);
                pool.unfix(pg);
                return pos;
            }
        }

        internal void setPos(int oid, long pos)
        {
            lock (objectCache)
            {
                dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                Page pg = pool.putPage(header.root[1 - currIndex].index + ((long)(oid >> dbHandlesPerPageBits) << Page.pageBits));
                Bytes.pack8(pg.data, (oid & (dbHandlesPerPage - 1)) << 3, pos);
                pool.unfix(pg);
            }
        }

        internal byte[] get(int oid)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                throw new DatabaseException(DatabaseException.ErrorCode.INVALID_OID);

            return pool.get(pos & ~dbFlagsMask);
        }

        internal Page getPage(int oid)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != dbPageObjectFlag)
                throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

            return pool.getPage(pos & ~dbFlagsMask);
        }

        internal Page putPage(int oid)
        {
            lock (objectCache)
            {
                long pos = getPos(oid);
                if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != dbPageObjectFlag)
                    throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

                if ((pos & dbModifiedFlag) == 0)
                {
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    allocate(Page.pageSize, oid);
                    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                    pos = getPos(oid);
                }
                modified = true;
                return pool.putPage(pos & ~dbFlagsMask);
            }
        }

        internal int allocatePage()
        {
            int oid = allocateId();
            setPos(oid, allocate(Page.pageSize, 0) | dbPageObjectFlag | dbModifiedFlag);
            return oid;
        }

        public void deallocateObject(IPersistent obj)
        {
            lock (this)
            {
                lock (objectCache)
                {
                    int oid = obj.Oid;
                    if (oid == 0)
                        return;

                    long pos = getPos(oid);
                    objectCache.Remove(oid);
                    int offs = (int)pos & (Page.pageSize - 1);
                    if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                        throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

                    Page pg = pool.getPage(pos - offs);
                    offs &= ~dbFlagsMask;
                    int size = ObjectHeader.getSize(pg.data, offs);
                    pool.unfix(pg);
                    freeId(oid);
                    if ((pos & dbModifiedFlag) != 0)
                        free(pos & ~dbFlagsMask, size);
                    else
                        cloneBitmap(pos, size);
                    obj.AssignOid(this, 0, false);
                }
            }
        }

        internal void freePage(int oid)
        {
            long pos = getPos(oid);
            Debug.Assert((pos & (dbFreeHandleFlag | dbPageObjectFlag)) == dbPageObjectFlag);
            if ((pos & dbModifiedFlag) != 0)
                free(pos & ~dbFlagsMask, Page.pageSize);
            else
                cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
            freeId(oid);
        }

        virtual protected bool isDirty()
        {
            return header.dirty;
        }

        internal void setDirty()
        {
            modified = true;
            if (header.dirty)
                return;
            header.dirty = true;
            Page pg = pool.putPage(0);
            header.pack(pg.data);
            pool.flush();
            pool.unfix(pg);
        }

        internal int allocateId()
        {
            lock (objectCache)
            {
                int oid;
                int curr = 1 - currIndex;
                setDirty();
                oid = header.root[curr].freeList;
                if (oid != 0)
                {
                    header.root[curr].freeList = (int)(getPos(oid) >> dbFlagsBits);
                    Debug.Assert(header.root[curr].freeList >= 0);
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)]
                        |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    return oid;
                }

                if (currIndexSize >= header.root[curr].indexSize)
                {
                    int oldIndexSize = header.root[curr].indexSize;
                    int newIndexSize = oldIndexSize * 2;
                    if (newIndexSize < oldIndexSize)
                    {
                        newIndexSize = int.MaxValue & ~(dbHandlesPerPage - 1);
                        if (newIndexSize <= oldIndexSize)
                        {
                            throw new DatabaseException(DatabaseException.ErrorCode.NOT_ENOUGH_SPACE);
                        }
                    }
                    long newIndex = allocate(newIndexSize * 8L, 0);
                    if (currIndexSize >= header.root[curr].indexSize)
                    {
                        long oldIndex = header.root[curr].index;
                        pool.copy(newIndex, oldIndex, currIndexSize * 8L);
                        header.root[curr].index = newIndex;
                        header.root[curr].indexSize = newIndexSize;
                        free(oldIndex, oldIndexSize * 8L);
                    }
                    else
                    {
                        // index was already reallocated
                        free(newIndex, newIndexSize * 8L);
                    }
                }
                oid = currIndexSize;
                header.root[curr].indexUsed = ++currIndexSize;
                modified = true;
                return oid;
            }
        }

        internal void freeId(int oid)
        {
            lock (objectCache)
            {
                setPos(oid, ((long)(header.root[1 - currIndex].freeList) << dbFlagsBits) | dbFreeHandleFlag);
                header.root[1 - currIndex].freeList = oid;
            }
        }

        internal static byte[] firstHoleSize = new byte[] { 8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0 };
        internal static byte[] lastHoleSize = new byte[] { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        internal static byte[] maxHoleSize = new byte[] { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 7, 6, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 0 };
        internal static byte[] maxHoleOffset = new byte[] { 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 5, 5, 5, 5, 5, 5, 0, 5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 0, 6, 6, 6, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 2, 2, 3, 3, 3, 3, 0, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 2, 0, 5, 5, 5, 0, 1, 5, 5, 0, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 0 };

        internal const int pageBits = Page.pageSize * 8;
        internal const int inc = Page.pageSize / dbAllocationQuantum / 8;

        internal static void memset(Page pg, int offs, int pattern, int len)
        {
            byte[] arr = pg.data;
            byte pat = (byte)pattern;
            while (--len >= 0)
            {
                arr[offs++] = pat;
            }
        }

        public long UsedSize
        {
            get
            {
                return usedSize;
            }
        }

        public long DatabaseSize
        {
            get
            {
                return header.root[1 - currIndex].size;
            }
        }

        internal void extend(long size)
        {
            if (size > header.root[1 - currIndex].size)
                header.root[1 - currIndex].size = size;
        }

        internal class Location
        {
            internal long pos;
            internal long size;
            internal Location next;
        }

        internal bool wasReserved(long pos, long size)
        {
            for (Location location = reservedChain; location != null; location = location.next)
            {
                if ((pos >= location.pos && pos - location.pos < location.size) || (pos <= location.pos && location.pos - pos < size))
                {
                    return true;
                }
            }
            return false;
        }

        internal void reserveLocation(long pos, long size)
        {
            Location location = new Location();
            location.pos = pos;
            location.size = size;
            location.next = reservedChain;
            reservedChain = location;
        }

        internal void commitLocation()
        {
            reservedChain = reservedChain.next;
        }

        Page putBitmapPage(int i)
        {
            return putPage(getBitmapPageId(i));
        }

        Page getBitmapPage(int i)
        {
            return getPage(getBitmapPageId(i));
        }

        internal long allocate(long size, int oid)
        {
            lock (objectCache)
            {
                setDirty();
                size = (size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1);
                Debug.Assert(size != 0);
                allocatedDelta += size;
                if (allocatedDelta > gcThreshold)
                    gc0();

                int objBitSize = (int)(size >> dbAllocationQuantumBits);
                Debug.Assert(objBitSize == (size >> dbAllocationQuantumBits));
                long pos;
                int holeBitSize = 0;
                int alignment = (int)size & (Page.pageSize - 1);
                int offs, firstPage, lastPage, i, j;
                int holeBeforeFreePage = 0;
                int freeBitmapPage = 0;
                int curr = 1 - currIndex;
                Page pg;

                lastPage = header.root[curr].bitmapEnd - dbBitmapId;
                usedSize += size;

                if (alignment == 0)
                {
                    firstPage = currPBitmapPage;
                    offs = (currPBitmapOffs + inc - 1) & ~(inc - 1);
                }
                else
                {
                    firstPage = currRBitmapPage;
                    offs = currRBitmapOffs;
                }

                while (true)
                {
                    if (alignment == 0)
                    {
                        // allocate page object 
                        for (i = firstPage; i < lastPage; i++)
                        {
                            int spaceNeeded = objBitSize - holeBitSize < pageBits
                                ? objBitSize - holeBitSize : pageBits;
                            if (bitmapPageAvailableSpace[i] <= spaceNeeded)
                            {
                                holeBitSize = 0;
                                offs = 0;
                                continue;
                            }
                            pg = getBitmapPage(i);
                            int startOffs = offs;
                            while (offs < Page.pageSize)
                            {
                                if (pg.data[offs++] != 0)
                                {
                                    offs = (offs + inc - 1) & ~(inc - 1);
                                    holeBitSize = 0;
                                }
                                else if ((holeBitSize += 8) == objBitSize)
                                {
                                    pos = (((long)i * Page.pageSize + offs) * 8 - holeBitSize)
                                        << dbAllocationQuantumBits;
                                    if (wasReserved(pos, size))
                                    {
                                        offs += objBitSize >> 3;
                                        startOffs = offs = (offs + inc - 1) & ~(inc - 1);
                                        holeBitSize = 0;
                                        continue;
                                    }
                                    reserveLocation(pos, size);
                                    currPBitmapPage = i;
                                    currPBitmapOffs = offs;
                                    extend(pos + size);
                                    if (oid != 0)
                                    {
                                        long prev = getPos(oid);
                                        uint marker = (uint)prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putBitmapPage(i);
                                    int holeBytes = holeBitSize >> 3;
                                    if (holeBytes > offs)
                                    {
                                        memset(pg, 0, 0xFF, offs);
                                        holeBytes -= offs;
                                        pool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                        offs = Page.pageSize;
                                    }
                                    while (holeBytes > Page.pageSize)
                                    {
                                        memset(pg, 0, 0xFF, Page.pageSize);
                                        holeBytes -= Page.pageSize;
                                        bitmapPageAvailableSpace[i] = 0;
                                        pool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                    }
                                    memset(pg, offs - holeBytes, 0xFF, holeBytes);
                                    commitLocation();
                                    pool.unfix(pg);
                                    return pos;
                                }
                            }
                            if (startOffs == 0 && holeBitSize == 0
                                && spaceNeeded < bitmapPageAvailableSpace[i])
                            {
                                bitmapPageAvailableSpace[i] = spaceNeeded;
                            }
                            offs = 0;
                            pool.unfix(pg);
                        }
                    }
                    else
                    {
                        for (i = firstPage; i < lastPage; i++)
                        {
                            int spaceNeeded = objBitSize - holeBitSize < pageBits
                                ? objBitSize - holeBitSize : pageBits;
                            if (bitmapPageAvailableSpace[i] <= spaceNeeded)
                            {
                                holeBitSize = 0;
                                offs = 0;
                                continue;
                            }
                            pg = getBitmapPage(i);
                            int startOffs = offs;
                            while (offs < Page.pageSize)
                            {
                                int mask = pg.data[offs] & 0xFF;
                                if (holeBitSize + firstHoleSize[mask] >= objBitSize)
                                {
                                    pos = (((long)i * Page.pageSize + offs) * 8
                                        - holeBitSize) << dbAllocationQuantumBits;
                                    if (wasReserved(pos, size))
                                    {
                                        startOffs = offs += (objBitSize + 7) >> 3;
                                        holeBitSize = 0;
                                        continue;
                                    }
                                    reserveLocation(pos, size);
                                    currRBitmapPage = i;
                                    currRBitmapOffs = offs;
                                    extend(pos + size);
                                    if (oid != 0)
                                    {
                                        long prev = getPos(oid);
                                        uint marker = (uint)prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putBitmapPage(i);
                                    pg.data[offs] |= (byte)((1 << (objBitSize - holeBitSize)) - 1);
                                    if (holeBitSize != 0)
                                    {
                                        if (holeBitSize > offs * 8)
                                        {
                                            memset(pg, 0, 0xFF, offs);
                                            holeBitSize -= offs * 8;
                                            pool.unfix(pg);
                                            pg = putBitmapPage(--i);
                                            offs = Page.pageSize;
                                        }
                                        while (holeBitSize > pageBits)
                                        {
                                            memset(pg, 0, 0xFF, Page.pageSize);
                                            holeBitSize -= pageBits;
                                            bitmapPageAvailableSpace[i] = 0;
                                            pool.unfix(pg);
                                            pg = putBitmapPage(--i);
                                        }
                                        while ((holeBitSize -= 8) > 0)
                                        {
                                            pg.data[--offs] = (byte)0xFF;
                                        }
                                        pg.data[offs - 1] |= (byte)~((1 << -holeBitSize) - 1);
                                    }
                                    pool.unfix(pg);
                                    commitLocation();
                                    return pos;
                                }
                                else if (maxHoleSize[mask] >= objBitSize)
                                {
                                    int holeBitOffset = maxHoleOffset[mask];
                                    pos = (((long)i * Page.pageSize + offs) * 8 + holeBitOffset) << dbAllocationQuantumBits;
                                    if (wasReserved(pos, size))
                                    {
                                        startOffs = offs += (objBitSize + 7) >> 3;
                                        holeBitSize = 0;
                                        continue;
                                    }
                                    reserveLocation(pos, size);
                                    currRBitmapPage = i;
                                    currRBitmapOffs = offs;
                                    extend(pos + size);
                                    if (oid != 0)
                                    {
                                        long prev = getPos(oid);
                                        uint marker = (uint)prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putBitmapPage(i);
                                    pg.data[offs] |= (byte)(((1 << objBitSize) - 1) << holeBitOffset);
                                    pool.unfix(pg);
                                    commitLocation();
                                    return pos;
                                }
                                offs += 1;
                                if (lastHoleSize[mask] == 8)
                                    holeBitSize += 8;
                                else
                                    holeBitSize = lastHoleSize[mask];
                            }
                            if (startOffs == 0 && holeBitSize == 0
                                && spaceNeeded < bitmapPageAvailableSpace[i])
                            {
                                bitmapPageAvailableSpace[i] = spaceNeeded;
                            }
                            offs = 0;
                            pool.unfix(pg);
                        }
                    }
                    if (firstPage == 0)
                    {
                        if (freeBitmapPage > i)
                        {
                            i = freeBitmapPage;
                            holeBitSize = holeBeforeFreePage;
                        }
                        objBitSize -= holeBitSize;
                        // number of bits reserved for the object and aligned on page boundary
                        int skip = (objBitSize + Page.pageSize / dbAllocationQuantum - 1)
                            & ~(Page.pageSize / dbAllocationQuantum - 1);
                        // page aligned position after allocated object
                        pos = ((long)i << dbBitmapSegmentBits) + ((long)skip << dbAllocationQuantumBits);

                        long extension = (size > extensionQuantum) ? size : extensionQuantum;
                        int oldIndexSize = 0;
                        long oldIndex = 0;
                        int morePages = (int)((extension + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1)
                            / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
                        if (i + morePages > dbLargeBitmapPages)
                        {
                            throw new DatabaseException(DatabaseException.ErrorCode.NOT_ENOUGH_SPACE);
                        }
                        if (i <= dbBitmapPages && i + morePages > dbBitmapPages)
                        {
                            // We are out of space mapped by memory default allocation bitmap
                            oldIndexSize = header.root[curr].indexSize;
                            if (oldIndexSize <= currIndexSize + dbLargeBitmapPages - dbBitmapPages)
                            {
                                int newIndexSize = oldIndexSize;
                                oldIndex = header.root[curr].index;
                                do
                                {
                                    newIndexSize <<= 1;
                                    if (newIndexSize < 0)
                                    {
                                        newIndexSize = int.MaxValue & ~(dbHandlesPerPage - 1);
                                        if (newIndexSize < currIndexSize + dbLargeBitmapPages - dbBitmapPages)
                                        {
                                            throw new DatabaseException(DatabaseException.ErrorCode.NOT_ENOUGH_SPACE);
                                        }
                                        break;
                                    }
                                } while (newIndexSize <= currIndexSize + dbLargeBitmapPages - dbBitmapPages);

                                if (size + newIndexSize * 8L > extensionQuantum)
                                {
                                    extension = size + newIndexSize * 8L;
                                    morePages = (int)((extension + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1)
                                        / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
                                }
                                extend(pos + (long)morePages * Page.pageSize + newIndexSize * 8L);
                                long newIndex = pos + (long)morePages * Page.pageSize;
                                fillBitmap(pos + (skip >> 3) + (long)morePages * (Page.pageSize / dbAllocationQuantum / 8),
                                    newIndexSize >> dbAllocationQuantumBits);
                                pool.copy(newIndex, oldIndex, oldIndexSize * 8L);
                                header.root[curr].index = newIndex;
                                header.root[curr].indexSize = newIndexSize;
                            }
                            int[] newBitmapPageAvailableSpace = new int[dbLargeBitmapPages];
                            Array.Copy(bitmapPageAvailableSpace, 0, newBitmapPageAvailableSpace, 0, dbBitmapPages);
                            for (j = dbBitmapPages; j < dbLargeBitmapPages; j++)
                            {
                                newBitmapPageAvailableSpace[j] = int.MaxValue;
                            }
                            bitmapPageAvailableSpace = newBitmapPageAvailableSpace;

                            for (j = 0; j < dbLargeBitmapPages - dbBitmapPages; j++)
                            {
                                setPos(currIndexSize + j, dbFreeHandleFlag);
                            }

                            header.root[curr].bitmapExtent = currIndexSize;
                            header.root[curr].indexUsed = currIndexSize += dbLargeBitmapPages - dbBitmapPages;
                        }
                        extend(pos + (long)morePages * Page.pageSize);
                        long adr = pos;
                        int len = objBitSize >> 3;
                        // fill bitmap pages used for allocation of object space with 0xFF 
                        while (len >= Page.pageSize)
                        {
                            pg = pool.putPage(adr);
                            memset(pg, 0, 0xFF, Page.pageSize);
                            pool.unfix(pg);
                            adr += Page.pageSize;
                            len -= Page.pageSize;
                        }
                        // fill part of last page responsible for allocation of object space
                        pg = pool.putPage(adr);
                        memset(pg, 0, 0xFF, len);
                        pg.data[len] = (byte)((1 << (objBitSize & 7)) - 1);
                        pool.unfix(pg);

                        // mark in bitmap newly allocated object
                        fillBitmap(pos + (skip >> 3), morePages * (Page.pageSize / dbAllocationQuantum / 8));

                        j = i;
                        while (--morePages >= 0)
                        {
                            setPos(getBitmapPageId(j++), pos | dbPageObjectFlag | dbModifiedFlag);
                            pos += Page.pageSize;
                        }
                        header.root[curr].bitmapEnd = j + dbBitmapId;
                        j = i + objBitSize / pageBits;
                        if (alignment != 0)
                        {
                            currRBitmapPage = j;
                            currRBitmapOffs = 0;
                        }
                        else
                        {
                            currPBitmapPage = j;
                            currPBitmapOffs = 0;
                        }
                        while (j > i)
                        {
                            bitmapPageAvailableSpace[--j] = 0;
                        }

                        pos = ((long)i * Page.pageSize * 8 - holeBitSize) << dbAllocationQuantumBits;
                        if (oid != 0)
                        {
                            long prev = getPos(oid);
                            uint marker = (uint)prev & dbFlagsMask;
                            pool.copy(pos, prev - marker, size);
                            setPos(oid, pos | marker | dbModifiedFlag);
                        }

                        if (holeBitSize != 0)
                        {
                            reserveLocation(pos, size);
                            while (holeBitSize > pageBits)
                            {
                                holeBitSize -= pageBits;
                                pg = putBitmapPage(--i);
                                memset(pg, 0, 0xFF, Page.pageSize);
                                bitmapPageAvailableSpace[i] = 0;
                                pool.unfix(pg);
                            }
                            pg = putBitmapPage(--i);
                            offs = Page.pageSize;
                            while ((holeBitSize -= 8) > 0)
                            {
                                pg.data[--offs] = (byte)0xFF;
                            }
                            pg.data[offs - 1] |= (byte)~((1 << -holeBitSize) - 1);
                            pool.unfix(pg);
                            commitLocation();
                        }
                        if (oldIndex != 0)
                            free(oldIndex, oldIndexSize * 8L);

                        return pos;
                    }
                    if (gcThreshold != Int64.MaxValue && !gcDone)
                    {
                        allocatedDelta -= size;
                        usedSize -= size;
                        gc0();
                        currRBitmapPage = currPBitmapPage = 0;
                        currRBitmapOffs = currPBitmapOffs = 0;
                        return allocate(size, oid);
                    }
                    freeBitmapPage = i;
                    holeBeforeFreePage = holeBitSize;
                    holeBitSize = 0;
                    lastPage = firstPage + 1;
                    firstPage = 0;
                    offs = 0;
                }
            }
        }

        void fillBitmap(long adr, int len)
        {
            while (true)
            {
                int off = (int)adr & (Page.pageSize - 1);
                Page pg = pool.putPage(adr - off);
                if (Page.pageSize - off >= len)
                {
                    memset(pg, off, 0xFF, len);
                    pool.unfix(pg);
                    break;
                }
                else
                {
                    memset(pg, off, 0xFF, Page.pageSize - off);
                    pool.unfix(pg);
                    adr += Page.pageSize - off;
                    len -= Page.pageSize - off;
                }
            }
        }

        internal void free(long pos, long size)
        {
            lock (objectCache)
            {
                Debug.Assert(pos != 0 && (pos & (dbAllocationQuantum - 1)) == 0);
                long quantNo = pos >> dbAllocationQuantumBits;
                int objBitSize = (int)((size + dbAllocationQuantum - 1) >> dbAllocationQuantumBits);
                int pageId = (int)(quantNo >> (Page.pageBits + 3));
                int offs = (int)(quantNo & (Page.pageSize * 8 - 1)) >> 3;
                Page pg = putBitmapPage(pageId);
                int bitOffs = (int)quantNo & 7;

                allocatedDelta -= (long)objBitSize << dbAllocationQuantumBits;
                usedSize -= (long)objBitSize << dbAllocationQuantumBits;

                if ((pos & (Page.pageSize - 1)) == 0 && size >= Page.pageSize)
                {
                    if (pageId == currPBitmapPage && offs < currPBitmapOffs)
                    {
                        currPBitmapOffs = offs;
                    }
                }
                if (pageId == currRBitmapPage && offs < currRBitmapOffs)
                {
                    currRBitmapOffs = offs;
                }
                bitmapPageAvailableSpace[pageId] = System.Int32.MaxValue;

                if (objBitSize > 8 - bitOffs)
                {
                    objBitSize -= 8 - bitOffs;
                    pg.data[offs++] &= (byte)((1 << bitOffs) - 1);
                    while (objBitSize + offs * 8 > Page.pageSize * 8)
                    {
                        memset(pg, offs, 0, Page.pageSize - offs);
                        pool.unfix(pg);
                        pg = putBitmapPage(++pageId);
                        bitmapPageAvailableSpace[pageId] = System.Int32.MaxValue;
                        objBitSize -= (Page.pageSize - offs) * 8;
                        offs = 0;
                    }
                    while ((objBitSize -= 8) > 0)
                    {
                        pg.data[offs++] = (byte)0;
                    }
                    pg.data[offs] &= (byte)(~((1 << (objBitSize + 8)) - 1));
                }
                else
                {
                    pg.data[offs] &= (byte)(~(((1 << objBitSize) - 1) << bitOffs));
                }
                pool.unfix(pg);
            }
        }

        internal void cloneBitmap(long pos, long size)
        {
            lock (objectCache)
            {
                long quantNo = pos >> dbAllocationQuantumBits;
                int objBitSize = (int)((size + dbAllocationQuantum - 1) >> dbAllocationQuantumBits);
                int pageId = (int)(quantNo >> (Page.pageBits + 3));
                int offs = (int)(quantNo & (Page.pageSize * 8 - 1)) >> 3;
                int bitOffs = (int)quantNo & 7;
                int oid = getBitmapPageId(pageId);
                pos = getPos(oid);
                if ((pos & dbModifiedFlag) == 0)
                {
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)]
                        |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    allocate(Page.pageSize, oid);
                    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                }

                if (objBitSize > 8 - bitOffs)
                {
                    objBitSize -= 8 - bitOffs;
                    offs += 1;
                    while (objBitSize + offs * 8 > Page.pageSize * 8)
                    {
                        oid = getBitmapPageId(++pageId);
                        pos = getPos(oid);
                        if ((pos & dbModifiedFlag) == 0)
                        {
                            dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)]
                                |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                            allocate(Page.pageSize, oid);
                            cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                        }
                        objBitSize -= (Page.pageSize - offs) * 8;
                        offs = 0;
                    }
                }
            }
        }

        public void Open(String filePath)
        {
            Open(filePath, DEFAULT_PAGE_POOL_SIZE);
        }

        public void Open(IFile file)
        {
            Open(file, DEFAULT_PAGE_POOL_SIZE);
        }

        public void Open(String filePath, int cacheSizeInBytes)
        {
            OsFile file = new OsFile(filePath);
            try
            {
                Open(file, cacheSizeInBytes);
            }
            catch (DatabaseException)
            {
                file.Close();
                throw;
            }
        }

        protected virtual OidHashTable createObjectCache(CacheType kind, int cacheSizeInBytes, int objectCacheSize)
        {
            if (cacheSizeInBytes == 0 || kind == CacheType.Strong)
                return new StrongHashTable(objectCacheSize);

            if (kind == CacheType.Weak)
                return new WeakHashTable(objectCacheSize);

            Debug.Assert(kind == CacheType.Lru);
            return new LruObjectCache(objectCacheSize);
        }

        public virtual void Open(IFile file, int cacheSizeInBytes)
        {
            lock (this)
            {
                if (opened)
                    throw new DatabaseException(DatabaseException.ErrorCode.DATABASE_ALREADY_OPENED);

                file.Lock();
                Page pg;
                int i;
                int indexSize = initIndexSize;
                if (indexSize < dbFirstUserId)
                    indexSize = dbFirstUserId;

                indexSize = (indexSize + dbHandlesPerPage - 1) & ~(dbHandlesPerPage - 1);

                dirtyPagesMap = new int[dbDirtyPageBitmapSize / 4 + 1];
                gcThreshold = Int64.MaxValue;
                backgroundGcMonitor = new object();
                backgroundGcStartMonitor = new object();
                gcThread = null;
                gcGo = false;
                gcActive = false;
                gcDone = false;
                allocatedDelta = 0;

                resolvedTypes = new Dictionary<string, Type>();

                nNestedTransactions = 0;
                nBlockedTransactions = 0;
                nCommittedTransactions = 0;
                scheduledCommitTime = Int64.MaxValue;
#if CF
                transactionMonitor = new CNetMonitor();
#else
                transactionMonitor = new object();
#endif
                transactionLock = new PersistentResource();

                modified = false;

                objectCache = createObjectCache(cacheKind, cacheSizeInBytes, objectCacheInitSize);

                classDescMap = new Dictionary<Type,ClassDescriptor>();
                descList = null;

                objectFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();

                header = new Header();
                byte[] buf = new byte[Header.Sizeof];
                int rc = file.Read(0, buf);
                if (rc > 0 && rc < Header.Sizeof)
                    throw new DatabaseException(DatabaseException.ErrorCode.DATABASE_CORRUPTED);

                header.unpack(buf);
                if (header.curr < 0 || header.curr > 1)
                    throw new DatabaseException(DatabaseException.ErrorCode.DATABASE_CORRUPTED);

                if (pool == null)
                {
                    pool = new PagePool(cacheSizeInBytes / Page.pageSize);
                    pool.open(file);
                }

                if (!header.initialized)
                {
                    header.curr = currIndex = 0;
                    long used = Page.pageSize;
                    header.root[0].index = used;
                    header.root[0].indexSize = indexSize;
                    header.root[0].indexUsed = dbFirstUserId;
                    header.root[0].freeList = 0;
                    used += indexSize * 8L;
                    header.root[1].index = used;
                    header.root[1].indexSize = indexSize;
                    header.root[1].indexUsed = dbFirstUserId;
                    header.root[1].freeList = 0;
                    used += indexSize * 8L;

                    header.root[0].shadowIndex = header.root[1].index;
                    header.root[1].shadowIndex = header.root[0].index;
                    header.root[0].shadowIndexSize = indexSize;
                    header.root[1].shadowIndexSize = indexSize;

                    int bitmapPages = (int)((used + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1) / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
                    long bitmapSize = (long)bitmapPages * Page.pageSize;
                    int usedBitmapSize = (int)((used + bitmapSize) >> (dbAllocationQuantumBits + 3));

                    for (i = 0; i < bitmapPages; i++)
                    {
                        pg = pool.putPage(used + (long)i * Page.pageSize);
                        byte[] bitmap = pg.data;
                        int n = usedBitmapSize > Page.pageSize ? Page.pageSize : usedBitmapSize;
                        for (int j = 0; j < n; j++)
                        {
                            bitmap[j] = (byte)0xFF;
                        }
                        pool.unfix(pg);
                    }

                    int bitmapIndexSize = ((dbBitmapId + dbBitmapPages) * 8 + Page.pageSize - 1) & ~(Page.pageSize - 1);
                    byte[] index = new byte[bitmapIndexSize];
                    Bytes.pack8(index, dbInvalidId * 8, dbFreeHandleFlag);
                    for (i = 0; i < bitmapPages; i++)
                    {
                        Bytes.pack8(index, (dbBitmapId + i) * 8, used | dbPageObjectFlag);
                        used += Page.pageSize;
                    }
                    header.root[0].bitmapEnd = dbBitmapId + i;
                    header.root[1].bitmapEnd = dbBitmapId + i;
                    while (i < dbBitmapPages)
                    {
                        Bytes.pack8(index, (dbBitmapId + i) * 8, dbFreeHandleFlag);
                        i += 1;
                    }
                    header.root[0].size = used;
                    header.root[1].size = used;
                    usedSize = used;
                    committedIndexSize = currIndexSize = dbFirstUserId;

                    pool.write(header.root[1].index, index);
                    pool.write(header.root[0].index, index);

                    header.dirty = true;
                    header.root[0].size = header.root[1].size;
                    pg = pool.putPage(0);
                    header.pack(pg.data);
                    pool.flush();
                    pool.modify(pg);
                    header.initialized = true;
                    header.pack(pg.data);
                    pool.unfix(pg);
                    pool.flush();
                }
                else
                {
                    int curr = header.curr;
                    currIndex = curr;
                    if (header.root[curr].indexSize != header.root[curr].shadowIndexSize)
                        throw new DatabaseException(DatabaseException.ErrorCode.DATABASE_CORRUPTED);

                    if (isDirty())
                    {
                        if (Listener != null)
                            Listener.DatabaseCorrupted();

                        //System.Console.WriteLine("Database was not normally closed: start recovery");
                        header.root[1 - curr].size = header.root[curr].size;
                        header.root[1 - curr].indexUsed = header.root[curr].indexUsed;
                        header.root[1 - curr].freeList = header.root[curr].freeList;
                        header.root[1 - curr].index = header.root[curr].shadowIndex;
                        header.root[1 - curr].indexSize = header.root[curr].shadowIndexSize;
                        header.root[1 - curr].shadowIndex = header.root[curr].index;
                        header.root[1 - curr].shadowIndexSize = header.root[curr].indexSize;
                        header.root[1 - curr].bitmapEnd = header.root[curr].bitmapEnd;
                        header.root[1 - curr].rootObject = header.root[curr].rootObject;
                        header.root[1 - curr].classDescList = header.root[curr].classDescList;
                        header.root[1 - curr].bitmapExtent = header.root[curr].bitmapExtent;

                        pg = pool.putPage(0);
                        header.pack(pg.data);
                        pool.unfix(pg);

                        pool.copy(header.root[1 - curr].index,
                                  header.root[curr].index,
                                  (header.root[curr].indexUsed * 8L + Page.pageSize - 1) & ~(Page.pageSize - 1));
                        if (Listener != null)
                            Listener.RecoveryCompleted();

                    }
                    currIndexSize = header.root[1 - curr].indexUsed;
                    committedIndexSize = currIndexSize;
                    usedSize = header.root[curr].size;
                }
                int nBitmapPages = header.root[1 - currIndex].bitmapExtent == 0 ? dbBitmapPages : dbLargeBitmapPages;
                bitmapPageAvailableSpace = new int[nBitmapPages];
                for (i = 0; i < bitmapPageAvailableSpace.Length; i++)
                {
                    bitmapPageAvailableSpace[i] = int.MaxValue;
                }
                currRBitmapPage = currPBitmapPage = 0;
                currRBitmapOffs = currPBitmapOffs = 0;

                opened = true;
                reloadScheme();
            }
        }

        public bool IsOpened
        {
            get { return opened; }
        }

        internal static void checkIfFinal(ClassDescriptor desc)
        {
            System.Type cls = desc.cls;
            for (ClassDescriptor next = desc.next; next != null; next = next.next)
            {
                next.Load();
                if (cls.IsAssignableFrom(next.cls))
                    desc.hasSubclasses = true;
                else if (next.cls.IsAssignableFrom(cls))
                    next.hasSubclasses = true;
            }
        }

        internal void reloadScheme()
        {
            classDescMap.Clear();
            int descListOid = header.root[1 - currIndex].classDescList;
            classDescMap[typeof(ClassDescriptor)] = new ClassDescriptor(this, typeof(ClassDescriptor));
            classDescMap[typeof(ClassDescriptor.FieldDescriptor)] = new ClassDescriptor(this, typeof(ClassDescriptor.FieldDescriptor));
            if (descListOid != 0)
            {
                ClassDescriptor desc;
                descList = findClassDescriptor(descListOid);
                for (desc = descList; desc != null; desc = desc.next)
                {
                    desc.Load();
                }
                for (desc = descList; desc != null; desc = desc.next)
                {
                    if (classDescMap[desc.cls] == desc)
                        desc.resolve();

                    checkIfFinal(desc);
                }
            }
            else
            {
                descList = null;
            }
#if !CF
            if (enableCodeGeneration)
            {
                codeGenerationThread = new Thread(new ThreadStart(generateSerializers));
                codeGenerationThread.Priority = ThreadPriority.BelowNormal;
                codeGenerationThread.IsBackground = true;
                codeGenerationThread.Start();
            }
#endif
        }

        internal void generateSerializers()
        {
            for (ClassDescriptor desc = descList; desc != null; desc = desc.next)
            {
                desc.generateSerializer();
            }
        }

        internal void assignOid(IPersistent obj, int oid)
        {
            obj.AssignOid(this, oid, false);
        }

        internal void registerClassDescriptor(ClassDescriptor desc)
        {
            classDescMap[desc.cls] = desc;
            desc.next = descList;
            descList = desc;
            checkIfFinal(desc);
            storeObject0(desc);
            header.root[1 - currIndex].classDescList = desc.Oid;
            modified = true;
        }

        internal ClassDescriptor getClassDescriptor(System.Type cls)
        {
            ClassDescriptor desc;
            var found = classDescMap.TryGetValue(cls, out desc);
            if (!found)
            {
                desc = new ClassDescriptor(this, cls);
                desc.generateSerializer();
                registerClassDescriptor(desc);
            }
            return desc;
        }

        public void Commit()
        {
            lock (backgroundGcMonitor)
            {
                lock (this)
                {
                    ensureOpened();
                    objectCache.Flush();

                    if (!modified)
                        return;

                    commit0();
                    modified = false;
                }
            }
        }

        private void commit0()
        {
            int curr = currIndex;
            int i, j, n;
            int[] map = dirtyPagesMap;
            int oldIndexSize = header.root[curr].indexSize;
            int newIndexSize = header.root[1 - curr].indexSize;
            int nPages = committedIndexSize >> dbHandlesPerPageBits;
            Page pg;

            if (newIndexSize > oldIndexSize)
            {
                cloneBitmap(header.root[curr].index, oldIndexSize * 8L);
                long newIndex;
                while (true)
                {
                    newIndex = allocate(newIndexSize * 8L, 0);
                    if (newIndexSize == header.root[1 - curr].indexSize)
                    {
                        break;
                    }
                    free(newIndex, newIndexSize * 8L);
                    newIndexSize = header.root[1 - curr].indexSize;
                }
                header.root[1 - curr].shadowIndex = newIndex;
                header.root[1 - curr].shadowIndexSize = newIndexSize;
                free(header.root[curr].index, oldIndexSize * 8L);
            }

            for (i = 0; i < nPages; i++)
            {
                if ((map[i >> 5] & (1 << (i & 31))) != 0)
                {
                    Page srcIndex = pool.getPage(header.root[1 - curr].index + (long)i * Page.pageSize);
                    Page dstIndex = pool.getPage(header.root[curr].index + (long)i * Page.pageSize);
                    for (j = 0; j < Page.pageSize; j += 8)
                    {
                        long pos = Bytes.unpack8(dstIndex.data, j);
                        if (Bytes.unpack8(srcIndex.data, j) != pos)
                        {
                            if ((pos & dbFreeHandleFlag) == 0)
                            {
                                if ((pos & dbPageObjectFlag) != 0)
                                {
                                    free(pos & ~dbFlagsMask, Page.pageSize);
                                }
                                else if (pos != 0)
                                {
                                    int offs = (int)pos & (Page.pageSize - 1);
                                    pg = pool.getPage(pos - offs);
                                    free(pos, ObjectHeader.getSize(pg.data, offs));
                                    pool.unfix(pg);
                                }
                            }
                        }
                    }
                    pool.unfix(srcIndex);
                    pool.unfix(dstIndex);
                }
            }
            n = committedIndexSize & (dbHandlesPerPage - 1);
            if (n != 0 && (map[i >> 5] & (1 << (i & 31))) != 0)
            {
                Page srcIndex = pool.getPage(header.root[1 - curr].index + (long)i * Page.pageSize);
                Page dstIndex = pool.getPage(header.root[curr].index + (long)i * Page.pageSize);
                j = 0;
                do
                {
                    long pos = Bytes.unpack8(dstIndex.data, j);
                    if (Bytes.unpack8(srcIndex.data, j) != pos)
                    {
                        if ((pos & dbFreeHandleFlag) == 0)
                        {
                            if ((pos & dbPageObjectFlag) != 0)
                            {
                                free(pos & ~dbFlagsMask, Page.pageSize);
                            }
                            else if (pos != 0)
                            {
                                int offs = (int)pos & (Page.pageSize - 1);
                                pg = pool.getPage(pos - offs);
                                free(pos, ObjectHeader.getSize(pg.data, offs));
                                pool.unfix(pg);
                            }
                        }
                    }
                    j += 8;
                }
                while (--n != 0);

                pool.unfix(srcIndex);
                pool.unfix(dstIndex);
            }

            for (i = 0; i <= nPages; i++)
            {
                if ((map[i >> 5] & (1 << (i & 31))) != 0)
                {
                    pg = pool.putPage(header.root[1 - curr].index + (long)i * Page.pageSize);
                    for (j = 0; j < Page.pageSize; j += 8)
                    {
                        Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
                    }
                    pool.unfix(pg);
                }
            }

            if (currIndexSize > committedIndexSize)
            {
                long page = (header.root[1 - curr].index + committedIndexSize * 8L) & ~(Page.pageSize - 1);
                long end = (header.root[1 - curr].index + Page.pageSize - 1 + currIndexSize * 8L) & ~(Page.pageSize - 1);
                while (page < end)
                {
                    pg = pool.putPage(page);
                    for (j = 0; j < Page.pageSize; j += 8)
                    {
                        Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
                    }
                    pool.unfix(pg);
                    page += Page.pageSize;
                }
            }
            header.root[1 - curr].usedSize = usedSize;
            pg = pool.putPage(0);
            header.pack(pg.data);
            pool.flush();
            pool.modify(pg);
            header.curr = curr ^= 1;
            header.dirty = true;
            header.pack(pg.data);
            pool.unfix(pg);
            pool.flush();

            header.root[1 - curr].size = header.root[curr].size;
            header.root[1 - curr].indexUsed = currIndexSize;
            header.root[1 - curr].freeList = header.root[curr].freeList;
            header.root[1 - curr].bitmapEnd = header.root[curr].bitmapEnd;
            header.root[1 - curr].rootObject = header.root[curr].rootObject;
            header.root[1 - curr].classDescList = header.root[curr].classDescList;
            header.root[1 - curr].bitmapExtent = header.root[curr].bitmapExtent;

            if (currIndexSize == 0 || newIndexSize != oldIndexSize)
            {
                if (currIndexSize == 0)
                {
                    currIndexSize = header.root[1 - curr].indexUsed;
                }
                header.root[1 - curr].index = header.root[curr].shadowIndex;
                header.root[1 - curr].indexSize = header.root[curr].shadowIndexSize;
                header.root[1 - curr].shadowIndex = header.root[curr].index;
                header.root[1 - curr].shadowIndexSize = header.root[curr].indexSize;
                pool.copy(header.root[1 - curr].index, header.root[curr].index, currIndexSize * 8L);
                i = (currIndexSize + dbHandlesPerPage * 32 - 1) >> (dbHandlesPerPageBits + 5);
                while (--i >= 0)
                {
                    map[i] = 0;
                }
            }
            else
            {
                for (i = 0; i < nPages; i++)
                {
                    if ((map[i >> 5] & (1 << (i & 31))) != 0)
                    {
                        map[i >> 5] -= (1 << (i & 31));
                        pool.copy(header.root[1 - curr].index + (long)i * Page.pageSize,
                                  header.root[curr].index + (long)i * Page.pageSize,
                                  Page.pageSize);
                    }
                }
                if (currIndexSize > i * dbHandlesPerPage && ((map[i >> 5] & (1 << (i & 31))) != 0 || currIndexSize != committedIndexSize))
                {
                    pool.copy(header.root[1 - curr].index + (long)i * Page.pageSize,
                              header.root[curr].index + (long)i * Page.pageSize,
                              8L * currIndexSize - (long)i * Page.pageSize);
                    j = i >> 5;
                    n = (currIndexSize + dbHandlesPerPage * 32 - 1) >> (dbHandlesPerPageBits + 5);
                    while (j < n)
                    {
                        map[j++] = 0;
                    }
                }
            }
            gcDone = false;
            currIndex = curr;
            committedIndexSize = currIndexSize;
        }

        public void Rollback()
        {
            lock (this)
            {
                ensureOpened();
                objectCache.Invalidate();

                if (!modified)
                    return;

                rollback0();
                modified = false;
            }
        }

        private void rollback0()
        {
            int curr = currIndex;
            int[] map = dirtyPagesMap;
            if (header.root[1 - curr].index != header.root[curr].shadowIndex)
            {
                pool.copy(header.root[curr].shadowIndex, header.root[curr].index, 8L * committedIndexSize);
            }
            else
            {
                int nPages = (committedIndexSize + dbHandlesPerPage - 1) >> dbHandlesPerPageBits;
                for (int i = 0; i < nPages; i++)
                {
                    if ((map[i >> 5] & (1 << (i & 31))) != 0)
                    {
                        pool.copy(header.root[curr].shadowIndex + (long)i * Page.pageSize,
                                  header.root[curr].index + (long)i * Page.pageSize,
                                  Page.pageSize);
                    }
                }
            }
            for (int j = (currIndexSize + dbHandlesPerPage * 32 - 1) >> (dbHandlesPerPageBits + 5); --j >= 0; map[j] = 0)
                ;
            header.root[1 - curr].index = header.root[curr].shadowIndex;
            header.root[1 - curr].indexSize = header.root[curr].shadowIndexSize;
            header.root[1 - curr].indexUsed = committedIndexSize;
            header.root[1 - curr].freeList = header.root[curr].freeList;
            header.root[1 - curr].bitmapEnd = header.root[curr].bitmapEnd;
            header.root[1 - curr].size = header.root[curr].size;
            header.root[1 - curr].rootObject = header.root[curr].rootObject;
            header.root[1 - curr].classDescList = header.root[curr].classDescList;
            header.root[1 - curr].bitmapExtent = header.root[curr].bitmapExtent;
            header.dirty = true;
            usedSize = header.root[curr].size;
            currIndexSize = committedIndexSize;

            currRBitmapPage = currPBitmapPage = 0;
            currRBitmapOffs = currPBitmapOffs = 0;

            reloadScheme();
        }

        private void memset(byte[] arr, int off, int len, byte val)
        {
            while (--len >= 0)
            {
                arr[off++] = val;
            }
        }

#if CF
        class PositionComparer : System.Collections.IComparer 
        {
            public int Compare(object o1, object o2) 
            {
                long i1 = (long)o1;
                long i2 = (long)o2;
                return i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
            }
        }
#else
        public IPersistent CreateClass(Type type)
        {
            lock (this)
            {
                ensureOpened();

                lock (objectCache)
                {
                    Type wrapper = getWrapper(type);
                    IPersistent obj = (IPersistent)wrapper.Assembly.CreateInstance(wrapper.Name);
                    int oid = allocateId();
                    obj.AssignOid(this, oid, false);
                    setPos(oid, 0);
                    objectCache.Put(oid, obj);
                    obj.Modify();
                    return obj;
                }
            }
        }

        internal Type getWrapper(Type original)
        {
            Type wrapper;
            bool ok = wrapperHash.TryGetValue(original, out wrapper);
            if (!ok)
            {
                wrapper = CodeGenerator.Instance.CreateWrapper(original);
                wrapperHash[original] = wrapper;
            }
            return wrapper;
        }
#endif
        public int MakePersistent(IPersistent obj)
        {
            if (obj == null)
                return 0;
            if (obj.Oid != 0)
                return obj.Oid;

            lock (this)
            {
                ensureOpened();
                lock (objectCache)
                {
                    int oid = allocateId();
                    obj.AssignOid(this, oid, false);
                    setPos(oid, 0);
                    objectCache.Put(oid, obj);
                    obj.Modify();
                    return oid;
                }
            }
        }

        public void Backup(System.IO.Stream stream)
        {
            lock (this)
            {
                ensureOpened();
                objectCache.Flush();
                int curr = 1 - currIndex;
                int nObjects = header.root[curr].indexUsed;
                long indexOffs = header.root[curr].index;
                int i, j, k;
                int nUsedIndexPages = (nObjects + dbHandlesPerPage - 1) / dbHandlesPerPage;
                int nIndexPages = (int)((header.root[curr].indexSize + dbHandlesPerPage - 1) / dbHandlesPerPage);
                long totalRecordsSize = 0;
                long nPagedObjects = 0;
                int bitmapExtent = header.root[curr].bitmapExtent;
                long[] index = new long[nObjects];
                int[] oids = new int[nObjects];

                if (bitmapExtent == 0)
                    bitmapExtent = int.MaxValue;

                for (i = 0, j = 0; i < nUsedIndexPages; i++)
                {
                    Page pg = pool.getPage(indexOffs + (long)i * Page.pageSize);
                    for (k = 0; k < dbHandlesPerPage && j < nObjects; k++, j++)
                    {
                        long pos = Bytes.unpack8(pg.data, k * 8);
                        index[j] = pos;
                        oids[j] = j;
                        if ((pos & dbFreeHandleFlag) == 0)
                        {
                            if ((pos & dbPageObjectFlag) != 0)
                            {
                                nPagedObjects += 1;
                            }
                            else if (pos != 0)
                            {
                                int offs = (int)pos & (Page.pageSize - 1);
                                Page op = pool.getPage(pos - offs);
                                int size = ObjectHeader.getSize(op.data, offs & ~dbFlagsMask);
                                size = (size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1);
                                totalRecordsSize += size;
                                pool.unfix(op);
                            }
                        }
                    }
                    pool.unfix(pg);

                }
                Header newHeader = new Header();
                newHeader.curr = 0;
                newHeader.dirty = false;
                newHeader.initialized = true;
                long newFileSize = (long)(nPagedObjects + nIndexPages * 2 + 1) * Page.pageSize + totalRecordsSize;
                newFileSize = (newFileSize + Page.pageSize - 1) & ~(Page.pageSize - 1);
                newHeader.root = new RootPage[2];
                newHeader.root[0] = new RootPage();
                newHeader.root[1] = new RootPage();
                newHeader.root[0].size = newHeader.root[1].size = newFileSize;
                newHeader.root[0].index = newHeader.root[1].shadowIndex = Page.pageSize;
                newHeader.root[0].shadowIndex = newHeader.root[1].index = Page.pageSize + (long)nIndexPages * Page.pageSize;
                newHeader.root[0].shadowIndexSize = newHeader.root[0].indexSize =
                    newHeader.root[1].shadowIndexSize = newHeader.root[1].indexSize = nIndexPages * dbHandlesPerPage;
                newHeader.root[0].indexUsed = newHeader.root[1].indexUsed = nObjects;
                newHeader.root[0].freeList = newHeader.root[1].freeList = header.root[curr].freeList;
                newHeader.root[0].bitmapEnd = newHeader.root[1].bitmapEnd = header.root[curr].bitmapEnd;

                newHeader.root[0].rootObject = newHeader.root[1].rootObject = header.root[curr].rootObject;
                newHeader.root[0].classDescList = newHeader.root[1].classDescList = header.root[curr].classDescList;
                newHeader.root[0].bitmapExtent = newHeader.root[1].bitmapExtent = bitmapExtent;

                byte[] page = new byte[Page.pageSize];
                newHeader.pack(page);
                stream.Write(page, 0, Page.pageSize);

                long pageOffs = (long)(nIndexPages * 2 + 1) * Page.pageSize;
                long recOffs = (long)(nPagedObjects + nIndexPages * 2 + 1) * Page.pageSize;
#if CF
                Array.Sort(index, oids, 0, nObjects, new PositionComparer());
#else
                Array.Sort(index, oids);
#endif
                byte[] newIndex = new byte[nIndexPages * dbHandlesPerPage * 8];
                for (i = 0; i < nObjects; i++)
                {
                    long pos = index[i];
                    int oid = oids[i];
                    if (pos != 0 && (pos & dbFreeHandleFlag) == 0)
                    {
                        if ((pos & dbPageObjectFlag) != 0)
                        {
                            Bytes.pack8(newIndex, oid * 8, pageOffs | dbPageObjectFlag);
                            pageOffs += Page.pageSize;
                        }
                        else
                        {
                            Bytes.pack8(newIndex, oid * 8, recOffs);
                            int offs = (int)pos & (Page.pageSize - 1);
                            Page op = pool.getPage(pos - offs);
                            int size = ObjectHeader.getSize(op.data, offs & ~dbFlagsMask);
                            size = (size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1);
                            recOffs += size;
                            pool.unfix(op);
                        }
                    }
                    else
                    {
                        Bytes.pack8(newIndex, oid * 8, pos);
                    }
                }
                stream.Write(newIndex, 0, newIndex.Length);
                stream.Write(newIndex, 0, newIndex.Length);

                for (i = 0; i < nObjects; i++)
                {
                    long pos = index[i];
                    if (((int)pos & (dbFreeHandleFlag | dbPageObjectFlag)) == dbPageObjectFlag)
                    {
                        if (oids[i] < dbBitmapId + dbBitmapPages
                            || (oids[i] >= bitmapExtent && oids[i] < bitmapExtent + dbLargeBitmapPages - dbBitmapPages))
                        {
                            int pageId = oids[i] < dbBitmapId + dbBitmapPages
                                ? oids[i] - dbBitmapId : oids[i] - bitmapExtent;
                            long mappedSpace = (long)pageId * Page.pageSize * 8 * dbAllocationQuantum;
                            if (mappedSpace >= newFileSize)
                            {
                                memset(page, 0, Page.pageSize, (byte)0);
                            }
                            else if (mappedSpace + Page.pageSize * 8 * dbAllocationQuantum <= newFileSize)
                            {
                                memset(page, 0, Page.pageSize, (byte)0xFF);
                            }
                            else
                            {
                                int nBits = (int)((newFileSize - mappedSpace) >> dbAllocationQuantumBits);
                                memset(page, 0, nBits >> 3, (byte)0xFF);
                                page[nBits >> 3] = (byte)((1 << (nBits & 7)) - 1);
                                memset(page, (nBits >> 3) + 1, Page.pageSize - (nBits >> 3) - 1, (byte)0);
                            }
                            stream.Write(page, 0, Page.pageSize);
                        }
                        else
                        {
                            Page pg = pool.getPage(pos & ~dbFlagsMask);
                            stream.Write(pg.data, 0, Page.pageSize);
                            pool.unfix(pg);
                        }
                    }
                }

                for (i = 0; i < nObjects; i++)
                {
                    long pos = index[i];
                    if (pos != 0 && ((int)pos & (dbFreeHandleFlag | dbPageObjectFlag)) == 0)
                    {
                        pos &= ~dbFlagsMask;
                        int offs = (int)pos & (Page.pageSize - 1);
                        Page pg = pool.getPage(pos - offs);
                        int size = ObjectHeader.getSize(pg.data, offs);
                        size = (size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1);

                        while (true)
                        {
                            if (Page.pageSize - offs >= size)
                            {
                                stream.Write(pg.data, offs, size);
                                break;
                            }
                            stream.Write(pg.data, offs, Page.pageSize - offs);
                            size -= Page.pageSize - offs;
                            pos += Page.pageSize - offs;
                            offs = 0;
                            pool.unfix(pg);
                            pg = pool.getPage(pos);
                        }
                        pool.unfix(pg);
                    }
                }
                if (recOffs != newFileSize)
                {
                    Debug.Assert(newFileSize - recOffs < Page.pageSize);
                    int align = (int)(newFileSize - recOffs);
                    memset(page, 0, align, (byte)0);
                    stream.Write(page, 0, align);
                }
            }
        }

        public IIndex<K, V> CreateIndex<K, V>(IndexType indexType) where V : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
#if WITH_OLD_BTREE
                IIndex<K, V> index = alternativeBtree
                    ? new Btree<K, V>(indexType)
                    : (IIndex<K, V>)new OldBtree<K, V>(indexType);
#else
                IIndex<K, V> index = new Btree<K, V>(indexType);
#endif
                index.AssignOid(this, 0, false);
                return index;
            }
        }

        public IIndex<K, V> CreateThickIndex<K, V>() where V : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                return new ThickIndex<K, V>(this);
            }
        }

#if WITH_OLD_BTREE
        public IBitIndex<T> CreateBitIndex<T>() where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                IBitIndex<T> index = new OldBitIndexImpl<T>();
                index.AssignOid(this, 0, false);
                return index;
            }
        }
#endif

        public ISpatialIndex<T> CreateSpatialIndex<T>() where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                Rtree<T> index = new Rtree<T>();
                index.AssignOid(this, 0, false);
                return index;
            }
        }

        public ISpatialIndexR2<T> CreateSpatialIndexR2<T>() where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                RtreeR2<T> index = new RtreeR2<T>();
                index.AssignOid(this, 0, false);
                return index;
            }
        }

        public ISortedCollection<K, V> CreateSortedCollection<K, V>(PersistentComparator<K, V> comparator, IndexType indexType) where V : class,IPersistent
        {
            ensureOpened();
            bool unique = (indexType == IndexType.Unique);
            return new Ttree<K, V>(comparator, unique);
        }

        public ISortedCollection<K, V> CreateSortedCollection<K, V>(IndexType indexType) where V : class,IPersistent, IComparable<K>, IComparable<V>
        {
            ensureOpened();
            bool unique = (indexType == IndexType.Unique);
            return new Ttree<K, V>(new DefaultPersistentComparator<K, V>(), unique);
        }

        internal ISet<T> CreateBtreeSet<T>() where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
#if WITH_OLD_BTREE
                ISet<T> s = alternativeBtree
                    ? (ISet<T>)new PersistentSet<T>()
                    : (ISet<T>)new OldPersistentSet<T>();
#else
                ISet<T> s = new PersistentSet<T>();
#endif
                s.AssignOid(this, 0, false);
                return s;
            }
        }

        public ISet<T> CreateSet<T>() where T : class,IPersistent
        {
            return CreateSet<T>(8);
        }

        public ISet<T> CreateSet<T>(int initialSize) where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                return new ScalableSet<T>(this, initialSize);
            }
        }

        public IFieldIndex<K, V> CreateFieldIndex<K, V>(String fieldName, IndexType indexType) where V : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                bool unique = (indexType == IndexType.Unique);
#if WITH_OLD_BTREE
                IFieldIndex<K, V> index = alternativeBtree
                    ? (IFieldIndex<K, V>)new BtreeFieldIndex<K, V>(fieldName, unique)
                    : (IFieldIndex<K, V>)new OldBtreeFieldIndex<K, V>(fieldName, unique);
#else
                IFieldIndex<K, V> index = (IFieldIndex<K, V>)new BtreeFieldIndex<K, V>(fieldName, unique);
#endif
                index.AssignOid(this, 0, false);
                return index;
            }
        }

        public IMultiFieldIndex<T> CreateFieldIndex<T>(string[] fieldNames, IndexType indexType) where T : class,IPersistent
        {
            lock (this)
            {
                ensureOpened();
                bool unique = (indexType == IndexType.Unique);
#if CF
                if (alternativeBtree) 
                {
                    throw new DatabaseError(DatabaseError.ErrorCode.UNSUPPORTED_INDEX_TYPE);
                }
                MultiFieldIndex<T> index = new BtreeMultiFieldIndex<T>(fieldNames, unique);
#else
#if WITH_OLD_BTREE
                IMultiFieldIndex<T> index = alternativeBtree
                    ? (IMultiFieldIndex<T>)new BtreeMultiFieldIndex<T>(fieldNames, unique)
                    : (IMultiFieldIndex<T>)new OldBtreeMultiFieldIndex<T>(fieldNames, unique);
#else
                IMultiFieldIndex<T> index = (IMultiFieldIndex<T>)new BtreeMultiFieldIndex<T>(fieldNames, unique);
#endif
#endif
                index.AssignOid(this, 0, false);
                return index;
            }
        }

        public ILink<T> CreateLink<T>() where T : class,IPersistent
        {
            return CreateLink<T>(8);
        }

        public ILink<T> CreateLink<T>(int initialSize) where T : class,IPersistent
        {
            return new LinkImpl<T>(initialSize);
        }

        internal ILink<T> ConstructLink<T>(IPersistent[] arr, IPersistent owner) where T : class,IPersistent
        {
            return new LinkImpl<T>(arr, owner);
        }

        public IPArray<T> CreateArray<T>() where T : class,IPersistent
        {
            return CreateArray<T>(8);
        }

        public IPArray<T> CreateArray<T>(int initialSize) where T : class,IPersistent
        {
            return new PArrayImpl<T>(this, initialSize);
        }

        internal IPArray<T> ConstructArray<T>(int[] arr, IPersistent owner) where T : class,IPersistent
        {
            return new PArrayImpl<T>(this, arr, owner);
        }

        public Relation<M, O> CreateRelation<M, O>(O owner)
            where M : class,IPersistent
            where O : class,IPersistent
        {
            return new RelationImpl<M, O>(owner);
        }

        public ITimeSeries<T> CreateTimeSeries<T>(int blockSize, long maxBlockTimeInterval) where T : ITimeSeriesTick
        {
            return new TimeSeriesImpl<T>(this, blockSize, maxBlockTimeInterval);
        }

#if WITH_PATRICIA
        public IPatriciaTrie<T> CreatePatriciaTrie<T>() where T : class,IPersistent
        {
            return new PTrie<T>();
        }
#endif

        public IBlob CreateBlob()
        {
            return new BlobImpl(Page.pageSize - ObjectHeader.Sizeof - 16);
        }

#if WITH_XML
        public void ExportXML(System.IO.StreamWriter writer)
        {
            lock (this)
            {
                ensureOpened();
                int rootOid = header.root[1 - currIndex].rootObject;
                if (rootOid != 0)
                {
                    XmlExporter xmlExporter = new XmlExporter(this, writer);
                    xmlExporter.exportDatabase(rootOid);
                }
            }
        }

        public void ImportXML(System.IO.StreamReader reader)
        {
            lock (this)
            {
                ensureOpened();
                XmlImporter xmlImporter = new XmlImporter(this, reader);
                xmlImporter.importDatabase();
            }
        }
#endif

        internal long getGCPos(int oid)
        {
            Page pg = pool.getPage(header.root[currIndex].index
                + ((long)(oid >> dbHandlesPerPageBits) << Page.pageBits));
            long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage - 1)) << 3);
            pool.unfix(pg);
            return pos;
        }

        internal void markOid(int oid)
        {
            if (0 == oid)
                return;
            long pos = getGCPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                throw new DatabaseException(DatabaseException.ErrorCode.INVALID_OID);

            int bit = (int)((ulong)pos >> dbAllocationQuantumBits);
            if ((blackBitmap[(uint)bit >> 5] & (1 << (bit & 31))) == 0)
                greyBitmap[(uint)bit >> 5] |= 1 << (bit & 31);
        }

        internal Page getGCPage(int oid)
        {
            return pool.getPage(getGCPos(oid) & ~dbFlagsMask);
        }

        public int Gc()
        {
            lock (this)
            {
                return gc0();
            }
        }

#if WITH_OLD_BTREE
        internal OldBtree createBtreeStub(byte[] data, int offs)
        {
            return new OldBtree<int, IPersistent>(data, ObjectHeader.Sizeof + offs);
        }
#endif

        private void mark()
        {
            // Console.WriteLine("Start GC, allocatedDelta=" + allocatedDelta + ", header[" + currIndex + "].size=" + header.root[currIndex].size + ", gcTreshold=" + gcThreshold);
            int bitmapSize = (int)((ulong)header.root[currIndex].size >> (dbAllocationQuantumBits + 5)) + 1;
            bool existsNotMarkedObjects;
            long pos;
            int i, j;

            if (Listener != null)
                Listener.GcStarted();

            greyBitmap = new int[bitmapSize];
            blackBitmap = new int[bitmapSize];
            int rootOid = header.root[currIndex].rootObject;
            if (rootOid != 0)
            {
                markOid(rootOid);
                do
                {
                    existsNotMarkedObjects = false;
                    for (i = 0; i < bitmapSize; i++)
                    {
                        if (greyBitmap[i] != 0)
                        {
                            existsNotMarkedObjects = true;
                            for (j = 0; j < 32; j++)
                            {
                                if ((greyBitmap[i] & (1 << j)) != 0)
                                {
                                    pos = (((long)i << 5) + j) << dbAllocationQuantumBits;
                                    greyBitmap[i] &= ~(1 << j);
                                    blackBitmap[i] |= 1 << j;
                                    int offs = (int)pos & (Page.pageSize - 1);
                                    Page pg = pool.getPage(pos - offs);
                                    int typeOid = ObjectHeader.getType(pg.data, offs);
                                    if (typeOid != 0)
                                    {
                                        ClassDescriptor desc = (ClassDescriptor)lookupObject(typeOid, typeof(ClassDescriptor));
#if WITH_OLD_BTREE
                                        if (typeof(OldBtree).IsAssignableFrom(desc.cls))
                                        {
                                            OldBtree btree = createBtreeStub(pg.data, offs);
                                            btree.AssignOid(this, 0, false);
                                            btree.markTree();
                                        }
                                        else
#endif
                                        if (desc.hasReferences)
                                            markObject(pool.get(pos), ObjectHeader.Sizeof, desc);
                                    }
                                    pool.unfix(pg);
                                }
                            }
                        }
                    }
                } while (existsNotMarkedObjects);
            }
        }

        private int sweep()
        {
            int nDeallocated = 0;
            long pos;
            gcDone = true;
            for (int i = dbFirstUserId, j = committedIndexSize; i < j; i++)
            {
                pos = getGCPos(i);
                if (pos != 0 && ((int)pos & (dbPageObjectFlag | dbFreeHandleFlag)) == 0)
                {
                    int bit = (int)((ulong)pos >> dbAllocationQuantumBits);
                    if ((blackBitmap[(uint)bit >> 5] & (1 << (bit & 31))) == 0)
                    {
                        // object is not accessible
                        if (getPos(i) != pos)
                            throw new DatabaseException(DatabaseException.ErrorCode.INVALID_OID);

                        int offs = (int)pos & (Page.pageSize - 1);
                        Page pg = pool.getPage(pos - offs);
                        int typeOid = ObjectHeader.getType(pg.data, offs);
                        if (typeOid != 0)
                        {
                            ClassDescriptor desc = findClassDescriptor(typeOid);
                            nDeallocated += 1;
#if WITH_OLD_BTREE
                            if (desc != null
                                && (typeof(OldBtree).IsAssignableFrom(desc.cls)))
                            {
                                OldBtree btree = createBtreeStub(pg.data, offs);
                                pool.unfix(pg);
                                btree.AssignOid(this, i, false);
                                btree.Deallocate();
                            }
                            else
#endif
                            {
                                int size = ObjectHeader.getSize(pg.data, offs);
                                pool.unfix(pg);
                                freeId(i);
                                objectCache.Remove(i);
                                cloneBitmap(pos, size);
                            }
                            if (Listener != null)
                                Listener.DeallocateObject(desc.cls, i);
                        }
                    }
                }
            }

            greyBitmap = null;
            blackBitmap = null;
            allocatedDelta = 0;
            gcActive = false;

            if (Listener != null)
                Listener.GcCompleted(nDeallocated);

            return nDeallocated;
        }

#if !CF
        public void backgroundGcThread()
        {
            while (true)
            {
                lock (backgroundGcStartMonitor)
                {
                    while (!gcGo && opened)
                    {
                        Monitor.Wait(backgroundGcStartMonitor);
                    }
                    if (!opened)
                        return;

                    gcGo = false;
                }
                lock (backgroundGcMonitor)
                {
                    if (!opened)
                        return;

                    mark();
                    lock (this)
                    {
                        lock (objectCache)
                        {
                            sweep();
                        }
                    }
                }
            }
        }

        private void activateGc()
        {
            lock (backgroundGcStartMonitor)
            {
                gcGo = true;
                Monitor.Pulse(backgroundGcStartMonitor);
            }
        }
#endif

        private int gc0()
        {
            lock (objectCache)
            {
                ensureOpened();

                if (gcDone || gcActive)
                    return 0;

                gcActive = true;
#if !CF
                if (backgroundGc)
                {
                    if (gcThread == null)
                    {
                        gcThread = new Thread(new ThreadStart(backgroundGcThread));
                        gcThread.Start();
                    }
                    activateGc();
                    return 0;
                }
#endif
                // System.out.println("Start GC, allocatedDelta=" + allocatedDelta + ", header[" + currIndex + "].size=" + header.root[currIndex].size + ", gcTreshold=" + gcThreshold);

                mark();
                return sweep();
            }
        }

        public Dictionary<Type, TypeMemoryUsage> GetMemoryUsage()
        {
            lock (this)
            {
                lock (objectCache)
                {
                    ensureOpened();

                    int bitmapSize = (int)(header.root[currIndex].size >> (dbAllocationQuantumBits + 5)) + 1;
                    bool existsNotMarkedObjects;
                    long pos;
                    int i, j;

                    // mark
                    greyBitmap = new int[bitmapSize];
                    blackBitmap = new int[bitmapSize];
                    int rootOid = header.root[currIndex].rootObject;
                    var map = new Dictionary<Type, TypeMemoryUsage>();
                    if (0 == rootOid)
                        return map;

                    TypeMemoryUsage indexUsage = new TypeMemoryUsage(typeof(IGenericIndex));
                    TypeMemoryUsage classUsage = new TypeMemoryUsage(typeof(Type));

                    markOid(rootOid);
                    do
                    {
                        existsNotMarkedObjects = false;
                        for (i = 0; i < bitmapSize; i++)
                        {
                            if (greyBitmap[i] != 0)
                            {
                                existsNotMarkedObjects = true;
                                for (j = 0; j < 32; j++)
                                {
                                    if ((greyBitmap[i] & (1 << j)) != 0)
                                    {
                                        pos = (((long)i << 5) + j) << dbAllocationQuantumBits;
                                        greyBitmap[i] &= ~(1 << j);
                                        blackBitmap[i] |= 1 << j;
                                        int offs = (int)pos & (Page.pageSize - 1);
                                        Page pg = pool.getPage(pos - offs);
                                        int typeOid = ObjectHeader.getType(pg.data, offs);
                                        int objSize = ObjectHeader.getSize(pg.data, offs);
                                        int alignedSize = (objSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1);
                                        if (typeOid != 0)
                                        {
                                            markOid(typeOid);
                                            ClassDescriptor desc = findClassDescriptor(typeOid);
#if WITH_OLD_BTREE
                                            if (typeof(OldBtree).IsAssignableFrom(desc.cls))
                                            {
                                                OldBtree btree = createBtreeStub(pg.data, offs);
                                                btree.AssignOid(this, 0, false);
                                                int nPages = btree.markTree();
                                                indexUsage.Count += 1;
                                                indexUsage.TotalSize += (long)nPages * Page.pageSize + objSize;
                                                indexUsage.AllocatedSize += (long)nPages * Page.pageSize + alignedSize;
                                            }
                                            else
#endif
                                            {
                                                TypeMemoryUsage usage;
                                                var ok = map.TryGetValue(desc.cls, out usage);
                                                if (!ok)
                                                {
                                                    usage = new TypeMemoryUsage(desc.cls);
                                                    map[desc.cls] = usage;
                                                }
                                                usage.Count += 1;
                                                usage.TotalSize += objSize;
                                                usage.AllocatedSize += alignedSize;

                                                if (desc.hasReferences)
                                                {
                                                    markObject(pool.get(pos), ObjectHeader.Sizeof, desc);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            classUsage.Count += 1;
                                            classUsage.TotalSize += objSize;
                                            classUsage.AllocatedSize += alignedSize;
                                        }
                                        pool.unfix(pg);
                                    }
                                }
                            }
                        }
                    } while (existsNotMarkedObjects);

                    if (indexUsage.Count != 0)
                        map[typeof(IGenericIndex)] = indexUsage;

                    if (classUsage.Count != 0)
                        map[typeof(Type)] = classUsage;

                    TypeMemoryUsage system = new TypeMemoryUsage(typeof(IDatabase));
                    system.TotalSize += header.root[0].indexSize * 8L;
                    system.TotalSize += header.root[1].indexSize * 8L;
                    system.TotalSize += (long)(header.root[currIndex].bitmapEnd - dbBitmapId) * Page.pageSize;
                    system.TotalSize += Page.pageSize; // root page

                    if (header.root[currIndex].bitmapExtent != 0)
                    {
                        system.AllocatedSize = getBitmapUsedSpace(dbBitmapId, dbBitmapId + dbBitmapPages)
                            + getBitmapUsedSpace(header.root[currIndex].bitmapExtent,
                            header.root[currIndex].bitmapExtent + header.root[currIndex].bitmapEnd - dbBitmapId);
                    }
                    else
                    {
                        system.AllocatedSize = getBitmapUsedSpace(dbBitmapId, header.root[currIndex].bitmapEnd);
                    }
                    system.Count = header.root[currIndex].indexSize;
                    map[typeof(IDatabase)] = system;
                    return map;
                }
            }
        }

        long getBitmapUsedSpace(int from, int till)
        {
            long allocated = 0;
            while (from < till)
            {
                Page pg = getGCPage(from);
                for (int j = 0; j < Page.pageSize; j++)
                {
                    int mask = pg.data[j] & 0xFF;
                    while (mask != 0)
                    {
                        if ((mask & 1) != 0)
                            allocated += dbAllocationQuantum;

                        mask >>= 1;
                    }
                }
                pool.unfix(pg);
                from += 1;
            }
            return allocated;
        }

        internal int markObject(byte[] obj, int offs, ClassDescriptor desc)
        {
            ClassDescriptor.FieldDescriptor[] all = desc.allFields;

            for (int i = 0, n = all.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = all[i];
                switch (fd.type)
                {
                    case ClassDescriptor.FieldType.tpBoolean:
                    case ClassDescriptor.FieldType.tpByte:
                    case ClassDescriptor.FieldType.tpSByte:
                        offs += 1;
                        continue;
                    case ClassDescriptor.FieldType.tpChar:
                    case ClassDescriptor.FieldType.tpShort:
                    case ClassDescriptor.FieldType.tpUShort:
                        offs += 2;
                        continue;
                    case ClassDescriptor.FieldType.tpInt:
                    case ClassDescriptor.FieldType.tpUInt:
                    case ClassDescriptor.FieldType.tpEnum:
                    case ClassDescriptor.FieldType.tpFloat:
                        offs += 4;
                        continue;
                    case ClassDescriptor.FieldType.tpLong:
                    case ClassDescriptor.FieldType.tpULong:
                    case ClassDescriptor.FieldType.tpDouble:
                    case ClassDescriptor.FieldType.tpDate:
                        offs += 8;
                        continue;
                    case ClassDescriptor.FieldType.tpDecimal:
                    case ClassDescriptor.FieldType.tpGuid:
                        offs += 16;
                        continue;
                    case ClassDescriptor.FieldType.tpString:
                        {
                            int strlen = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (strlen > 0)
                                offs += strlen * 2;
                            else if (strlen < -1)
                                offs -= strlen + 2;
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpObject:
                    case ClassDescriptor.FieldType.tpOid:
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                        continue;
                    case ClassDescriptor.FieldType.tpValue:
                        offs = markObject(obj, offs, fd.valueDesc);
                        continue;
                    case ClassDescriptor.FieldType.tpRaw:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (len > 0)
                            {
                                offs += len;
                            }
                            else if (len == -2 - (int)ClassDescriptor.FieldType.tpObject)
                            {
                                markOid(Bytes.unpack4(obj, offs));
                                offs += 4;
                            }
                            else if (len < -1)
                            {
                                offs += ClassDescriptor.Sizeof[-2 - len];
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfByte:
                    case ClassDescriptor.FieldType.tpArrayOfSByte:
                    case ClassDescriptor.FieldType.tpArrayOfBoolean:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (len > 0)
                            {
                                offs += len;
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfShort:
                    case ClassDescriptor.FieldType.tpArrayOfUShort:
                    case ClassDescriptor.FieldType.tpArrayOfChar:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (len > 0)
                            {
                                offs += len * 2;
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfInt:
                    case ClassDescriptor.FieldType.tpArrayOfUInt:
                    case ClassDescriptor.FieldType.tpArrayOfEnum:
                    case ClassDescriptor.FieldType.tpArrayOfFloat:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (len > 0)
                            {
                                offs += len * 4;
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfLong:
                    case ClassDescriptor.FieldType.tpArrayOfULong:
                    case ClassDescriptor.FieldType.tpArrayOfDouble:
                    case ClassDescriptor.FieldType.tpArrayOfDate:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            if (len > 0)
                            {
                                offs += len * 8;
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfString:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            while (--len >= 0)
                            {
                                int strlen = Bytes.unpack4(obj, offs);
                                offs += 4;
                                if (strlen > 0)
                                {
                                    offs += strlen * 2;
                                }
                                else if (strlen < -1)
                                {
                                    offs -= strlen + 2;
                                }
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfObject:
                    case ClassDescriptor.FieldType.tpArrayOfOid:
                    case ClassDescriptor.FieldType.tpLink:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            while (--len >= 0)
                            {
                                markOid(Bytes.unpack4(obj, offs));
                                offs += 4;
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfValue:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            ClassDescriptor valueDesc = fd.valueDesc;
                            while (--len >= 0)
                            {
                                offs = markObject(obj, offs, valueDesc);
                            }
                            continue;
                        }
                    case ClassDescriptor.FieldType.tpArrayOfRaw:
                        {
                            int len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            while (--len >= 0)
                            {
                                int rawlen = Bytes.unpack4(obj, offs);
                                offs += 4;
                                if (rawlen >= 0)
                                {
                                    offs += rawlen;
                                }
                                else if (rawlen == -2 - (int)ClassDescriptor.FieldType.tpObject)
                                {
                                    markOid(Bytes.unpack4(obj, offs));
                                    offs += 4;
                                }
                                else if (rawlen < -1)
                                {
                                    offs += ClassDescriptor.Sizeof[-2 - rawlen];
                                }
                            }
                            continue;
                        }
                }
            }
            return offs;
        }

        internal class ThreadTransactionContext
        {
            internal int nested;
            internal ArrayList locked;
            internal ArrayList modified;

            internal ThreadTransactionContext()
            {
                locked = new ArrayList();
                modified = new ArrayList();
            }
        }

        internal static ThreadTransactionContext TransactionContext
        {
            get
            {
                ThreadTransactionContext ctx = (ThreadTransactionContext)Thread.GetData(transactionContext);
                if (ctx == null)
                {
                    ctx = new ThreadTransactionContext();
                    Thread.SetData(transactionContext, ctx);
                }
                return ctx;
            }
        }

        public void EndThreadTransaction()
        {
            EndThreadTransaction(Int32.MaxValue);
        }

#if CF
        public void RegisterAssembly(System.Reflection.Assembly assembly) 
        {
            assemblies.Add(assembly);
        }

        public void BeginThreadTransaction(TransactionMode mode)
        {
            if (mode == TransactionMode.Serializable) 
            { 
                useSerializableTransactions = true;
                TransactionContext.nested += 1;;
            } 
            else 
            { 
                transactionMonitor.Enter(); 
                try {
                    if (scheduledCommitTime != Int64.MaxValue) 
                    { 
                        nBlockedTransactions += 1;
                        while (DateTime.Now.Ticks >= scheduledCommitTime) 
                        { 
                            transactionMonitor.Wait();
                        }
                        nBlockedTransactions -= 1;
                    }
                    nNestedTransactions += 1;
                } finally { 
                    transactionMonitor.Exit(); 
                }
                if (mode == TransactionMode.Exclusive) 
                { 
                    transactionLock.ExclusiveLock();
                } 
                else 
                { 
                    transactionLock.SharedLock();
                }
            }
        }
        
        public void EndThreadTransaction(int maxDelay)
        {
            ThreadTransactionContext ctx = TransactionContext;
            if (ctx.nested != 0) 
            { // serializable transaction
                if (--ctx.nested == 0) 
                { 
                    int i = ctx.modified.Count;
                    if (i != 0) 
                    { 
                        do 
                        { 
                            ((IPersistent)ctx.modified[--i]).Store();
                        } while (i != 0);

                        lock (backgroundGcMonitor) 
                        { 
                            lock (this) 
                            { 
                                commit0();
                            }
                        }
                    }
                    for (i = ctx.locked.Count; --i >= 0;) 
                    { 
                        ((IResource)ctx.locked[i]).Reset();
                    }
                    ctx.modified.Clear();
                    ctx.locked.Clear();
                } 
            } 
            else 
            { // exclusive or cooperative transaction        
                transactionMonitor.Enter(); 
                try { 
                    transactionLock.Unlock();
                    if (nNestedTransactions != 0) 
                    { // may be everything is already aborted
                        if (--nNestedTransactions == 0) 
                        { 
                            nCommittedTransactions += 1;
                            Commit();
                            scheduledCommitTime = Int64.MaxValue;
                            if (nBlockedTransactions != 0) 
                            { 
                                transactionMonitor.PulseAll();
                            }
                        } 
                        else 
                        {
                            if (maxDelay != Int32.MaxValue) 
                            { 
                                long nextCommit = DateTime.Now.Ticks + maxDelay;
                                if (nextCommit < scheduledCommitTime) 
                                { 
                                    scheduledCommitTime = nextCommit;
                                }
                                if (maxDelay == 0) 
                                { 
                                    int n = nCommittedTransactions;
                                    nBlockedTransactions += 1;
                                    do 
                                    { 
                                        transactionMonitor.Wait();
                                    } while (nCommittedTransactions == n);
                                    nBlockedTransactions -= 1;
                                }				    
                            }
                        }
                    }
                } finally { 
                    transactionMonitor.Exit();
                }
            }
        }

        public void RollbackThreadTransaction()
        {
            ThreadTransactionContext ctx = TransactionContext;
            if (ctx.nested != 0) 
            { // serializable transaction
                ctx.nested = 0; 
                int i = ctx.modified.Count;
                if (i != 0) 
                { 
                    do 
                    { 
                        ((IPersistent)ctx.modified[--i]).Invalidate();
                    } while (i != 0);
                
                    lock (this) 
                    { 
                        rollback0();
                    }
                }
                for (i = ctx.locked.Count; --i >= 0;) 
                { 
                    ((IResource)ctx.locked[i]).Reset();
                } 
                ctx.modified.Clear();
                ctx.locked.Clear();
            } 
            else 
            { 
                try { 
                    transactionMonitor.Enter(); 
                    transactionLock.Reset();
                    nNestedTransactions = 0;
                    if (nBlockedTransactions != 0) 
                    { 
                        transactionMonitor.PulseAll();
                    }
                    Rollback();
                } finally { 
                   transactionMonitor.Exit();
                }
            }
        }
#else
        public virtual void BeginThreadTransaction(TransactionMode mode)
        {
            if (mode == TransactionMode.Serializable)
            {
                useSerializableTransactions = true;
                TransactionContext.nested += 1; ;
            }
            else
            {
                lock (transactionMonitor)
                {
                    if (scheduledCommitTime != Int64.MaxValue)
                    {
                        nBlockedTransactions += 1;
                        while (DateTime.Now.Ticks >= scheduledCommitTime)
                        {
                            Monitor.Wait(transactionMonitor);
                        }
                        nBlockedTransactions -= 1;
                    }
                    nNestedTransactions += 1;
                }

                if (mode == TransactionMode.Exclusive)
                    transactionLock.ExclusiveLock();
                else
                    transactionLock.SharedLock();
            }
        }

        public virtual void EndThreadTransaction(int maxDelay)
        {
            ThreadTransactionContext ctx = TransactionContext;
            if (ctx.nested != 0)
            { // serializable transaction
                if (--ctx.nested == 0)
                {
                    int i = ctx.modified.Count;
                    if (i != 0)
                    {
                        do
                        {
                            ((IPersistent)ctx.modified[--i]).Store();
                        } while (i != 0);

                        lock (backgroundGcMonitor)
                        {
                            lock (this)
                            {
                                commit0();
                            }
                        }
                    }
                    for (i = ctx.locked.Count; --i >= 0; )
                    {
                        ((IResource)ctx.locked[i]).Reset();
                    }
                    ctx.modified.Clear();
                    ctx.locked.Clear();
                }
            }
            else
            { // exclusive or cooperative transaction        
                lock (transactionMonitor)
                {
                    transactionLock.Unlock();
                    if (nNestedTransactions != 0)
                    { // may be everything is already aborted
                        if (--nNestedTransactions == 0)
                        {
                            nCommittedTransactions += 1;
                            Commit();
                            scheduledCommitTime = Int64.MaxValue;
                            if (nBlockedTransactions != 0)
                                Monitor.PulseAll(transactionMonitor);
                        }
                        else
                        {
                            if (maxDelay != Int32.MaxValue)
                            {
                                long nextCommit = DateTime.Now.Ticks + maxDelay;
                                if (nextCommit < scheduledCommitTime)
                                {
                                    scheduledCommitTime = nextCommit;
                                }
                                if (maxDelay == 0)
                                {
                                    int n = nCommittedTransactions;
                                    nBlockedTransactions += 1;
                                    do
                                    {
                                        Monitor.Wait(transactionMonitor);
                                    } while (nCommittedTransactions == n);
                                    nBlockedTransactions -= 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        public void RollbackThreadTransaction()
        {
            ThreadTransactionContext ctx = TransactionContext;
            if (ctx.nested != 0)
            { // serializable transaction
                ctx.nested = 0;
                int i = ctx.modified.Count;
                if (i != 0)
                {
                    do
                    {
                        ((IPersistent)ctx.modified[--i]).Invalidate();
                    } while (i != 0);

                    lock (this)
                    {
                        rollback0();
                    }
                }
                for (i = ctx.locked.Count; --i >= 0; )
                {
                    ((IResource)ctx.locked[i]).Reset();
                }
                ctx.modified.Clear();
                ctx.locked.Clear();
            }
            else
            {
                lock (transactionMonitor)
                {
                    transactionLock.Reset();
                    nNestedTransactions = 0;
                    if (nBlockedTransactions != 0)
                        Monitor.PulseAll(transactionMonitor);

                    Rollback();
                }
            }
        }

#endif

        public virtual void Close()
        {
            lock (backgroundGcMonitor)
            {
                try
                {
                    Commit();
                }
                catch
                {
                    opened = false;
                    throw;
                }
                opened = false;
            }
#if !CF
            if (codeGenerationThread != null)
            {
                codeGenerationThread.Join();
                codeGenerationThread = null;
            }

            if (gcThread != null)
            {
                activateGc();
                gcThread.Join();
            }
#endif
            if (isDirty())
            {
                Page pg = pool.putPage(0);
                header.pack(pg.data);
                pool.flush();
                pool.modify(pg);
                header.dirty = false;
                header.pack(pg.data);
                pool.unfix(pg);
                pool.flush();
            }
            pool.close();
            pool = null;
            objectCache = null;
            classDescMap = null;
            resolvedTypes = null;
            bitmapPageAvailableSpace = null;
            dirtyPagesMap = null;
            descList = null;
        }

#if WITH_OLD_BTREE
        public bool AlternativeBtree
        {
            set { alternativeBtree = value; }
            get { return alternativeBtree; }
        }
#endif

        // TODO: needs tests
        public int ObjectIndexInitSize
        {
            set { initIndexSize = value; }
            get { return initIndexSize; }
        }

        // TODO: needs tests
        public long ExtensionQuantum
        {
            set { extensionQuantum = value; }
            get { return extensionQuantum; }
        }

        // TODO: needs tests
        public long GcThreshold
        {
            set { gcThreshold = value; }
            get { return gcThreshold; }
        }

        public bool CodeGeneration
        {
            set { enableCodeGeneration = value; }
            get { return enableCodeGeneration; }
        }

        public bool BackgroundGc
        {
            set { backgroundGc = value; }
            get { return backgroundGc; }
        }

#if WITH_REPLICATION
        public bool ReplicationAck
        {
            set { replicationAck = value; }
            get { return replicationAck; }
        }
#endif

        // TODO: needs tests
        public int ObjectCacheInitSize
        {
            set { objectCacheInitSize = value; }
            get { return objectCacheInitSize; }
        }

        //TODO: needs tests
        public CacheType CacheKind
        {
            set { cacheKind = value; }
            get { return cacheKind; }
        }

        public DatabaseListener Listener { get; set; }

        public IFile File
        {
            get { return pool == null ? null : pool.file; }
        }

        public IPersistent GetObjectByOid(int oid)
        {
            lock (this)
            {
                return oid == 0 ? null : lookupObject(oid, null);
            }
        }

        public void modifyObject(IPersistent obj)
        {
            lock (this)
            {
                lock (objectCache)
                {
                    if (obj.IsModified())
                        return;

                    if (useSerializableTransactions)
                    {
                        ThreadTransactionContext ctx = TransactionContext;
                        if (ctx.nested != 0)
                        { // serializable transaction
                            ctx.modified.Add(obj);
                        }
                    }
                    objectCache.SetDirty(obj.Oid);
                }
            }
        }

        public void lockObject(IPersistent obj)
        {
            if (useSerializableTransactions)
            {
                ThreadTransactionContext ctx = TransactionContext;
                if (ctx.nested != 0)
                { // serializable transaction
                    ctx.locked.Add(obj);
                }
            }
        }

        public void storeObject(IPersistent obj)
        {
            lock (this)
            {
                ensureOpened();

                lock (objectCache)
                {
                    storeObject0(obj);
                }
            }
        }

        public void storeFinalizedObject(IPersistent obj)
        {
            if (!opened)
                return;
            lock (objectCache)
            {
                if (obj.Oid != 0)
                    storeObject0(obj);
            }
        }

        void storeObject0(IPersistent obj)
        {
            obj.OnStore();
            int oid = obj.Oid;
            bool newObject = false;
            if (oid == 0)
            {
                oid = allocateId();
                if (!obj.IsDeleted())
                    objectCache.Put(oid, obj);

                obj.AssignOid(this, oid, false);
                newObject = true;
            }
            else if (obj.IsModified())
            {
                objectCache.ClearDirty(oid);
            }
            byte[] data = packObject(obj);
            long pos;
            int newSize = ObjectHeader.getSize(data, 0);
            if (newObject || (pos = getPos(oid)) == 0)
            {
                pos = allocate(newSize, 0);
                setPos(oid, pos | dbModifiedFlag);
            }
            else
            {
                int offs = (int)pos & (Page.pageSize - 1);
                if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                    throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

                Page pg = pool.getPage(pos - offs);
                offs &= ~dbFlagsMask;
                int size = ObjectHeader.getSize(pg.data, offs);
                pool.unfix(pg);
                if ((pos & dbModifiedFlag) == 0)
                {
                    cloneBitmap(pos & ~dbFlagsMask, size);
                    pos = allocate(newSize, 0);
                    setPos(oid, pos | dbModifiedFlag);
                }
                else
                {
                    if (((newSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1)) > ((size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum - 1)))
                    {
                        long newPos = allocate(newSize, 0);
                        cloneBitmap(pos & ~dbFlagsMask, size);
                        free(pos & ~dbFlagsMask, size);
                        pos = newPos;
                        setPos(oid, pos | dbModifiedFlag);
                    }
                    else if (newSize < size)
                    {
                        ObjectHeader.setSize(data, 0, size);
                    }
                }
            }
            modified = true;
            pool.put(pos & ~dbFlagsMask, data, newSize);
        }

        public void loadObject(IPersistent obj)
        {
            lock (this)
            {
                if (obj.IsRaw())
                    loadStub(obj.Oid, obj, obj.GetType());
            }
        }

        internal IPersistent lookupObject(int oid, System.Type cls)
        {
            IPersistent obj = objectCache.Get(oid);
            if (obj == null || obj.IsRaw())
                obj = loadStub(oid, obj, cls);

            return obj;
        }

        protected virtual int swizzle(IPersistent obj)
        {
            if (null == obj)
                return 0;

            if (!obj.IsPersistent())
                storeObject0(obj);

            return obj.Oid;
        }

        internal ClassDescriptor findClassDescriptor(int oid)
        {
            return (ClassDescriptor)lookupObject(oid, typeof(ClassDescriptor));
        }

        protected virtual IPersistent unswizzle(int oid, System.Type cls, bool recursiveLoading)
        {
            if (oid == 0)
                return null;

            if (recursiveLoading)
                return lookupObject(oid, cls);

            IPersistent stub = objectCache.Get(oid);
            if (stub != null)
                return stub;

            ClassDescriptor desc;
            if (cls == typeof(Persistent) || (desc = (ClassDescriptor)classDescMap[cls]) == null || desc.hasSubclasses)
            {
                long pos = getPos(oid);
                int offs = (int)pos & (Page.pageSize - 1);
                if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                    throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

                Page pg = pool.getPage(pos - offs);
                int typeOid = ObjectHeader.getType(pg.data, offs & ~dbFlagsMask);
                pool.unfix(pg);
                desc = findClassDescriptor(typeOid);
            }
            if (desc.serializer != null)
                stub = desc.serializer.newInstance();
            else
                stub = (IPersistent)desc.newInstance();

            stub.AssignOid(this, oid, true);
            objectCache.Put(oid, stub);
            return stub;
        }

        internal IPersistent loadStub(int oid, IPersistent obj, System.Type cls)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                throw new DatabaseException(DatabaseException.ErrorCode.DELETED_OBJECT);

            byte[] body = pool.get(pos & ~dbFlagsMask);
            ClassDescriptor desc;
            int typeOid = ObjectHeader.getType(body, 0);
            if (typeOid == 0)
                desc = classDescMap[cls];
            else
                desc = findClassDescriptor(typeOid);

            if (obj == null)
            {
                if (desc.serializer != null)
                    obj = desc.serializer.newInstance();
                else
                    obj = (IPersistent)desc.newInstance();

                objectCache.Put(oid, obj);
            }
            obj.AssignOid(this, oid, false);
            if (desc.serializer != null)
                desc.serializer.unpack(this, obj, body, obj.RecursiveLoading());
            else
                unpackObject(obj, desc, obj.RecursiveLoading(), body, ObjectHeader.Sizeof, obj);

            obj.OnLoad();
            return obj;
        }

        internal int unpackObject(object obj, ClassDescriptor desc, bool recursiveLoading, byte[] body, int offs, IPersistent po)
        {
            ClassDescriptor.FieldDescriptor[] all = desc.allFields;
            for (int i = 0, n = all.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = all[i];
                if (obj == null || fd.field == null)
                {
                    offs = skipField(body, offs, fd, fd.type);
                }
                else
                {
                    object val = obj;
                    offs = unpackField(body, offs, recursiveLoading, ref val, fd, fd.type, po);
                    fd.field.SetValue(obj, val);
                }
            }
            return offs;
        }

        public int skipField(byte[] body, int offs, ClassDescriptor.FieldDescriptor fd, ClassDescriptor.FieldType type)
        {
            int len;
            switch (type)
            {
                case ClassDescriptor.FieldType.tpBoolean:
                case ClassDescriptor.FieldType.tpByte:
                case ClassDescriptor.FieldType.tpSByte:
                    return offs + 1;
                case ClassDescriptor.FieldType.tpChar:
                case ClassDescriptor.FieldType.tpShort:
                case ClassDescriptor.FieldType.tpUShort:
                    return offs + 2;
                case ClassDescriptor.FieldType.tpInt:
                case ClassDescriptor.FieldType.tpUInt:
                case ClassDescriptor.FieldType.tpFloat:
                case ClassDescriptor.FieldType.tpObject:
                case ClassDescriptor.FieldType.tpOid:
                    return offs + 4;
                case ClassDescriptor.FieldType.tpLong:
                case ClassDescriptor.FieldType.tpULong:
                case ClassDescriptor.FieldType.tpDouble:
                case ClassDescriptor.FieldType.tpDate:
                    return offs + 8;
                case ClassDescriptor.FieldType.tpDecimal:
                case ClassDescriptor.FieldType.tpGuid:
                    return offs + 16;
                case ClassDescriptor.FieldType.tpString:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        offs += len * 2;
                    }
                    else if (len < -1)
                    {
                        offs -= len + 2;
                    }

                    break;
                case ClassDescriptor.FieldType.tpValue:
                    return unpackObject(null, fd.valueDesc, false, body, offs, null);
                case ClassDescriptor.FieldType.tpRaw:
                case ClassDescriptor.FieldType.tpArrayOfByte:
                case ClassDescriptor.FieldType.tpArrayOfSByte:
                case ClassDescriptor.FieldType.tpArrayOfBoolean:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        offs += len;
                    }
                    else if (len < -1)
                    {
                        offs += ClassDescriptor.Sizeof[-2 - len];
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfShort:
                case ClassDescriptor.FieldType.tpArrayOfUShort:
                case ClassDescriptor.FieldType.tpArrayOfChar:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        offs += len * 2;
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfInt:
                case ClassDescriptor.FieldType.tpArrayOfUInt:
                case ClassDescriptor.FieldType.tpArrayOfFloat:
                case ClassDescriptor.FieldType.tpArrayOfObject:
                case ClassDescriptor.FieldType.tpArrayOfOid:
                case ClassDescriptor.FieldType.tpLink:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        offs += len * 4;
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfLong:
                case ClassDescriptor.FieldType.tpArrayOfULong:
                case ClassDescriptor.FieldType.tpArrayOfDouble:
                case ClassDescriptor.FieldType.tpArrayOfDate:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        offs += len * 8;
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfString:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        for (int j = 0; j < len; j++)
                        {
                            int strlen = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (strlen > 0)
                            {
                                offs += strlen * 2;
                            }
                            else if (strlen < -1)
                            {
                                offs -= strlen + 2;
                            }

                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfValue:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        ClassDescriptor valueDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++)
                        {
                            offs = unpackObject(null, valueDesc, false, body, offs, null);
                        }
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfRaw:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0)
                    {
                        for (int j = 0; j < len; j++)
                        {
                            int rawlen = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (rawlen > 0)
                            {
                                len += rawlen;
                            }
                            else if (rawlen < -1)
                            {
                                offs += ClassDescriptor.Sizeof[-2 - rawlen];
                            }
                        }
                    }
                    break;
            }
            return offs;
        }

        private int unpackRawValue(byte[] body, int offs, out object val, bool recursiveLoading)
        {
            int len = Bytes.unpack4(body, offs);
            offs += 4;
            if (len >= 0)
            {
                System.IO.MemoryStream ms = new System.IO.MemoryStream(body, offs, len);
                val = objectFormatter.Deserialize(ms);
                ms.Close();
                offs += len;
            }
            else
            {
                switch ((ClassDescriptor.FieldType)(-2 - len))
                {
                    case ClassDescriptor.FieldType.tpBoolean:
                        val = body[offs++] != 0;
                        break;
                    case ClassDescriptor.FieldType.tpByte:
                        val = body[offs++];
                        break;
                    case ClassDescriptor.FieldType.tpSByte:
                        val = (sbyte)body[offs++];
                        break;
                    case ClassDescriptor.FieldType.tpChar:
                        val = (char)Bytes.unpack2(body, offs);
                        offs += 2;
                        break;
                    case ClassDescriptor.FieldType.tpShort:
                        val = Bytes.unpack2(body, offs);
                        offs += 2;
                        break;
                    case ClassDescriptor.FieldType.tpUShort:
                        val = (ushort)Bytes.unpack2(body, offs);
                        offs += 2;
                        break;
                    case ClassDescriptor.FieldType.tpInt:
                    case ClassDescriptor.FieldType.tpOid:
                        val = Bytes.unpack4(body, offs);
                        offs += 4;
                        break;
                    case ClassDescriptor.FieldType.tpUInt:
                        val = (uint)Bytes.unpack4(body, offs);
                        offs += 4;
                        break;
                    case ClassDescriptor.FieldType.tpLong:
                        val = Bytes.unpack8(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpULong:
                        val = (ulong)Bytes.unpack8(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpFloat:
                        val = Bytes.unpackF4(body, offs);
                        offs += 4;
                        break;
                    case ClassDescriptor.FieldType.tpDouble:
                        val = Bytes.unpackF8(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpDate:
                        val = Bytes.unpackDate(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpGuid:
                        val = Bytes.unpackGuid(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpDecimal:
                        val = Bytes.unpackDecimal(body, offs);
                        offs += 8;
                        break;
                    case ClassDescriptor.FieldType.tpObject:
                        val = unswizzle(Bytes.unpack4(body, offs),
                            typeof(Persistent),
                            recursiveLoading);
                        offs += 4;
                        break;
                    default:
                        val = null;
                        break;
                }
            }
            return offs;
        }

        public int unpackField(byte[] body, int offs, bool recursiveLoading, ref object val, ClassDescriptor.FieldDescriptor fd, ClassDescriptor.FieldType type, IPersistent po)
        {
            int len;
            switch (type)
            {
                case ClassDescriptor.FieldType.tpBoolean:
                    val = body[offs++] != 0;
                    break;

                case ClassDescriptor.FieldType.tpByte:
                    val = body[offs++];
                    break;

                case ClassDescriptor.FieldType.tpSByte:
                    val = (sbyte)body[offs++];
                    break;

                case ClassDescriptor.FieldType.tpChar:
                    val = (char)Bytes.unpack2(body, offs);
                    offs += 2;
                    break;

                case ClassDescriptor.FieldType.tpShort:
                    val = Bytes.unpack2(body, offs);
                    offs += 2;
                    break;

                case ClassDescriptor.FieldType.tpUShort:
                    val = (ushort)Bytes.unpack2(body, offs);
                    offs += 2;
                    break;

                case ClassDescriptor.FieldType.tpEnum:
                    val = Enum.ToObject(fd.field.FieldType, Bytes.unpack4(body, offs));
                    offs += 4;
                    break;

                case ClassDescriptor.FieldType.tpInt:
                case ClassDescriptor.FieldType.tpOid:
                    val = Bytes.unpack4(body, offs);
                    offs += 4;
                    break;

                case ClassDescriptor.FieldType.tpUInt:
                    val = (uint)Bytes.unpack4(body, offs);
                    offs += 4;
                    break;

                case ClassDescriptor.FieldType.tpLong:
                    val = Bytes.unpack8(body, offs);
                    offs += 8;
                    break;

                case ClassDescriptor.FieldType.tpULong:
                    val = (ulong)Bytes.unpack8(body, offs);
                    offs += 8;
                    break;

                case ClassDescriptor.FieldType.tpFloat:
                    val = Bytes.unpackF4(body, offs);
                    offs += 4;
                    break;

                case ClassDescriptor.FieldType.tpDouble:
                    val = Bytes.unpackF8(body, offs);
                    offs += 8;
                    break;

                case ClassDescriptor.FieldType.tpDecimal:
                    val = Bytes.unpackDecimal(body, offs);
                    offs += 16;
                    break;

                case ClassDescriptor.FieldType.tpGuid:
                    val = Bytes.unpackGuid(body, offs);
                    offs += 16;
                    break;

                case ClassDescriptor.FieldType.tpString:
                    {
                        string str;
                        offs = Bytes.unpackString(body, offs, out str);
                        val = str;
                        break;
                    }

                case ClassDescriptor.FieldType.tpDate:
                    val = Bytes.unpackDate(body, offs);
                    offs += 8;
                    break;

                case ClassDescriptor.FieldType.tpObject:
                    if (fd == null)
                    {
                        val = unswizzle(Bytes.unpack4(body, offs), typeof(Persistent), recursiveLoading);
                    }
                    else
                    {
                        val = unswizzle(Bytes.unpack4(body, offs), fd.field.FieldType,
                                        fd.recursiveLoading | recursiveLoading);
                    }
                    offs += 4;
                    break;

                case ClassDescriptor.FieldType.tpValue:
                    val = fd.field.GetValue(val);
                    offs = unpackObject(val, fd.valueDesc, recursiveLoading, body, offs, po);
                    break;

                case ClassDescriptor.FieldType.tpRaw:
                    offs = unpackRawValue(body, offs, out val, recursiveLoading);
                    break;

                case ClassDescriptor.FieldType.tpArrayOfByte:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        byte[] arr = new byte[len];
                        Array.Copy(body, offs, arr, 0, len);
                        offs += len;
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfSByte:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        sbyte[] arr = new sbyte[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = (sbyte)body[offs++];
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfBoolean:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        bool[] arr = new bool[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = body[offs++] != 0;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfShort:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        short[] arr = new short[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfUShort:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        ushort[] arr = new ushort[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = (ushort)Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfChar:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        char[] arr = new char[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = (char)Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfEnum:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        System.Type elemType = fd.field.FieldType.GetElementType();
                        Array arr = Array.CreateInstance(elemType, len);
                        for (int j = 0; j < len; j++)
                        {
                            arr.SetValue(Enum.ToObject(elemType, Bytes.unpack4(body, offs)), j);
                            offs += 4;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfInt:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        int[] arr = new int[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpack4(body, offs);
                            offs += 4;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfUInt:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        uint[] arr = new uint[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = (uint)Bytes.unpack4(body, offs);
                            offs += 4;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfLong:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        long[] arr = new long[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpack8(body, offs);
                            offs += 8;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfULong:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        ulong[] arr = new ulong[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = (ulong)Bytes.unpack8(body, offs);
                            offs += 8;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfFloat:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        float[] arr = new float[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpackF4(body, offs);
                            offs += 4;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDouble:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        double[] arr = new double[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpackF8(body, offs);
                            offs += 8;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDate:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        System.DateTime[] arr = new System.DateTime[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpackDate(body, offs);
                            offs += 8;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfString:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        string[] arr = new string[len];
                        for (int j = 0; j < len; j++)
                        {
                            offs = Bytes.unpackString(body, offs, out arr[j]);
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDecimal:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        decimal[] arr = new decimal[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpackDecimal(body, offs);
                            offs += 16;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfGuid:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        Guid[] arr = new Guid[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpackGuid(body, offs);
                            offs += 16;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfObject:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        Type elemType = fd.field.FieldType.GetElementType();
                        IPersistent[] arr = (IPersistent[])Array.CreateInstance(elemType, len);
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = unswizzle(Bytes.unpack4(body, offs), elemType, recursiveLoading);
                            offs += 4;
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfValue:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        Type elemType = fd.field.FieldType.GetElementType();
                        Array arr = Array.CreateInstance(elemType, len);
                        ClassDescriptor valueDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++)
                        {
                            object elem = arr.GetValue(j);
                            offs = unpackObject(elem, valueDesc, recursiveLoading, body, offs, po);
                            arr.SetValue(elem, j);
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfRaw:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        Type elemType = fd.field.FieldType.GetElementType();
                        Array arr = Array.CreateInstance(elemType, len);
                        for (int j = 0; j < len; j++)
                        {
                            object elem;
                            offs = unpackRawValue(body, offs, out elem, recursiveLoading);
                            arr.SetValue(elem, j);
                        }
                        val = arr;
                    }
                    break;

                case ClassDescriptor.FieldType.tpLink:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        IPersistent[] arr = new IPersistent[len];
                        for (int j = 0; j < len; j++)
                        {
                            int elemOid = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (elemOid != 0)
                            {
                                arr[j] = new PersistentStub(this, elemOid);
                            }
                        }
                        val = fd.constructor.Invoke(this, new object[] { arr, po });
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfOid:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0)
                    {
                        val = null;
                    }
                    else
                    {
                        int[] arr = new int[len];
                        for (int j = 0; j < len; j++)
                        {
                            arr[j] = Bytes.unpack4(body, offs);
                            offs += 4;
                        }
                        val = fd.constructor.Invoke(this, new object[] { arr, po });
                    }
                    break;
            }
            return offs;
        }

        internal byte[] packObject(IPersistent obj)
        {
            ByteBuffer buf = new ByteBuffer();
            int offs = ObjectHeader.Sizeof;
            buf.extend(offs);
            ClassDescriptor desc = getClassDescriptor(obj.GetType());
            if (desc.serializer != null)
            {
                offs = desc.serializer.pack(this, obj, buf);
            }
            else
            {
                offs = packObject(obj, desc, offs, buf, obj);
            }
            ObjectHeader.setSize(buf.arr, 0, offs);
            ObjectHeader.setType(buf.arr, 0, desc.Oid);
            return buf.arr;
        }

        public int packObject(object obj, ClassDescriptor desc, int offs, ByteBuffer buf, IPersistent po)
        {
            ClassDescriptor.FieldDescriptor[] flds = desc.allFields;

            for (int i = 0, n = flds.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = flds[i];
                offs = packField(buf, offs, fd.field.GetValue(obj), fd, fd.type, po);
            }
            return offs;
        }

        public int packRawValue(ByteBuffer buf, int offs, object val)
        {
            if (val == null)
            {
                buf.extend(offs + 4);
                Bytes.pack4(buf.arr, offs, -1);
                offs += 4;
            }
            else if (val is IPersistent)
            {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpObject);
                Bytes.pack4(buf.arr, offs + 4, swizzle((IPersistent)val));
                offs += 8;
            }
            else
            {
                Type t = val.GetType();
                if (t == typeof(bool))
                {
                    buf.extend(offs + 5);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpBoolean);
                    buf.arr[offs + 4] = (byte)((bool)val ? 1 : 0);
                    offs += 5;
                }
                else if (t == typeof(char))
                {
                    buf.extend(offs + 6);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpChar);
                    Bytes.pack2(buf.arr, offs + 4, (short)(char)val);
                    offs += 6;
                }
                else if (t == typeof(byte))
                {
                    buf.extend(offs + 5);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpByte);
                    buf.arr[offs + 4] = (byte)val;
                    offs += 5;
                }
                else if (t == typeof(sbyte))
                {
                    buf.extend(offs + 5);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpSByte);
                    buf.arr[offs + 4] = (byte)(sbyte)val;
                    offs += 5;
                }
                else if (t == typeof(short))
                {
                    buf.extend(offs + 6);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpShort);
                    Bytes.pack2(buf.arr, offs + 4, (short)val);
                    offs += 6;
                }
                else if (t == typeof(ushort))
                {
                    buf.extend(offs + 6);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpUShort);
                    Bytes.pack2(buf.arr, offs + 4, (short)(ushort)val);
                    offs += 6;
                }
                else if (t == typeof(int))
                {
                    buf.extend(offs + 8);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpInt);
                    Bytes.pack4(buf.arr, offs + 4, (int)val);
                    offs += 8;
                }
                else if (t == typeof(uint))
                {
                    buf.extend(offs + 8);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpUInt);
                    Bytes.pack4(buf.arr, offs + 4, (int)(uint)val);
                    offs += 8;
                }
                else if (t == typeof(long))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpLong);
                    Bytes.pack8(buf.arr, offs + 4, (long)val);
                    offs += 12;
                }
                else if (t == typeof(ulong))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpULong);
                    Bytes.pack8(buf.arr, offs + 4, (long)(ulong)val);
                    offs += 12;
                }
                else if (t == typeof(float))
                {
                    buf.extend(offs + 8);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpFloat);
                    Bytes.packF4(buf.arr, offs + 4, (float)val);
                    offs += 8;
                }
                else if (t == typeof(double))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpDouble);
                    Bytes.packF8(buf.arr, offs + 4, (double)val);
                    offs += 12;
                }
                else if (t == typeof(DateTime))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpDate);
                    Bytes.packDate(buf.arr, offs + 4, (DateTime)val);
                    offs += 12;
                }
                else if (t == typeof(Guid))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpGuid);
                    Bytes.packGuid(buf.arr, offs + 4, (Guid)val);
                    offs += 12;
                }
                else if (t == typeof(Decimal))
                {
                    buf.extend(offs + 12);
                    Bytes.pack4(buf.arr, offs, -2 - (int)ClassDescriptor.FieldType.tpDecimal);
                    Bytes.packDecimal(buf.arr, offs + 4, (decimal)val);
                    offs += 12;
                }
                else
                {
                    System.IO.MemoryStream ms = new System.IO.MemoryStream();
                    objectFormatter.Serialize(ms, val);
                    ms.Close();
                    byte[] arr = ms.ToArray();
                    int len = arr.Length;
                    buf.extend(offs + 4 + len);
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    Array.Copy(arr, 0, buf.arr, offs, len);
                    offs += len;
                }
            }
            return offs;
        }

        public int packField(ByteBuffer buf, int offs, object val, ClassDescriptor.FieldDescriptor fd, ClassDescriptor.FieldType type, IPersistent po)
        {
            switch (type)
            {
                case ClassDescriptor.FieldType.tpByte:
                    return buf.packI1(offs, (byte)val);
                case ClassDescriptor.FieldType.tpSByte:
                    return buf.packI1(offs, (sbyte)val);
                case ClassDescriptor.FieldType.tpBoolean:
                    return buf.packBool(offs, (bool)val);
                case ClassDescriptor.FieldType.tpShort:
                    return buf.packI2(offs, (short)val);
                case ClassDescriptor.FieldType.tpUShort:
                    return buf.packI2(offs, (ushort)val);
                case ClassDescriptor.FieldType.tpChar:
                    return buf.packI2(offs, (char)val);
                case ClassDescriptor.FieldType.tpEnum:
                case ClassDescriptor.FieldType.tpInt:
                case ClassDescriptor.FieldType.tpOid:
                    return buf.packI4(offs, (int)val);
                case ClassDescriptor.FieldType.tpUInt:
                    return buf.packI4(offs, (int)(uint)val);
                case ClassDescriptor.FieldType.tpLong:
                    return buf.packI8(offs, (long)val);
                case ClassDescriptor.FieldType.tpULong:
                    return buf.packI8(offs, (long)(ulong)val);
                case ClassDescriptor.FieldType.tpFloat:
                    return buf.packF4(offs, (float)val);
                case ClassDescriptor.FieldType.tpDouble:
                    return buf.packF8(offs, (double)val);
                case ClassDescriptor.FieldType.tpDecimal:
                    return buf.packDecimal(offs, (decimal)val);
                case ClassDescriptor.FieldType.tpGuid:
                    return buf.packGuid(offs, (Guid)val);
                case ClassDescriptor.FieldType.tpDate:
                    return buf.packDate(offs, (DateTime)val);
                case ClassDescriptor.FieldType.tpString:
                    return buf.packString(offs, (string)val);
                case ClassDescriptor.FieldType.tpValue:
                    return packObject(val, fd.valueDesc, offs, buf, po);
                case ClassDescriptor.FieldType.tpObject:
                    return buf.packI4(offs, swizzle((IPersistent)val));
                case ClassDescriptor.FieldType.tpRaw:
                    offs = packRawValue(buf, offs, val);
                    break;
                case ClassDescriptor.FieldType.tpArrayOfByte:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        byte[] arr = (byte[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        Array.Copy(arr, 0, buf.arr, offs, len);
                        offs += len;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfSByte:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        sbyte[] arr = (sbyte[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++, offs++)
                        {
                            buf.arr[offs] = (byte)arr[j];
                        }
                        offs += len;
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfBoolean:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        bool[] arr = (bool[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++, offs++)
                        {
                            buf.arr[offs] = (byte)(arr[j] ? 1 : 0);
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfShort:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        short[] arr = (short[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack2(buf.arr, offs, arr[j]);
                            offs += 2;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfUShort:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        ushort[] arr = (ushort[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack2(buf.arr, offs, (short)arr[j]);
                            offs += 2;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfChar:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        char[] arr = (char[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack2(buf.arr, offs, (short)arr[j]);
                            offs += 2;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfEnum:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        Array arr = (Array)val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, (int)arr.GetValue(j));
                            offs += 4;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfInt:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        int[] arr = (int[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfUInt:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        uint[] arr = (uint[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, (int)arr[j]);
                            offs += 4;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfLong:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        long[] arr = (long[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfULong:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        ulong[] arr = (ulong[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack8(buf.arr, offs, (long)arr[j]);
                            offs += 8;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfFloat:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        float[] arr = (float[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.packF4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDouble:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        double[] arr = (double[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.packF8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfValue:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        Array arr = (Array)val;
                        int len = arr.Length;
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        ClassDescriptor elemDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++)
                        {
                            offs = packObject(arr.GetValue(j), elemDesc, offs, buf, po);
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDate:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        DateTime[] arr = (DateTime[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.packDate(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfDecimal:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        decimal[] arr = (decimal[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 16);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.packDecimal(buf.arr, offs, arr[j]);
                            offs += 16;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfGuid:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        Guid[] arr = (Guid[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 16);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.packGuid(buf.arr, offs, arr[j]);
                            offs += 16;
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfString:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        string[] arr = (string[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            offs = buf.packString(offs, arr[j]);
                        }
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfObject:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        IPersistent[] arr = (IPersistent[])val;
                        int len = arr.Length;
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, swizzle(arr[j]));
                            offs += 4;
                        }
                    }
                    break;
                case ClassDescriptor.FieldType.tpArrayOfRaw:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        Array arr = (Array)val;
                        int len = arr.Length;
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            offs = packRawValue(buf, offs, arr.GetValue(j));
                        }
                    }
                    break;
                case ClassDescriptor.FieldType.tpLink:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        IGenericLink link = (IGenericLink)val;
                        link.SetOwner(po);
                        int len = link.Size();
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, swizzle(link.GetRaw(j)));
                            offs += 4;
                        }
                        link.Unpin();
                    }
                    break;

                case ClassDescriptor.FieldType.tpArrayOfOid:
                    if (val == null)
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    }
                    else
                    {
                        IGenericPArray arr = (IGenericPArray)val;
                        arr.SetOwner(po);
                        int len = arr.Size();
                        buf.extend(offs + 4 + len * 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++)
                        {
                            Bytes.pack4(buf.arr, offs, arr.GetOid(j));
                            offs += 4;
                        }
                    }
                    break;
            }
            return offs;
        }

        public IClassLoader Loader
        {
            set
            {
                loader = value;
            }

            get
            {
                return loader;
            }
        }

        private int initIndexSize = dbDefaultInitIndexSize;
        private int objectCacheInitSize = dbDefaultObjectCacheInitSize;
        private long extensionQuantum = dbDefaultExtensionQuantum;
        private CacheType cacheKind = CacheType.Lru;
#if WITH_OLD_BTREE
        private bool alternativeBtree = false;
#endif
        private bool backgroundGc = false;

#if WITH_REPLICATION
        internal bool replicationAck = false;
#endif

        internal PagePool pool;
        internal Header header; // base address of database file mapping
        internal int[] dirtyPagesMap; // bitmap of changed pages in current index
        internal bool modified;

        internal int currRBitmapPage; //current bitmap page for allocating records
        internal int currRBitmapOffs; //offset in current bitmap page for allocating 
        //unaligned records
        internal int currPBitmapPage; //current bitmap page for allocating page objects
        internal int currPBitmapOffs; //offset in current bitmap page for allocating 
        //page objects
        internal Location reservedChain;

        internal int committedIndexSize;
        internal int currIndexSize;

        internal bool enableCodeGeneration = true;

#if CF
        internal static ArrayList assemblies;
        CNetMonitor transactionMonitor;
#else
        internal Thread codeGenerationThread;
        object transactionMonitor;
        Dictionary<Type, Type> wrapperHash = new Dictionary<Type, Type>();
#endif
        int nNestedTransactions;
        int nBlockedTransactions;
        int nCommittedTransactions;
        long scheduledCommitTime;
        PersistentResource transactionLock;

        internal System.Runtime.Serialization.Formatters.Binary.BinaryFormatter objectFormatter;
        internal int currIndex; // copy of header.root, used to allow read access to the database 
        // during transaction commit
        internal long usedSize; // total size of allocated objects since the beginning of the session
        internal int[] bitmapPageAvailableSpace;
        internal bool opened;

        internal int[] greyBitmap; // bitmap of objects visited but not yet marked during GC
        internal int[] blackBitmap;    // bitmap of objects marked during GC 
        internal long gcThreshold;
        internal long allocatedDelta;
        internal bool gcDone;
        internal bool gcActive;
        internal bool gcGo;
        internal object backgroundGcMonitor;
        internal object backgroundGcStartMonitor;
        internal Thread gcThread;

        private IClassLoader loader;

        internal Dictionary<string, Type> resolvedTypes;

        internal OidHashTable objectCache;
        internal Dictionary<Type, ClassDescriptor> classDescMap;
        internal ClassDescriptor descList;

        internal static readonly LocalDataStoreSlot transactionContext = Thread.AllocateDataSlot();
        internal bool useSerializableTransactions;
    }

    class RootPage
    {
        internal long size; // database file size
        internal long index; // offset to object index
        internal long shadowIndex; // offset to shadow index
        internal long usedSize; // size used by objects
        internal int indexSize; // size of object index
        internal int shadowIndexSize; // size of object index
        internal int indexUsed; // used part of the index   
        internal int freeList; // L1 list of free descriptors
        internal int bitmapEnd; // index of last allocated bitmap page
        internal int rootObject; // oid of root object
        internal int classDescList; // List of class descriptors
        internal int bitmapExtent;     // Allocation bitmap offset and size

        internal const int Sizeof = 64;
    }

    class Header
    {
        internal int curr; // current root
        internal bool dirty; // database was not closed normally
        internal bool initialized; // database is initilaized

        internal RootPage[] root;

        internal static int Sizeof = 3 + RootPage.Sizeof * 2;

        internal void pack(byte[] rec)
        {
            int offs = 0;
            rec[offs++] = (byte)curr;
            rec[offs++] = (byte)(dirty ? 1 : 0);
            rec[offs++] = (byte)(initialized ? 1 : 0);
            for (int i = 0; i < 2; i++)
            {
                Bytes.pack8(rec, offs, root[i].size);
                offs += 8;
                Bytes.pack8(rec, offs, root[i].index);
                offs += 8;
                Bytes.pack8(rec, offs, root[i].shadowIndex);
                offs += 8;
                Bytes.pack8(rec, offs, root[i].usedSize);
                offs += 8;
                Bytes.pack4(rec, offs, root[i].indexSize);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].shadowIndexSize);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].indexUsed);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].freeList);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].bitmapEnd);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].rootObject);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].classDescList);
                offs += 4;
                Bytes.pack4(rec, offs, root[i].bitmapExtent);
                offs += 4;
            }
        }

        internal void unpack(byte[] rec)
        {
            int offs = 0;
            curr = rec[offs++];
            dirty = rec[offs++] != 0;
            initialized = rec[offs++] != 0;
            root = new RootPage[2];
            for (int i = 0; i < 2; i++)
            {
                root[i] = new RootPage();
                root[i].size = Bytes.unpack8(rec, offs);
                offs += 8;
                root[i].index = Bytes.unpack8(rec, offs);
                offs += 8;
                root[i].shadowIndex = Bytes.unpack8(rec, offs);
                offs += 8;
                root[i].usedSize = Bytes.unpack8(rec, offs);
                offs += 8;
                root[i].indexSize = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].shadowIndexSize = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].indexUsed = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].freeList = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].bitmapEnd = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].rootObject = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].classDescList = Bytes.unpack4(rec, offs);
                offs += 4;
                root[i].bitmapExtent = Bytes.unpack4(rec, offs);
                offs += 4;
            }
        }
    }
}
