namespace Perst.Impl    
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Threading;
    using Perst;
	
    public class StorageImpl:Storage
    {
#if COMPACT_NET_FRAMEWORK
        static StorageImpl() 
        {
            assemblies = new System.Collections.ArrayList();
        }
        public StorageImpl(Assembly callingAssembly) 
        {
            assemblies.Add(callingAssembly);
            assemblies.Add(Assembly.GetExecutingAssembly());
        }
#endif

        public override IPersistent Root
        {
            get
            {
                lock(this)
                {
                    if (!opened)
                    {
                        throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                    }
                    int rootOid = header.root[1 - currIndex].rootObject;
                    return (rootOid == 0) ? null : lookupObject(rootOid, null);
                }
            }
			
            set
            {
                lock(this)
                {
                    if (!opened)
                    {
                        throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                    }
                    if (!value.IsPersistent())
                    {
                        storeObject(value);
                    }
                    header.root[1 - currIndex].rootObject = value.Oid;
                    modified = true;
                }
            }
			
        }
 
        /// <summary> Initialial database index size - increasing it reduce number of inde reallocation but increase
        /// initial database size. Should be set before openning connection.
        /// </summary>
        internal const int dbDefaultInitIndexSize = 1024;
		
        /// <summary> Initial capacity of object hash
        /// </summary>
        internal const int dbDefaultObjectCacheInitSize = 1319;
		
        /// <summary> Database extension quantum. Memory is allocate by scanning bitmap. If there is no
        /// large enough hole, then database is extended by the value of dbDefaultExtensionQuantum 
        /// This parameter should not be smaller than dbFirstUserId
        /// </summary>
        internal static long dbDefaultExtensionQuantum = 1024 * 1024;
		
        internal const int dbDatabaseOffsetBits = 32; // up to 1 gigabyte, 37 - up to 1 terabyte database
		
        internal const int dbAllocationQuantumBits = 5;
        internal const int dbAllocationQuantum = 1 << dbAllocationQuantumBits;
        internal const int dbBitmapSegmentBits = Page.pageBits + 3 + dbAllocationQuantumBits;
        internal const int dbBitmapSegmentSize = 1 << dbBitmapSegmentBits;
        internal const int dbBitmapPages = 1 << (dbDatabaseOffsetBits - dbBitmapSegmentBits);
        internal const int dbHandlesPerPageBits = Page.pageBits - 3;
        internal const int dbHandlesPerPage = 1 << dbHandlesPerPageBits;
        internal const int dbDirtyPageBitmapSize = 1 << (32 - Page.pageBits - 3);
		
        internal const int dbInvalidId = 0;
        internal const int dbBitmapId = 1;
        internal const int dbFirstUserId = dbBitmapId + dbBitmapPages;
		
        internal const int dbPageObjectFlag = 1;
        internal const int dbModifiedFlag = 2;
        internal const int dbFreeHandleFlag = 4;
        internal const int dbFlagsMask = 7;
        internal const int dbFlagsBits = 3;
		
		
        internal long getPos(int oid)
        {
            lock (objectCache) 
            {
                if (oid == 0 && oid >= currIndexSize)
                {
                    throw new StorageError(StorageError.ErrorCode.INVALID_OID);
                }
                Page pg = pool.getPage(header.root[1 - currIndex].index + (oid >> dbHandlesPerPageBits << Page.pageBits));
                long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage - 1)) << 3);
                pool.unfix(pg);
                return pos;
            }
        }
		
        internal void  setPos(int oid, long pos)
        {
            lock (objectCache) 
            {
                dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                Page pg = pool.putPage(header.root[1 - currIndex].index + (oid >> dbHandlesPerPageBits << Page.pageBits));
                Bytes.pack8(pg.data, (oid & (dbHandlesPerPage - 1)) << 3, pos);
                pool.unfix(pg);
            }
        }
		
        internal byte[] get(int oid)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
            {
                throw new StorageError(StorageError.ErrorCode.INVALID_OID);
            }
            return pool.get(pos & ~ dbFlagsMask);
        }
		
        internal Page getPage(int oid)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != dbPageObjectFlag)
            {
                throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
            }
            return pool.getPage(pos & ~ dbFlagsMask);
        }
		
        internal Page putPage(int oid)
        {
            lock (objectCache) 
            {
                long pos = getPos(oid);
                if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != dbPageObjectFlag)
                {
                    throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
                }
                if ((pos & dbModifiedFlag) == 0)
                {
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    allocate(Page.pageSize, oid);
                    cloneBitmap(pos & ~ dbFlagsMask, Page.pageSize);
                    pos = getPos(oid);
                }
                modified = true;
                return pool.putPage(pos & ~ dbFlagsMask);
            }
        }
		
		
        internal int allocatePage()
        {
            int oid = allocateId();
            setPos(oid, allocate(Page.pageSize, 0) | dbPageObjectFlag | dbModifiedFlag);
            return oid;
        }
		
        protected internal override void  deallocateObject(IPersistent obj)
        {
            lock(this)
            {
                lock (objectCache) 
                { 
                    int oid = obj.Oid;
                    if (oid == 0) 
                    { 
                        return;
                    }       
                    long pos = getPos(oid);
                    objectCache.remove(oid);
                    int offs = (int) pos & (Page.pageSize - 1);
                    if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                    {
                        throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
                    }
                    Page pg = pool.getPage(pos - offs);
                    offs &= ~ dbFlagsMask;
                    int size = ObjectHeader.getSize(pg.data, offs);
                    pool.unfix(pg);
                    freeId(oid);
                    if ((pos & dbModifiedFlag) != 0) 
                    {
                        free(pos & ~ dbFlagsMask, size);
                    }
                    else
                    {
                        cloneBitmap(pos, size);
                    }
                    setObjectOid(obj, 0, false);
                }
            }
        }
    		
        internal void  freePage(int oid)
        {
            long pos = getPos(oid);
            Assert.That((pos & (dbFreeHandleFlag | dbPageObjectFlag)) == dbPageObjectFlag);
            if ((pos & dbModifiedFlag) != 0)
            {
                free(pos & ~ dbFlagsMask, Page.pageSize);
            }
            else
            {
                cloneBitmap(pos & ~ dbFlagsMask, Page.pageSize);
            }
            freeId(oid);
        }
		
 	
        internal void setDirty() 
        {
            modified = true;
            if (!header.dirty) 
            { 
                header.dirty = true;
                Page pg = pool.putPage(0);
                header.pack(pg.data);
                pool.flush();
                pool.unfix(pg);
            }
        }

        internal int allocateId()
        {
            lock (objectCache) 
            {
                int oid;
                int curr = 1 - currIndex;
                setDirty();
                if ((oid = header.root[curr].freeList) != 0)
                {
                    header.root[curr].freeList = (int) (getPos(oid) >> dbFlagsBits);
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] 
                        |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    return oid;
                }
			
                if (currIndexSize + 1 > header.root[curr].indexSize)
                {
                    int oldIndexSize = header.root[curr].indexSize;
                    int newIndexSize = oldIndexSize * 2;
                    while (newIndexSize < oldIndexSize + 1)
                    {
                        newIndexSize = newIndexSize * 2;
                    }
                    long newIndex = allocate(newIndexSize * 8, 0);
                    pool.copy(newIndex, header.root[curr].index, currIndexSize * 8);
                    free(header.root[curr].index, oldIndexSize * 8);
                    header.root[curr].index = newIndex;
                    header.root[curr].indexSize = newIndexSize;
                }
                oid = currIndexSize;
                header.root[curr].indexUsed = ++currIndexSize;
                modified = true;
                return oid;
            }
        }
		
        internal void  freeId(int oid)
        {
            lock (objectCache) 
            {
                setPos(oid, ((long) (header.root[1 - currIndex].freeList) << dbFlagsBits) | dbFreeHandleFlag);
                header.root[1 - currIndex].freeList = oid;
            }
        }
		
        internal static byte[] firstHoleSize = new byte[]{8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};
        internal static byte[] lastHoleSize = new byte[]{8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        internal static byte[] maxHoleSize = new byte[]{8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 7, 6, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 0};
        internal static byte[] maxHoleOffset = new byte[]{0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 5, 5, 5, 5, 5, 5, 0, 5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 0, 6, 6, 6, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 2, 2, 3, 3, 3, 3, 0, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 2, 0, 5, 5, 5, 0, 1, 5, 5, 0, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 0};
		
        internal const int pageBits = Page.pageSize * 8;
        internal const int inc = Page.pageSize / dbAllocationQuantum / 8;
		
        internal static void  memset(Page pg, int offs, int pattern, int len)
        {
            byte[] arr = pg.data;
            byte pat = (byte) pattern;
            while (--len >= 0)
            {
                arr[offs++] = pat;
            }
        }
		
        internal void  extend(long size)
        {
            if (size > header.root[1 - currIndex].size)
            {
                header.root[1 - currIndex].size = size;
            }
        }
		
        internal class Location
        {
            internal long pos;
            internal int size;
            internal Location next;
        }
		
        internal bool wasReserved(long pos, int size)
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
		
        internal void  reserveLocation(long pos, int size)
        {
            Location location = new Location();
            location.pos = pos;
            location.size = size;
            location.next = reservedChain;
            reservedChain = location;
        }
		
        internal void  commitLocation()
        {
            reservedChain = reservedChain.next;
        }
		
		
        internal long allocate(int size, int oid)
        {
            lock (objectCache) 
            {
                setDirty();
                size = (size + dbAllocationQuantum - 1) & ~ (dbAllocationQuantum - 1);
                Assert.That(size != 0);
                allocatedDelta += size;
                if (allocatedDelta > gcThreshold) 
                {
                    Gc();
                }
                int objBitSize = size >> dbAllocationQuantumBits;
                long pos;
                int holeBitSize = 0;
                int alignment = size & (Page.pageSize - 1);
                int offs, firstPage, lastPage, i;
                int holeBeforeFreePage = 0;
                int freeBitmapPage = 0;
                Page pg;
			
                lastPage = header.root[1 - currIndex].bitmapEnd;
                usedSize += size;
			
                if (alignment == 0)
                {
                    firstPage = currPBitmapPage;
                    offs = (currPBitmapOffs + inc - 1) & ~ (inc - 1);
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
                            int spaceNeeded = objBitSize - holeBitSize < pageBits?objBitSize - holeBitSize:pageBits;
                            if (bitmapPageAvailableSpace[i] <= spaceNeeded)
                            {
                                holeBitSize = 0;
                                offs = 0;
                                continue;
                            }
                            pg = getPage(i);
                            int startOffs = offs;
                            while (offs < Page.pageSize)
                            {
                                if (pg.data[offs++] != 0)
                                {
                                    offs = (offs + inc - 1) & ~ (inc - 1);
                                    holeBitSize = 0;
                                }
                                else if ((holeBitSize += 8) == objBitSize)
                                {
                                    pos = (((long) (i - dbBitmapId) * Page.pageSize + offs) * 8 - holeBitSize) << dbAllocationQuantumBits;
                                    if (wasReserved(pos, size))
                                    {
                                        offs += objBitSize >> 3;
                                        startOffs = offs = (offs + inc - 1) & ~ (inc - 1);
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
                                        uint marker = (uint) prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putPage(i);
                                    int holeBytes = holeBitSize >> 3;
                                    if (holeBytes > offs)
                                    {
                                        memset(pg, 0, 0xFF, offs);
                                        holeBytes -= offs;
                                        pool.unfix(pg);
                                        pg = putPage(--i);
                                        offs = Page.pageSize;
                                    }
                                    while (holeBytes > Page.pageSize)
                                    {
                                        memset(pg, 0, 0xFF, Page.pageSize);
                                        holeBytes -= Page.pageSize;
                                        bitmapPageAvailableSpace[i] = 0;
                                        pool.unfix(pg);
                                        pg = putPage(--i);
                                    }
                                    memset(pg, offs - holeBytes, 0xFF, holeBytes);
                                    commitLocation();
                                    pool.unfix(pg);
                                    return pos;
                                }
                            }
                            if (startOffs == 0 && holeBitSize == 0 && spaceNeeded < bitmapPageAvailableSpace[i])
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
                            int spaceNeeded = objBitSize - holeBitSize < pageBits?objBitSize - holeBitSize:pageBits;
                            if (bitmapPageAvailableSpace[i] <= spaceNeeded)
                            {
                                holeBitSize = 0;
                                offs = 0;
                                continue;
                            }
                            pg = getPage(i);
                            int startOffs = offs;
                            while (offs < Page.pageSize)
                            {
                                int mask = pg.data[offs] & 0xFF;
                                if (holeBitSize + firstHoleSize[mask] >= objBitSize)
                                {
                                    pos = (((long) (i - dbBitmapId) * Page.pageSize + offs) * 8 - holeBitSize) << dbAllocationQuantumBits;
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
                                        uint marker = (uint) prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putPage(i);
                                    pg.data[offs] |= (byte) ((1 << (objBitSize - holeBitSize)) - 1);
                                    if (holeBitSize != 0)
                                    {
                                        if (holeBitSize > offs * 8)
                                        {
                                            memset(pg, 0, 0xFF, offs);
                                            holeBitSize -= offs * 8;
                                            pool.unfix(pg);
                                            pg = putPage(--i);
                                            offs = Page.pageSize;
                                        }
                                        while (holeBitSize > pageBits)
                                        {
                                            memset(pg, 0, 0xFF, Page.pageSize);
                                            holeBitSize -= pageBits;
                                            bitmapPageAvailableSpace[i] = 0;
                                            pool.unfix(pg);
                                            pg = putPage(--i);
                                        }
                                        while ((holeBitSize -= 8) > 0)
                                        {
                                            pg.data[--offs] = (byte) 0xFF;
                                        }
                                        pg.data[offs - 1] |= (byte) (~ ((1 << - holeBitSize) - 1));
                                    }
                                    pool.unfix(pg);
                                    commitLocation();
                                    return pos;
                                }
                                else if (maxHoleSize[mask] >= objBitSize)
                                {
                                    int holeBitOffset = maxHoleOffset[mask];
                                    pos = (((long) (i - dbBitmapId) * Page.pageSize + offs) * 8 + holeBitOffset) << dbAllocationQuantumBits;
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
                                        uint marker = (uint) prev & dbFlagsMask;
                                        pool.copy(pos, prev - marker, size);
                                        setPos(oid, pos | marker | dbModifiedFlag);
                                    }
                                    pool.unfix(pg);
                                    pg = putPage(i);
                                    pg.data[offs] |= (byte) (((1 << objBitSize) - 1) << holeBitOffset);
                                    pool.unfix(pg);
                                    commitLocation();
                                    return pos;
                                }
                                offs += 1;
                                if (lastHoleSize[mask] == 8)
                                {
                                    holeBitSize += 8;
                                }
                                else
                                {
                                    holeBitSize = lastHoleSize[mask];
                                }
                            }
                            if (startOffs == 0 && holeBitSize == 0 && spaceNeeded < bitmapPageAvailableSpace[i])
                            {
                                bitmapPageAvailableSpace[i] = spaceNeeded;
                            }
                            offs = 0;
                            pool.unfix(pg);
                        }
                    }
                    if (firstPage == dbBitmapId)
                    {
                        if (freeBitmapPage > i)
                        {
                            i = freeBitmapPage;
                            holeBitSize = holeBeforeFreePage;
                        }
                        if (i == dbBitmapId + dbBitmapPages)
                        {
                            throw new StorageError(StorageError.ErrorCode.NOT_ENOUGH_SPACE);
                        }
                        long extension = (size > extensionQuantum) ? size : extensionQuantum;
                        int morePages = (int) ((extension + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1) / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
					
                        if (i + morePages > dbBitmapId + dbBitmapPages)
                        {
                            morePages = (int) ((size + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1) / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
                            if (i + morePages > dbBitmapId + dbBitmapPages)
                            {
                                throw new StorageError(StorageError.ErrorCode.NOT_ENOUGH_SPACE);
                            }
                        }
                        objBitSize -= holeBitSize;
                        int skip = (objBitSize + Page.pageSize / dbAllocationQuantum - 1) & ~ (Page.pageSize / dbAllocationQuantum - 1);
                        pos = ((long) (i - dbBitmapId) << (Page.pageBits + dbAllocationQuantumBits + 3)) + (skip << dbAllocationQuantumBits);
                        extend(pos + morePages * Page.pageSize);
                        int len = objBitSize >> 3;
                        long adr = pos;
                        while (len >= Page.pageSize)
                        {
                            pg = pool.putPage(adr);
                            memset(pg, 0, 0xFF, Page.pageSize);
                            pool.unfix(pg);
                            adr += Page.pageSize;
                            len -= Page.pageSize;
                        }
                        pg = pool.putPage(adr);
                        memset(pg, 0, 0xFF, len);
                        pg.data[len] = (byte) ((1 << (objBitSize & 7)) - 1);
                        pool.unfix(pg);
                        adr = pos + (skip >> 3);
                        len = morePages * (Page.pageSize / dbAllocationQuantum / 8);
                        while (true)
                        {
                            int off = (int) adr & (Page.pageSize - 1);
                            pg = pool.putPage(adr - off);
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
                        int j = i;
                        while (--morePages >= 0)
                        {
                            setPos(j++, pos | dbPageObjectFlag | dbModifiedFlag);
                            pos += Page.pageSize;
                        }
                        freeBitmapPage = header.root[1 - currIndex].bitmapEnd = j;
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
					
                        pos = ((long) (i - dbBitmapId) * Page.pageSize * 8 - holeBitSize) << dbAllocationQuantumBits;
                        if (oid != 0)
                        {
                            long prev = getPos(oid);
                            uint marker = (uint) prev & dbFlagsMask;
                            pool.copy(pos, prev - marker, size);
                            setPos(oid, pos | marker | dbModifiedFlag);
                        }
					
                        if (holeBitSize != 0)
                        {
                            reserveLocation(pos, size);
                            while (holeBitSize > pageBits)
                            {
                                holeBitSize -= pageBits;
                                pg = putPage(--i);
                                memset(pg, 0, 0xFF, Page.pageSize);
                                bitmapPageAvailableSpace[i] = 0;
                                pool.unfix(pg);
                            }
                            pg = putPage(--i);
                            offs = Page.pageSize;
                            while ((holeBitSize -= 8) > 0)
                            {
                                pg.data[--offs] = (byte) 0xFF;
                            }
                            pg.data[offs - 1] |= (byte) (~ ((1 << - holeBitSize) - 1));
                            pool.unfix(pg);
                            commitLocation();
                        }
                        return pos;
                    }
                    if (gcThreshold != Int64.MaxValue && !gcDone) 
                    {
                        allocatedDelta -= size;
                        usedSize -= size;
                        Gc();
                        currRBitmapPage = currPBitmapPage = dbBitmapId;
                        currRBitmapOffs = currPBitmapOffs = 0;                
                        return allocate(size, oid);
                    }
                    freeBitmapPage = i;
                    holeBeforeFreePage = holeBitSize;
                    holeBitSize = 0;
                    lastPage = firstPage + 1;
                    firstPage = dbBitmapId;
                    offs = 0;
                }
            }
        }
		
		
		
        internal void  free(long pos, int size)
        {
            lock (objectCache) 
            {
                Assert.That(pos != 0 && (pos & (dbAllocationQuantum - 1)) == 0);
                long quantNo = pos >> dbAllocationQuantumBits;
                int objBitSize = (size + dbAllocationQuantum - 1) >> dbAllocationQuantumBits;
                int pageId = dbBitmapId + (int) (quantNo >> (Page.pageBits + 3));
                int offs = (int) (quantNo & (Page.pageSize * 8 - 1)) >> 3;
                Page pg = putPage(pageId);
                int bitOffs = (int) quantNo & 7;
			
                allocatedDelta -= objBitSize << dbAllocationQuantumBits;
                usedSize -= objBitSize << dbAllocationQuantumBits;
			
                if ((pos & (Page.pageSize - 1)) == 0 && size >= Page.pageSize)
                {
                    if (pageId == currPBitmapPage && offs < currPBitmapOffs)
                    {
                        currPBitmapOffs = offs;
                    }
                }
                else
                {
                    if (pageId == currRBitmapPage && offs < currRBitmapOffs)
                    {
                        currRBitmapOffs = offs;
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
                        pg = putPage(++pageId);
                        bitmapPageAvailableSpace[pageId] = System.Int32.MaxValue;
                        objBitSize -= (Page.pageSize - offs) * 8;
                        offs = 0;
                    }
                    while ((objBitSize -= 8) > 0)
                    {
                        pg.data[offs++] = (byte) 0;
                    }
                    pg.data[offs] &= (byte) (~ ((1 << (objBitSize + 8)) - 1));
                }
                else
                {
                    pg.data[offs] &= (byte) (~ (((1 << objBitSize) - 1) << bitOffs));
                }
                pool.unfix(pg);
            }
        }
		
        internal void  cloneBitmap(long pos, int size)
        {
            lock (objectCache) 
            {
                long quantNo = pos >> dbAllocationQuantumBits;
                long objBitSize = (size + dbAllocationQuantum - 1) >> dbAllocationQuantumBits;
                int pageId = dbBitmapId + (int) (quantNo >> (Page.pageBits + 3));
                int offs = (int) (quantNo & (Page.pageSize * 8 - 1)) >> 3;
                int bitOffs = (int) quantNo & 7;
                int oid = pageId;
                pos = getPos(oid);
                if ((pos & dbModifiedFlag) == 0)
                {
                    dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] 
                        |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                    allocate(Page.pageSize, oid);
                    cloneBitmap(pos & ~ dbFlagsMask, Page.pageSize);
                }
			
                if (objBitSize > 8 - bitOffs)
                {
                    objBitSize -= 8 - bitOffs;
                    offs += 1;
                    while (objBitSize + offs * 8 > Page.pageSize * 8)
                    {
                        oid = ++pageId;
                        pos = getPos(oid);
                        if ((pos & dbModifiedFlag) == 0)
                        {
                            dirtyPagesMap[oid >> (dbHandlesPerPageBits + 5)] 
                                |= 1 << ((oid >> dbHandlesPerPageBits) & 31);
                            allocate(Page.pageSize, oid);
                            cloneBitmap(pos & ~ dbFlagsMask, Page.pageSize);
                        }
                        objBitSize -= (Page.pageSize - offs) * 8;
                        offs = 0;
                    }
                }
            }
        }
		
        public override void Open(System.String filePath, int pagePoolSize)
        {
            OSFile file = new OSFile(filePath);      
            try 
            {
                Open(file, pagePoolSize);
            } 
            catch (StorageError ex) 
            {
                file.Close();            
                throw ex;
            }
        }

        public override void Open(IFile file, int pagePoolSize)
        {
            lock(this)
            {
                if (opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_ALREADY_OPENED);
                }
                Page pg;
                int i;
                int indexSize = initIndexSize;
                if (indexSize < dbFirstUserId)
                {
                    indexSize = dbFirstUserId;
                }
                indexSize = (indexSize + dbHandlesPerPage - 1) & ~ (dbHandlesPerPage - 1);
				
                dirtyPagesMap = new int[dbDirtyPageBitmapSize / 4 + 1];
                bitmapPageAvailableSpace = new int[dbBitmapId + dbBitmapPages];
                for (i = dbBitmapId + dbBitmapPages; --i >= 0; )
                {
                    bitmapPageAvailableSpace[i] = System.Int32.MaxValue;
                }
				
                currRBitmapPage = currPBitmapPage = dbBitmapId;
                currRBitmapOffs = currPBitmapOffs = 0;
                gcThreshold = Int64.MaxValue;
#if !COMPACT_NET_FRAMEWORK
                nNestedTransactions = 0;
                nBlockedTransactions = 0;
                nCommittedTransactions = 0;
                scheduledCommitTime = Int64.MaxValue;
                transactionMonitor = new Object();
                transactionLock = new PersistentResource();
#endif
                allocatedDelta = 0;
                gcDone = false;
                modified = false;
                pool = new PagePool(pagePoolSize / Page.pageSize);
				
                objectCache = (pagePoolSize == INFINITE_PAGE_POOL)
                    ? (OidHashTable)new StrongHashTable(objectCacheInitSize) 
                    : (OidHashTable)new WeakHashTable(objectCacheInitSize);
                classDescMap = new Hashtable();
                descList = null;
				
#if SUPPORT_RAW_TYPE
                objectFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
#endif                
                header = new Header();
                byte[] buf = new byte[Header.Sizeof];
                int rc = file.Read(0, buf);
                if (rc > 0 && rc < Header.Sizeof)
                {
                    throw new StorageError(StorageError.ErrorCode.DATABASE_CORRUPTED);
                }
                header.unpack(buf);
                if (header.curr < 0 || header.curr > 1)
                {
                    throw new StorageError(StorageError.ErrorCode.DATABASE_CORRUPTED);
                }
                if (!header.initialized)
                {
                    header.curr = currIndex = 0;
                    long used = Page.pageSize;
                    header.root[0].index = used;
                    header.root[0].indexSize = indexSize;
                    header.root[0].indexUsed = dbFirstUserId;
                    header.root[0].freeList = 0;
                    used += indexSize * 8;
                    header.root[1].index = used;
                    header.root[1].indexSize = indexSize;
                    header.root[1].indexUsed = dbFirstUserId;
                    header.root[1].freeList = 0;
                    used += indexSize * 8;
					
                    header.root[0].shadowIndex = header.root[1].index;
                    header.root[1].shadowIndex = header.root[0].index;
                    header.root[0].shadowIndexSize = indexSize;
                    header.root[1].shadowIndexSize = indexSize;
					
                    int bitmapPages = (int) ((used + Page.pageSize * (dbAllocationQuantum * 8 - 1) - 1) / (Page.pageSize * (dbAllocationQuantum * 8 - 1)));
                    int bitmapSize = bitmapPages * Page.pageSize;
                    int usedBitmapSize = (int) ((used + bitmapSize) >> (dbAllocationQuantumBits + 3));
					
                    pool.open(file);

                    for (i = 0; i < bitmapPages; i++) 
                    { 
                        pg = pool.putPage(used + i*Page.pageSize);
                        byte[] bitmap = pg.data;
                        for (int j = 0; j < Page.pageSize; j++) 
                        { 
                            bitmap[j] = (byte)0xFF;
                        }
                        pool.unfix(pg);
                    }
                    
                    int bitmapIndexSize = ((dbBitmapId + dbBitmapPages) * 8 + Page.pageSize - 1) & ~ (Page.pageSize - 1);
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
                    {
                        throw new StorageError(StorageError.ErrorCode.DATABASE_CORRUPTED);
                    }
                    pool.open(file);
                    if (header.dirty)
                    {
                        System.Console.WriteLine("Database was not normally closed: start recovery");
                        header.root[1-curr].size = header.root[curr].size;
                        header.root[1-curr].indexUsed = header.root[curr].indexUsed;
                        header.root[1-curr].freeList = header.root[curr].freeList;
                        header.root[1-curr].index = header.root[curr].shadowIndex;
                        header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
                        header.root[1-curr].shadowIndex = header.root[curr].index;
                        header.root[1-curr].shadowIndexSize = header.root[curr].indexSize;
                        header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd;
                        header.root[1-curr].rootObject = header.root[curr].rootObject;
                        header.root[1-curr].classDescList = header.root[curr].classDescList;
						
                        pg = pool.putPage(0);
                        header.pack(pg.data);
                        pool.unfix(pg);
						
                        pool.copy(header.root[1-curr].index, header.root[curr].index, (header.root[curr].indexUsed * 8 + Page.pageSize - 1) & ~ (Page.pageSize - 1));
                        System.Console.WriteLine("Recovery completed");
                    }
                    currIndexSize = header.root[1-curr].indexUsed;
                    committedIndexSize = currIndexSize;
                    usedSize = header.root[curr].size;
                }
                opened = true;
                reloadScheme();
            }
        }

        public override bool IsOpened() 
        { 
            return opened;
        }
		
        internal static void  checkIfFinal(ClassDescriptor desc)
        {
            System.Type cls = desc.cls;
            for (ClassDescriptor next = desc.next; next != null; next = next.next)
            {
                if (cls.IsAssignableFrom(next.cls))
                {
                    desc.hasSubclasses = true;
                }
                else if (next.cls.IsAssignableFrom(cls))
                {
                    next.hasSubclasses = true;
                }
            }
        }
		
		
        internal void  reloadScheme()
        {
            classDescMap.Clear();
            int descListOid = header.root[1-currIndex].classDescList;
            classDescMap[typeof(ClassDescriptor)] = new ClassDescriptor(this, typeof(ClassDescriptor));
            classDescMap[typeof(ClassDescriptor.FieldDescriptor)] = new ClassDescriptor(this, typeof(ClassDescriptor.FieldDescriptor));
            if (descListOid != 0)
            {
                ClassDescriptor desc;
                descList = findClassDescriptor(descListOid);
                for (desc = descList; desc != null; desc = desc.next)
                {
                    desc.resolve();
                    checkIfFinal(desc);
                }
            }
            else
            {
                descList = null;
            }
        }
 
        internal void  assignOid(IPersistent obj, int oid)
        {
            setObjectOid(obj, oid, false);
        }
			
        internal void registerClassDescriptor(ClassDescriptor desc) 
        { 
            classDescMap[desc.cls] = desc;
            desc.next = descList;
            descList = desc;
            checkIfFinal(desc);
            storeObject(desc);
            header.root[1-currIndex].classDescList = desc.Oid;
            modified = true;
        }      


        internal ClassDescriptor getClassDescriptor(System.Type cls)
        {
            ClassDescriptor desc = (ClassDescriptor) classDescMap[cls];
            if (desc == null)
            {
                desc = new ClassDescriptor(this, cls);
                registerClassDescriptor(desc);
            }
            return desc;
        }
		    

        public override void Commit()
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                objectCache.flush();
           
                if (!modified)
                {
                    return;
                }
                int curr = currIndex;
                int i, j, n;
                int[] map = dirtyPagesMap;
                int oldIndexSize = header.root[curr].indexSize;
                int newIndexSize = header.root[1-curr].indexSize;
                int nPages = committedIndexSize >> dbHandlesPerPageBits;
                Page pg;
                if (newIndexSize > oldIndexSize)
                {
                    long newIndex = allocate(newIndexSize * 8, 0);
                    header.root[1-curr].shadowIndex = newIndex;
                    header.root[1-curr].shadowIndexSize = newIndexSize;
                    cloneBitmap(header.root[curr].index, oldIndexSize * 8);
                    free(header.root[curr].index, oldIndexSize * 8);
                }
                for (i = 0; i < nPages; i++)
                {
                    if ((map[i >> 5] & (1 << (i & 31))) != 0)
                    {
                        Page srcIndex = pool.getPage(header.root[1-curr].index + i * Page.pageSize);
                        Page dstIndex = pool.getPage(header.root[curr].index + i * Page.pageSize);
                        for (j = 0; j < Page.pageSize; j += 8)
                        {
                            long pos = Bytes.unpack8(dstIndex.data, j);
                            if (Bytes.unpack8(srcIndex.data, j) != pos)
                            {
                                if ((pos & dbFreeHandleFlag) == 0)
                                {
                                    if ((pos & dbPageObjectFlag) != 0)
                                    {
                                        free(pos & ~ dbFlagsMask, Page.pageSize);
                                    }
                                    else
                                    {
                                        int offs = (int) pos & (Page.pageSize - 1);
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
                    Page srcIndex = pool.getPage(header.root[1-curr].index + i * Page.pageSize);
                    Page dstIndex = pool.getPage(header.root[curr].index + i * Page.pageSize);
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
                                    free(pos & ~ dbFlagsMask, Page.pageSize);
                                }
                                else
                                {
                                    int offs = (int) pos & (Page.pageSize - 1);
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
                        pg = pool.putPage(header.root[1-curr].index + i * Page.pageSize);
                        for (j = 0; j < Page.pageSize; j += 8)
                        {
                            Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~ dbModifiedFlag);
                        }
                        pool.unfix(pg);
                    }
                }
                if (currIndexSize > committedIndexSize)
                {
                    long page = (header.root[1-curr].index + committedIndexSize * 8) & ~ (Page.pageSize - 1);
                    long end = (header.root[1-curr].index + Page.pageSize - 1 + currIndexSize * 8) & ~ (Page.pageSize - 1);
                    while (page < end)
                    {
                        pg = pool.putPage(page);
                        for (j = 0; j < Page.pageSize; j += 8)
                        {
                            Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~ dbModifiedFlag);
                        }
                        pool.unfix(pg);
                        page += Page.pageSize;
                    }
                }
                header.root[1-curr].usedSize = usedSize;
                pg = pool.putPage(0);
                header.pack(pg.data);
                pool.flush();
                pool.modify(pg);
                header.curr = curr ^= 1;
                header.dirty = true;
                header.pack(pg.data);
                pool.unfix(pg);
                pool.flush();
	
                header.root[1-curr].size = header.root[curr].size;
                header.root[1-curr].indexUsed = currIndexSize;
                header.root[1-curr].freeList = header.root[curr].freeList;
                header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd;
                header.root[1-curr].rootObject = header.root[curr].rootObject;
                header.root[1-curr].classDescList = header.root[curr].classDescList;
	
                if (currIndexSize == 0 || newIndexSize != oldIndexSize)
                {
                    if (currIndexSize == 0)
                    {
                        currIndexSize = header.root[1-curr].indexUsed;
                    }
                    header.root[1-curr].index = header.root[curr].shadowIndex;
                    header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
                    header.root[1-curr].shadowIndex = header.root[curr].index;
                    header.root[1-curr].shadowIndexSize = header.root[curr].indexSize;
                    pool.copy(header.root[1-curr].index, header.root[curr].index, currIndexSize * 8);
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
                            pool.copy(header.root[1-curr].index + i * Page.pageSize, header.root[curr].index + i * Page.pageSize, Page.pageSize);
                        }
                    }
                    if (currIndexSize > i * dbHandlesPerPage && ((map[i >> 5] & (1 << (i & 31))) != 0 || currIndexSize != committedIndexSize))
                    {
                        pool.copy(header.root[1-curr].index + i * Page.pageSize, header.root[curr].index + i * Page.pageSize, 8 * currIndexSize - i * Page.pageSize);
                        j = i >> 5;
                        n = (currIndexSize + dbHandlesPerPage * 32 - 1) >> (dbHandlesPerPageBits + 5);
                        while (j < n)
                        {
                            map[j++] = 0;
                        }
                    }
                }
                modified = false;
                gcDone = false;
                currIndex = curr;
                committedIndexSize = currIndexSize;
            }
        }
		
        public override void Rollback()
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                objectCache.invalidate();
        
                if (!modified) 
                { 
                    return;
                }
                int curr = currIndex;
                int[] map = dirtyPagesMap;
                if (header.root[1-curr].index != header.root[curr].shadowIndex)
                {
                    pool.copy(header.root[curr].shadowIndex, header.root[curr].index, 8 * committedIndexSize);
                }
                else
                {
                    int nPages = (committedIndexSize + dbHandlesPerPage - 1) >> dbHandlesPerPageBits;
                    for (int i = 0; i < nPages; i++)
                    {
                        if ((map[i >> 5] & (1 << (i & 31))) != 0)
                        {
                            pool.copy(header.root[curr].shadowIndex + i * Page.pageSize, header.root[curr].index + i * Page.pageSize, Page.pageSize);
                        }
                    }
                }
                for (int j = (currIndexSize + dbHandlesPerPage * 32 - 1) >> (dbHandlesPerPageBits + 5); --j >= 0; map[j] = 0)
                    ;
                header.root[1-curr].index = header.root[curr].shadowIndex;
                header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
                header.root[1-curr].indexUsed = committedIndexSize;
                header.root[1-curr].freeList = header.root[curr].freeList;
                header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd;
                header.root[1-curr].size = header.root[curr].size;
                header.root[1-curr].rootObject = header.root[curr].rootObject;
                header.root[1-curr].classDescList = header.root[curr].classDescList;
                header.dirty = true;
                modified = false;
                usedSize = header.root[curr].size;
                currIndexSize = committedIndexSize;
				
                currRBitmapPage = currPBitmapPage = dbBitmapId;
                currRBitmapOffs = currPBitmapOffs = 0;
				
                reloadScheme();
            }
        }
		
        public override Index CreateIndex(System.Type keyType, bool unique)
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                Btree index = new Btree(keyType, unique);
                setObjectOid(index, 0, false);
                return index;
            }
        }
        
        public override SpatialIndex CreateSpatialIndex() 
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                Rtree index = new Rtree();
                setObjectOid(index, 0, false);
                return index;
            }
        }
		
        public override SortedCollection CreateSortedCollection(PersistentComparator comparator, bool unique) 
        {
            if (!opened) 
            { 
                throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
            }        
            return new Ttree(comparator, unique);
        }

        public override ISet CreateSet() 
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                PersistentSet s = new PersistentSet();
                setObjectOid(s, 0, false);
                return s;
            }
        }
		
        public override FieldIndex CreateFieldIndex(System.Type type, String fieldName, bool unique)
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                BtreeFieldIndex index = new BtreeFieldIndex(type, fieldName, unique);
                setObjectOid(index, 0, false);
                return index;
            }
        }
		
        public override FieldIndex CreateFieldIndex(System.Type type, String[] fieldNames, bool unique)
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                BtreeMultiFieldIndex index = new BtreeMultiFieldIndex(type, fieldNames, unique);
                setObjectOid(index, 0, false);
                return index;
            }
        }

        public override Link CreateLink()
        {
            return CreateLink(8);
        }
		
        public override Link CreateLink(int initialSize)
        {
            return new LinkImpl(initialSize);
        }
		
        public override Relation CreateRelation(IPersistent owner)
        {
            return new RelationImpl(owner);
        }

        public override Blob CreateBlob() 
        {
            return new BlobImpl(Page.pageSize - ObjectHeader.Sizeof - 16);
        }

        public override void  ExportXML(System.IO.StreamWriter writer)
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                int rootOid = header.root[1-currIndex].rootObject;
                if (rootOid != 0)
                {
                    XMLExporter xmlExporter = new XMLExporter(this, writer);
                    xmlExporter.exportDatabase(rootOid);
                }
            }
        }
		
        public override void  ImportXML(System.IO.StreamReader reader)
        {
            lock(this)
            {
                if (!opened)
                {
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                XMLImporter xmlImporter = new XMLImporter(this, reader);
                xmlImporter.importDatabase();
            }
        }

        internal long getGCPos(int oid) 
        { 
            Page pg = pool.getPage(header.root[currIndex].index 
                + ((uint)oid >> dbHandlesPerPageBits << Page.pageBits));
            long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3);
            pool.unfix(pg);
            return pos;
        }
        
        internal void markOid(int oid) 
        { 
            if (oid != 0) 
            {  
                long pos = getGCPos(oid);
                int bit = (int)((ulong)pos >> dbAllocationQuantumBits);
                if ((blackBitmap[(uint)bit >> 5] & (1 << (bit & 31))) == 0) 
                { 
                    greyBitmap[(uint)bit >> 5] |= 1 << (bit & 31);
                }
            }
        }

        internal Page getGCPage(int oid) 
        {  
            return pool.getPage(getGCPos(oid) & ~dbFlagsMask);
        }

        public override void SetGcThreshold(long maxAllocatedDelta) 
        {
            gcThreshold = maxAllocatedDelta;
        }

        public override void Gc() 
        { 
            lock (this) 
            { 
                lock (objectCache) 
                {
                    if (gcDone) 
                    { 
                        return;
                    }
                    // Console.WriteLine("Start GC, allocatedDelta=" + allocatedDelta + ", header[" + currIndex + "].size=" + header.root[currIndex].size + ", gcTreshold=" + gcThreshold);
                    int bitmapSize = (int)((ulong)header.root[currIndex].size >> (dbAllocationQuantumBits + 5)) + 1;
                    bool existsNotMarkedObjects;
                    long pos;
                    int  i, j;

                    // mark
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
                                            int offs = (int)pos & (Page.pageSize-1);
                                            Page pg = pool.getPage(pos - offs);
                                            int typeOid = ObjectHeader.getType(pg.data, offs);
                                            if (typeOid != 0) 
                                            { 
                                                ClassDescriptor desc = (ClassDescriptor)lookupObject(typeOid, typeof(ClassDescriptor));
                                                if (typeof(Btree).IsAssignableFrom(desc.cls)) 
                                                { 
                                                    Btree btree = new Btree(pg.data, ObjectHeader.Sizeof + offs);
                                                    setObjectOid(btree, 0, false);
                                                    btree.markTree();
                                                } 
                                                else if (desc.hasReferences) 
                                                { 
                                                    markObject(pool.get(pos), ObjectHeader.Sizeof, desc);
                                                
                                                }
                                            }
                                            pool.unfix(pg);                                
                                        }
                                    }
                                }
                            }
                        } while (existsNotMarkedObjects);
                    }
        
                    // sweep
                    gcDone = true;
                    for (i = dbFirstUserId, j = committedIndexSize; i < j; i++) 
                    {
                        pos = getGCPos(i);
                        if (((int)pos & (dbPageObjectFlag|dbFreeHandleFlag)) == 0) 
                        {
                            int bit = (int)((ulong)pos >> dbAllocationQuantumBits);
                            if ((blackBitmap[(uint)bit >> 5] & (1 << (bit & 31))) == 0) 
                            { 
                                // object is not accessible
                                if (getPos(i) != pos) 
                                { 
                                    throw new StorageError(StorageError.ErrorCode.INVALID_OID);
                                }
                                int offs = (int)pos & (Page.pageSize-1);
                                Page pg = pool.getPage(pos - offs);
                                int typeOid = ObjectHeader.getType(pg.data, offs);
                                if (typeOid != 0) 
                                { 
                                    ClassDescriptor desc = findClassDescriptor(typeOid);
                                    if (desc != null 
                                        && (typeof(Btree).IsAssignableFrom(desc.cls))) 
                                    { 
                                        Btree btree = new Btree(pg.data, ObjectHeader.Sizeof + offs);
                                        pool.unfix(pg);
                                        setObjectOid(btree, i, false);
                                        btree.Deallocate();
                                    }
                                    else 
                                    { 
                                        int size = ObjectHeader.getSize(pg.data, offs);
                                        pool.unfix(pg);
                                        freeId(i);
                                        objectCache.remove(i);                        
                                        cloneBitmap(pos, size);
                                    }
                                }
                            }
                        }   
                    }

                    greyBitmap = null;
                    blackBitmap = null;
                    allocatedDelta = 0;
                }
            }
        }


        public override Hashtable GetMemoryDump() 
        { 
            lock(this) 
            { 
                lock (objectCache) 
                { 
                    if (!opened) 
                    {
                        throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                    }
                    int bitmapSize = (int)(header.root[currIndex].size >> (dbAllocationQuantumBits + 5)) + 1;
                    bool existsNotMarkedObjects;
                    long pos;
                    int  i, j, n;

                    // mark
                    greyBitmap = new int[bitmapSize];
                    blackBitmap = new int[bitmapSize];
                    int rootOid = header.root[currIndex].rootObject;
                    Hashtable map = new Hashtable();

                    if (rootOid != 0) 
                    { 
                        MemoryUsage indexUsage = new MemoryUsage(typeof(Index));
                        MemoryUsage fieldIndexUsage = new MemoryUsage(typeof(FieldIndex));
                        MemoryUsage classUsage = new MemoryUsage(typeof(Type));

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
                                            int offs = (int)pos & (Page.pageSize-1);
                                            Page pg = pool.getPage(pos - offs);
                                            int typeOid = ObjectHeader.getType(pg.data, offs);
                                            int objSize = ObjectHeader.getSize(pg.data, offs);
                                            int alignedSize = (objSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1);                                    
                                            if (typeOid != 0) 
                                            { 
                                                markOid(typeOid);
                                                ClassDescriptor desc = findClassDescriptor(typeOid);
                                                if (typeof(Btree).IsAssignableFrom(desc.cls)) 
                                                { 
                                                    Btree btree = new Btree(pg.data, ObjectHeader.Sizeof + offs);
                                                    setObjectOid(btree, 0, false);
                                                    int nPages = btree.markTree();
                                                    if (typeof(FieldIndex).IsAssignableFrom(desc.cls)) 
                                                    { 
                                                        fieldIndexUsage.nInstances += 1;
                                                        fieldIndexUsage.totalSize += nPages*Page.pageSize + objSize;
                                                        fieldIndexUsage.allocatedSize += nPages*Page.pageSize + alignedSize;
                                                    } 
                                                    else 
                                                    {
                                                        indexUsage.nInstances += 1;
                                                        indexUsage.totalSize += nPages*Page.pageSize + objSize;
                                                        indexUsage.allocatedSize += nPages*Page.pageSize + alignedSize;
                                                    }
                                                } 
                                                else 
                                                { 
                                                    MemoryUsage usage = (MemoryUsage)map[desc.cls];
                                                    if (usage == null) 
                                                    { 
                                                        usage = new MemoryUsage(desc.cls);
                                                        map[desc.cls] = usage;
                                                    }
                                                    usage.nInstances += 1;
                                                    usage.totalSize += objSize;
                                                    usage.allocatedSize += alignedSize;
                                                      
                                                    if (desc.hasReferences) 
                                                    { 
                                                        markObject(pool.get(pos), ObjectHeader.Sizeof, desc);
                                                    }
                                                }
                                            } 
                                            else 
                                            { 
                                                classUsage.nInstances += 1;
                                                classUsage.totalSize += objSize;
                                                classUsage.allocatedSize += alignedSize;
                                            }
                                            pool.unfix(pg);                                
                                        }
                                    }
                                }
                            }
                        } while (existsNotMarkedObjects);
                
                        if (indexUsage.nInstances != 0) 
                        { 
                            map[typeof(Index)] = indexUsage;
                        }
                        if (fieldIndexUsage.nInstances != 0) 
                        { 
                            map[typeof(FieldIndex)] = fieldIndexUsage;
                        }
                        if (classUsage.nInstances != 0) 
                        { 
                            map[typeof(Type)] = classUsage;
                        }
                        MemoryUsage system = new MemoryUsage(typeof(Storage));
                        system.totalSize += header.root[0].indexSize*8;
                        system.totalSize += header.root[1].indexSize*8;
                        system.totalSize += (header.root[0].bitmapEnd - dbBitmapId + 1)*Page.pageSize;
                        system.totalSize += (header.root[1].bitmapEnd - dbBitmapId + 1)*Page.pageSize;
                        system.totalSize += Page.pageSize; // root page

                        long allocated = 0;
                        for (i = dbBitmapId, n  = header.root[currIndex].bitmapEnd; i < n; i++) {
                            Page pg = getGCPage(i);
                            for (j = 0; j < Page.pageSize; j++) 
                            {
                                int mask = pg.data[j] & 0xFF;
                                while (mask != 0) 
                                { 
                                    if ((mask & 1) != 0) 
                                    { 
                                        allocated += dbAllocationQuantum;
                                    }
                                    mask >>= 1;
                                }
                            }
                            pool.unfix(pg);
                        }
                        system.allocatedSize = allocated;

                        system.nInstances = header.root[currIndex].indexSize;
                        map[typeof(Storage)] = system;
                    } 
                    return map;
                }
            }
        }

        
        internal int markObject(byte[] obj, int offs,  ClassDescriptor desc)
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
                        {
                            offs += strlen*2;
                        }
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpObject:
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                        continue;
                    case ClassDescriptor.FieldType.tpValue:
                        offs = markObject(obj, offs, fd.valueDesc);
                        continue;
#if SUPPORT_RAW_TYPE
                    case ClassDescriptor.FieldType.tpRaw:
#endif
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
                            offs += len*2;
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
                            offs += len*4;
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
                            offs += len*8;
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
                                offs += strlen*2;
                            }
                        }
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfObject:
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
#if SUPPORT_RAW_TYPE
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
                        }
                        continue;
                    }
#endif
                }
            }
            return offs;
        }

 
#if COMPACT_NET_FRAMEWORK
        public override void RegisterAssembly(System.Reflection.Assembly assembly) 
        {
            assemblies.Add(assembly);
        }
#else
        public override void BeginThreadTransaction(TransactionMode mode)
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
            { 
                transactionLock.ExclusiveLock();
            } 
            else 
            { 
                transactionLock.SharedLock();
            }
        }

        public override void EndThreadTransaction(int maxDelay)
        {
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
                        { 
                            Monitor.PulseAll(transactionMonitor);
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
                                    Monitor.Wait(transactionMonitor);
                                } while (nCommittedTransactions == n);
                                nBlockedTransactions -= 1;
                            }				    
                        }
                    }
                }
            }
        }


        public override void RollbackThreadTransaction()
        {
            lock (transactionMonitor) 
            { 
                transactionLock.Reset();
                nNestedTransactions = 0;
                if (nBlockedTransactions != 0) 
                { 
                    Monitor.PulseAll(transactionMonitor);
                }
                Rollback();
            }
        }
	    

#endif

        public override void Close()
        {
            Commit();
            opened = false;
            if (header.dirty)
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
            // make GC easier
            pool = null;
            objectCache = null;
            classDescMap = null;
            bitmapPageAvailableSpace = null;
            dirtyPagesMap = null;
            descList = null;
        }
		
        private bool getBooleanValue(Object val) 
        { 
            if (val is bool)  
            { 
                return (bool)val;
            }
            else if (val is string) 
            {
                return bool.Parse((string)val);
            }
            throw new StorageError(StorageError.ErrorCode.BAD_PROPERTY_VALUE);
        }

        private long getIntegerValue(Object val) 
        { 
            if (val is int)  
            {
                return (int)val;
            } 
            else if (val is long) 
            {
                return (long)val;
            } 
            else if (val is string) 
            { 
                return long.Parse((string)val);
            } 
            else 
            {                                                                  
                throw new StorageError(StorageError.ErrorCode.BAD_PROPERTY_VALUE);            
            }
        }

     
        public override void SetProperties(System.Collections.Specialized.NameValueCollection props) 
        {
            string val;
            if ((val = props["perst.serialize.transient.objects"]) != null) 
            { 
                ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(val);
            } 
            if ((val = props["perst.object.cache.init.size"]) != null) 
            { 
                objectCacheInitSize = (int)getIntegerValue(val);
            }
            if ((val = props["perst.object.index.init.size"]) != null) 
            { 
                initIndexSize = (int)getIntegerValue(val);
            }
            if ((val = props["perst.extension.quantum"]) != null) 
            { 
                extensionQuantum = getIntegerValue(val);
            } 
           if ((val = props["perst.gc.threshold"]) != null) 
            { 
                 gcThreshold = getIntegerValue(val);
            }
        }

        public override void SetProperty(String name, Object val)
        {
            if (name.Equals("perst.serialize.transient.objects")) 
            { 
                ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(val);
            } 
            else if (name.Equals("perst.object.cache.init.size")) 
            { 
                objectCacheInitSize = (int)getIntegerValue(val);
            } 
            else if (name.Equals("perst.object.index.init.size")) 
            { 
                initIndexSize = (int)getIntegerValue(val);
            } 
            else if (name.Equals("perst.extension.quantum")) 
            { 
                extensionQuantum = getIntegerValue(val);
            } 
            else if (name.Equals("perst.gc.threshold")) 
            { 
                gcThreshold = getIntegerValue(val);
            }
            else 
            { 
                throw new StorageError(StorageError.ErrorCode.NO_SUCH_PROPERTY);
            }
        }

    
        public override IPersistent GetObjectByOID(int oid)
        {
            lock (this) 
            { 
                return oid == 0 ? null : lookupObject(oid, null);
            }
        }

        
        protected internal override void modifyObject(IPersistent obj) 
        {
            lock (this) 
            {                 
                lock (objectCache) 
                { 
                    if (!obj.IsModified()) 
                    { 
                        objectCache.setDirty(obj.Oid);
                    }
                }
            }
        }

        protected internal override void storeObject(IPersistent obj) 
        {
            lock (this) 
            {
                if (!opened) 
                { 
                    throw new StorageError(StorageError.ErrorCode.STORAGE_NOT_OPENED);
                }
                lock(objectCache) 
                { 
                    storeObject0(obj);
                }
            }
        }

        protected internal override void storeFinalizedObject(IPersistent obj) 
        {
            if (opened) 
            { 
                lock (objectCache) 
                { 
                    if (obj.Oid != 0) { 
                        storeObject0(obj);
                    }
                }
            }
        }

        void storeObject0(IPersistent obj) 
        {
            int oid = obj.Oid;
            bool newObject = false;
            if (oid == 0)
            {
                oid = allocateId();
                objectCache.put(oid, obj);
                setObjectOid(obj, oid, false);
                newObject = true;
            } else if (obj.IsModified()) {
                objectCache.clearDirty(oid);
            } 
            byte[] data = packObject(obj);
            long pos;
            int newSize = ObjectHeader.getSize(data, 0);
            if (newObject)
            {
                pos = allocate(newSize, 0);
                setPos(oid, pos | dbModifiedFlag);
            }
            else
            {
                pos = getPos(oid);
                int offs = (int) pos & (Page.pageSize - 1);
                if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                {
                    throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
                }
                Page pg = pool.getPage(pos - offs);
                offs &= ~ dbFlagsMask;
                int size = ObjectHeader.getSize(pg.data, offs);
                pool.unfix(pg);
                if ((pos & dbModifiedFlag) == 0)
                {
                    cloneBitmap(pos & ~ dbFlagsMask, size);
                    pos = allocate(newSize, 0);
                    setPos(oid, pos | dbModifiedFlag);
                }
                else
                {
                    if (((newSize + dbAllocationQuantum - 1) & ~ (dbAllocationQuantum - 1)) > ((size + dbAllocationQuantum - 1) & ~ (dbAllocationQuantum - 1)))
                    {
                        long newPos = allocate(newSize, 0);
                        cloneBitmap(pos & ~ dbFlagsMask, size);
                        free(pos & ~ dbFlagsMask, size);
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
		
        protected internal override void loadObject(IPersistent obj)
        {
            lock(this)
            {
                if (obj.IsRaw()) 
                { 
                    loadStub(obj.Oid, obj, obj.GetType());
                }
            }
        }
		
        internal IPersistent lookupObject(int oid, System.Type cls)
        {
            IPersistent obj = objectCache.get(oid);
            if (obj == null || obj.IsRaw())
            {
                obj = loadStub(oid, obj, cls);
            }
            return obj;
        }
		
        internal int swizzle(IPersistent obj)
        {
            int oid = 0;
            if (obj != null)
            {
                if (!obj.IsPersistent())
                {
                    storeObject(obj);
                }
                oid = obj.Oid;
            }
            return oid;
        }
		
        internal ClassDescriptor findClassDescriptor(int oid) 
        { 
            return (ClassDescriptor)lookupObject(oid, typeof(ClassDescriptor));
                                                                                                                                            
        }

        internal IPersistent unswizzle(int oid, System.Type cls, bool recursiveLoading)
        {
            if (oid == 0)
            {
                return null;
            }
            if (recursiveLoading)
            {
                return lookupObject(oid, cls);
            }
            IPersistent stub = objectCache.get(oid);
            if (stub != null)
            {
                return stub;
            }
            ClassDescriptor desc;
            if (cls == typeof(Persistent) || (desc = (ClassDescriptor) classDescMap[cls]) == null || desc.hasSubclasses)
            {
                long pos = getPos(oid);
                int offs = (int) pos & (Page.pageSize - 1);
                if ((offs & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
                {
                    throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
                }
                Page pg = pool.getPage(pos - offs);
                int typeOid = ObjectHeader.getType(pg.data, offs & ~ dbFlagsMask);
                pool.unfix(pg);
                desc = findClassDescriptor(typeOid);
            }
            stub = (IPersistent)desc.newInstance();
            setObjectOid(stub, oid, true);
            objectCache.put(oid, stub);
            return stub;
        }
		
        internal IPersistent loadStub(int oid, IPersistent obj, System.Type cls)
        {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag | dbPageObjectFlag)) != 0)
            {
                throw new StorageError(StorageError.ErrorCode.DELETED_OBJECT);
            }
            byte[] body = pool.get(pos & ~ dbFlagsMask);
            ClassDescriptor desc;
            int typeOid = ObjectHeader.getType(body, 0);
            if (typeOid == 0) 
            { 
                desc = (ClassDescriptor)classDescMap[cls];
            } 
            else 
            { 
                desc = findClassDescriptor(typeOid);
            }
            if (obj == null) 
            { 
                obj = (IPersistent)desc.newInstance();
                objectCache.put(oid, obj);
            }
            setObjectOid(obj, oid, false);
            unpackObject(obj, desc, obj.RecursiveLoading(), body, ObjectHeader.Sizeof);
            obj.OnLoad();
            return obj;
       }

       internal int unpackObject(Object obj, ClassDescriptor desc, bool recursiveLoading, byte[] body, int offs) 
       {
            ClassDescriptor.FieldDescriptor[] all = desc.allFields;
			int len;

            for (int i = 0, n = all.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = all[i];
                FieldInfo f = fd.field;
                if (f == null || obj == null) 
                {
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
                        case ClassDescriptor.FieldType.tpFloat:
                        case ClassDescriptor.FieldType.tpObject:
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
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                offs += len*2;
                            } 
                            continue;
                        case ClassDescriptor.FieldType.tpValue:
                            offs = unpackObject(null, fd.valueDesc, recursiveLoading, body, offs);
                            continue;
#if SUPPORT_RAW_TYPE
                        case ClassDescriptor.FieldType.tpRaw:
#endif
                        case ClassDescriptor.FieldType.tpArrayOfByte:
                        case ClassDescriptor.FieldType.tpArrayOfSByte:
                        case ClassDescriptor.FieldType.tpArrayOfBoolean:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                offs += len;
                            }
                            continue;
                        case ClassDescriptor.FieldType.tpArrayOfShort:
                        case ClassDescriptor.FieldType.tpArrayOfUShort:
                        case ClassDescriptor.FieldType.tpArrayOfChar:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                offs += len*2;
                            }
                            continue;
                        case ClassDescriptor.FieldType.tpArrayOfInt:
                        case ClassDescriptor.FieldType.tpArrayOfUInt:
                        case ClassDescriptor.FieldType.tpArrayOfFloat:
                        case ClassDescriptor.FieldType.tpArrayOfObject:
                        case ClassDescriptor.FieldType.tpLink:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                offs += len*4;
                            }
                            continue;
                        case ClassDescriptor.FieldType.tpArrayOfLong:
                        case ClassDescriptor.FieldType.tpArrayOfULong:
                        case ClassDescriptor.FieldType.tpArrayOfDouble:
                        case ClassDescriptor.FieldType.tpArrayOfDate:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                offs += len*8;
                            }
                            continue;
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
                                        len += strlen*2;
                                    }
                                }
                            }
                            continue;
                        case ClassDescriptor.FieldType.tpArrayOfValue:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len > 0) 
                            { 
                                ClassDescriptor valueDesc = fd.valueDesc;
                                for (int j = 0; j < len; j++) 
                                { 
                                    offs = unpackObject(null, valueDesc, recursiveLoading, body, offs);
                                }
                            }
                            continue;
#if SUPPORT_RAW_TYPE
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
                                }
                            }
                            continue;
#endif
                    }                 
                }
                else 
                { 
                    switch (fd.type)
                    {
                        case ClassDescriptor.FieldType.tpBoolean: 
                            f.SetValue(obj, body[offs++] != 0);
                            continue;
					
                        case ClassDescriptor.FieldType.tpByte: 
                            f.SetValue(obj, body[offs++]);
                            continue;

                        case ClassDescriptor.FieldType.tpSByte: 
                            f.SetValue(obj, (sbyte)body[offs++]);
                            continue;
										
                        case ClassDescriptor.FieldType.tpChar: 
                            f.SetValue(obj, (char) Bytes.unpack2(body, offs));
                            offs += 2;
                            continue;
					
                        case ClassDescriptor.FieldType.tpShort: 
                            f.SetValue(obj, Bytes.unpack2(body, offs));
                            offs += 2;
                            continue;

                        case ClassDescriptor.FieldType.tpUShort: 
                            f.SetValue(obj, (ushort)Bytes.unpack2(body, offs));
                            offs += 2;
                            continue;
					
                        case ClassDescriptor.FieldType.tpEnum: 
                            f.SetValue(obj, Enum.ToObject(f.FieldType, Bytes.unpack4(body, offs)));
                            offs += 4;
                            continue;

                        case ClassDescriptor.FieldType.tpInt: 
                            f.SetValue(obj, Bytes.unpack4(body, offs));
                            offs += 4;
                            continue;

                        case ClassDescriptor.FieldType.tpUInt: 
                            f.SetValue(obj, (uint)Bytes.unpack4(body, offs));
                            offs += 4;
                            continue;
					
                        case ClassDescriptor.FieldType.tpLong: 
                            f.SetValue(obj, Bytes.unpack8(body, offs));
                            offs += 8;
                            continue;
                        case ClassDescriptor.FieldType.tpULong: 
                            f.SetValue(obj, (ulong)Bytes.unpack8(body, offs));
                            offs += 8;
                            continue;
					
                        case ClassDescriptor.FieldType.tpFloat: 
                            f.SetValue(obj, BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(body, offs)), 0));
                            offs += 4;
                            continue;
					
                        case ClassDescriptor.FieldType.tpDouble: 
#if COMPACT_NET_FRAMEWORK 
                        f.SetValue(obj, BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(body, offs)), 0));
#else
                            f.SetValue(obj, BitConverter.Int64BitsToDouble(Bytes.unpack8(body, offs)));
#endif
                            offs += 8;
                            continue;
					
                        case ClassDescriptor.FieldType.tpDecimal:
                        {
                            int[] bits = new int[4];
                            for (int j = 0; j < 4; j++) 
                            { 
                                bits[j] = Bytes.unpack4(body, offs);
                                offs += 4;
                            }
                            f.SetValue(obj, new decimal(bits));
                            break;
                        }

                        case ClassDescriptor.FieldType.tpGuid:
                        {
                            byte[] bits = new byte[16];
                            Array.Copy(body, offs, bits, 0, 16);
                            offs += 16;
                            f.SetValue(obj, new Guid(bits));
                            break;
                        }

                        case ClassDescriptor.FieldType.tpString: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            String str = null;
                            if (len >= 0)
                            {
                                char[] chars = new char[len];
                                for (int j = 0; j < len; j++)
                                {
                                    chars[j] = (char) Bytes.unpack2(body, offs);
                                    offs += 2;
                                }
                                str = new String(chars);
                            }
                            f.SetValue(obj, str);
                            continue;
					
                        case ClassDescriptor.FieldType.tpDate: 
                            f.SetValue(obj, new System.DateTime(Bytes.unpack8(body, offs)));
                            offs += 8;
                            continue;
					
                        case ClassDescriptor.FieldType.tpObject: 
                            f.SetValue(obj, unswizzle(Bytes.unpack4(body, offs), f.FieldType, recursiveLoading));
                            offs += 4;
                            continue;

                        case ClassDescriptor.FieldType.tpValue: 
                        {
                            ClassDescriptor valueDesc = fd.valueDesc;
                            Object value = valueDesc.newInstance();
                            offs = unpackObject(value, valueDesc, recursiveLoading, body, offs);
                            f.SetValue(obj, value);
                            continue;
                        }
					
#if SUPPORT_RAW_TYPE
                        case ClassDescriptor.FieldType.tpRaw: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len >= 0)
                            {
                                System.IO.MemoryStream ms = new System.IO.MemoryStream(body, offs, len);
                                f.SetValue(obj, objectFormatter.Deserialize(ms));
                                ms.Close();
                                offs += len;
                            }
                            continue;
#endif					
                        case ClassDescriptor.FieldType.tpArrayOfByte: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                byte[] arr = new byte[len];
                                Array.Copy(body, offs, arr, 0, len);
                                offs += len;
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfSByte: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                sbyte[] arr = new sbyte[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = (sbyte)body[offs++];
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfBoolean: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                bool[] arr = new bool[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = body[offs++] != 0;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfShort: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                short[] arr = new short[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = Bytes.unpack2(body, offs);
                                    offs += 2;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfUShort: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                ushort[] arr = new ushort[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = (ushort)Bytes.unpack2(body, offs);
                                    offs += 2;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfChar: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                char[] arr = new char[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = (char) Bytes.unpack2(body, offs);
                                    offs += 2;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfEnum: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                System.Type elemType = f.FieldType.GetElementType();
                                Array arr = Array.CreateInstance(elemType, len);
                                for (int j = 0; j < len; j++)
                                {
                                    arr.SetValue(Enum.ToObject(elemType, Bytes.unpack4(body, offs)), j);
                                    offs += 4;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfInt: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                int[] arr = new int[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = Bytes.unpack4(body, offs);
                                    offs += 4;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfUInt: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                uint[] arr = new uint[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = (uint)Bytes.unpack4(body, offs);
                                    offs += 4;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfLong: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                long[] arr = new long[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = Bytes.unpack8(body, offs);
                                    offs += 8;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

                        case ClassDescriptor.FieldType.tpArrayOfULong: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                ulong[] arr = new ulong[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = (ulong)Bytes.unpack8(body, offs);
                                    offs += 8;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfFloat: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                float[] arr = new float[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(body, offs)), 0);
                                    offs += 4;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfDouble: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                double[] arr = new double[len];
                                for (int j = 0; j < len; j++)
                                {
#if COMPACT_NET_FRAMEWORK 
                                arr[j] = BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(body, offs)), 0);
#else
                                    arr[j] =  BitConverter.Int64BitsToDouble(Bytes.unpack8(body, offs));
#endif
                                    offs += 8;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfDate: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                System.DateTime[] arr = new System.DateTime[len];
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = new System.DateTime(Bytes.unpack8(body, offs));
                                    offs += 8;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfString: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                System.String[] arr = new System.String[len];
                                for (int j = 0; j < len; j++)
                                {
                                    int strlen = Bytes.unpack4(body, offs);
                                    offs += 4;
                                    if (strlen >= 0)
                                    {
                                        char[] chars = new char[strlen];
                                        for (int k = 0; k < strlen; k++)
                                        {
                                            chars[k] = (char) Bytes.unpack2(body, offs);
                                            offs += 2;
                                        }
                                        arr[j] = new String(chars);
                                    }
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfObject: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                System.Type elemType = f.FieldType.GetElementType();
                                IPersistent[] arr = (IPersistent[]) System.Array.CreateInstance(elemType, len);
                                for (int j = 0; j < len; j++)
                                {
                                    arr[j] = unswizzle(Bytes.unpack4(body, offs), elemType, recursiveLoading);
                                    offs += 4;
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
					
                        case ClassDescriptor.FieldType.tpArrayOfValue:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0) 
                            { 
                                f.SetValue(obj, null);
                            } 
                            else 
                            {
                                Type elemType = f.FieldType.GetElementType();
                                Array arr = Array.CreateInstance(elemType, len);
                                ClassDescriptor valueDesc = fd.valueDesc;
                                for (int j = 0; j < len; j++) 
                                { 
                                    Object value = valueDesc.newInstance();
                                    offs = unpackObject(value, valueDesc, recursiveLoading, body, offs);
                                    arr.SetValue(value, j);
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;

#if SUPPORT_RAW_TYPE
                        case ClassDescriptor.FieldType.tpArrayOfRaw:
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0) 
                            {
                                f.SetValue(obj, null);
                            }
                            else 
                            {
                                Type elemType = f.FieldType.GetElementType();
                                Array arr = Array.CreateInstance(elemType, len);
                                ClassDescriptor valueDesc = fd.valueDesc;
                                for (int j = 0; j < len; j++) 
                                { 
                                    int rawlen = Bytes.unpack4(body, offs);
                                    offs += 4;
                                    if (rawlen >= 0) 
                                    {
                                        System.IO.MemoryStream ms = new System.IO.MemoryStream(body, offs, rawlen);
                                        arr.SetValue(objectFormatter.Deserialize(ms), j);
                                        ms.Close();
                                        offs += rawlen;
                                    }
                                }
                                f.SetValue(obj, arr);
                            }
                            continue;
#endif                    
                        case ClassDescriptor.FieldType.tpLink: 
                            len = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (len < 0)
                            {
                                f.SetValue(obj, null);
                            }
                            else
                            {
                                IPersistent[] arr = new IPersistent[len];
                                for (int j = 0; j < len; j++)
                                {
                                    int elemOid = Bytes.unpack4(body, offs);
                                    offs += 4;
                                    IPersistent stub = null;
                                    if (elemOid != 0)
                                    {
                                        stub = objectCache.get(elemOid);
                                        if (stub == null)
                                        {
                                            stub = new Persistent();
                                            setObjectOid(stub, elemOid, true);
                                        }
                                    }
                                    arr[j] = stub;
                                }
                                f.SetValue(obj, new LinkImpl(arr));
                            }
                            continue;
                    }
                }
            }
            return offs;
        }
		
        internal byte[] packObject(System.Object obj)
        {
            ByteBuffer buf = new ByteBuffer();
            int offs = ObjectHeader.Sizeof;
            buf.extend(offs);
            ClassDescriptor desc = getClassDescriptor(obj.GetType());
            offs = packObject(obj, desc, offs, buf);
            ObjectHeader.setSize(buf.arr, 0, offs);
            ObjectHeader.setType(buf.arr, 0, desc.Oid);
            return buf.arr;        
        }

        internal int packObject(Object obj, ClassDescriptor desc, int offs, ByteBuffer buf)
        { 
            ClassDescriptor.FieldDescriptor[] flds = desc.allFields;

            for (int i = 0, n = flds.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = flds[i];
                FieldInfo f = fd.field;
                switch (fd.type)
                {
                    case ClassDescriptor.FieldType.tpByte: 
                    case ClassDescriptor.FieldType.tpSByte: 
                        buf.extend(offs + 1);
                        buf.arr[offs++] = (byte) f.GetValue(obj);
                        continue;
					
                    case ClassDescriptor.FieldType.tpBoolean: 
                        buf.extend(offs + 1);
                        buf.arr[offs++] = (byte) ((bool) f.GetValue(obj)?1:0);
                        continue;
					
                    case ClassDescriptor.FieldType.tpShort: 
                    case ClassDescriptor.FieldType.tpUShort: 
                        buf.extend(offs + 2);
                        Bytes.pack2(buf.arr, offs, (short) f.GetValue(obj));
                        offs += 2;
                        continue;
					
                    case ClassDescriptor.FieldType.tpChar: 
                        buf.extend(offs + 2);
                        Bytes.pack2(buf.arr, offs, (short) (char)f.GetValue(obj));
                        offs += 2;
                        continue;
					
                    case ClassDescriptor.FieldType.tpEnum: 
                    case ClassDescriptor.FieldType.tpInt: 
                    case ClassDescriptor.FieldType.tpUInt: 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, (int) f.GetValue(obj));
                        offs += 4;
                        continue;
					
                    case ClassDescriptor.FieldType.tpLong: 
                    case ClassDescriptor.FieldType.tpULong: 
                        buf.extend(offs + 8);
                        Bytes.pack8(buf.arr, offs, (long) f.GetValue(obj));
                        offs += 8;
                        continue;
					
                    case ClassDescriptor.FieldType.tpFloat: 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes((float) f.GetValue(obj)), 0));
                        offs += 4;
                        continue;
					
                    case ClassDescriptor.FieldType.tpDouble: 
                        buf.extend(offs + 8);
#if COMPACT_NET_FRAMEWORK 
                        Bytes.pack8(buf.arr, offs, BitConverter.ToInt64(BitConverter.GetBytes((double) f.GetValue(obj)), 0));
#else
                        Bytes.pack8(buf.arr, offs, BitConverter.DoubleToInt64Bits((double) f.GetValue(obj)));
#endif
                        offs += 8;
                        continue;
					
                    case ClassDescriptor.FieldType.tpDecimal:
                    {
                        buf.extend(offs + 16);
                        int[] bits = Decimal.GetBits((decimal)f.GetValue(obj));
                        for (int j = 0; j < 4; j++) 
                        { 
                            Bytes.pack4(buf.arr, offs, bits[j]);
                            offs += 4;
                        }
                        break;
                    }

                    case ClassDescriptor.FieldType.tpGuid:
                        buf.extend(offs + 16);
                        Array.Copy(buf.arr, offs, ((Guid)f.GetValue(obj)).ToByteArray(), 0, 16);
                        offs += 16;
                        break;

                    case ClassDescriptor.FieldType.tpDate: 
                    {
                        buf.extend(offs + 8);
                        System.DateTime d = (System.DateTime) f.GetValue(obj);
                        Bytes.pack8(buf.arr, offs, d.Ticks);
                        offs += 8;
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpString: 
                    {
                        System.String s = (System.String) f.GetValue(obj);
                        if (s == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = s.Length;
                            buf.extend(offs + 4 + len * 2);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                Bytes.pack2(buf.arr, offs, (short) s[j]);
                                offs += 2;
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpObject: 
                    {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, swizzle((IPersistent) f.GetValue(obj)));
                        offs += 4;
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpValue:
                    {
                        Object value = f.GetValue(obj);
                        offs = packObject(value, fd.valueDesc, offs, buf);
                        continue;
                    }
 
#if SUPPORT_RAW_TYPE
                    case ClassDescriptor.FieldType.tpRaw:
                    {
                        Object raw = f.GetValue(obj);
                        if (raw == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            System.IO.MemoryStream ms = new System.IO.MemoryStream();
                            objectFormatter.Serialize(ms, raw);
                            ms.Close();
                            byte[] arr = ms.ToArray();
                            int len = arr.Length;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            Array.Copy(arr, 0, buf.arr, offs, len);
                            offs += len;
                        }
                        continue;
                    }
#endif
                    case ClassDescriptor.FieldType.tpArrayOfByte: 
                    {
                        byte[] arr = (byte[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            Array.Copy(arr, 0, buf.arr, offs, len);
                            offs += len;
                        }
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfSByte: 
                    {
                        sbyte[] arr = (sbyte[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfBoolean: 
                    {
                        bool[] arr = (bool[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++, offs++)
                            {
                                buf.arr[offs] = (byte) (arr[j]?1:0);
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfShort: 
                    {
                        short[] arr = (short[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfUShort: 
                    {
                        ushort[] arr = (ushort[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfChar: 
                    {
                        char[] arr = (char[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len * 2);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                Bytes.pack2(buf.arr, offs, (short) arr[j]);
                                offs += 2;
                            }
                        }
                        continue;
                    }

                    case ClassDescriptor.FieldType.tpArrayOfEnum: 
                    {
                        Array arr = (Array)f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
 					
                    case ClassDescriptor.FieldType.tpArrayOfInt: 
                    {
                        int[] arr = (int[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfUInt: 
                    {
                        uint[] arr = (uint[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfLong: 
                    {
                        long[] arr = (long[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfULong: 
                    {
                        ulong[] arr = (ulong[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfFloat: 
                    {
                        float[] arr = (float[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes(arr[j]), 0));
                                offs += 4;
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfDouble: 
                    {
                        double[] arr = (double[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
#if COMPACT_NET_FRAMEWORK 
                                Bytes.pack8(buf.arr, offs, BitConverter.ToInt64(BitConverter.GetBytes(arr[j]), 0));
#else
                                Bytes.pack8(buf.arr, offs, BitConverter.DoubleToInt64Bits(arr[j]));
#endif
                                offs += 8;
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfDate: 
                    {
                        System.DateTime[] arr = (System.DateTime[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                System.DateTime d = arr[j];
                                Bytes.pack8(buf.arr, offs, d.Ticks);
                                offs += 8;
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfString: 
                    {
                        System.String[] arr = (System.String[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                System.String str = (System.String) arr[j];
                                if (str == null)
                                {
                                    Bytes.pack4(buf.arr, offs, - 1);
                                    offs += 4;
                                }
                                else
                                {
                                    int strlen = str.Length;
                                    buf.extend(offs + 4 + strlen * 2);
                                    Bytes.pack4(buf.arr, offs, strlen);
                                    offs += 4;
                                    for (int k = 0; k < strlen; k++)
                                    {
                                        Bytes.pack2(buf.arr, offs, (short) str[k]);
                                        offs += 2;
                                    }
                                }
                            }
                        }
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfObject: 
                    {
                        IPersistent[] arr = (IPersistent[]) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
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
                        continue;
                    }
					
                    case ClassDescriptor.FieldType.tpArrayOfValue: 
                    {
                        Array arr = (Array) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            ClassDescriptor elemDesc = fd.valueDesc;
                            for (int j = 0; j < len; j++)
                            {
                                offs = packObject(arr.GetValue(i), elemDesc, offs, buf);
                            }
                        }
                        continue;
                    }

#if SUPPORT_RAW_TYPE
                    case ClassDescriptor.FieldType.tpArrayOfRaw: 
                    {
                        Array arr = (Array) f.GetValue(obj);
                        if (arr == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = arr.Length;
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            ClassDescriptor elemDesc = fd.valueDesc;
                            for (int j = 0; j < len; j++)
                            {
                                Object raw = arr.GetValue(i);
                                if (raw == null)
                                {
                                    buf.extend(offs + 4);
                                    Bytes.pack4(buf.arr, offs, - 1);
                                    offs += 4;
                                }
                                else
                                {
                                    System.IO.MemoryStream ms = new System.IO.MemoryStream();
                                    objectFormatter.Serialize(ms, raw);
                                    ms.Close();
                                    byte[] rawarr = ms.ToArray();
                                    int rawlen = rawarr.Length;
                                    buf.extend(offs + 4 + rawlen);
                                    Bytes.pack4(buf.arr, offs, rawlen);
                                    offs += 4;
                                    Array.Copy(rawarr, 0, buf.arr, offs, rawlen);
                                    offs += len;
                                }
                            }
                        }
                        continue;
                    }
#endif		
                    case ClassDescriptor.FieldType.tpLink: 
                    {
                        Link link = (Link) f.GetValue(obj);
                        if (link == null)
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            int len = link.Length;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++)
                            {
                                Bytes.pack4(buf.arr, offs, swizzle(link.GetRaw(j)));
                                offs += 4;
                            }
                        }
                        continue;
                    }
					
                }
            }
            ObjectHeader.setSize(buf.arr, 0, offs);
            ObjectHeader.setType(buf.arr, 0, desc.Oid);
            return offs;
        }
		
        private int  initIndexSize        = dbDefaultInitIndexSize;
        private int  objectCacheInitSize  = dbDefaultObjectCacheInitSize;
        private long extensionQuantum     = dbDefaultExtensionQuantum;

        internal PagePool pool;
        internal Header   header; // base address of database file mapping
        internal int[]    dirtyPagesMap; // bitmap of changed pages in current index
        internal bool     modified;
		
        internal int currRBitmapPage; //current bitmap page for allocating records
        internal int currRBitmapOffs; //offset in current bitmap page for allocating 
        //unaligned records
        internal int currPBitmapPage; //current bitmap page for allocating page objects
        internal int currPBitmapOffs; //offset in current bitmap page for allocating 
        //page objects
        internal Location reservedChain;
		
        internal int committedIndexSize;
        internal int currIndexSize;
        
#if COMPACT_NET_FRAMEWORK
        internal static ArrayList assemblies;
#else
        int       nNestedTransactions;
        int       nBlockedTransactions;
        int       nCommittedTransactions;
        long      scheduledCommitTime;
        object    transactionMonitor;
        PersistentResource transactionLock;
#endif

#if SUPPORT_RAW_TYPE
        internal System.Runtime.Serialization.Formatters.Binary.BinaryFormatter objectFormatter;
#endif	
        internal int currIndex; // copy of header.root, used to allow read access to the database 
        // during transaction commit
        internal long usedSize; // total size of allocated objects since the beginning of the session
        internal int[] bitmapPageAvailableSpace;
        internal bool opened;
		
        internal int[]     greyBitmap; // bitmap of visited during GC but not yet marked object
        internal int[]     blackBitmap;    // bitmap of objects marked during GC 
        internal long      gcThreshold;
        internal long      allocatedDelta;
        internal bool      gcDone;

        internal OidHashTable     objectCache;
        internal Hashtable        classDescMap;
        internal ClassDescriptor  descList;
    }
	
    class RootPage
    {
        internal long size; // database file size
        internal long index; // offset to object index
        internal long shadowIndex; // offset to shadow index
        internal long usedSize; // size used by objects
        internal int indexSize; // size of object index
        internal int shadowIndexSize; // size of object index
        internal int indexUsed; // userd part of the index   
        internal int freeList; // L1 list of free descriptors
        internal int bitmapEnd; // index of last allocated bitmap page
        internal int rootObject; // OID of root object
        internal int classDescList; // List of class descriptors
        internal int reserved;
		
        internal const int Sizeof = 64;
    }
	
    class Header
    {
        internal int curr; // current root
        internal bool dirty; // database was not closed normally
        internal bool initialized; // database is initilaized
		
        internal RootPage[] root;
		
        internal static int Sizeof = 3 + RootPage.Sizeof * 2;
		
        internal void  pack(byte[] rec)
        {
            int offs = 0;
            rec[offs++] = (byte) curr;
            rec[offs++] = (byte) (dirty?1:0);
            rec[offs++] = (byte) (initialized?1:0);
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
                offs += 8;
            }
        }
		
        internal void  unpack(byte[] rec)
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
                offs += 8;
            }
        }
    }
}