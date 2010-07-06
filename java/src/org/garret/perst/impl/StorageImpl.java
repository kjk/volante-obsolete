package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.lang.reflect.*;
import  java.util.HashMap;
import  java.util.Date;

public class StorageImpl extends Storage { 
    /**
     * Initialial database index size - increasing it reduce number of inde reallocation but increase
     * initial database size. Should be set before openning connection.
     */
    static final int  dbDefaultInitIndexSize = 1024;

    /**
     * Initial capacity of object hash
     */
    static final int dbObjectCacheInitSize = 1319;

    /**
     * Database extension quantum. Memory is allocate by scanning bitmap. If there is no
     * large enough hole, then database is extended by the value of dbDefaultExtensionQuantum 
     * This parameter should not be smaller than dbFirstUserId
     */
    static final long dbDefaultExtensionQuantum = 4*1024*1024;

    static final int  dbDatabaseOffsetBits = 32;  // up to 1 gigabyte, 37 - up to 1 terabyte database

    static final int  dbAllocationQuantumBits = 6;
    static final int  dbAllocationQuantum = 1 << dbAllocationQuantumBits;
    static final int  dbBitmapSegmentBits = Page.pageBits + 3 + dbAllocationQuantumBits;
    static final int  dbBitmapSegmentSize = 1 << dbBitmapSegmentBits;
    static final int  dbBitmapPages = 1 << (dbDatabaseOffsetBits-dbBitmapSegmentBits);
    static final int  dbHandlesPerPageBits = Page.pageBits - 3;
    static final int  dbHandlesPerPage = 1 << dbHandlesPerPageBits;
    static final int  dbDirtyPageBitmapSize = 1 << (32-Page.pageBits-3);

    static final int  dbInvalidId   = 0;
    static final int  dbBitmapId    = 1;
    static final int  dbFirstUserId = dbBitmapId + dbBitmapPages;
    
    static final int  dbPageObjectFlag = 1;
    static final int  dbModifiedFlag   = 2;
    static final int  dbFreeHandleFlag = 4;
    static final int  dbFlagsMask      = 7;
    static final int  dbFlagsBits      = 3;


    final long getPos(int oid) { 
	if (oid == 0 && oid >= currIndexSize) { 
	    throw new StorageError(StorageError.INVALID_OID);
	}
	Page pg = pool.getPage(header.root[1-currIndex].index 
			       + (oid >>> dbHandlesPerPageBits << Page.pageBits));
	long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3);
	pool.unfix(pg);
	return pos;
    }
    
    final void setPos(int oid, long pos) { 
        if (oid == 1 && (pos &  dbPageObjectFlag) == 0) {
            throw new Error("Something is definitly wrong");
        }
	dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
	    |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
	Page pg = pool.putPage(header.root[1-currIndex].index 
			       + (oid >>> dbHandlesPerPageBits << Page.pageBits));
	Bytes.pack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3, pos);
	pool.unfix(pg);
    }

    final byte[] get(int oid) { 
	long pos = getPos(oid);
	if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
	    throw new StorageError(StorageError.INVALID_OID);
	}
	return pool.get(pos & ~dbFlagsMask);
    }
    
    final Page getPage(int oid) {  
	long pos = getPos(oid);
	if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != dbPageObjectFlag) { 
	    throw new StorageError(StorageError.DELETED_OBJECT);
	}
	return pool.getPage(pos & ~dbFlagsMask);
    }

    final Page putPage(int oid) {  
	long pos = getPos(oid);
	if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != dbPageObjectFlag) { 
	    throw new StorageError(StorageError.DELETED_OBJECT);
	}
	if ((pos & dbModifiedFlag) == 0) { 
	    dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
		|= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
	    allocate(Page.pageSize, oid);
	    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
	    pos = getPos(oid);
	}
	modified = true;
	return pool.putPage(pos & ~dbFlagsMask);
    }


    int allocatePage() { 
	int oid = allocateId();
	setPos(oid, allocate(Page.pageSize, 0) | dbPageObjectFlag | dbModifiedFlag);
	return oid;
    }

    protected synchronized void deallocateObject(IPersistent obj) 
    {
        int oid = obj.getOid();
        if (oid == 0) { 
            return;
        }
	long pos = getPos(oid);
	int offs = (int)pos & (Page.pageSize-1);
	if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
	    throw new StorageError(StorageError.DELETED_OBJECT);
	}
	Page pg = pool.getPage(pos - offs);
	offs &= ~dbFlagsMask;
	int size = ObjectHeader.getSize(pg.data, offs);
	pool.unfix(pg);
        freeId(oid);
	if ((pos & dbModifiedFlag) != 0) { 
	    free(pos & ~dbFlagsMask, size);
	} else { 
	    cloneBitmap(pos, size);
	}
	modified = true;
        setObjectOid(obj, 0, false);
    }

    final void freePage(int oid) {
	long pos = getPos(oid);
	Assert.that((pos & (dbFreeHandleFlag|dbPageObjectFlag)) == dbPageObjectFlag);
 	if ((pos & dbModifiedFlag) != 0) { 
	    free(pos & ~dbFlagsMask, Page.pageSize);
	} else { 
	    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
	} 
        freeId(oid);
    }

    final void updatePage(int oid, byte[] pageBody) { 
	long pos = getPos(oid);
	if ((pos & dbModifiedFlag) == 0) { 
	    cloneBitmap(pos & ~dbFlagsMask, pageBody.length);
	    pos = allocate(pageBody.length, 0);
	    setPos(oid, pos | dbModifiedFlag);
	}	
	modified = true;
	pool.put(pos & ~dbFlagsMask, pageBody);
    }

    
    int allocateId() {
	int oid;
	int curr = 1-currIndex;
        setDirty();
	if ((oid = header.root[curr].freeList) != 0) { 
	    header.root[curr].freeList = (int)(getPos(oid) >> dbFlagsBits);
	    dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
		|= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
	    return oid;
	}

	if (currIndexSize + 1 > header.root[curr].indexSize) {
	    int oldIndexSize = header.root[curr].indexSize;
	    int newIndexSize = oldIndexSize * 2;
	    while (newIndexSize < oldIndexSize + 1) { 
		newIndexSize = newIndexSize*2;
	    }
	    long newIndex = allocate(newIndexSize * 8, 0);
	    pool.copy(newIndex, header.root[curr].index, currIndexSize*8);
	    free(header.root[curr].index, oldIndexSize*8);
	    header.root[curr].index = newIndex;
	    header.root[curr].indexSize = newIndexSize;
	}
	oid = currIndexSize;
	header.root[curr].indexUsed = ++currIndexSize;
	return oid;
    }
    
    void freeId(int oid)
    {
	setPos(oid, ((long)(header.root[1-currIndex].freeList) << dbFlagsBits)
	       | dbFreeHandleFlag);
	header.root[1-currIndex].freeList = oid;
    }
    
    final static byte firstHoleSize [] = {
	8,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	7,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,
	5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0
    };
    final static byte lastHoleSize [] = {
        8,7,6,6,5,5,5,5,4,4,4,4,4,4,4,4,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,
	2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    };
    final static byte maxHoleSize [] = {
	8,7,6,6,5,5,5,5,4,4,4,4,4,4,4,4,4,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,
	5,4,3,3,2,2,2,2,3,2,2,2,2,2,2,2,4,3,2,2,2,2,2,2,3,2,2,2,2,2,2,2,
	6,5,4,4,3,3,3,3,3,2,2,2,2,2,2,2,4,3,2,2,2,1,1,1,3,2,1,1,2,1,1,1,
	5,4,3,3,2,2,2,2,3,2,1,1,2,1,1,1,4,3,2,2,2,1,1,1,3,2,1,1,2,1,1,1,
	7,6,5,5,4,4,4,4,3,3,3,3,3,3,3,3,4,3,2,2,2,2,2,2,3,2,2,2,2,2,2,2,
	5,4,3,3,2,2,2,2,3,2,1,1,2,1,1,1,4,3,2,2,2,1,1,1,3,2,1,1,2,1,1,1,
	6,5,4,4,3,3,3,3,3,2,2,2,2,2,2,2,4,3,2,2,2,1,1,1,3,2,1,1,2,1,1,1,
	5,4,3,3,2,2,2,2,3,2,1,1,2,1,1,1,4,3,2,2,2,1,1,1,3,2,1,1,2,1,1,0
    };
    final static byte maxHoleOffset [] = {
	0,1,2,2,3,3,3,3,4,4,4,4,4,4,4,4,0,1,5,5,5,5,5,5,0,5,5,5,5,5,5,5,
        0,1,2,2,0,3,3,3,0,1,6,6,0,6,6,6,0,1,2,2,0,6,6,6,0,1,6,6,0,6,6,6,
        0,1,2,2,3,3,3,3,0,1,4,4,0,4,4,4,0,1,2,2,0,1,0,3,0,1,0,2,0,1,0,5,
	0,1,2,2,0,3,3,3,0,1,0,2,0,1,0,4,0,1,2,2,0,1,0,3,0,1,0,2,0,1,0,7,
	0,1,2,2,3,3,3,3,0,4,4,4,4,4,4,4,0,1,2,2,0,5,5,5,0,1,5,5,0,5,5,5,
	0,1,2,2,0,3,3,3,0,1,0,2,0,1,0,4,0,1,2,2,0,1,0,3,0,1,0,2,0,1,0,6,
	0,1,2,2,3,3,3,3,0,1,4,4,0,4,4,4,0,1,2,2,0,1,0,3,0,1,0,2,0,1,0,5,
	0,1,2,2,0,3,3,3,0,1,0,2,0,1,0,4,0,1,2,2,0,1,0,3,0,1,0,2,0,1,0,0
    };

    static final int pageBits = Page.pageSize*8;
    static final int inc = Page.pageSize/dbAllocationQuantum/8;

    static final void memset(Page pg, int offs, int pattern, int len) { 
	byte[] arr = pg.data;
	byte pat = (byte)pattern;
	while (--len >= 0) { 
	    arr[offs++] = pat;
	}
    }

    final void extend(long size)
    {
	if (size > header.root[1-currIndex].size) { 
	    header.root[1-currIndex].size = size;
	}
    }

    static class Location { 
	long     pos;
	int      size;
	Location next;
    }

    final boolean wasReserved(long pos, int size) 
    {
	for (Location location = reservedChain; location != null; location = location.next) { 
	    if ((pos >= location.pos && pos - location.pos < location.size) 
		|| (pos <= location.pos && location.pos - pos < size)) 
	    {
		return true;
	    }
	}
	return false;
    }

    final void reserveLocation(long pos, int size)
    {
	Location location = new Location();
	location.pos = pos;
	location.size = size;
	location.next = reservedChain;
	reservedChain = location;
    }

    final void commitLocation()
    {
	reservedChain = reservedChain.next;
    }


    final void setDirty() 
    {
        modified = true;
        if (!header.dirty) { 
            header.dirty = true;
	    Page pg = pool.putPage(0);
	    header.pack(pg.data);
	    pool.flush();
	    pool.unfix(pg);
        }
    }

    final long allocate(int size, int oid)
    {
        setDirty();
	size = (size + dbAllocationQuantum-1) & ~(dbAllocationQuantum-1);
	Assert.that(size != 0);
	int  objBitSize = size >> dbAllocationQuantumBits;
	long pos;    
	int  holeBitSize = 0;
	int  alignment = size & (Page.pageSize-1);
	int  offs, firstPage, lastPage, i;
	int  holeBeforeFreePage  = 0;
	int  freeBitmapPage = 0;
	Page pg;

	lastPage = header.root[1-currIndex].bitmapEnd;
	usedSize += size;

	if (alignment == 0) { 
	    firstPage = currPBitmapPage;
	    offs = (currPBitmapOffs+inc-1) & ~(inc-1);
	} else { 
	    firstPage = currRBitmapPage;
	    offs = currRBitmapOffs;
	}
	
	while (true) { 
	    if (alignment == 0) { 
		// allocate page object 
		for (i = firstPage; i < lastPage; i++){
		    int spaceNeeded = objBitSize - holeBitSize < pageBits 
			? objBitSize - holeBitSize : pageBits;
		    if (bitmapPageAvailableSpace[i] <= spaceNeeded) {
			holeBitSize = 0;
			offs = 0;
			continue;
		    }
		    pg = getPage(i);
		    int startOffs = offs;	
		    while (offs < Page.pageSize) { 
			if (pg.data[offs++] != 0) { 
			    offs = (offs + inc - 1) & ~(inc-1);
			    holeBitSize = 0;
			} else if ((holeBitSize += 8) == objBitSize) { 
			    pos = (((long)(i-dbBitmapId)*Page.pageSize + offs)*8 
				   - holeBitSize) << dbAllocationQuantumBits;
			    if (wasReserved(pos, size)) { 
				offs += objBitSize >> 3;
				startOffs = offs = (offs + inc - 1) & ~(inc-1);
				holeBitSize = 0;
				continue;
			    }	
			    reserveLocation(pos, size);
			    currPBitmapPage = i;
			    currPBitmapOffs = offs;
			    extend(pos + size);
			    if (oid != 0) { 
				long prev = getPos(oid);
				int marker = (int)prev & dbFlagsMask;
				pool.copy(pos, prev - marker, size);
				setPos(oid, pos | marker | dbModifiedFlag);
			    }
			    pool.unfix(pg);
			    pg = putPage(i);
			    int holeBytes = holeBitSize >> 3;
			    if (holeBytes > offs) { 
				memset(pg, 0, 0xFF, offs);
				holeBytes -= offs;
				pool.unfix(pg);
				pg = putPage(--i);
				offs = Page.pageSize;
			    }
			    while (holeBytes > Page.pageSize) { 
				memset(pg, 0, 0xFF, Page.pageSize);
				holeBytes -= Page.pageSize;
				bitmapPageAvailableSpace[i] = 0;
				pool.unfix(pg);
				pg = putPage(--i);
			    }
			    memset(pg, offs-holeBytes, 0xFF, holeBytes);
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
	    } else { 
		for (i = firstPage; i < lastPage; i++){
		    int spaceNeeded = objBitSize - holeBitSize < pageBits 
			? objBitSize - holeBitSize : pageBits;
		    if (bitmapPageAvailableSpace[i] <= spaceNeeded) {
			holeBitSize = 0;
			offs = 0;
			continue;
		    }
		    pg = getPage(i);
		    int startOffs = offs;
		    while (offs < Page.pageSize) { 
			int mask = pg.data[offs] & 0xFF; 
			if (holeBitSize + firstHoleSize[mask] >= objBitSize) { 
			    pos = (((long)(i-dbBitmapId)*Page.pageSize + offs)*8 
				   - holeBitSize) << dbAllocationQuantumBits;
			    if (wasReserved(pos, size)) { 			    
				startOffs = offs += (objBitSize + 7) >> 3;
				holeBitSize = 0;
				continue;
			    }	
			    reserveLocation(pos, size);
			    currRBitmapPage = i;
			    currRBitmapOffs = offs;
			    extend(pos + size);
			    if (oid != 0) { 
				long prev = getPos(oid);
				int marker = (int)prev & dbFlagsMask;
				pool.copy(pos, prev - marker, size);
				setPos(oid, pos | marker | dbModifiedFlag);
			    }
			    pool.unfix(pg);
			    pg = putPage(i);
			    pg.data[offs] |= (byte)((1 << (objBitSize - holeBitSize)) - 1); 
			    if (holeBitSize != 0) { 
				if (holeBitSize > offs*8) { 
				    memset(pg, 0, 0xFF, offs);
				    holeBitSize -= offs*8;
				    pool.unfix(pg);
				    pg = putPage(--i);
				    offs = Page.pageSize;
				}
				while (holeBitSize > pageBits) { 
				    memset(pg, 0, 0xFF, Page.pageSize);
				    holeBitSize -= pageBits;
				    bitmapPageAvailableSpace[i] = 0;
				    pool.unfix(pg);
				    pg = putPage(--i);
				}
				while ((holeBitSize -= 8) > 0) { 
				    pg.data[--offs] = (byte)0xFF; 
				}
				pg.data[offs-1] |= (byte)~((1 << -holeBitSize) - 1);
			    }
			    pool.unfix(pg);
			    commitLocation();
			    return pos;
			} else if (maxHoleSize[mask] >= objBitSize) { 
			    int holeBitOffset = maxHoleOffset[mask];
			    pos = (((long)(i-dbBitmapId)*Page.pageSize + offs)*8 + 
				   holeBitOffset) << dbAllocationQuantumBits;
			    if (wasReserved(pos, size)) { 
				startOffs = offs += (objBitSize + 7) >> 3;
				holeBitSize = 0;
				continue;
			    }	
			    reserveLocation(pos, size);
			    currRBitmapPage = i;
			    currRBitmapOffs = offs;
			    extend(pos + size);
			    if (oid != 0) { 
				long prev = getPos(oid);
				int marker = (int)prev & dbFlagsMask;
				pool.copy(pos, prev - marker, size);
				setPos(oid, pos | marker | dbModifiedFlag);
			    }
			    pool.unfix(pg);
			    pg = putPage(i);
			    pg.data[offs] |= (byte)((1<<objBitSize) - 1) << holeBitOffset;
			    pool.unfix(pg);
			    commitLocation();
			    return pos;
			}
			offs += 1;
			if (lastHoleSize[mask] == 8) { 
			    holeBitSize += 8;
			} else { 
			    holeBitSize = lastHoleSize[mask];
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
	    if (firstPage == dbBitmapId) { 
		if (freeBitmapPage > i) { 
		    i = freeBitmapPage;
		    holeBitSize = holeBeforeFreePage;
		}
		if (i == dbBitmapId + dbBitmapPages) { 
		    throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
		}
		long extension = (size > dbDefaultExtensionQuantum) 
		    ? size : dbDefaultExtensionQuantum;
		int morePages = (int) 
		    ((extension + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
		     / (Page.pageSize*(dbAllocationQuantum*8-1)));
		
		if (i + morePages > dbBitmapId + dbBitmapPages) { 
		    morePages = (int)  
			((size + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
			 / (Page.pageSize*(dbAllocationQuantum*8-1)));
		    if (i + morePages > dbBitmapId + dbBitmapPages) { 
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
		    }
		}
		objBitSize -= holeBitSize;
		int skip = (objBitSize + Page.pageSize/dbAllocationQuantum - 1) 
		    & ~(Page.pageSize/dbAllocationQuantum - 1);
		pos = ((long)(i-dbBitmapId) 
		       << (Page.pageBits+dbAllocationQuantumBits+3)) 
		    + (skip << dbAllocationQuantumBits);
		extend(pos + morePages*Page.pageSize);
		int len = objBitSize >> 3;
		long adr = pos;
		while (len >= Page.pageSize) { 
		    pg = pool.putPage(adr);
		    memset(pg, 0, 0xFF, Page.pageSize);
		    pool.unfix(pg);
		    adr += Page.pageSize;
		    len -= Page.pageSize;
		}
		pg = pool.putPage(adr);
		memset(pg, 0, 0xFF, len);
		pg.data[len] = (byte)((1 << (objBitSize&7))-1);
		pool.unfix(pg);
		adr = pos + (skip>>3);
		len = morePages * (Page.pageSize/dbAllocationQuantum/8);
		while (true) { 
		    int off = (int)adr & (Page.pageSize-1);
		    pg = pool.putPage(adr - off);
		    if (Page.pageSize - off >= len) { 
			memset(pg, off, 0xFF, len);
			pool.unfix(pg);
			break;
		    } else { 
			memset(pg, off, 0xFF, Page.pageSize - off);
			pool.unfix(pg);
			adr += Page.pageSize - off;
			len -= Page.pageSize - off;
		    }
		}
		int j = i;
		while (--morePages >= 0) { 
		    setPos(j++, pos | dbPageObjectFlag | dbModifiedFlag);
		    pos += Page.pageSize;
		}
		freeBitmapPage = header.root[1-currIndex].bitmapEnd = j;
		j = i + objBitSize / pageBits; 
		if (alignment != 0) { 
		    currRBitmapPage = j;
		    currRBitmapOffs = 0;
		} else { 
		    currPBitmapPage = j;
		    currPBitmapOffs = 0;
		}
		while (j > i) { 
		    bitmapPageAvailableSpace[--j] = 0;
		}
		
		pos = ((long)(i-dbBitmapId)*Page.pageSize*8 - holeBitSize)
		    << dbAllocationQuantumBits;
		if (oid != 0) { 
		    long prev = getPos(oid);
		    int marker = (int)prev & dbFlagsMask;
		    pool.copy(pos, prev - marker, size);
		    setPos(oid, pos | marker | dbModifiedFlag);
		}
		
		if (holeBitSize != 0) { 
		    reserveLocation(pos, size);
		    while (holeBitSize > pageBits) { 
			holeBitSize -= pageBits;
			pg = putPage(--i);
			memset(pg, 0, 0xFF, Page.pageSize);
			bitmapPageAvailableSpace[i] = 0;
			pool.unfix(pg);
		    }
		    pg = putPage(--i);
		    offs = Page.pageSize;
		    while ((holeBitSize -= 8) > 0) { 
			pg.data[--offs] = (byte)0xFF; 
		    }
		    pg.data[offs-1] |= (byte)~((1 << -holeBitSize) - 1);
		    pool.unfix(pg);
		    commitLocation();
		}
		return pos;
	    } 
	    freeBitmapPage = i;
	    holeBeforeFreePage = holeBitSize;
	    holeBitSize = 0;
	    lastPage = firstPage + 1;
	    firstPage = dbBitmapId;
	    offs = 0;
	}
    } 



    final void free(long pos, int size)
    {
	Assert.that(pos != 0 && (pos & (dbAllocationQuantum-1)) == 0);
	long quantNo = pos >>> dbAllocationQuantumBits;
	int  objBitSize = (size+dbAllocationQuantum-1) >>> dbAllocationQuantumBits;
	int  pageId = dbBitmapId + (int)(quantNo >>> (Page.pageBits+3));
	int  offs = (int)(quantNo & (Page.pageSize*8-1)) >> 3;
	Page pg = putPage(pageId);
	int  bitOffs = (int)quantNo & 7;

	usedSize -= objBitSize << dbAllocationQuantumBits;

	if ((pos & (Page.pageSize-1)) == 0 && size >= Page.pageSize) { 
	    if (pageId == currPBitmapPage && offs < currPBitmapOffs) { 
		currPBitmapOffs = offs;
	    }
	} else { 
	    if (pageId == currRBitmapPage && offs < currRBitmapOffs) { 
		currRBitmapOffs = offs;
	    }
	}
	if (pageId == currRBitmapPage && offs < currRBitmapOffs) { 
	    currRBitmapOffs = offs;
	}
	bitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
	
	if (objBitSize > 8 - bitOffs) { 
	    objBitSize -= 8 - bitOffs;
	    pg.data[offs++] &= (1 << bitOffs) - 1;
	    while (objBitSize + offs*8 > Page.pageSize*8) { 
		memset(pg, offs, 0, Page.pageSize - offs);
		pool.unfix(pg);
		pg = putPage(++pageId);
		bitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
		objBitSize -= (Page.pageSize - offs)*8;
		offs = 0;
	    }
	    while ((objBitSize -= 8) > 0) { 
		pg.data[offs++] = (byte)0;
	    }
	    pg.data[offs] &= (byte)~((1 << (objBitSize + 8)) - 1);
	} else { 
	    pg.data[offs] &= (byte)~(((1 << objBitSize) - 1) << bitOffs); 
	}
	pool.unfix(pg);
    }

    final void cloneBitmap(long pos, int size)
    {
	long quantNo = pos >>> dbAllocationQuantumBits;
	long objBitSize = (size+dbAllocationQuantum-1) >>> dbAllocationQuantumBits;
	int  pageId = dbBitmapId + (int)(quantNo >>> (Page.pageBits + 3));
	int  offs = (int)(quantNo & (Page.pageSize*8-1)) >> 3;
	int  bitOffs = (int)quantNo & 7;
	int  oid = pageId;
	pos = getPos(oid);
	if ((pos & dbModifiedFlag) == 0) { 
	    dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
		|= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
	    allocate(Page.pageSize, oid);
	    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
	}
	
	if (objBitSize > 8 - bitOffs) { 
	    objBitSize -= 8 - bitOffs;
	    offs += 1;
	    while (objBitSize + offs*8 > Page.pageSize*8) { 
		oid = ++pageId;
		pos = getPos(oid);
		if ((pos & dbModifiedFlag) == 0) { 
		    dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
			|= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
		    allocate(Page.pageSize, oid);
		    cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
		}
		objBitSize -= (Page.pageSize - offs)*8;
		offs = 0;
	    }
	}
    }

    public synchronized void open(String filePath, int pagePoolSize) {
        if (opened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }
	Page pg;
	int i;
	int indexSize = dbDefaultInitIndexSize;
	if (indexSize < dbFirstUserId) { 
	    indexSize = dbFirstUserId;
	}
	indexSize = (indexSize + dbHandlesPerPage - 1) & ~(dbHandlesPerPage-1);

	dirtyPagesMap = new int[dbDirtyPageBitmapSize/4+1];
	bitmapPageAvailableSpace = new int[dbBitmapId + dbBitmapPages];
 	for (i = dbBitmapId + dbBitmapPages; --i >= 0;) { 
	    bitmapPageAvailableSpace[i] = Integer.MAX_VALUE;
	}
	
	currRBitmapPage = currPBitmapPage = dbBitmapId;
	currRBitmapOffs = currPBitmapOffs = 0;
	modified = false;        
        if (pagePoolSize == 0) { 
            pagePoolSize = Page.pageSize*256;
        }
        pool = new PagePool(pagePoolSize/Page.pageSize);

        objectCache = new WeakHashTable(dbObjectCacheInitSize);
        classDescMap = new HashMap();
        descList = null;

	FileIO file = new FileIO(filePath);
	header = new Header();
	byte[] buf = new byte[Header.sizeof];
	int rc = file.read(0, buf);
	if (rc > 0 && rc < Header.sizeof) { 
	    throw new StorageError(StorageError.DATABASE_CORRUPTED);
	}
	header.unpack(buf);
	if (header.curr < 0 || header.curr > 1) { 
	    throw new StorageError(StorageError.DATABASE_CORRUPTED);
	}
	if (!header.initialized) {	    
	    header.curr = currIndex = 0;
	    long used = Page.pageSize;
	    header.root[0].index = used;
	    header.root[0].indexSize = indexSize;
	    header.root[0].indexUsed = dbFirstUserId;
	    header.root[0].freeList = 0;
	    used += indexSize*8;
	    header.root[1].index = used;
	    header.root[1].indexSize = indexSize;
	    header.root[1].indexUsed = dbFirstUserId;
	    header.root[1].freeList = 0;
	    used += indexSize*8;
	
	    header.root[0].shadowIndex = header.root[1].index;
	    header.root[1].shadowIndex = header.root[0].index;
	    header.root[0].shadowIndexSize = indexSize;
	    header.root[1].shadowIndexSize = indexSize;
	    
	    int bitmapPages = 
		(int)((used + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
		      / (Page.pageSize*(dbAllocationQuantum*8-1)));
	    int bitmapSize = bitmapPages*Page.pageSize;
	    int usedBitmapSize = (int)((used + bitmapSize) >>> (dbAllocationQuantumBits + 3));

	    byte[] bitmap = new byte[bitmapSize];
	    for (i = 0; i < usedBitmapSize; i++) { 
		bitmap[i] = (byte)0xFF;
	    }
	    file.write(used, bitmap);
	    int bitmapIndexSize = 
		((dbBitmapId + dbBitmapPages)*8 + Page.pageSize - 1)
		& ~(Page.pageSize - 1);
	    byte[] index = new byte[bitmapIndexSize];
	    Bytes.pack8(index, dbInvalidId*8, dbFreeHandleFlag);
	    for (i = 0; i < bitmapPages; i++) { 
		Bytes.pack8(index, (dbBitmapId+i)*8, used | dbPageObjectFlag | dbModifiedFlag);
		used += Page.pageSize;
	    }
	    header.root[0].bitmapEnd = dbBitmapId + i;
	    header.root[1].bitmapEnd = dbBitmapId + i;
	    while (i < dbBitmapPages) { 
		Bytes.pack8(index, (dbBitmapId+i)*8, dbFreeHandleFlag);
		i += 1;
	    }
	    file.write(header.root[1].index, index);
	    header.root[0].size = used;
	    header.root[1].size = used;
            usedSize = used;
	    committedIndexSize = currIndexSize = dbFirstUserId;
	    pool.open(file, used);

	    long indexPage = header.root[1].index;
	    long lastIndexPage = indexPage + header.root[1].bitmapEnd*8;
	    while (indexPage < lastIndexPage) { 
		pg = pool.putPage(indexPage);
		for (i = 0; i < Page.pageSize; i += 8) { 
		    Bytes.pack8(pg.data, i, 
				Bytes.unpack8(pg.data, i) & ~dbModifiedFlag);
		}
		pool.unfix(pg);
		indexPage += Page.pageSize;
	    }
	    pool.copy(header.root[0].index, header.root[1].index, currIndexSize*8);
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
	} else {
	    int curr = header.curr;
	    currIndex = curr;
	    if (header.root[curr].indexSize != header.root[curr].shadowIndexSize) {
                throw new StorageError(StorageError.DATABASE_CORRUPTED);
	    }		
	    pool.open(file, header.root[curr].size);
	    if (header.dirty) { 
		System.err.println("Database was not normally closed: start recovery");
		header.root[1-curr].size = header.root[curr].size;
		header.root[1-curr].indexUsed = header.root[curr].indexUsed; 
		header.root[1-curr].freeList = header.root[curr].freeList; 
		header.root[1-curr].index = header.root[curr].shadowIndex;
		currIndexSize = header.root[1-curr].indexSize = 
		    header.root[curr].shadowIndexSize;
		header.root[1-curr].shadowIndex = header.root[curr].index;
		header.root[1-curr].shadowIndexSize = 
		    header.root[curr].indexSize;
		header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd;
		header.root[1-curr].rootObject = header.root[curr].rootObject;
		header.root[1-curr].classDescList = header.root[curr].classDescList;

		pg = pool.putPage(0);
		header.pack(pg.data);
		pool.unfix(pg);

		pool.copy(header.root[1-curr].index, header.root[curr].index, 
			  (header.root[curr].indexUsed*8 + Page.pageSize - 1) & ~(Page.pageSize-1));
		System.err.println("Recovery completed");
	    }  else { 
                currIndexSize = header.root[1-curr].indexUsed;
            }
            committedIndexSize = currIndexSize;
	    usedSize = header.root[curr].size;
            reloadScheme();
	}
        opened = true;
    }

    static void checkIfFinal(ClassDescriptor desc) {
        Class cls = desc.cls;
        for (ClassDescriptor next = desc.next; next != null; next = next.next) { 
            if (cls.isAssignableFrom(next.cls)) { 
                desc.hasSubclasses = true;
            } else if (next.cls.isAssignableFrom(cls)) { 
                next.hasSubclasses = true;
            }
        }
    }
        

    final void reloadScheme() {
        classDescMap.clear();
        int descListOid = header.root[1-currIndex].classDescList;
        classDescMap.put(ClassDescriptor.class, new ClassDescriptor(ClassDescriptor.class));
        if (descListOid != 0) {             
            ClassDescriptor desc;
            descList = (ClassDescriptor)lookupObject(descListOid, ClassDescriptor.class);
            for (desc = descList; desc != null; desc = desc.next) { 
                desc.resolve();
                classDescMap.put(desc.cls, desc);
            }
            for (desc = descList; desc != null; desc = desc.next) { 
                checkIfFinal(desc);
            }
        } else { 
            descList = null;
        }
    }

    final ClassDescriptor getClassDescriptor(Class cls) { 
        ClassDescriptor desc = (ClassDescriptor)classDescMap.get(cls);
        if (desc == null) { 
            desc = new ClassDescriptor(cls);
            classDescMap.put(cls, desc);
            desc.next = descList;
            descList = desc;
            checkIfFinal(desc);
            storeObject(desc);
            header.root[1-currIndex].classDescList = desc.getOid();
            modified = true;
        }
        return desc;
    }


    public synchronized IPersistent getRoot() {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        int rootOid = header.root[1-currIndex].rootObject;
        return (rootOid == 0) ? null : lookupObject(rootOid, null);
    }
    
    public synchronized void setRoot(IPersistent root) {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (!root.isPersistent()) { 
            storeObject(root);
        }
        header.root[1-currIndex].rootObject = root.getOid();
        modified = true;
    }


    public synchronized void commit() 
    {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
	if (modified) { 
	    int curr = currIndex;
	    int i, j, n;
	    int[] map = dirtyPagesMap;
	    int oldIndexSize = header.root[curr].indexSize;
	    int newIndexSize = header.root[1-curr].indexSize;
	    int nPages = committedIndexSize >>> dbHandlesPerPageBits;
	    Page pg;
	    if (newIndexSize > oldIndexSize) { 
		long newIndex = allocate(newIndexSize*8, 0);
		header.root[1-curr].shadowIndex = newIndex;
		header.root[1-curr].shadowIndexSize = newIndexSize;
		cloneBitmap(header.root[curr].index, oldIndexSize*8);
		free(header.root[curr].index, oldIndexSize*8);
	    }
	    for (i = 0; i < nPages; i++) { 
		if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
		    Page srcIndex = pool.getPage(header.root[1-curr].index+i*Page.pageSize);
		    Page dstIndex = pool.getPage(header.root[curr].index+i*Page.pageSize);
		    for (j = 0; j < Page.pageSize; j += 8) {
			long pos = Bytes.unpack8(dstIndex.data, j);
			if (Bytes.unpack8(srcIndex.data, j) != pos) { 
			    if ((pos & dbFreeHandleFlag) == 0) {
				if ((pos & dbPageObjectFlag) != 0) {  
				    free(pos & ~dbFlagsMask, Page.pageSize);
				} else { 
				    int offs = (int)pos & (Page.pageSize-1);
				    pg = pool.getPage(pos-offs);
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
	    n = committedIndexSize & (dbHandlesPerPage-1);
	    if (n != 0 && (map[i >> 5] & (1 << (i & 31))) != 0) { 
		Page srcIndex = pool.getPage(header.root[1-curr].index+i*Page.pageSize);
		Page dstIndex = pool.getPage(header.root[curr].index+i*Page.pageSize);
		j = 0;
		do { 
		    long pos = Bytes.unpack8(dstIndex.data, j);
		    if (Bytes.unpack8(srcIndex.data, j) != pos) { 
			if ((pos & dbFreeHandleFlag) == 0) {
			    if ((pos & dbPageObjectFlag) != 0) { 
				free(pos & ~dbFlagsMask, Page.pageSize);
			    } else { 
				int offs = (int)pos & (Page.pageSize-1);
				pg = pool.getPage(pos - offs);
				free(pos, ObjectHeader.getSize(pg.data, offs));
				pool.unfix(pg);
			    }
			}
		    }
		    j += 8;
		} while (--n != 0);

		pool.unfix(srcIndex);
		pool.unfix(dstIndex);
	    }
	    for (i = 0; i <= nPages; i++) { 
		if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
		    pg = pool.putPage(header.root[1-curr].index+i*Page.pageSize);
		    for (j = 0; j < Page.pageSize; j += 8) {
			Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
		    }
		    pool.unfix(pg);
		}
	    }
	    if (currIndexSize > committedIndexSize) { 
		long page = (header.root[1-curr].index 
			   + committedIndexSize*8) & ~(Page.pageSize-1);
		long end = (header.root[1-curr].index + Page.pageSize - 1
			    + currIndexSize*8) & ~(Page.pageSize-1);
		while (page < end) { 
		    pg = pool.putPage(page);
		    for (j = 0; j < Page.pageSize; j += 8) {
			Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
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
	    header.root[1-curr].freeList  = header.root[curr].freeList; 
	    header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd; 
	    header.root[1-curr].rootObject = header.root[curr].rootObject; 
	    header.root[1-curr].classDescList = header.root[curr].classDescList; 
	    
	    if (currIndexSize == 0 || newIndexSize != oldIndexSize) {
		if (currIndexSize == 0) { 
		    currIndexSize = header.root[1-curr].indexUsed;
		}
		header.root[1-curr].index = header.root[curr].shadowIndex;
		header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
		header.root[1-curr].shadowIndex = header.root[curr].index;
		header.root[1-curr].shadowIndexSize = header.root[curr].indexSize;
		pool.copy(header.root[1-curr].index, header.root[curr].index,
			  currIndexSize*8);
		i = (currIndexSize+dbHandlesPerPage*32-1) >>> (dbHandlesPerPageBits+5);
		while (--i >= 0) { 
		    map[i] = 0;
		}
	    } else { 
		for (i = 0; i < nPages; i++) { 
		    if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
			map[i >> 5] -= (1 << (i & 31));
			pool.copy(header.root[1-curr].index + i*Page.pageSize,
				  header.root[curr].index + i*Page.pageSize,
				  Page.pageSize);
		    }
		}
		if (currIndexSize > i*dbHandlesPerPage &&
		    ((map[i >> 5] & (1 << (i & 31))) != 0
		     || currIndexSize != committedIndexSize))
		{
		    pool.copy(header.root[1-curr].index + i*Page.pageSize,
			      header.root[curr].index + i*Page.pageSize,
			      8*currIndexSize - i*Page.pageSize);
		    j = i>>>5;
		    n = (currIndexSize + dbHandlesPerPage*32 - 1) >>> (dbHandlesPerPageBits+5); 
		    while (j < n) { 
			map[j++] = 0;
		    }
		}
	    }
            modified = false;
            currIndex = curr;
            committedIndexSize = currIndexSize;
	}
    }

    public synchronized void rollback() {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (!modified) { 
            return;
        }
	int curr = currIndex;
	int[] map = dirtyPagesMap;
	if (header.root[1-curr].index != header.root[curr].shadowIndex) { 
	    pool.copy(header.root[curr].shadowIndex, header.root[curr].index, 8*committedIndexSize);
	} else { 
	    int nPages = (committedIndexSize + dbHandlesPerPage - 1) >>> dbHandlesPerPageBits;
	    for (int i = 0; i < nPages; i++) { 
		if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
		    pool.copy(header.root[curr].shadowIndex + i*Page.pageSize,
			      header.root[curr].index + i*Page.pageSize,
			      Page.pageSize);
		}
	    }
	}
	for (int j = (currIndexSize+dbHandlesPerPage*32-1) >>> (dbHandlesPerPageBits+5);
	     --j >= 0;
	     map[j] = 0);
	header.root[1-curr].index = header.root[curr].shadowIndex;
	header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
	header.root[1-curr].indexUsed = committedIndexSize;
	header.root[1-curr].freeList  = header.root[curr].freeList; 
	header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd; 
	header.root[1-curr].size = header.root[curr].size;
        header.root[1-curr].rootObject = header.root[curr].rootObject;
        header.root[1-curr].classDescList = header.root[curr].classDescList;
	modified = false;
	usedSize = header.root[curr].size;
        currIndexSize = committedIndexSize;

	currRBitmapPage = currPBitmapPage = dbBitmapId;
	currRBitmapOffs = currPBitmapOffs = 0;

        reloadScheme();
    }

    public synchronized Index createIndex(Class keyType, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new Btree(keyType, unique);
    }

    public synchronized FieldIndex createFieldIndex(Class type, String fieldName, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        return new BtreeFieldIndex(type, fieldName, unique);
    }

    public Link createLink() {
        return new LinkImpl(8);
    }

    public Relation createRelation(IPersistent owner) {
        return new RelationImpl(owner);
    }

    public synchronized void close() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
	if (modified) { 
	    commit();
	}
	opened = false;
	if (header.dirty) { 
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
        dirtyPagesMap  = null;
        descList = null;
    }

   
    protected synchronized void storeObject(IPersistent obj) {
        int oid = obj.getOid();
        boolean newObject = false;
        if (oid == 0) { 
            oid = allocateId();
            objectCache.put(oid, obj);
            setObjectOid(obj, oid, false);
            newObject = true;
        }        
        byte[] data;
        data = packObject(obj);
	long pos;
        int newSize = ObjectHeader.getSize(data, 0);
        if (newObject) { 
            pos = allocate(newSize, 0);
            setPos(oid, pos | dbModifiedFlag);
        } else {
            pos = getPos(oid);
            int offs = (int)pos & (Page.pageSize-1);
            if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            Page pg = pool.getPage(pos - offs);
            offs &= ~dbFlagsMask;
            int size = ObjectHeader.getSize(pg.data, offs);
            pool.unfix(pg);
            if ((pos & dbModifiedFlag) == 0) { 
                cloneBitmap(pos & ~dbFlagsMask, size);
                pos = allocate(newSize, 0);
                setPos(oid, pos | dbModifiedFlag);
            } else { 
                if (((newSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1))
                    > ((size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1)))
                { 
                    long newPos = allocate(newSize, 0);
                    cloneBitmap(pos & ~dbFlagsMask, size);
                    free(pos & ~dbFlagsMask, size);
                    pos = newPos;
                    setPos(oid, pos | dbModifiedFlag);
                } else if (newSize < size) { 
                    ObjectHeader.setSize(data, 0, size);
                }
            }
        }        
        modified = true;
	pool.put(pos & ~dbFlagsMask, data, newSize);
    }

    protected synchronized void loadObject(IPersistent obj) {
        loadStub(obj.getOid(), obj, obj.getClass());
    }

    final IPersistent lookupObject(int oid, Class cls) {
        IPersistent obj = objectCache.get(oid);
        if (obj == null || obj.isRaw()) { 
            obj = loadStub(oid, obj, cls);
        }
        return obj;
    }
 
    final int swizzle(IPersistent obj) { 
        int oid = 0;
        if (obj != null) { 
            if (!obj.isPersistent()) { 
                storeObject(obj);
            }
            oid = obj.getOid();
        }
        return oid;
    }
        
    final IPersistent unswizzle(int oid, Class cls, boolean recursiveLoading) { 
        if (oid == 0) { 
            return null;
        } 
        if (recursiveLoading) {
            return lookupObject(oid, cls);
        }
        IPersistent stub = objectCache.get(oid);
        if (stub != null) { 
            return stub;
        }
        ClassDescriptor desc;
        if (cls == Persistent.class
            || (desc = (ClassDescriptor)classDescMap.get(cls)) == null
            || desc.hasSubclasses) 
        { 
            long pos = getPos(oid);
            int offs = (int)pos & (Page.pageSize-1);
            if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            Page pg = pool.getPage(pos - offs);
            int typeOid = ObjectHeader.getType(pg.data, offs & ~dbFlagsMask);
            pool.unfix(pg);
            desc = (ClassDescriptor)lookupObject(typeOid, ClassDescriptor.class);
        }
        stub = (IPersistent)desc.newInstance();
        setObjectOid(stub, oid, true);
        objectCache.put(oid, stub);
        return stub;
    }

    final IPersistent loadStub(int oid, IPersistent obj, Class cls)
    {
	long pos = getPos(oid);
	if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
	    throw new StorageError(StorageError.DELETED_OBJECT);
	}
        byte[] body = pool.get(pos & ~dbFlagsMask);
        ClassDescriptor desc;
        if (obj == null) { 
            if (cls == null 
                || cls == Persistent.class
                || (desc = (ClassDescriptor)classDescMap.get(cls)) == null
                || desc.hasSubclasses)
            {
                int typeOid = ObjectHeader.getType(body, 0);
                desc = (ClassDescriptor)lookupObject(typeOid, ClassDescriptor.class);
            }
            obj = (IPersistent)desc.newInstance();
            objectCache.put(oid, obj);
        } else { 
            desc = getClassDescriptor(cls);
        }
        setObjectOid(obj, oid, false);
        try { 
            unpackObject(obj, desc, obj.recursiveLoading(), body, ObjectHeader.sizeof);
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        obj.onLoad();
        return obj;
    }

    final int unpackObject(Object obj, ClassDescriptor desc, boolean recursiveLoading, byte[] body, int offs) 
      throws Exception
    {
        Field[] all = desc.allFields;
        int[] type = desc.fieldTypes;

        for (int i = 0, n = all.length; i < n; i++) { 
            Field f = all[i];
            switch (type[i]) { 
                case ClassDescriptor.tpBoolean:
                    f.setBoolean(obj, body[offs++] != 0);
                    continue;
                case ClassDescriptor.tpByte:
                    f.setByte(obj, body[offs++]);
                    continue;
                case ClassDescriptor.tpChar:
                    f.setChar(obj, (char)Bytes.unpack2(body, offs));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpShort:
                    f.setShort(obj, Bytes.unpack2(body, offs));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    f.setInt(obj, Bytes.unpack4(body, offs));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    f.setLong(obj, Bytes.unpack8(body, offs));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    f.setFloat(obj, Float.intBitsToFloat(Bytes.unpack4(body, offs)));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    f.setDouble(obj, Double.longBitsToDouble(Bytes.unpack8(body, offs)));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpString:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    String str = null;
                    if (len >= 0) { 
                        char[] chars = new char[len];
                        for (int j = 0; j < len; j++) { 
                            chars[j] = (char)Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        str = new String(chars);
                    } 
                    f.set(obj, str);
                    continue;
                }
                case ClassDescriptor.tpDate:
                {
                    long msec = Bytes.unpack8(body, offs);
                    offs += 8;
                    Date date = null;
                    if (msec >= 0) { 
                        date = new Date(msec);
                    }
                    f.set(obj, date);
                    continue;
                }
                case ClassDescriptor.tpObject:
                {
                    f.set(obj, unswizzle(Bytes.unpack4(body, offs), f.getType(), recursiveLoading));
                    offs += 4;
                    continue;
                }
                case ClassDescriptor.tpValue:
                {
                    ClassDescriptor valueDesc = getClassDescriptor(f.getType());
                    Object value = valueDesc.newInstance();
                    offs = unpackObject(value, valueDesc, recursiveLoading, body, offs);
                    f.set(obj, value);
                    continue;
                }
                case ClassDescriptor.tpArrayOfByte:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        byte[] arr = new byte[len];
                        System.arraycopy(body, offs, arr, 0, len);
                        offs += len;
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfBoolean:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        boolean[] arr = new boolean[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = body[offs++] != 0;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfShort:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        short[] arr = new short[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfChar:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        char[] arr = new char[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = (char)Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfInt:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        int[] arr = new int[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack4(body, offs);
                            offs += 4;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfLong:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        long[] arr = new long[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack8(body, offs);
                            offs += 8;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfFloat:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        float[] arr = new float[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Float.intBitsToFloat(Bytes.unpack4(body, offs));
                            offs += 4;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDouble:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        double[] arr = new double[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Double.longBitsToDouble(Bytes.unpack8(body, offs));
                            offs += 8;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDate:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        Date[] arr = new Date[len];
                        for (int j = 0; j < len; j++) { 
                            long msec = Bytes.unpack8(body, offs);
                            offs += 8;
                            if (msec >= 0) { 
                                arr[j] = new Date(msec);
                            }
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfString:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        String[] arr = new String[len];
                        for (int j = 0; j < len; j++) {
                            int strlen = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (strlen >= 0) {
                                char[] chars = new char[strlen];
                                for (int k = 0; k < strlen; k++) { 
                                    chars[k] = (char)Bytes.unpack2(body, offs);
                                    offs += 2;
                                }
                                arr[j] = new String(chars);
                            }
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfObject:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        Class elemType = f.getType().getComponentType();
                        IPersistent[] arr = (IPersistent[])Array.newInstance(elemType, len);
                        for (int j = 0; j < len; j++) { 
                            arr[j] = unswizzle(Bytes.unpack4(body, offs), elemType, recursiveLoading);
                            offs += 4;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfValue:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        Class elemType = f.getType().getComponentType();
                        Object[] arr = (Object[])Array.newInstance(elemType, len);
                        ClassDescriptor valueDesc = getClassDescriptor(elemType);
                        for (int j = 0; j < len; j++) { 
                            Object value = valueDesc.newInstance();
                            offs = unpackObject(value, valueDesc, recursiveLoading, body, offs);
                            arr[j] = value;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                }
                case ClassDescriptor.tpLink:
                { 
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        IPersistent[] arr = new IPersistent[len];
                        for (int j = 0; j < len; j++) { 
                            int elemOid = Bytes.unpack4(body, offs);
                            offs += 4;
                            IPersistent stub = null;
                            if (elemOid != 0) { 
                                stub = objectCache.get(elemOid);
                                if (stub == null) { 
                                    stub = new Persistent();
                                    setObjectOid(stub, elemOid, true);
                                }
                            }
                            arr[j] = stub;
                        }
                        f.set(obj, new LinkImpl(arr));
                    }
                }
            }
        }
        return offs;
    }


    final byte[] packObject(Object obj) { 
        ByteBuffer buf = new ByteBuffer();
        int offs = ObjectHeader.sizeof;
        buf.extend(offs);
        ClassDescriptor desc = getClassDescriptor(obj.getClass());
        try {
            offs = packObject(obj, desc, offs, buf);
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }        
        ObjectHeader.setSize(buf.arr, 0, offs);
        ObjectHeader.setType(buf.arr, 0, desc.getOid());
        return buf.arr;        
    }

    final int packObject(Object obj, ClassDescriptor desc, int offs, ByteBuffer buf) throws Exception 
    { 
        Field[] flds = desc.allFields;
        int[] types = desc.fieldTypes;
        for (int i = 0, n = flds.length; i < n; i++) {
            Field f = flds[i];
            switch(types[i]) {
                case ClassDescriptor.tpByte:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = f.getByte(obj);
                    continue;
                case ClassDescriptor.tpBoolean:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = (byte)(f.getBoolean(obj) ? 1 : 0);
                    continue;
                case ClassDescriptor.tpShort:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, f.getShort(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpChar:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, (short)f.getChar(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, f.getInt(obj));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, f.getLong(obj));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, Float.floatToIntBits(f.getFloat(obj)));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, Double.doubleToLongBits(f.getDouble(obj)));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpDate:
                {
                    buf.extend(offs + 8);
                    Date d = (Date)f.get(obj);
                    long msec = (d == null) ? -1 : d.getTime();                
                    Bytes.pack8(buf.arr, offs, msec);
                    offs += 8;
                    continue;
                }
                case ClassDescriptor.tpString:
                {
                    String s = (String)f.get(obj);
                    if (s == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = s.length();
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) { 
                            Bytes.pack2(buf.arr, offs, (short)s.charAt(j));
                            offs += 2;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpObject:
                {
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, swizzle((IPersistent)f.get(obj)));
                    offs += 4;
                    continue;
                }
                case ClassDescriptor.tpValue:
                {
                    Object value = f.get(obj);
                    if (value == null) { 
                        throw new StorageError(StorageError.NULL_VALUE);
                    }
                    offs = packObject(value, getClassDescriptor(value.getClass()), offs, buf);
                    continue;
                }
                case ClassDescriptor.tpArrayOfByte:
                {
                    byte[] arr = (byte[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        System.arraycopy(arr, 0, buf.arr, offs, len);
                        offs += len;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfBoolean:
                {
                    boolean[] arr = (boolean[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++, offs++) {
                            buf.arr[offs] = (byte)(arr[j] ? 1 : 0);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfShort:
                {
                    short[] arr = (short[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, arr[j]);
                            offs += 2;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfChar:
                {
                    char[] arr = (char[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, (short)arr[j]);
                            offs += 2;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfInt:
                {
                    int[] arr = (int[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfLong:
                {
                    long[] arr = (long[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfFloat:
                {
                    float[] arr = (float[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, Float.floatToIntBits(arr[j]));
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDouble:
                {
                    double[] arr = (double[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack8(buf.arr, offs, Double.doubleToLongBits(arr[j]));
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDate:
                {
                    Date[] arr = (Date[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Date d = arr[j];
                            long msec = (d == null) ? -1 : d.getTime();                            
                            Bytes.pack8(buf.arr, offs, msec);
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfString:
                {
                    String[] arr = (String[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            String str = (String)arr[j];
                            if (str == null) { 
                                Bytes.pack4(buf.arr, offs, -1);
                                offs += 4;
                            } else { 
                                int strlen = str.length();
                                buf.extend(offs + strlen*2);
                                Bytes.pack4(buf.arr, offs, strlen);
                                offs += 4;
                                for (int k = 0; k < strlen; k++) { 
                                    Bytes.pack2(buf.arr, offs, (short)str.charAt(k));
                                    offs += 2;
                                }
                            }
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfObject:
                {
                    IPersistent[] arr = (IPersistent[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, swizzle(arr[j]));
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfValue:
                {
                    Object[] arr = (Object[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        ClassDescriptor elemDesc = getClassDescriptor(f.getType().getComponentType());
                        for (int j = 0; j < len; j++) {
                            Object value = arr[j];
                            if (value == null) { 
                                throw new StorageError(StorageError.NULL_VALUE);
                            }
                            offs = packObject(value, elemDesc, offs, buf);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpLink:
                {
                    Link link = (Link)f.get(obj);
                    if (link == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = link.size();                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, swizzle(link.getRaw(j)));
                            offs += 4;
                        }
                    }
                    continue;
                }
            }
        }
        return offs;
    }
                
    PagePool  pool;
    Header    header;           // base address of database file mapping
    int       dirtyPagesMap[];  // bitmap of changed pages in current index
    boolean   modified;

    int       currRBitmapPage;//current bitmap page for allocating records
    int       currRBitmapOffs;//offset in current bitmap page for allocating 
                              //unaligned records
    int       currPBitmapPage;//current bitmap page for allocating page objects
    int       currPBitmapOffs;//offset in current bitmap page for allocating 
                              //page objects
    Location  reservedChain;

    int       committedIndexSize;
    int       currIndexSize;

    int       currIndex;  // copy of header.root, used to allow read access to the database 
                          // during transaction commit
    long      usedSize;   // total size of allocated objects since the beginning of the session
    int[]     bitmapPageAvailableSpace;
    boolean   opened;

    WeakHashTable   objectCache;
    HashMap         classDescMap;
    ClassDescriptor descList;
}

class RootPage { 
    long size;            // database file size
    long index;           // offset to object index
    long shadowIndex;     // offset to shadow index
    long usedSize;        // size used by objects
    int  indexSize;       // size of object index
    int  shadowIndexSize; // size of object index
    int  indexUsed;       // userd part of the index   
    int  freeList;        // L1 list of free descriptors
    int  bitmapEnd;       // index of last allocated bitmap page
    int  rootObject;      // OID of root object
    int  classDescList;   // List of class descriptors
    int  reserved; 

    final static int sizeof = 64;
} 

class Header { 
    int      curr;  // current root
    boolean  dirty; // database was not closed normally
    boolean  initialized; // database is initilaized

    RootPage root[];
    
    final static int sizeof = 3 + RootPage.sizeof*2;
    
    final void pack(byte[] rec) { 
	int offs = 0;
	rec[offs++] = (byte)curr;
	rec[offs++] = (byte)(dirty ? 1 : 0);
	rec[offs++] = (byte)(initialized ? 1 : 0);
	for (int i = 0; i < 2; i++) { 
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
    
    final void unpack(byte[] rec) { 
	int offs = 0;
	curr = rec[offs++];
	dirty = rec[offs++] != 0;
	initialized = rec[offs++] != 0;
	root = new RootPage[2];
	for (int i = 0; i < 2; i++) { 
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

