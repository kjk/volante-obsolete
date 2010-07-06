package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.*;

class Btree extends PersistentResource implements Index { 
    int       root;
    int       height;
    int       type;
    int       nElems;
    boolean   unique;

    static final int sizeof = ObjectHeader.sizeof + 4*4 + 1;

    Btree() {}

    Btree(Class cls, boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.getTypeCode(cls);
        if (type >= ClassDescriptor.tpLink) { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, cls);
        }
    }

    Btree(int type, boolean unique) { 
        this.type = type;
        this.unique = unique;
    }

    Btree(byte[] obj, int offs) {
        root = Bytes.unpack4(obj, offs);
        offs += 4;
        height = Bytes.unpack4(obj, offs);
        offs += 4;
        type = Bytes.unpack4(obj, offs);
        offs += 4;
        nElems = Bytes.unpack4(obj, offs);
        offs += 4;
        unique = obj[offs] != 0;
    }

    static final int op_done      = 0;
    static final int op_overflow  = 1;
    static final int op_underflow = 2;
    static final int op_not_found = 3;
    static final int op_duplicate = 4;
    static final int op_overwrite = 5;

    public IPersistent get(Key key) { 
        if (key.type != type) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        if (root != 0) { 
            ArrayList list = new ArrayList();
            BtreePage.find((StorageImpl)getStorage(), root, key, key, type, height, list);
            if (list.size() > 1) { 
                throw new StorageError(StorageError.KEY_NOT_UNIQUE);
            } else if (list.size() == 0) { 
                return null;
            } else { 
                return (IPersistent)list.get(0);
            }
        }
        return null;
    }

    static final IPersistent[] emptySelection = new IPersistent[0];

    public IPersistent[] get(Key from, Key till) {
        if ((from != null && from.type != type) || (till != null && till.type != type)) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        if (root != 0) { 
            ArrayList list = new ArrayList();
            BtreePage.find((StorageImpl)getStorage(), root, from, till, type, height, list);
            if (list.size() == 0) { 
                return emptySelection;
            } else { 
                return (IPersistent[])list.toArray(new IPersistent[list.size()]);
            }
        }
        return emptySelection;
    }

    public boolean put(Key key, IPersistent obj) {
        return insert(key, obj, false);
    }

    public void set(Key key, IPersistent obj) {
        insert(key, obj, true);
    }

    final boolean insert(Key key, IPersistent obj, boolean overwrite) {
        StorageImpl db = (StorageImpl)getStorage();
        if (key.type != type) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        if (!obj.isPersistent()) { 
            db.storeObject(obj);
        }
        BtreeKey ins = new BtreeKey(key, obj.getOid());
        if (root == 0) { 
	    root = BtreePage.allocate(db, 0, type, ins);
	    height = 1;
        } else { 
            int result = BtreePage.insert(db, root, type, ins, height, unique, overwrite);
	    if (result == op_overflow) { 
		root = BtreePage.allocate(db, root, type, ins);
		height += 1;
	    } else if (result == op_duplicate) { 
                return false;
	    } else if (result == op_overwrite) { 
                return true;
            }
        }
        nElems += 1;
        modify();
        return true;
    }

    public void  remove(Key key, IPersistent obj) {
        remove(new BtreeKey(key, obj.getOid()));
    }

    
    void remove(BtreeKey rem) {
        StorageImpl db = (StorageImpl)getStorage();
        if (rem.key.type != type) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        if (root == 0) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
	int result = BtreePage.remove(db, root, type, rem, height);
        if (result == op_not_found) { 
	    throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        nElems -= 1;
	if (result == op_underflow) { 
	    Page pg = db.getPage(root);
	    if (BtreePage.getnItems(pg) == 0) { 			
                int newRoot = 0;
                if (height != 1) { 
                    newRoot = (type == ClassDescriptor.tpString) 
                        ? BtreePage.getKeyStrOid(pg, 0)
                        : BtreePage.getReference(pg, BtreePage.maxItems-1);
                }
		db.freePage(root);
                root = newRoot;
		height -= 1;
	    }
	    db.pool.unfix(pg);
	} else if (result == op_overflow) { 
	    root = BtreePage.allocate(db, root, type, rem);
	    height += 1;
	}
        modify();
    }
        
    public void remove(Key key) {
        if (!unique) { 
	    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }
        remove(new BtreeKey(key, 0));
    }
        
        
    public int size() {
        return nElems;
    }
    
    public void clear() {
        if (root != 0) { 
            BtreePage.purge((StorageImpl)getStorage(), root, type, height);
            root = 0;
            nElems = 0;
            height = 0;
            modify();
        }
    }
        
    public IPersistent[] toArray() {
        IPersistent[] arr = new IPersistent[nElems];
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        return arr;
    }

    public void deallocate() { 
        if (root != 0) { 
            BtreePage.purge((StorageImpl)getStorage(), root, type, height);
        }
        super.deallocate();
    }

    public void markTree() 
    { 
        if (root != 0) { 
            BtreePage.markPage((StorageImpl)getStorage(), root, type, height);
        }
    }        

    public void export(XMLExporter exporter) throws java.io.IOException 
    { 
        if (root != 0) { 
            BtreePage.exportPage((StorageImpl)getStorage(), exporter, root, type, height);
        }
    }        

    static class BtreeEntry implements Map.Entry {
        public Object getKey() { 
            return key;
        }

        public Object getValue() { 
            return db.lookupObject(oid, null);
        }

        public Object setValue(Object value) { 
            throw new UnsupportedOperationException();
        }

        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry e = (Map.Entry)o;
            return (getKey()==null 
                    ? e.getKey()==null : getKey().equals(e.getKey())) 
                && (getValue()==null 
                    ? getValue()==null : getValue().equals(e.getValue()));
        }

        BtreeEntry(StorageImpl db, Object key, int oid) {
            this.db = db;
            this.key = key;
            this.oid = oid;
        }

        private Object      key;
        private StorageImpl db;
        private int         oid;
    }

    static Object unpackKey(StorageImpl db, Page pg, int pos, int type) { 
        byte[] data = pg.data;
        int offs =  BtreePage.firstKeyOffs + pos;
        switch (type) { 
	  case ClassDescriptor.tpBoolean:
	    return new Boolean(data[offs] != 0);
	  case ClassDescriptor.tpByte:
	    return new Byte(data[offs]);
	  case ClassDescriptor.tpShort:
	    return new Short(Bytes.unpack2(data, offs));
	  case ClassDescriptor.tpChar:
	    return new Character((char)Bytes.unpack2(data, offs));
	  case ClassDescriptor.tpInt:
            return new Integer(Bytes.unpack4(data, offs));
	  case ClassDescriptor.tpObject:
            return db.lookupObject(Bytes.unpack4(data, offs), null);
	  case ClassDescriptor.tpLong:
            return new Long(Bytes.unpack8(data, offs));
	  case ClassDescriptor.tpDate:
	    return new Date(Bytes.unpack8(data, offs));
	  case ClassDescriptor.tpFloat:
	    return new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
	  case ClassDescriptor.tpDouble:
	    return new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
	  case ClassDescriptor.tpString:
            return unpackStrKey(pg, pos);
	  default:
	    Assert.failed("Invalid type");
        }
        return null;
    }
    
    static String unpackStrKey(Page pg, int pos) {
        int len = BtreePage.getKeyStrSize(pg, pos);
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        byte[] data = pg.data;
        char[] sval = new char[len];
        for (int j = 0; j < len; j++) { 
            sval[j] = (char)Bytes.unpack2(data, offs);
            offs += 2;
        }
        return new String(sval);
    }
            
            
    static class BtreeIterator implements Iterator { 
        BtreeIterator(StorageImpl db, int pageId, int height) { 
            this.db = db;
            pageStack = new int[height];
            posStack =  new int[height];
            sp = 0;
            while (--height >= 0) { 
                posStack[sp] = 0;
                pageStack[sp] = pageId;
                Page pg = db.getPage(pageId);
                pageId = getReference(pg, 0);
                end = BtreePage.getnItems(pg);
                db.pool.unfix(pg);
                sp += 1;
            }
        }

        protected int getReference(Page pg, int pos) { 
            return BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
        }

        public boolean hasNext() {
            return sp > 0 && posStack[sp-1] < end;
        }

        protected Object getCurrent(Page pg, int pos) {
            return db.lookupObject(getReference(pg, pos), null);
        }

        public Object next() {
            if (sp == 0 || posStack[sp-1] >= end) { 
                throw new NoSuchElementException();
            }
            int pos = posStack[sp-1];   
            Page pg = db.getPage(pageStack[sp-1]);
            Object curr = getCurrent(pg, pos);
            if (++pos == end) { 
                while (--sp != 0) { 
                    db.pool.unfix(pg);
                    pos = posStack[sp-1];
                    pg = db.getPage(pageStack[sp-1]);
                    if (++pos <= BtreePage.getnItems(pg)) {
                        posStack[sp-1] = pos;
                        do { 
                            int pageId = getReference(pg, pos);
                            db.pool.unfix(pg);
                            pg = db.getPage(pageId);
                            end = BtreePage.getnItems(pg);
                            pageStack[sp] = pageId;
                            posStack[sp] = pos = 0;
                        } while (++sp < pageStack.length);
                        break;
                    }
                }
            } else {
                posStack[sp-1] = pos;
            }
            db.pool.unfix(pg);
            return curr;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        StorageImpl db;
        int[]       pageStack;
        int[]       posStack;
        int         sp;
        int         end;
    }

    static class BtreeStrIterator extends BtreeIterator { 
        BtreeStrIterator(StorageImpl db, int pageId, int height) { 
            super(db, pageId, height);
        }

        protected int getReference(Page pg, int pos) { 
            return BtreePage.getKeyStrOid(pg, pos);
        }
    }

    static class BtreeEntryIterator extends BtreeIterator { 
        BtreeEntryIterator(StorageImpl db, int pageId, int type, int height) { 
            super(db, pageId, height);
            this.type = type;
        }

        protected Object getCurrent(Page pg, int pos) {
            return new BtreeEntry(db, unpackKey(db, pg, pos, type), getReference(pg, pos));
        }

        int type;
    }

    static class BtreeEntryStrIterator extends BtreeStrIterator { 
        BtreeEntryStrIterator(StorageImpl db, int pageId, int height) { 
            super(db, pageId, height);
        }

        protected Object getCurrent(Page pg, int pos) {
            return new BtreeEntry(db, unpackStrKey(pg, pos), getReference(pg, pos));
        }
    }

    public Iterator iterator() { 
        return type == ClassDescriptor.tpString 
            ? new BtreeStrIterator((StorageImpl)getStorage(), root, height)
            : new BtreeIterator((StorageImpl)getStorage(), root, height);
    }

    public Iterator entryIterator() { 
        return type == ClassDescriptor.tpString 
            ? (Iterator)new BtreeEntryStrIterator((StorageImpl)getStorage(), root, height)
            : (Iterator)new BtreeEntryIterator((StorageImpl)getStorage(), root, type, height);
    }


    static class BtreeSelectionIterator implements Iterator { 
        BtreeSelectionIterator(StorageImpl db, int pageId, int height, int type, Key from, Key till, int order) { 
            int i, l, r;
            
            sp = 0;
            if (height == 0) { 
                return;
            }

            this.db = db;
            this.from = from;
            this.till = till;
            this.type = type;
            this.order = order;
            
            pageStack = new int[height];
            posStack =  new int[height];
            
	    if (type == ClassDescriptor.tpString) { 
                if (order == ASCENT_ORDER) { 
                    if (from == null) { 
                        while (--height >= 0) { 
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            end = BtreePage.getnItems(pg);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                    } else { 
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (BtreePage.compareStr(from, pg, i) >= from.inclusion) {
                                    l = i + 1; 
                                } else { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        l = 0;
                        end = r = BtreePage.getnItems(pg);
                        while (l < r)  {
                            i = (l+r) >> 1;
                            if (BtreePage.compareStr(from, pg, i) >= from.inclusion) {
                                l = i + 1; 
                            } else { 
                                r = i;
                            }
                        }
                        Assert.that(r == l); 
                        if (r == end) {
                            sp += 1;
                            gotoNextItem(pg, r-1);
                        } else { 
                            posStack[sp++] = r;
                            db.pool.unfix(pg);
                        }
                    }
                    if (sp != 0 && till != null) { 
                        Page pg = db.getPage(pageStack[sp-1]);
                        if (-BtreePage.compareStr(till, pg, posStack[sp-1]) >= till.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) { 
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            posStack[sp] = BtreePage.getnItems(pg);
                            pageId = BtreePage.getKeyStrOid(pg, posStack[sp]);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        posStack[sp++] = BtreePage.getnItems(pg)-1;
                        db.pool.unfix(pg);
                    } else {
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (BtreePage.compareStr(till, pg, i) >= 1-till.inclusion) {
                                    l = i + 1; 
                                } else { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        l = 0;
                        r = BtreePage.getnItems(pg);
                        while (l < r)  {
                            i = (l+r) >> 1;
                            if (BtreePage.compareStr(till, pg, i) >= 1-till.inclusion) {
                                l = i + 1; 
                            } else { 
                                r = i;
                            }
                        }
                        Assert.that(r == l); 
                        if (r == 0) {
                            sp += 1;
                            gotoNextItem(pg, r);
                        } else { 
                            posStack[sp++] = r-1;
                            db.pool.unfix(pg);
                        }
                    }
                    if (sp != 0 && from != null) { 
                        Page pg = db.getPage(pageStack[sp-1]);
                        if (BtreePage.compareStr(from, pg, posStack[sp-1]) >= from.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                }
            } else { // scalar type
                if (order == ASCENT_ORDER) { 
                    if (from == null) { 
                        while (--height >= 0) { 
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems-1);
                            end = BtreePage.getnItems(pg);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                    } else { 
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (BtreePage.compare(from, pg, i) >= from.inclusion) {
                                    l = i + 1; 
                                } else { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            posStack[sp] = r;
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-r);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        l = 0;
                        r = end = BtreePage.getnItems(pg);
                        while (l < r)  {
                            i = (l+r) >> 1;
                            if (BtreePage.compare(from, pg, i) >= from.inclusion) {
                                l = i + 1; 
                            } else { 
                                r = i;
                            }
                        }
                        Assert.that(r == l); 
                        if (r == end) {
                            sp += 1;
                            gotoNextItem(pg, r-1);
                        } else { 
                            posStack[sp++] = r;
                            db.pool.unfix(pg);
                        }
                    }
                    if (sp != 0 && till != null) { 
                        Page pg = db.getPage(pageStack[sp-1]);
                        if (-BtreePage.compare(till, pg, posStack[sp-1]) >= till.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) { 
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            posStack[sp] = BtreePage.getnItems(pg);
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-posStack[sp]);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        posStack[sp++] = BtreePage.getnItems(pg)-1;
                        db.pool.unfix(pg);
                     } else {
                        while (--height > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (BtreePage.compare(till, pg, i) >= 1-till.inclusion) {
                                    l = i + 1; 
                                } else { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            posStack[sp] = r;
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-r);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                        pageStack[sp] = pageId;
                        Page pg = db.getPage(pageId);
                        l = 0;
                        r = BtreePage.getnItems(pg);
                        while (l < r)  {
                            i = (l+r) >> 1;
                            if (BtreePage.compare(till, pg, i) >= 1-till.inclusion) {
                                l = i + 1; 
                            } else { 
                                r = i;
                            }
                        }
                        Assert.that(r == l);  
                        if (r == 0) { 
                            sp += 1;
                            gotoNextItem(pg, r);
                        } else { 
                            posStack[sp++] = r-1;
                            db.pool.unfix(pg);
                        }
                    }
                    if (sp != 0 && from != null) { 
                        Page pg = db.getPage(pageStack[sp-1]);
                        if (BtreePage.compare(from, pg, posStack[sp-1]) >= from.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                }
            }
        }
                

        public boolean hasNext() {
            return sp != 0;
        }

        public Object next() {
            if (sp == 0) { 
                throw new NoSuchElementException();
            }
            int pos = posStack[sp-1];   
            Page pg = db.getPage(pageStack[sp-1]);
            Object curr = getCurrent(pg, pos);
            gotoNextItem(pg, pos);
            return curr;
        }

        protected Object getCurrent(Page pg, int pos) { 
            return db.lookupObject((type == ClassDescriptor.tpString)
                                    ? BtreePage.getKeyStrOid(pg, pos)
                                    : BtreePage.getReference(pg, BtreePage.maxItems-1-pos), 
                                   null);
        }

        protected final void gotoNextItem(Page pg, int pos)
        {
	    if (type == ClassDescriptor.tpString) { 
                if (order == ASCENT_ORDER) {                     
                    if (++pos == end) { 
                        while (--sp != 0) { 
                            db.pool.unfix(pg);
                            pos = posStack[sp-1];
                            pg = db.getPage(pageStack[sp-1]);
                            if (++pos <= BtreePage.getnItems(pg)) {
                                posStack[sp-1] = pos;
                                do { 
                                    int pageId = BtreePage.getKeyStrOid(pg, pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.length);
                                break;
                            }
                        }
                    } else { 
                        posStack[sp-1] = pos;
                    }
                    if (sp != 0 && till != null && -BtreePage.compareStr(till, pg, pos) >= till.inclusion) { 
                        sp = 0;
                    }
                } else { // descent order
                    if (--pos < 0) { 
                        while (--sp != 0) { 
                            db.pool.unfix(pg);
                            pos = posStack[sp-1];
                            pg = db.getPage(pageStack[sp-1]);
                            if (--pos >= 0) {
                                posStack[sp-1] = pos;
                                do { 
                                    int pageId = BtreePage.getKeyStrOid(pg, pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = BtreePage.getnItems(pg);
                                } while (++sp < pageStack.length);
                                posStack[sp-1] = --pos;
                                break;
                            }
                        }
                    } else { 
                        posStack[sp-1] = pos;
                    }
                    if (sp != 0 && from != null && BtreePage.compareStr(from, pg, pos) >= from.inclusion) { 
                        sp = 0;
                    }                    
                }
            } else { // scalar type
                if (order == ASCENT_ORDER) {                     
                    if (++pos == end) { 
                        while (--sp != 0) { 
                            db.pool.unfix(pg);
                            pos = posStack[sp-1];
                            pg = db.getPage(pageStack[sp-1]);
                            if (++pos <= BtreePage.getnItems(pg)) {
                                posStack[sp-1] = pos;
                                do { 
                                    int pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.length);
                                break;
                            }
                        }
                    } else { 
                        posStack[sp-1] = pos;
                    }
                    if (sp != 0 && till != null && -BtreePage.compare(till, pg, pos) >= till.inclusion) { 
                        sp = 0;
                    }
                } else { // descent order
                    if (--pos < 0) { 
                        while (--sp != 0) { 
                            db.pool.unfix(pg);
                            pos = posStack[sp-1];
                            pg = db.getPage(pageStack[sp-1]);
                            if (--pos >= 0) {
                                posStack[sp-1] = pos;
                                do { 
                                    int pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = BtreePage.getnItems(pg);
                                } while (++sp < pageStack.length);
                                posStack[sp-1] = --pos;
                                break;
                            }
                        }
                    } else { 
                        posStack[sp-1] = pos;
                    }
                    if (sp != 0 && from != null && BtreePage.compare(from, pg, pos) >= from.inclusion) { 
                        sp = 0;
                    }                    
                }
            }
            db.pool.unfix(pg);
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        StorageImpl db;
        int[]       pageStack;
        int[]       posStack;
        int         sp;
        int         end;
        Key         from;
        Key         till;
        int         order;
        int         type;
    }

    static class BtreeSelectionEntryIterator extends BtreeSelectionIterator { 
        BtreeSelectionEntryIterator(StorageImpl db, int pageId, int height, int type, Key from, Key till, int order) {
            super(db, pageId, height, type, from, till, order);
        }
            
        protected Object getCurrent(Page pg, int pos) { 
            return (type == ClassDescriptor.tpString)
                ? new BtreeEntry(db, unpackStrKey(pg, pos), BtreePage.getKeyStrOid(pg, pos))
                : new BtreeEntry(db, unpackKey(db, pg, pos, type), BtreePage.getReference(pg, BtreePage.maxItems-1-pos));
        }
    }

    public Iterator iterator(Key from, Key till, int order) { 
        if ((from != null && from.type != type) || (till != null && till.type != type)) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new BtreeSelectionIterator((StorageImpl)getStorage(), root, height, type, from, till, order);
    }

    public Iterator entryIterator(Key from, Key till, int order) { 
        if ((from != null && from.type != type) || (till != null && till.type != type)) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new BtreeSelectionEntryIterator((StorageImpl)getStorage(), root, height, type, from, till, order);
    }
}

