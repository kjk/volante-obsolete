package org.nachodb.impl;
import  org.nachodb.*;
import  java.util.*;
import  java.lang.reflect.Array;

class Btree extends PersistentResource implements Index { 
    int       root;
    int       height;
    int       type;
    int       nElems;
    boolean   unique;

    transient int updateCounter;

    static final int sizeof = ObjectHeader.sizeof + 4*4 + 1;

    Btree() {}

    static int checkType(Class c) { 
        int elemType = ClassDescriptor.getTypeCode(c);
        if (elemType > ClassDescriptor.tpObject && elemType != ClassDescriptor.tpArrayOfByte) { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, c);
        }
        return elemType;
    }
       
    int compareByteArrays(byte[] key, byte[] item, int offs, int length) { 
        int n = key.length >= length ? length : key.length;
        for (int i = 0; i < n; i++) { 
            int diff = key[i] - item[i+offs];
            if (diff != 0) { 
                return diff;
            }
        }
        return key.length - length;
    }

    Btree(Class cls, boolean unique) {
        this.unique = unique;
        type = checkType(cls);
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

    public Class getKeyType() {
        switch (type) { 
        case ClassDescriptor.tpBoolean:
            return boolean.class;
        case ClassDescriptor.tpByte:
            return byte.class;
        case ClassDescriptor.tpChar:
            return char.class;
        case ClassDescriptor.tpShort:
            return short.class;
        case ClassDescriptor.tpInt:
            return int.class;
        case ClassDescriptor.tpLong:
            return long.class;
        case ClassDescriptor.tpFloat:
            return float.class;
        case ClassDescriptor.tpDouble:
            return double.class;
        case ClassDescriptor.tpString:
            return String.class;
        case ClassDescriptor.tpDate:
            return Date.class;
        case ClassDescriptor.tpObject:
            return IPersistent.class;
        case ClassDescriptor.tpArrayOfByte:
            return byte[].class;
        default:
            return null;
        }
    }

    Key checkKey(Key key) { 
        if (key != null) { 
            if (key.type != type) { 
                throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
            }
            if (key.oval instanceof String) { 
                key = new Key(((String)key.oval).toCharArray(), key.inclusion != 0);
            }
        }
        return key;
    }            

    public IPersistent get(Key key) { 
        key = checkKey(key);
        if (root != 0) { 
            ArrayList list = new ArrayList();
            BtreePage.find((StorageImpl)getStorage(), root, key, key, this, height, list);
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

    public IPersistent[] prefixSearch(String key) { 
        if (ClassDescriptor.tpString != type) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        if (root != 0) { 
            ArrayList list = new ArrayList();
            BtreePage.prefixSearch((StorageImpl)getStorage(), root, key.toCharArray(), height, list);
            if (list.size() != 0) { 
                return (IPersistent[])list.toArray(new IPersistent[list.size()]);
            }
        }
        return emptySelection;
    }

    public IPersistent[] get(Key from, Key till) {
        if (root != 0) { 
            ArrayList list = new ArrayList();
            BtreePage.find((StorageImpl)getStorage(), root, checkKey(from), checkKey(till), this, height, list);
            if (list.size() != 0) { 
                return (IPersistent[])list.toArray(new IPersistent[list.size()]);
            }
        }
        return emptySelection;
    }

    public boolean put(Key key, IPersistent obj) {
        return insert(key, obj, false) >= 0;
    }

    public IPersistent set(Key key, IPersistent obj) {
        int oid = insert(key, obj, true);
        return (oid != 0) ? ((StorageImpl)getStorage()).lookupObject(oid, null) :  null;
    }

    final int insert(Key key, IPersistent obj, boolean overwrite) {
        StorageImpl db = (StorageImpl)getStorage();
        if (db == null) {             
            throw new StorageError(StorageError.DELETED_OBJECT);
        }
        key = checkKey(key);
        if (!obj.isPersistent()) { 
            db.makePersistent(obj);
        }
        BtreeKey ins = new BtreeKey(key, obj.getOid());
        if (root == 0) { 
            root = BtreePage.allocate(db, 0, type, ins);
            height = 1;
        } else { 
            int result = BtreePage.insert(db, root, this, ins, height, unique, overwrite);
            if (result == op_overflow) { 
                root = BtreePage.allocate(db, root, type, ins);
                height += 1;
            } else if (result == op_duplicate) { 
                return -1;
            } else if (result == op_overwrite) { 
                return ins.oldOid;
            }
        }
        updateCounter += 1;
        nElems += 1;
        modify();
        return 0;
    }

    public void remove(Key key, IPersistent obj) {
        remove(new BtreeKey(checkKey(key), obj.getOid()));
    }

    
    void remove(BtreeKey rem) {
        StorageImpl db = (StorageImpl)getStorage();
        if (db == null) {             
            throw new StorageError(StorageError.DELETED_OBJECT);
        }
        if (root == 0) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        int result = BtreePage.remove(db, root, this, rem, height);
        if (result == op_not_found) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        nElems -= 1;
        if (result == op_underflow) { 
            Page pg = db.getPage(root);
            if (BtreePage.getnItems(pg) == 0) {                         
                int newRoot = 0;
                if (height != 1) { 
                    newRoot = (type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte) 
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
        updateCounter += 1;
        modify();
    }
        
    public IPersistent remove(Key key) {
        if (!unique) { 
            throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }
        BtreeKey rk = new BtreeKey(checkKey(key), 0);
        StorageImpl db = (StorageImpl)getStorage();
        remove(rk);
        return db.lookupObject(rk.oldOid, null);
    }
        
        
    public IPersistent get(String key) { 
        return get(new Key(key.toCharArray(), true));
    }

    public IPersistent[] getPrefix(String prefix) { 
        return get(new Key(prefix.toCharArray(), true), 
                   new Key((prefix + Character.MAX_VALUE).toCharArray(), false));
    }

    public boolean put(String key, IPersistent obj) {
        return put(new Key(key.toCharArray(), true), obj);
    }

    public IPersistent set(String key, IPersistent obj) {
        return set(new Key(key.toCharArray(), true), obj);
    }

    public void  remove(String key, IPersistent obj) {
        remove(new Key(key.toCharArray(), true), obj);
    }
    
    public IPersistent remove(String key) {
        return remove(new Key(key.toCharArray(), true));
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
            updateCounter += 1;
            modify();
        }
    }
        
    public IPersistent[] toPersistentArray() {
        IPersistent[] arr = new IPersistent[nElems];
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        return arr;
    }

    public IPersistent[] toPersistentArray(IPersistent[] arr) {
        if (arr.length < nElems) { 
            arr = (IPersistent[])Array.newInstance(arr.getClass().getComponentType(), nElems);
        }
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        if (arr.length > nElems) { 
            arr[nElems] = null;
        }
        return arr;
    }

    public void deallocate() { 
        if (root != 0) { 
            BtreePage.purge((StorageImpl)getStorage(), root, type, height);
        }
        super.deallocate();
    }

    public int markTree() 
    { 
        if (root != 0) { 
            return BtreePage.markPage((StorageImpl)getStorage(), root, type, height);
        }
        return 0;
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

    Object unpackKey(StorageImpl db, Page pg, int pos) { 
        byte[] data = pg.data;
        int offs =  BtreePage.firstKeyOffs + pos*ClassDescriptor.sizeof[type];
        switch (type) { 
          case ClassDescriptor.tpBoolean:
            return Boolean.valueOf(data[offs] != 0);
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
          case ClassDescriptor.tpArrayOfByte:
            return unpackByteArrayKey(pg, pos);
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
            
    Object unpackByteArrayKey(Page pg, int pos) {
        int len = BtreePage.getKeyStrSize(pg, pos);
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        byte[] val = new byte[len];
        System.arraycopy(pg.data, offs, val, 0, len);
        return val;
    }
            
              
    class BtreeIterator implements Iterator { 
        BtreeIterator() { 
            StorageImpl db = (StorageImpl)getStorage();
            if (db == null) {             
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            int pageId = root;
            int h = height;
            counter = updateCounter;
            pageStack = new int[h];
            posStack =  new int[h];
            sp = 0;
            while (--h >= 0) { 
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
            return (type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte)
                ? BtreePage.getKeyStrOid(pg, pos)
                : BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
        }

        public boolean hasNext() {
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            return sp > 0 && posStack[sp-1] < end;
        }

        protected Object getCurrent(Page pg, int pos) {            
            StorageImpl db = (StorageImpl)getStorage();
            return db.lookupObject(getReference(pg, pos), null);
        }

        public Object next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            StorageImpl db = (StorageImpl)getStorage();
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

        int[]       pageStack;
        int[]       posStack;
        int         sp;
        int         end;
        int         counter;
    }

    class BtreeEntryIterator extends BtreeIterator { 
        protected Object getCurrent(Page pg, int pos) {
            StorageImpl db = (StorageImpl)getStorage();
            if (db == null) {             
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            switch (type) { 
              case ClassDescriptor.tpString:
                return new BtreeEntry(db, unpackStrKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
              case ClassDescriptor.tpArrayOfByte:
                return new BtreeEntry(db, unpackByteArrayKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
              default:
                return new BtreeEntry(db, unpackKey(db, pg, pos), BtreePage.getReference(pg, BtreePage.maxItems-1-pos));
            }
        }
    }


    public Iterator iterator() { 
        return new BtreeIterator();
    }

    public Iterator entryIterator() { 
        return new BtreeEntryIterator();
    }


    final int compareByteArrays(Key key, Page pg, int i) { 
        return compareByteArrays((byte[])key.oval, 
                                 pg.data, 
                                 BtreePage.getKeyStrOffs(pg, i) + BtreePage.firstKeyOffs, 
                                 BtreePage.getKeyStrSize(pg, i));
    }


    class BtreeSelectionIterator implements Iterator { 
        BtreeSelectionIterator(Key from, Key till, int order) { 
            int i, l, r;
            
            sp = 0;
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            int pageId = root;
            StorageImpl db = (StorageImpl)getStorage();
            if (db == null) {             
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            int h = height;
            this.from = from;
            this.till = till;
            this.order = order;
            
            pageStack = new int[h];
            posStack =  new int[h];
            
            if (type == ClassDescriptor.tpString) { 
                if (order == ASCENT_ORDER) { 
                    if (from == null) { 
                        while (--h >= 0) { 
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            end = BtreePage.getnItems(pg);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                    } else { 
                        while (--h > 0) { 
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
                        while (--h > 0) { 
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
                        while (--h > 0) { 
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
            } else if (type == ClassDescriptor.tpArrayOfByte) { 
                if (order == ASCENT_ORDER) { 
                    if (from == null) { 
                        while (--h >= 0) { 
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            end = BtreePage.getnItems(pg);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                    } else { 
                        while (--h > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (compareByteArrays(from, pg, i) >= from.inclusion) {
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
                            if (compareByteArrays(from, pg, i) >= from.inclusion) {
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
                        if (-compareByteArrays(till, pg, posStack[sp-1]) >= till.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) { 
                        while (--h > 0) { 
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
                        while (--h > 0) { 
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  {
                                i = (l+r) >> 1;
                                if (compareByteArrays(till, pg, i) >= 1-till.inclusion) {
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
                            if (compareByteArrays(till, pg, i) >= 1-till.inclusion) {
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
                        if (compareByteArrays(from, pg, posStack[sp-1]) >= from.inclusion) { 
                            sp = 0;
                        }
                        db.pool.unfix(pg);
                    }
                }
            } else { // scalar type
                if (order == ASCENT_ORDER) { 
                    if (from == null) { 
                        while (--h >= 0) { 
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;
                            Page pg = db.getPage(pageId);
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems-1);
                            end = BtreePage.getnItems(pg);
                            db.pool.unfix(pg);
                            sp += 1;
                        }
                    } else { 
                        while (--h > 0) { 
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
                        while (--h > 0) { 
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
                        while (--h > 0) { 
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
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            return sp != 0;
        }

        public Object next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            StorageImpl db = (StorageImpl)getStorage();
            int pos = posStack[sp-1];   
            Page pg = db.getPage(pageStack[sp-1]);
            Object curr = getCurrent(pg, pos);
            gotoNextItem(pg, pos);
            return curr;
        }

        protected Object getCurrent(Page pg, int pos) { 
            StorageImpl db = (StorageImpl)getStorage();
            return db.lookupObject((type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte)
                                   ? BtreePage.getKeyStrOid(pg, pos)
                                   : BtreePage.getReference(pg, BtreePage.maxItems-1-pos), 
                                   null);
        }

        protected final void gotoNextItem(Page pg, int pos)
        {
            StorageImpl db = (StorageImpl)getStorage();
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
            } else if (type == ClassDescriptor.tpArrayOfByte) { 
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
                    if (sp != 0 && till != null && -compareByteArrays(till, pg, pos) >= till.inclusion) { 
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
                    if (sp != 0 && from != null && compareByteArrays(from, pg, pos) >= from.inclusion) { 
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

        int[]       pageStack;
        int[]       posStack;
        int         sp;
        int         end;
        Key         from;
        Key         till;
        int         order;
        int         counter;
    }

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator { 
        BtreeSelectionEntryIterator(Key from, Key till, int order) {
            super(from, till, order);
        }
            
        protected Object getCurrent(Page pg, int pos) { 
            StorageImpl db = (StorageImpl)getStorage();
            switch (type) { 
              case ClassDescriptor.tpString:
                return new BtreeEntry(db, unpackStrKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
              case ClassDescriptor.tpArrayOfByte:
                return new BtreeEntry(db, unpackByteArrayKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
              default:
                return new BtreeEntry(db, unpackKey(db, pg, pos), BtreePage.getReference(pg, BtreePage.maxItems-1-pos));
            }
        }
    }

    public Iterator iterator(Key from, Key till, int order) { 
        return new BtreeSelectionIterator(checkKey(from), checkKey(till), order);
    }

    public Iterator prefixIterator(String prefix) {
        return iterator(new Key(prefix.toCharArray()), 
                        new Key((prefix + Character.MAX_VALUE).toCharArray(), false), ASCENT_ORDER);
    }


    public Iterator entryIterator(Key from, Key till, int order) { 
        return new BtreeSelectionEntryIterator(checkKey(from), checkKey(till), order);
    }
}

