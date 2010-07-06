namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    class Btree:PersistentResource, Index
    {
        internal int root;
        internal int height;
        internal ClassDescriptor.FieldType type;
        internal int nElems;
        internal bool unique;
		
        internal Btree()
        {
        }
		
        internal Btree(byte[] obj, int offs) 
        {
            root = Bytes.unpack4(obj, offs);
            offs += 4;
            height = Bytes.unpack4(obj, offs);
            offs += 4;
            type = (ClassDescriptor.FieldType)Bytes.unpack4(obj, offs);
            offs += 4;
            nElems = Bytes.unpack4(obj, offs);
            offs += 4;
            unique = obj[offs] != 0;
        }

        internal Btree(System.Type cls, bool unique)
        {
            type = ClassDescriptor.getTypeCode(cls);
            if ((int)type >= (int)ClassDescriptor.FieldType.tpLink)
            {
                throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE, cls);
            }
            this.unique = unique;
        }
		
        internal const int op_done     = 0;
        internal const int op_overflow = 1;
        internal const int op_underflow = 2;
        internal const int op_not_found = 3;
        internal const int op_duplicate = 4;
        internal const int op_overwrite = 5;
		
        public virtual IPersistent get(Key key)
        {
            if (key.type != type)
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (root != 0)
            {
                ArrayList list = new ArrayList();
                BtreePage.find((StorageImpl) Storage, root, key, key, type, height, list);
                if (list.Count > 1)
                {
                    throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
                }
                else if (list.Count == 0)
                {
                    return null;
                }
                else
                {
                    return (IPersistent) list[0];
                }
            }
            return null;
        }
		
        internal static IPersistent[] emptySelection = new IPersistent[0];
		
        public virtual IPersistent[] get(Key from, Key till)
        {
            if ((from != null && from.type != type) || (till != null && till.type != type))
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (root != 0)
            {
                ArrayList list = new ArrayList();
                BtreePage.find((StorageImpl) Storage, root, from, till, type, height, list);
                if (list.Count == 0)
                {
                    return emptySelection;
                }
                else
                {
                    return (IPersistent[]) list.ToArray(typeof(IPersistent));
                }
            }
            return emptySelection;
        }
		
        public virtual bool put(Key key, IPersistent obj)
        {
            return insert(key, obj, false);
        }

        public virtual void set(Key key, IPersistent obj)
        {
            insert(key, obj, true);
        }

        internal bool insert(Key key, IPersistent obj, bool overwrite)
        {
            StorageImpl db = (StorageImpl) Storage;
            if (key.type != type)
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (!obj.isPersistent())
            {
                db.storeObject(obj);
            }
            BtreeKey ins = new BtreeKey(key, obj.Oid);
            if (root == 0)
            {
                root = BtreePage.allocate(db, 0, type, ins);
                height = 1;
            }
            else
            {
                int result = BtreePage.insert(db, root, type, ins, height, unique, overwrite);
                if (result == op_overflow)
                {
                    root = BtreePage.allocate(db, root, type, ins);
                    height += 1;
                }
                else if (result == op_duplicate)
                {
                    return false;
                }
                else if (result == op_overwrite)
                {
                    return true;
                }
            }
            nElems += 1;
            modify();
            return true;
        }
		
        public virtual void  remove(Key key, IPersistent obj)
        {
            remove(new BtreeKey(key, obj.Oid));
        }
		
		
        internal virtual void  remove(BtreeKey rem)
        {
            StorageImpl db = (StorageImpl) Storage;
            if (rem.key.type != type)
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (root == 0)
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            int result = BtreePage.remove(db, root, type, rem, height);
            if (result == op_not_found)
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            nElems -= 1;
            if (result == op_underflow && height != 1)
            {
                Page pg = db.getPage(root);
                if (BtreePage.getnItems(pg) == 0)
                {
                    int newRoot = (type == ClassDescriptor.FieldType.tpString)?BtreePage.getKeyStrOid(pg, 0):BtreePage.getReference(pg, BtreePage.maxItems - 1);
                    db.freePage(root);
                    root = newRoot;
                    height -= 1;
                }
                db.pool.unfix(pg);
            }
            else if (result == op_overflow)
            {
                root = BtreePage.allocate(db, root, type, rem);
                height += 1;
            }
            modify();
        }
		
        public virtual void  remove(Key key)
        {
            if (!unique)
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
            }
            remove(new BtreeKey(key, 0));
        }
		
		
        public virtual int size()
        {
            return nElems;
        }
		
        public virtual void  clear()
        {
            if (root != 0)
            {
                BtreePage.purge((StorageImpl) Storage, root, type, height);
                root = 0;
                nElems = 0;
                height = 0;
                modify();
            }
        }
		
        public virtual IPersistent[] toArray()
        {
            IPersistent[] arr = new IPersistent[nElems];
            if (root != 0)
            {
                BtreePage.traverseForward((StorageImpl) Storage, root, type, height, arr, 0);
            }
            return arr;
        }
		
        public override void  deallocate()
        {
            if (root != 0)
            {
                BtreePage.purge((StorageImpl) Storage, root, type, height);
            }
            base.deallocate();
        }

        public void markTree() 
        { 
            if (root != 0) 
            { 
                BtreePage.markPage((StorageImpl)Storage, root, type, height);
            }
        }        

        class BtreeEnumerator : IEnumerator 
        { 
            internal BtreeEnumerator(StorageImpl db, int pageId, int height) 
            { 
                this.db = db;
                pageStack = new int[height];
                posStack =  new int[height];
                sp = 0;
                rootId = pageId;
                while (--height >= 0) 
                { 
                    posStack[sp] = 0;
                    pageStack[sp] = pageId;
                    Page pg = db.getPage(pageId);
                    pageId = getReference(pg, 0);
                    end = BtreePage.getnItems(pg);
                    db.pool.unfix(pg);
                    sp += 1;
                }
            }

            protected virtual int getReference(Page pg, int pos) 
            { 
                return BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
            }

            public bool MoveNext() 
            {
                return sp > 0 && posStack[sp-1] < end;
            }

            public object Current 
            {
                get 
                {
                    if (sp == 0 || posStack[sp-1] >= end) 
                    { 
                        throw new InvalidOperationException();
                    }
                    int pos = posStack[sp-1];   
                    Page pg = db.getPage(pageStack[sp-1]);
                    int oid = getReference(pg, pos);        
                    Object obj = db.lookupObject(oid, null);
                    if (++pos == end) 
                    { 
                        while (--sp != 0) 
                        { 
                            db.pool.unfix(pg);
                            pos = posStack[sp-1];
                            pg = db.getPage(pageStack[sp-1]);
                            if (++pos <= BtreePage.getnItems(pg)) 
                            {
                                posStack[sp-1] = pos;
                                do 
                                { 
                                    int pageId = getReference(pg, pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.Length);
                                break;
                            }
                        }
                    } 
                    else 
                    {
                        posStack[sp-1] = pos;
                    }
                    db.pool.unfix(pg);
                    return obj;
                }
            }

            public void Reset() 
            {
                sp = 0;
                int pageId = rootId;
                for (int height = pageStack.Length; --height >= 0;) 
                { 
                    posStack[sp] = 0;
                    pageStack[sp] = pageId;
                    Page pg = db.getPage(pageId);
                    pageId = getReference(pg, 0);
                    end = BtreePage.getnItems(pg);
                    db.pool.unfix(pg);
                    sp += 1;
                }
            }

            StorageImpl db;
            int         rootId;
            int[]       pageStack;
            int[]       posStack;
            int         sp;
            int         end;
        }

        class BtreeStrEnumerator : BtreeEnumerator 
        { 
            internal BtreeStrEnumerator(StorageImpl db, int pageId, int height) 
                : base(db, pageId, height)
            {
            }

            protected override int getReference(Page pg, int pos) 
            { 
                return BtreePage.getKeyStrOid(pg, pos);
            }
        }

        public IEnumerator GetEnumerator() 
        { 
            return type == ClassDescriptor.FieldType.tpString 
                ? new BtreeStrEnumerator((StorageImpl)Storage, root, height)
                : new BtreeEnumerator((StorageImpl)Storage, root, height);
        }


        class BtreeSelectionIterator : IEnumerator 
        { 
            internal BtreeSelectionIterator(StorageImpl db, int pageId, int height, ClassDescriptor.FieldType type, Key from, Key till, IterationOrder order) 
            { 
                this.db = db;
                this.from = from;
                this.till = till;
                this.type = type;
                this.order = order;
                this.rootId = pageId;
                pageStack = new int[height];
                posStack =  new int[height];
                Reset();
            }

            public void Reset() 
            {
                int i, l, r;
                Page pg;
                int height = pageStack.Length;
                int pageId = rootId;
                sp = 0;
            
                if (type == ClassDescriptor.FieldType.tpString) 
                { 
                    if (order == IterationOrder.AscentOrder) 
                    { 
                        if (from == null) 
                        { 
                            while (--height >= 0) 
                            { 
                                posStack[sp] = 0;
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                pageId = BtreePage.getKeyStrOid(pg, 0);
                                end = BtreePage.getnItems(pg);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                        } 
                        else 
                        { 
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = BtreePage.getnItems(pg);
                                while (l < r)  
                                {
                                    i = (l+r) >> 1;
                                    if (BtreePage.compareStr(from, pg, i) >= from.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
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
                            pg = db.getPage(pageId);
                            l = 0;
                            end = r = BtreePage.getnItems(pg);
                            while (l < r)  
                            {
                                i = (l+r) >> 1;
                                if (BtreePage.compareStr(from, pg, i) >= from.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            if (r == end) 
                            {
                                sp += 1;
                                gotoNextItem(pg, r-1);
                            } 
                            else 
                            { 
                                posStack[sp++] = r;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && till != null) 
                        { 
                            pg = db.getPage(pageStack[sp-1]);
                            if (-BtreePage.compareStr(till, pg, posStack[sp-1]) >= till.inclusion) 
                            { 
                                sp = 0;
                            }
                            db.pool.unfix(pg);
                        }
                    } 
                    else 
                    { // descent order
                        if (till == null) 
                        { 
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                posStack[sp] = BtreePage.getnItems(pg);
                                pageId = BtreePage.getKeyStrOid(pg, posStack[sp]);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            posStack[sp++] = BtreePage.getnItems(pg)-1;
                            db.pool.unfix(pg);
                        } 
                        else 
                        {
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = BtreePage.getnItems(pg);
                                while (l < r)  
                                {
                                    i = (l+r) >> 1;
                                    if (BtreePage.compareStr(till, pg, i) >= 1-till.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
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
                            pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  
                            {
                                i = (l+r) >> 1;
                                if (BtreePage.compareStr(till, pg, i) >= 1-till.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            if (r == 0) 
                            {
                                sp += 1;
                                gotoNextItem(pg, r);
                            } 
                            else 
                            { 
                                posStack[sp++] = r-1;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && from != null) 
                        { 
                            pg = db.getPage(pageStack[sp-1]);
                            if (BtreePage.compareStr(from, pg, posStack[sp-1]) >= from.inclusion) 
                            { 
                                sp = 0;
                            }
                            db.pool.unfix(pg);
                        }
                    }
                } 
                else 
                { // scalar type
                    if (order == IterationOrder.AscentOrder) 
                    { 
                        if (from == null) 
                        { 
                            while (--height >= 0) 
                            { 
                                posStack[sp] = 0;
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                pageId = BtreePage.getReference(pg, BtreePage.maxItems-1);
                                end = BtreePage.getnItems(pg);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                        } 
                        else 
                        { 
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = BtreePage.getnItems(pg);
                                while (l < r)  
                                {
                                    i = (l+r) >> 1;
                                    if (BtreePage.compare(from, pg, i) >= from.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
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
                            pg = db.getPage(pageId);
                            l = 0;
                            r = end = BtreePage.getnItems(pg);
                            while (l < r)  
                            {
                                i = (l+r) >> 1;
                                if (BtreePage.compare(from, pg, i) >= from.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l); 
                            if (r == end) 
                            {
                                sp += 1;
                                gotoNextItem(pg, r-1);
                            } 
                            else 
                            { 
                                posStack[sp++] = r;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && till != null) 
                        { 
                            pg = db.getPage(pageStack[sp-1]);
                            if (-BtreePage.compare(till, pg, posStack[sp-1]) >= till.inclusion) 
                            { 
                                sp = 0;
                            }
                            db.pool.unfix(pg);
                        }
                    } 
                    else 
                    { // descent order
                        if (till == null) 
                        { 
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                posStack[sp] = BtreePage.getnItems(pg);
                                pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-posStack[sp]);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            posStack[sp++] = BtreePage.getnItems(pg)-1;
                            db.pool.unfix(pg);
                        } 
                        else 
                        {
                            while (--height > 0) 
                            { 
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = BtreePage.getnItems(pg);
                                while (l < r)  
                                {
                                    i = (l+r) >> 1;
                                    if (BtreePage.compare(till, pg, i) >= 1-till.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
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
                            pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);
                            while (l < r)  
                            {
                                i = (l+r) >> 1;
                                if (BtreePage.compare(till, pg, i) >= 1-till.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.that(r == l);  
                            if (r == 0) 
                            { 
                                sp += 1;
                                gotoNextItem(pg, r);
                            } 
                            else 
                            { 
                                posStack[sp++] = r-1;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && from != null) 
                        { 
                            pg = db.getPage(pageStack[sp-1]);
                            if (BtreePage.compare(from, pg, posStack[sp-1]) >= from.inclusion) 
                            { 
                                sp = 0;
                            }
                            db.pool.unfix(pg);
                        }
                    }
                }
            }
                

            public bool MoveNext() 
            {
                return sp != 0;
            }

            public object Current 
            {
                get 
                {
                    if (sp == 0) 
                    { 
                        throw new InvalidOperationException();
                    }
                    int pos = posStack[sp-1];   
                    Page pg = db.getPage(pageStack[sp-1]);
                    Object obj = (type == ClassDescriptor.FieldType.tpString)
                        ? db.lookupObject(BtreePage.getKeyStrOid(pg, pos), null)
                        : db.lookupObject(BtreePage.getReference(pg, BtreePage.maxItems-1-pos), null);
                    gotoNextItem(pg, pos);
                    return obj;
                }
            }

            protected void gotoNextItem(Page pg, int pos)
            {
                if (type == ClassDescriptor.FieldType.tpString) 
                { 
                    if (order == IterationOrder.AscentOrder) 
                    {                     
                        if (++pos == end) 
                        { 
                            while (--sp != 0) 
                            { 
                                db.pool.unfix(pg);
                                pos = posStack[sp-1];
                                pg = db.getPage(pageStack[sp-1]);
                                if (++pos <= BtreePage.getnItems(pg)) 
                                {
                                    posStack[sp-1] = pos;
                                    do 
                                    { 
                                        int pageId = BtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        end = BtreePage.getnItems(pg);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = 0;
                                    } while (++sp < pageStack.Length);
                                    break;
                                }
                            }
                        } 
                        else 
                        { 
                            posStack[sp-1] = pos;
                        }
                        if (sp != 0 && till != null && -BtreePage.compareStr(till, pg, pos) >= till.inclusion) 
                        { 
                            sp = 0;
                        }
                    } 
                    else 
                    { // descent order
                        if (--pos < 0) 
                        { 
                            while (--sp != 0) 
                            { 
                                db.pool.unfix(pg);
                                pos = posStack[sp-1];
                                pg = db.getPage(pageStack[sp-1]);
                                if (--pos >= 0) 
                                {
                                    posStack[sp-1] = pos;
                                    do 
                                    { 
                                        int pageId = BtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = BtreePage.getnItems(pg);
                                    } while (++sp < pageStack.Length);
                                    posStack[sp-1] = --pos;
                                    break;
                                }
                            }
                        } 
                        else 
                        { 
                            posStack[sp-1] = pos;
                        }
                        if (sp != 0 && from != null && BtreePage.compareStr(from, pg, pos) >= from.inclusion) 
                        { 
                            sp = 0;
                        }                    
                    }
                } 
                else 
                { // scalar type
                    if (order == IterationOrder.AscentOrder) 
                    {                     
                        if (++pos == end) 
                        { 
                            while (--sp != 0) 
                            { 
                                db.pool.unfix(pg);
                                pos = posStack[sp-1];
                                pg = db.getPage(pageStack[sp-1]);
                                if (++pos <= BtreePage.getnItems(pg)) 
                                {
                                    posStack[sp-1] = pos;
                                    do 
                                    { 
                                        int pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        end = BtreePage.getnItems(pg);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = 0;
                                    } while (++sp < pageStack.Length);
                                    break;
                                }
                            }
                        } 
                        else 
                        { 
                            posStack[sp-1] = pos;
                        }
                        if (sp != 0 && till != null && -BtreePage.compare(till, pg, pos) >= till.inclusion) 
                        { 
                            sp = 0;
                        }
                    } 
                    else 
                    { // descent order
                        if (--pos < 0) 
                        { 
                            while (--sp != 0) 
                            { 
                                db.pool.unfix(pg);
                                pos = posStack[sp-1];
                                pg = db.getPage(pageStack[sp-1]);
                                if (--pos >= 0) 
                                {
                                    posStack[sp-1] = pos;
                                    do 
                                    { 
                                        int pageId = BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = BtreePage.getnItems(pg);
                                    } while (++sp < pageStack.Length);
                                    posStack[sp-1] = --pos;
                                    break;
                                }
                            }
                        } 
                        else 
                        { 
                            posStack[sp-1] = pos;
                        }
                        if (sp != 0 && from != null && BtreePage.compare(from, pg, pos) >= from.inclusion) 
                        { 
                            sp = 0;
                        }                    
                    }
                }
                db.pool.unfix(pg);
            }
            
 
            StorageImpl     db;
            int[]           pageStack;
            int[]           posStack;
            int             rootId;
            int             sp;
            int             end;
            Key             from;
            Key             till;
            IterationOrder  order;
            ClassDescriptor.FieldType type;
        }

        public IEnumerator GetEnumerator(Key from, Key till, IterationOrder order) 
        { 
            if ((from != null && from.type != type) || (till != null && till.type != type)) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new BtreeSelectionIterator((StorageImpl)Storage, root, height, type, from, till, order);
        }
    }
}