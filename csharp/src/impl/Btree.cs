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
		
        internal static int Sizeof = ObjectHeader.Sizeof + 4 * 4 + 1;

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

        internal Btree(ClassDescriptor.FieldType type, bool unique)
        {
            this.type = type;
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

        internal Key getKeyFromObject(object o) 
        {
            if (o == null) 
            { 
                return null;
            }
            else if (o is byte) 
            { 
                return new Key((byte)o);
            }
            else if (o is sbyte) 
            {
                return new Key((sbyte)o);
            }
            else if (o is short) 
            {
                return new Key((short)o);
            }
            else if (o is ushort) 
            {
                return new Key((ushort)o);
            }
            else if (o is int) 
            {
                return new Key((int)o);
            }
            else if (o is uint) 
            {
                return new Key((uint)o);
            }
            else if (o is long) 
            {
                return new Key((long)o);
            }
            else if (o is ulong) 
            {
                return new Key((ulong)o);
            }
            else if (o is float) 
            {
                return new Key((float)o);
            }
            else if (o is double) 
            {
                return new Key((double)o);
            }
            else if (o is bool) 
            {
                return new Key((bool)o);
            }
            else if (o is char) 
            {
                return new Key((char)o);
            }
            else if (o is String) 
            {
                return new Key((String)o);
            }
            else if (o is DateTime) 
            {
                return new Key((DateTime)o);
            }
            else if (o is Enum) 
            {
                return new Key((Enum)o);
            }
            else if (o is IPersistent) 
            {
                return new Key((IPersistent)o);
            }
            throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_TYPE);
        }


        public virtual IPersistent get(object key) 
        {
            return get(getKeyFromObject(key));
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
		
        public virtual IPersistent[] get(object from, object till)
        {
            return get(getKeyFromObject(from), getKeyFromObject(till));
        }
            
            
        public virtual bool put(Key key, IPersistent obj)
        {
            return insert(key, obj, false);
        }

        public virtual bool put(object key, IPersistent obj)
        {
            return insert(getKeyFromObject(key), obj, false);
        }

        public virtual void set(Key key, IPersistent obj)
        {
            insert(key, obj, true);
        }

        public virtual void set(object key, IPersistent obj)
        {
            insert(getKeyFromObject(key), obj, true);
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
		
        public virtual void  remove(object key, IPersistent obj)
        {
            remove(new BtreeKey(getKeyFromObject(key), obj.Oid));    
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
                    int newRoot = 0;
                    if (height != 1)  
                    {         
                        newRoot = (type == ClassDescriptor.FieldType.tpString)
                            ? BtreePage.getKeyStrOid(pg, 0)
                            : BtreePage.getReference(pg, BtreePage.maxItems - 1);
                    }
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
            
        public virtual void  remove(object key)
        {
            remove(getKeyFromObject(key));
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
		
        public virtual IPersistent[] ToArray()
        {
            IPersistent[] arr = new IPersistent[nElems];
            if (root != 0)
            {
                BtreePage.traverseForward((StorageImpl) Storage, root, type, height, arr, 0);
            }
            return arr;
        }
		
        public virtual Array ToArray(Type elemType)
        {
            Array arr = Array.CreateInstance(elemType, nElems);
            if (root != 0)
            {
                BtreePage.traverseForward((StorageImpl) Storage, root, type, height, (IPersistent[])arr, 0);
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

        public virtual void  export(XMLExporter exporter)
        {
            if (root != 0)
            {
                BtreePage.exportPage((StorageImpl) Storage, exporter, root, type, height);
            }
        }		

        public void markTree() 
        { 
            if (root != 0) 
            { 
                BtreePage.markPage((StorageImpl)Storage, root, type, height);
            }
        }        

        internal static object unpackKey(StorageImpl db, Page pg, int pos, ClassDescriptor.FieldType type)
        {
            int offs = BtreePage.firstKeyOffs + pos;
            byte[] data = pg.data;
			
            switch (type)
            {
                case ClassDescriptor.FieldType.tpBoolean: 
                    return data[offs] != 0;
				
                case ClassDescriptor.FieldType.tpSByte: 
                    return (sbyte)data[offs];
                   
                case ClassDescriptor.FieldType.tpByte: 
                    return data[offs];
 				
                case ClassDescriptor.FieldType.tpShort: 
                    return Bytes.unpack2(data, offs);

                case ClassDescriptor.FieldType.tpUShort: 
                    return (ushort)Bytes.unpack2(data, offs);
				
                case ClassDescriptor.FieldType.tpChar: 
                    return (char) Bytes.unpack2(data, offs);

                case ClassDescriptor.FieldType.tpInt: 
                    return Bytes.unpack4(data, offs);
                    
                case ClassDescriptor.FieldType.tpEnum: 
                    //!!! FIXME
                    return Bytes.unpack4(data, offs);
            
                case ClassDescriptor.FieldType.tpUInt:
                    return (uint)Bytes.unpack4(data, offs);
 
                case ClassDescriptor.FieldType.tpObject: 
                    return db.lookupObject(Bytes.unpack4(data, offs), null);
 				
                case ClassDescriptor.FieldType.tpLong: 
                    return Bytes.unpack8(data, offs);

                case ClassDescriptor.FieldType.tpDate: 
                    return new DateTime(Bytes.unpack8(data, offs));

                case ClassDescriptor.FieldType.tpULong: 
                    return (ulong)Bytes.unpack8(data, offs);
 				
                case ClassDescriptor.FieldType.tpFloat: 
                    return BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(data, offs)), 0);

                case ClassDescriptor.FieldType.tpDouble: 
                    return BitConverter.Int64BitsToDouble(Bytes.unpack8(data, offs));

                case ClassDescriptor.FieldType.tpString:
                {
                    int len = BtreePage.getKeyStrSize(pg, pos);
                    char[] sval = new char[len];
                    for (int j = 0; j < len; j++)
                    {
                        sval[j] = (char) Bytes.unpack2(pg.data, offs);
                        offs += 2;
                    }
                    return new String(sval);
                }
                default: 
                    Assert.failed("Invalid type");
                    return null;
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
                hasCurrent = false;
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

            protected virtual void getCurrent(Page pg, int pos) 
            { 
                oid = getReference(pg, pos);
            }

            public bool MoveNext() 
            {
                if (sp > 0 && posStack[sp-1] < end) 
                {
                    int pos = posStack[sp-1];   
                    Page pg = db.getPage(pageStack[sp-1]);
                    getCurrent(pg, pos);
                    hasCurrent = true;
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
                    return true;
                }
                hasCurrent = false;
                return false;
            }

            public object Current
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return db.lookupObject(oid, null);
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
                hasCurrent = false;
            }

            protected StorageImpl db;
            protected int         rootId;
            protected int[]       pageStack;
            protected int[]       posStack;
            protected int         sp;
            protected int         end;
            protected int         oid;
            protected bool        hasCurrent;
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

        class BtreeDictionaryEnumerator : BtreeEnumerator, IDictionaryEnumerator 
        {
            internal BtreeDictionaryEnumerator(StorageImpl db, int pageId, int height, ClassDescriptor.FieldType type) 
                : base(db, pageId, height) 
            {   
                this.type = type;
            }

                
            protected override void getCurrent(Page pg, int pos) 
            { 
                oid = getReference(pg, pos);
                key = unpackKey(db, pg, pos, type);
            }

            public object Current 
            {
                get 
                {
                    return Entry;
                }
            }

            public DictionaryEntry Entry 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return new DictionaryEntry(key, db.lookupObject(oid, null));
                }
            }

            public object Key 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return key;
                }
            }

            public object Value 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return db.lookupObject(oid, null);
                }
            }

            protected object key;
            protected ClassDescriptor.FieldType type;
        }

        class BtreeDictionaryStrEnumerator : BtreeDictionaryEnumerator 
        {
            internal BtreeDictionaryStrEnumerator(StorageImpl db, int pageId, int height) 
                : base(db, pageId, height, ClassDescriptor.FieldType.tpString)
            {}

            protected override int getReference(Page pg, int pos) 
            {
                return BtreePage.getKeyStrOid(pg, pos);
            }
        }
    
        public IDictionaryEnumerator GetDictionaryEnumerator() 
        { 
            return type == ClassDescriptor.FieldType.tpString 
                ? new BtreeDictionaryStrEnumerator((StorageImpl)Storage, root, height)
                : new BtreeDictionaryEnumerator((StorageImpl)Storage, root, height, type);
        }

        public IEnumerator GetEnumerator() 
        { 
            return type == ClassDescriptor.FieldType.tpString 
                ? new BtreeStrEnumerator((StorageImpl)Storage, root, height)
                : new BtreeEnumerator((StorageImpl)Storage, root, height);
        }

        class BtreeSelectionIterator : IEnumerator, IEnumerable 
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

            public IEnumerator GetEnumerator() 
            {
                return this;
            }

            public void Reset() 
            {
                int i, l, r;
                Page pg;
                int height = pageStack.Length;
                int pageId = rootId;
                hasCurrent = false;
                sp = 0;
            
                if (height == 0) 
                { 
                    return;
                }

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
                if (sp != 0) 
                {
                    int pos = posStack[sp-1];   
                    Page pg = db.getPage(pageStack[sp-1]);
                    hasCurrent = true;
                    getCurrent(pg, pos);
                    gotoNextItem(pg, pos);
                    return true;
                }
                hasCurrent = false;
                return false;
            }

            protected virtual void getCurrent(Page pg, int pos)
            {
                oid = (type == ClassDescriptor.FieldType.tpString)
                    ? BtreePage.getKeyStrOid(pg, pos)
                    : BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
            }
 
            public object Current 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return db.lookupObject(oid, null);
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
            
 
            protected StorageImpl     db;
            protected int[]           pageStack;
            protected int[]           posStack;
            protected int             rootId;
            protected int             sp;
            protected int             end;
            protected int             oid;
            protected Key             from;
            protected Key             till;
            protected bool            hasCurrent;
            protected IterationOrder  order;
            protected ClassDescriptor.FieldType type;
        }

        class BtreeDictionarySelectionIterator : BtreeSelectionIterator, IDictionaryEnumerator 
        { 
            internal BtreeDictionarySelectionIterator(StorageImpl db, int pageId, int height, ClassDescriptor.FieldType type, Key from, Key till, IterationOrder order) 
                : base(db, pageId, height, type, from, till, order)
            {}
               
            protected override void getCurrent(Page pg, int pos)
            {
                base.getCurrent(pg, pos);
                key = unpackKey(db, pg, pos, type);
            }
             
            public object Current 
            {
                get 
                {
                    return Entry;
                }
            }

            public DictionaryEntry Entry 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return new DictionaryEntry(key, db.lookupObject(oid, null));
                }
            }

            public object Key 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return key;
                }
            }

            public object Value 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return db.lookupObject(oid, null);
                }
            }

            protected object key;
        }

  
        public IEnumerator GetEnumerator(Key from, Key till, IterationOrder order) 
        {
            return range(from, till, order).GetEnumerator();
        }

        public IEnumerable range(Key from, Key till, IterationOrder order) 
        { 
            if ((from != null && from.type != type) || (till != null && till.type != type)) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new BtreeSelectionIterator((StorageImpl)Storage, root, height, type, from, till, order);
        }

        public IEnumerable range(object from, object till, IterationOrder order) 
        { 
            return range(getKeyFromObject(from), getKeyFromObject(till), order);
        }

        public IEnumerable range(object from, object till) 
        { 
            return range(getKeyFromObject(from), getKeyFromObject(till), IterationOrder.AscentOrder);
        }
 
        public IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order) 
        { 
            if ((from != null && from.type != type) || (till != null && till.type != type)) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new BtreeDictionarySelectionIterator((StorageImpl)Storage, root, height, type, from, till, order);
        }
    }
}