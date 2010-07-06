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


        internal Btree(Type cls, bool unique)
        {
            type = checkType(cls);
            this.unique = unique;
        }

        internal Btree(ClassDescriptor.FieldType type, bool unique)
        {
            this.type = type;
            this.unique = unique;
        }
		
        static protected ClassDescriptor.FieldType checkType(Type c) 
        { 
            ClassDescriptor.FieldType elemType = ClassDescriptor.getTypeCode(c);
            if ((int)elemType >= (int)ClassDescriptor.FieldType.tpLink 
                && elemType != ClassDescriptor.FieldType.tpArrayOfByte) 
            { 
                throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE, c);
            }
            return elemType;
        }

        protected virtual int compareByteArrays(byte[] key, byte[] item, int offs, int length) 
        { 
            int n = key.Length >= length ? length : key.Length;
            for (int i = 0; i < n; i++) 
            { 
                int diff = key[i] - item[i+offs];
                if (diff != 0) 
                { 
                    return diff;
                }
            }
            return key.Length - length;
        }
        
        internal const int op_done     = 0;
        internal const int op_overflow = 1;
        internal const int op_underflow = 2;
        internal const int op_not_found = 3;
        internal const int op_duplicate = 4;
        internal const int op_overwrite = 5;
		
        public int Count 
        { 
            get 
            {
                return nElems;
            }
        }

        public bool IsSynchronized 
        {
            get 
            {
                return true;
            }
        }

        public object SyncRoot 
        {
            get 
            {
                return this;
            }
        }

        public void CopyTo(Array dst, int i) 
        {
            foreach (object o in this) 
            { 
                dst.SetValue(o, i++);
            }
        }

        public IPersistent this[object key] 
        {
            get 
            {
                return Get(key);
            }
            set 
            {
                Set(key, (IPersistent)value);
            }
        } 
      
        public virtual IPersistent Get(Key key)
        {
            if (key.type != type)
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (root != 0)
            {
                ArrayList list = new ArrayList();
                BtreePage.find((StorageImpl) Storage, root, key, key, this, height, list);
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
            else if (o is byte[]) 
            {
                return new Key((byte[])o);
            }
            else if (o is object[]) 
            {
                return new Key((object[])o);
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


        public virtual IPersistent Get(object key) 
        {
            return Get(getKeyFromObject(key));
        }

        internal static IPersistent[] emptySelection = new IPersistent[0];
		
        public virtual IPersistent[] Get(Key from, Key till)
        {
            if ((from != null && from.type != type) || (till != null && till.type != type))
            {
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            if (root != 0)
            {
                ArrayList list = new ArrayList();
                BtreePage.find((StorageImpl) Storage, root, from, till, this, height, list);
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
		
        public virtual IPersistent[] Get(object from, object till)
        {
            return Get(getKeyFromObject(from), getKeyFromObject(till));
        }
            
            
        public virtual bool Put(Key key, IPersistent obj)
        {
            return insert(key, obj, false);
        }

        public virtual bool Put(object key, IPersistent obj)
        {
            return insert(getKeyFromObject(key), obj, false);
        }

        public virtual void Set(Key key, IPersistent obj)
        {
            insert(key, obj, true);
        }

        public virtual void Set(object key, IPersistent obj)
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
            if (!obj.IsPersistent())
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
                int result = BtreePage.insert(db, root, this, ins, height, unique, overwrite);
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
            Modify();
            return true;
        }
		
        public virtual void  Remove(Key key, IPersistent obj)
        {
            remove(new BtreeKey(key, obj.Oid));
        }
		
        public virtual void  Remove(object key, IPersistent obj)
        {
            remove(new BtreeKey(getKeyFromObject(key), obj.Oid));    
        }
 		
        internal virtual void remove(BtreeKey rem)
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
            int result = BtreePage.remove(db, root, this, rem, height);
            if (result == op_not_found)
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
            nElems -= 1;
            if (result == op_underflow)
            {
                Page pg = db.getPage(root);
                if (BtreePage.getnItems(pg) == 0)
                {
                    int newRoot = 0;
                    if (height != 1)  
                    {         
                        newRoot = (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
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
            Modify();
        }
		
               
                
        public virtual void Remove(Key key)
        {
            if (!unique)
            {
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_UNIQUE);
            }
            remove(new BtreeKey(key, 0));
        }		
            
        public virtual void Remove(object key)
        {
            Remove(getKeyFromObject(key));
        }

        public virtual int Size()
        {
            return nElems;
        }
		
        public virtual void Clear()
        {
            if (root != 0)
            {
                BtreePage.purge((StorageImpl) Storage, root, type, height);
                root = 0;
                nElems = 0;
                height = 0;
                Modify();
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
		
        public override void Deallocate()
        {
            if (root != 0)
            {
                BtreePage.purge((StorageImpl) Storage, root, type, height);
            }
            base.Deallocate();
        }

        internal virtual void export(XMLExporter exporter)
        {
            if (root != 0)
            {
                BtreePage.exportPage((StorageImpl) Storage, exporter, root, type, height);
            }
        }		

        internal int markTree() 
        { 
            return (root != 0) ? BtreePage.markPage((StorageImpl)Storage, root, type, height) : 0;
        }        

        protected virtual object unpackEnum(int val) 
        {
            // Base B-Tree class has no information about particular enum type
            // so it is not able to correctly unpack enum key
            return val;
        }

        internal object unpackKey(StorageImpl db, Page pg, int pos)
        {
            int offs = BtreePage.firstKeyOffs + pos*ClassDescriptor.Sizeof[(int)type];
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
                    return unpackEnum(Bytes.unpack4(data, offs));
            
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
#if COMPACT_NET_FRAMEWORK 
                    return BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(data, offs)), 0);
#else
                    return BitConverter.Int64BitsToDouble(Bytes.unpack8(data, offs));
#endif
                case ClassDescriptor.FieldType.tpGuid:
                {
                    byte[] bits = new byte[16];
                    Array.Copy(data, offs, bits, 0, 16);
                    return new Guid(bits);
                }
                case ClassDescriptor.FieldType.tpDecimal:
                {
                    int[] bits = new int[4];
                    bits[0] = Bytes.unpack4(data, offs);
                    bits[1] = Bytes.unpack4(data, offs+4);
                    bits[2] = Bytes.unpack4(data, offs+8);
                    bits[3] = Bytes.unpack4(data, offs+12);
                    return new decimal(bits);
                }
                case ClassDescriptor.FieldType.tpString:
                {
                    int len = BtreePage.getKeyStrSize(pg, pos);
                    offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
                    char[] sval = new char[len];
                    for (int j = 0; j < len; j++)
                    {
                        sval[j] = (char) Bytes.unpack2(pg.data, offs);
                        offs += 2;
                    }
                    return new String(sval);
                }
                case ClassDescriptor.FieldType.tpArrayOfByte:
                {
                    return unpackByteArrayKey(pg, pos);
                }
                default: 
                    Assert.Failed("Invalid type");
                    return null;
            }
        }

        protected virtual object unpackByteArrayKey(Page pg, int pos)
        {
            int len = BtreePage.getKeyStrSize(pg, pos);
            int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
            byte[] val = new byte[len];
            Array.Copy(pg.data, offs, val, 0, len);
            return val;
        }

        class BtreeEnumerator : IEnumerator 
        {
            internal BtreeEnumerator(Btree tree) 
            { 
                this.tree = tree;
                db = (StorageImpl)tree.Storage;
                int height = tree.height;
                pageStack = new int[height];
                posStack =  new int[height];
                sp = 0;
                int pageId = tree.root;
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

            public virtual object Current
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
                int pageId = tree.root;
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
            protected Btree       tree;
            protected int[]       pageStack;
            protected int[]       posStack;
            protected int         sp;
            protected int         end;
            protected int         oid;
            protected bool        hasCurrent;
        }
        
        class BtreeStrEnumerator : BtreeEnumerator 
        { 
            internal BtreeStrEnumerator(Btree tree) 
                : base(tree)
            {
            }

            protected override int getReference(Page pg, int pos) 
            { 
                return BtreePage.getKeyStrOid(pg, pos);
            }
        }

        class BtreeDictionaryEnumerator : BtreeEnumerator, IDictionaryEnumerator 
        {
            internal BtreeDictionaryEnumerator(Btree tree) 
                : base(tree) 
            {   
            }

                
            protected override void getCurrent(Page pg, int pos) 
            { 
                oid = getReference(pg, pos);
                key = tree.unpackKey(db, pg, pos);
            }

            public override object Current 
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

        class BtreeDictionaryStrEnumerator : BtreeDictionaryEnumerator 
        {
            internal BtreeDictionaryStrEnumerator(Btree tree) 
                : base(tree)
            {}

            protected override int getReference(Page pg, int pos) 
            {
                return BtreePage.getKeyStrOid(pg, pos);
            }
        }
    
        public IDictionaryEnumerator GetDictionaryEnumerator() 
        { 
            return type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte
                ? new BtreeDictionaryStrEnumerator(this)
                : new BtreeDictionaryEnumerator(this);
        }

        public IEnumerator GetEnumerator() 
        { 
            return type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte
                ? new BtreeStrEnumerator(this)
                : new BtreeEnumerator(this);
        }

        internal int compareByteArrays(Key key, Page pg, int i) 
        { 
            return compareByteArrays((byte[])key.oval, 
                pg.data, 
                BtreePage.getKeyStrOffs(pg, i) + BtreePage.firstKeyOffs, 
                BtreePage.getKeyStrSize(pg, i));
        }


        class BtreeSelectionIterator : IEnumerator, IEnumerable 
        { 
            internal BtreeSelectionIterator(Btree tree, Key from, Key till, IterationOrder order) 
            { 
                this.db = (StorageImpl)tree.Storage;
                this.from = from;
                this.till = till;
                this.type = tree.type;
                this.order = order;
                this.tree = tree;
                pageStack = new int[tree.height];
                posStack =  new int[tree.height];
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
                int pageId = tree.root;
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
                                Assert.That(r == l); 
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
                            Assert.That(r == l); 
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
                                Assert.That(r == l); 
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
                            Assert.That(r == l); 
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
                else if (type == ClassDescriptor.FieldType.tpArrayOfByte) 
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
                                    if (tree.compareByteArrays(from, pg, i) >= from.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
                                        r = i;
                                    }
                                }
                                Assert.That(r == l); 
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
                                if (tree.compareByteArrays(from, pg, i) >= from.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.That(r == l); 
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
                            if (-tree.compareByteArrays(till, pg, posStack[sp-1]) >= till.inclusion) 
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
                                    if (tree.compareByteArrays(till, pg, i) >= 1-till.inclusion) 
                                    {
                                        l = i + 1; 
                                    } 
                                    else 
                                    { 
                                        r = i;
                                    }
                                }
                                Assert.That(r == l); 
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
                                if (tree.compareByteArrays(till, pg, i) >= 1-till.inclusion) 
                                {
                                    l = i + 1; 
                                } 
                                else 
                                { 
                                    r = i;
                                }
                            }
                            Assert.That(r == l); 
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
                            if (tree.compareByteArrays(from, pg, posStack[sp-1]) >= from.inclusion) 
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
                                Assert.That(r == l); 
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
                            Assert.That(r == l); 
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
                                Assert.That(r == l); 
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
                            Assert.That(r == l);  
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
                oid = (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    ? BtreePage.getKeyStrOid(pg, pos)
                    : BtreePage.getReference(pg, BtreePage.maxItems-1-pos);
            }
 
            public virtual object Current 
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
                else if (type == ClassDescriptor.FieldType.tpArrayOfByte) 
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
                        if (sp != 0 && till != null && -tree.compareByteArrays(till, pg, pos) >= till.inclusion) 
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
                        if (sp != 0 && from != null && tree.compareByteArrays(from, pg, pos) >= from.inclusion) 
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
            protected Btree           tree;
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
            internal BtreeDictionarySelectionIterator(Btree tree, Key from, Key till, IterationOrder order) 
                : base(tree, from, till, order)
            {}
               
            protected override void getCurrent(Page pg, int pos)
            {
                base.getCurrent(pg, pos);
                key = tree.unpackKey(db, pg, pos);
            }
             
            public override object Current 
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
            return Range(from, till, order).GetEnumerator();
        }

        public virtual IEnumerable Range(Key from, Key till, IterationOrder order) 
        { 
            if ((from != null && from.type != type) || (till != null && till.type != type)) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new BtreeSelectionIterator(this, from, till, order);
        }

        public IEnumerable Range(object from, object till, IterationOrder order) 
        { 
            return Range(getKeyFromObject(from), getKeyFromObject(till), order);
        }

        public IEnumerable Range(object from, object till) 
        { 
            return Range(getKeyFromObject(from), getKeyFromObject(till), IterationOrder.AscentOrder);
        }
 
        public virtual IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order) 
        { 
            if ((from != null && from.type != type) || (till != null && till.type != type)) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            return new BtreeDictionarySelectionIterator(this, from, till, order);
        }
    }
}