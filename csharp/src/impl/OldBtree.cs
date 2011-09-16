#if WITH_OLD_BTREE
namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Volante;

    enum OldBtreeResult
    {
        Done,
        Overflow,
        Underflow,
        NotFound,
        Duplicate,
        Overwrite
    }

    interface OldBtree : IPersistent
    {
        int markTree();
#if WITH_XML
        void export(XmlExporter exporter);
#endif
        int insert(Key key, IPersistent obj, bool overwrite);
        ClassDescriptor.FieldType FieldType { get; }
        ClassDescriptor.FieldType[] FieldTypes { get; }
        bool IsUnique { get; }
        int compareByteArrays(Key key, Page pg, int i);
        int HeaderSize { get; }
        void init(Type cls, ClassDescriptor.FieldType type, string[] fieldNames, bool unique, long autoincCount);
    }

    class OldBtree<K, V> : PersistentCollection<V>, IIndex<K, V>, OldBtree where V : class,IPersistent
    {
        internal int root;
        internal int height;
        internal ClassDescriptor.FieldType type;
        internal int nElems;
        internal bool unique;
        [NonSerialized()]
        internal int updateCounter;

        internal static int Sizeof = ObjectHeader.Sizeof + 4 * 4 + 1;

        internal OldBtree()
        {
        }

        internal OldBtree(byte[] obj, int offs)
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

        internal OldBtree(IndexType indexType)
        {
            type = checkType(typeof(K));
            this.unique = (indexType == IndexType.Unique);
        }

        public override void OnLoad()
        {
            if (type != ClassDescriptor.getTypeCode(typeof(K)))
                throw new DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE, typeof(K));
        }

        internal OldBtree(ClassDescriptor.FieldType type, bool unique)
        {
            this.type = type;
            this.unique = unique;
        }

        public virtual void init(Type cls, ClassDescriptor.FieldType type, string[] fieldNames, bool unique, long autoincCount)
        {
            this.type = type;
            this.unique = unique;
        }

        static protected ClassDescriptor.FieldType checkType(Type c)
        {
            ClassDescriptor.FieldType elemType = ClassDescriptor.getTypeCode(c);
            if ((int)elemType > (int)ClassDescriptor.FieldType.tpOid
                && elemType != ClassDescriptor.FieldType.tpArrayOfByte
                && elemType != ClassDescriptor.FieldType.tpDecimal
                && elemType != ClassDescriptor.FieldType.tpGuid)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE, c);
            }
            return elemType;
        }

        public virtual int compareByteArrays(byte[] key, byte[] item, int offs, int length)
        {
            int n = key.Length >= length ? length : key.Length;
            for (int i = 0; i < n; i++)
            {
                int diff = key[i] - item[i + offs];
                if (diff != 0)
                    return diff;
            }
            return key.Length - length;
        }

        public override int Count
        {
            get
            {
                return nElems;
            }
        }

        public bool IsUnique
        {
            get
            {
                return unique;
            }
        }

        public int HeaderSize
        {
            get
            {
                return Sizeof;
            }
        }

        public ClassDescriptor.FieldType FieldType
        {
            get
            {
                return type;
            }
        }

        public virtual ClassDescriptor.FieldType[] FieldTypes
        {
            get
            {
                return new ClassDescriptor.FieldType[] { type };
            }
        }

        public Type KeyType
        {
            get
            {
                return typeof(K);
            }
        }

        public V[] this[K from, K till]
        {
            get
            {
                return Get(from, till);
            }
        }

        public V this[K key]
        {
            get
            {
                return Get(key);
            }
            set
            {
                Set(key, value);
            }
        }

        protected Key checkKey(Key key)
        {
            if (key == null)
                return null;

            if (key.type != type)
                throw new DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);

            if ((type == ClassDescriptor.FieldType.tpObject
                    || type == ClassDescriptor.FieldType.tpOid)
                && key.ival == 0 && key.oval != null)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.INVALID_OID);
            }
            if (type == ClassDescriptor.FieldType.tpString && key.oval is string)
                key = new Key(((string)key.oval).ToCharArray(), key.inclusion != 0);

            return key;
        }

        public virtual V Get(Key key)
        {
            key = checkKey(key);
            if (0 == root)
                return default(V);

            ArrayList list = new ArrayList();
            OldBtreePage.find((DatabaseImpl)Database, root, key, key, this, height, list);
            if (list.Count > 1)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
            else if (list.Count == 0)
                return default(V);
            else
                return (V)list[0];
        }

        public virtual V Get(K key)
        {
            return Get(KeyBuilder.getKeyFromObject(key));
        }

        internal static V[] emptySelection = new V[0];

        public virtual V[] Get(Key from, Key till)
        {
            if (0 == root)
                return emptySelection;

            ArrayList list = new ArrayList();
            OldBtreePage.find((DatabaseImpl)Database, root, checkKey(from), checkKey(till), this, height, list);
            if (0 == list.Count)
                return emptySelection;

            return (V[])list.ToArray(typeof(V));
        }

        public V[] PrefixSearch(string key)
        {
            if (ClassDescriptor.FieldType.tpString != type)
                throw new DatabaseException(DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            if (0 == root)
                return emptySelection;

            ArrayList list = new ArrayList();
            OldBtreePage.prefixSearch((DatabaseImpl)Database, root, key, height, list);
            if (list.Count != 0)
                return (V[])list.ToArray(typeof(V));
            return emptySelection;
        }

        public virtual V[] Get(K from, K till)
        {
            return Get(KeyBuilder.getKeyFromObject(from), KeyBuilder.getKeyFromObject(till));
        }

        public virtual V[] GetPrefix(string prefix)
        {
            return Get(new Key(prefix.ToCharArray()),
                       new Key((prefix + Char.MaxValue).ToCharArray(), false));
        }

        public virtual bool Put(Key key, V obj)
        {
            return insert(key, obj, false) >= 0;
        }

        public virtual bool Put(K key, V obj)
        {
            return Put(KeyBuilder.getKeyFromObject(key), obj);
        }

        public virtual V Set(Key key, V obj)
        {
            int oid = insert(key, obj, true);
            return (oid != 0) ? (V)((DatabaseImpl)Database).lookupObject(oid, null) : null;
        }

        public virtual V Set(K key, V obj)
        {
            return Set(KeyBuilder.getKeyFromObject(key), obj);
        }

        public int insert(Key key, IPersistent obj, bool overwrite)
        {
            DatabaseImpl db = (DatabaseImpl)Database;
            if (db == null)
                throw new DatabaseException(Volante.DatabaseException.ErrorCode.DELETED_OBJECT);

            if (!obj.IsPersistent())
                db.MakePersistent(obj);
 
            OldBtreeKey ins = new OldBtreeKey(checkKey(key), obj.Oid);
            if (root == 0)
            {
                root = OldBtreePage.allocate(db, 0, type, ins);
                height = 1;
            }
            else
            {
                OldBtreeResult result = OldBtreePage.insert(db, root, this, ins, height, unique, overwrite);
                if (result == OldBtreeResult.Overflow)
                {
                    root = OldBtreePage.allocate(db, root, type, ins);
                    height += 1;
                }
                else if (result == OldBtreeResult.Duplicate)
                {
                    return -1;
                }
                else if (result == OldBtreeResult.Overwrite)
                {
                    return ins.oldOid;
                }
            }
            nElems += 1;
            updateCounter += 1;
            Modify();
            return 0;
        }

        public virtual void Remove(Key key, V obj)
        {
            remove(new OldBtreeKey(checkKey(key), obj.Oid));
        }

        public virtual void Remove(K key, V obj)
        {
            remove(new OldBtreeKey(KeyBuilder.getKeyFromObject(key), obj.Oid));
        }

        internal virtual void remove(OldBtreeKey rem)
        {
            DatabaseImpl db = (DatabaseImpl)Database;
            if (db == null)
                throw new DatabaseException(Volante.DatabaseException.ErrorCode.DELETED_OBJECT);

            if (root == 0)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);

            OldBtreeResult result = OldBtreePage.remove(db, root, this, rem, height);
            if (result == OldBtreeResult.NotFound)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);

            nElems -= 1;
            if (result == OldBtreeResult.Underflow)
            {
                Page pg = db.getPage(root);
                if (OldBtreePage.getnItems(pg) == 0)
                {
                    int newRoot = 0;
                    if (height != 1)
                    {
                        newRoot = (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                            ? OldBtreePage.getKeyStrOid(pg, 0)
                            : OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1);
                    }
                    db.freePage(root);
                    root = newRoot;
                    height -= 1;
                }
                db.pool.unfix(pg);
            }
            else if (result == OldBtreeResult.Overflow)
            {
                root = OldBtreePage.allocate(db, root, type, rem);
                height += 1;
            }
            updateCounter += 1;
            Modify();
        }

        public virtual V Remove(Key key)
        {
            if (!unique)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_UNIQUE);

            OldBtreeKey rk = new OldBtreeKey(checkKey(key), 0);
            DatabaseImpl db = (DatabaseImpl)Database;
            remove(rk);
            return (V)db.lookupObject(rk.oldOid, null);
        }

        public virtual V RemoveKey(K key)
        {
            return Remove(KeyBuilder.getKeyFromObject(key));
        }

        public override void Clear()
        {
            if (0 == root)
                return;
            OldBtreePage.purge((DatabaseImpl)Database, root, type, height);
            root = 0;
            nElems = 0;
            height = 0;
            updateCounter += 1;
            Modify();
        }

        public virtual V[] ToArray()
        {
            V[] arr = new V[nElems];
            if (root != 0)
                OldBtreePage.traverseForward((DatabaseImpl)Database, root, type, height, arr, 0);
            return arr;
        }

        public override void Deallocate()
        {
            if (root != 0)
                OldBtreePage.purge((DatabaseImpl)Database, root, type, height);
            base.Deallocate();
        }

#if WITH_XML
        public void export(XmlExporter exporter)
        {
            if (root != 0)
                OldBtreePage.exportPage((DatabaseImpl)Database, exporter, root, type, height);
        }
#endif

        public int markTree()
        {
            return (root != 0) ? OldBtreePage.markPage((DatabaseImpl)Database, root, type, height) : 0;
        }

        protected virtual object unpackEnum(int val)
        {
            // Base B-Tree class has no information about particular enum type
            // so it is not able to correctly unpack enum key
            return val;
        }

        internal object unpackKey(DatabaseImpl db, Page pg, int pos)
        {
            int offs = OldBtreePage.firstKeyOffs + pos * ClassDescriptor.Sizeof[(int)type];
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
                    return (char)Bytes.unpack2(data, offs);

                case ClassDescriptor.FieldType.tpInt:
                    return Bytes.unpack4(data, offs);

                case ClassDescriptor.FieldType.tpEnum:
                    return unpackEnum(Bytes.unpack4(data, offs));

                case ClassDescriptor.FieldType.tpUInt:
                    return (uint)Bytes.unpack4(data, offs);

                case ClassDescriptor.FieldType.tpOid:
                case ClassDescriptor.FieldType.tpObject:
                    return db.lookupObject(Bytes.unpack4(data, offs), null);

                case ClassDescriptor.FieldType.tpLong:
                    return Bytes.unpack8(data, offs);

                case ClassDescriptor.FieldType.tpDate:
                    return new DateTime(Bytes.unpack8(data, offs));

                case ClassDescriptor.FieldType.tpULong:
                    return (ulong)Bytes.unpack8(data, offs);

                case ClassDescriptor.FieldType.tpFloat:
                    return Bytes.unpackF4(data, offs);

                case ClassDescriptor.FieldType.tpDouble:
                    return Bytes.unpackF8(data, offs);

                case ClassDescriptor.FieldType.tpGuid:
                    return Bytes.unpackGuid(data, offs);

                case ClassDescriptor.FieldType.tpDecimal:
                    return Bytes.unpackDecimal(data, offs);

                case ClassDescriptor.FieldType.tpString:
                    {
                        int len = OldBtreePage.getKeyStrSize(pg, pos);
                        offs = OldBtreePage.firstKeyOffs + OldBtreePage.getKeyStrOffs(pg, pos);
                        char[] sval = new char[len];
                        for (int j = 0; j < len; j++)
                        {
                            sval[j] = (char)Bytes.unpack2(pg.data, offs);
                            offs += 2;
                        }
                        return new String(sval);
                    }
                case ClassDescriptor.FieldType.tpArrayOfByte:
                    {
                        return unpackByteArrayKey(pg, pos);
                    }
                default:
                    Debug.Assert(false, "Invalid type");
                    return null;
            }
        }

        protected virtual object unpackByteArrayKey(Page pg, int pos)
        {
            int len = OldBtreePage.getKeyStrSize(pg, pos);
            int offs = OldBtreePage.firstKeyOffs + OldBtreePage.getKeyStrOffs(pg, pos);
            byte[] val = new byte[len];
            Array.Copy(pg.data, offs, val, 0, len);
            return val;
        }

        class BtreeEnumerator : IEnumerator<V>
        {
            internal BtreeEnumerator(OldBtree<K, V> tree)
            {
                this.tree = tree;
                Reset();
            }

            protected virtual int getReference(Page pg, int pos)
            {
                return OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - pos);
            }

            protected virtual void getCurrent(Page pg, int pos)
            {
                oid = getReference(pg, pos);
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (updateCounter != tree.updateCounter)
                    throw new InvalidOperationException("B-Tree was modified");

                if (sp > 0 && posStack[sp - 1] < end)
                {
                    int pos = posStack[sp - 1];
                    Page pg = db.getPage(pageStack[sp - 1]);
                    getCurrent(pg, pos);
                    hasCurrent = true;
                    if (++pos == end)
                    {
                        while (--sp != 0)
                        {
                            db.pool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);
                            if (++pos <= OldBtreePage.getnItems(pg))
                            {
                                posStack[sp - 1] = pos;
                                do
                                {
                                    int pageId = getReference(pg, pos);
                                    db.pool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = OldBtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.Length);
                                break;
                            }
                        }
                    }
                    else
                    {
                        posStack[sp - 1] = pos;
                    }
                    db.pool.unfix(pg);
                    return true;
                }
                hasCurrent = false;
                return false;
            }

            public virtual V Current
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return (V)db.lookupObject(oid, null);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Reset()
            {
                db = (DatabaseImpl)tree.Database;
                if (db == null)
                    throw new DatabaseException(Volante.DatabaseException.ErrorCode.DELETED_OBJECT);

                sp = 0;
                int height = tree.height;
                pageStack = new int[height];
                posStack = new int[height];
                updateCounter = tree.updateCounter;
                int pageId = tree.root;
                while (--height >= 0)
                {
                    posStack[sp] = 0;
                    pageStack[sp] = pageId;
                    Page pg = db.getPage(pageId);
                    pageId = getReference(pg, 0);
                    end = OldBtreePage.getnItems(pg);
                    db.pool.unfix(pg);
                    sp += 1;
                }
                hasCurrent = false;
            }

            protected DatabaseImpl db;
            protected OldBtree<K, V> tree;
            protected int[] pageStack;
            protected int[] posStack;
            protected int sp;
            protected int end;
            protected int oid;
            protected bool hasCurrent;
            protected int updateCounter;
        }

        class BtreeStrEnumerator : BtreeEnumerator
        {
            internal BtreeStrEnumerator(OldBtree<K, V> tree)
                : base(tree)
            {
            }

            protected override int getReference(Page pg, int pos)
            {
                return OldBtreePage.getKeyStrOid(pg, pos);
            }
        }

        class BtreeDictionaryEnumerator : BtreeEnumerator, IDictionaryEnumerator
        {
            internal BtreeDictionaryEnumerator(OldBtree<K, V> tree)
                : base(tree)
            {
            }

            protected override void getCurrent(Page pg, int pos)
            {
                oid = getReference(pg, pos);
                key = tree.unpackKey(db, pg, pos);
            }

            public new virtual object Current
            {
                get
                {
                    return Entry;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public DictionaryEntry Entry
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return new DictionaryEntry(key, db.lookupObject(oid, null));
                }
            }

            public object Key
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return key;
                }
            }

            public object Value
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return db.lookupObject(oid, null);
                }
            }

            protected object key;
        }

        class BtreeDictionaryStrEnumerator : BtreeDictionaryEnumerator
        {
            internal BtreeDictionaryStrEnumerator(OldBtree<K, V> tree)
                : base(tree)
            { }

            protected override int getReference(Page pg, int pos)
            {
                return OldBtreePage.getKeyStrOid(pg, pos);
            }
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator()
        {
            return type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte
                ? new BtreeDictionaryStrEnumerator(this)
                : new BtreeDictionaryEnumerator(this);
        }

        public override IEnumerator<V> GetEnumerator()
        {
            return type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte
                ? new BtreeStrEnumerator(this)
                : new BtreeEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int compareByteArrays(Key key, Page pg, int i)
        {
            return compareByteArrays((byte[])key.oval,
                pg.data,
                OldBtreePage.getKeyStrOffs(pg, i) + OldBtreePage.firstKeyOffs,
                OldBtreePage.getKeyStrSize(pg, i));
        }

        class BtreeSelectionIterator : IEnumerator<V>, IEnumerable<V>
        {
            internal BtreeSelectionIterator(OldBtree<K, V> tree, Key from, Key till, IterationOrder order)
            {
                this.from = from;
                this.till = till;
                this.type = tree.type;
                this.order = order;
                this.tree = tree;
                Reset();
            }

            public IEnumerator<V> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
            }

            public void Reset()
            {
                int i, l, r;
                Page pg;
                int height = tree.height;
                int pageId = tree.root;
                updateCounter = tree.updateCounter;
                hasCurrent = false;
                sp = 0;

                if (height == 0)
                    return;

                db = (DatabaseImpl)tree.Database;
                if (db == null)
                    throw new DatabaseException(Volante.DatabaseException.ErrorCode.DELETED_OBJECT);

                pageStack = new int[height];
                posStack = new int[height];

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
                                pageId = OldBtreePage.getKeyStrOid(pg, 0);
                                end = OldBtreePage.getnItems(pg);
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
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (OldBtreePage.compareStr(from, pg, i) >= from.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getKeyStrOid(pg, r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            end = r = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (OldBtreePage.compareStr(from, pg, i) >= from.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == end)
                            {
                                sp += 1;
                                gotoNextItem(pg, r - 1);
                            }
                            else
                            {
                                posStack[sp++] = r;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && till != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (-OldBtreePage.compareStr(till, pg, posStack[sp - 1]) >= till.inclusion)
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
                                posStack[sp] = OldBtreePage.getnItems(pg);
                                pageId = OldBtreePage.getKeyStrOid(pg, posStack[sp]);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            posStack[sp++] = OldBtreePage.getnItems(pg) - 1;
                            db.pool.unfix(pg);
                        }
                        else
                        {
                            while (--height > 0)
                            {
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (OldBtreePage.compareStr(till, pg, i) >= 1 - till.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getKeyStrOid(pg, r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            r = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (OldBtreePage.compareStr(till, pg, i) >= 1 - till.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == 0)
                            {
                                sp += 1;
                                gotoNextItem(pg, r);
                            }
                            else
                            {
                                posStack[sp++] = r - 1;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && from != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (OldBtreePage.compareStr(from, pg, posStack[sp - 1]) >= from.inclusion)
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
                                pageId = OldBtreePage.getKeyStrOid(pg, 0);
                                end = OldBtreePage.getnItems(pg);
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
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (tree.compareByteArrays(from, pg, i) >= from.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getKeyStrOid(pg, r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            end = r = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (tree.compareByteArrays(from, pg, i) >= from.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == end)
                            {
                                sp += 1;
                                gotoNextItem(pg, r - 1);
                            }
                            else
                            {
                                posStack[sp++] = r;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && till != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (-tree.compareByteArrays(till, pg, posStack[sp - 1]) >= till.inclusion)
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
                                posStack[sp] = OldBtreePage.getnItems(pg);
                                pageId = OldBtreePage.getKeyStrOid(pg, posStack[sp]);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            posStack[sp++] = OldBtreePage.getnItems(pg) - 1;
                            db.pool.unfix(pg);
                        }
                        else
                        {
                            while (--height > 0)
                            {
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (tree.compareByteArrays(till, pg, i) >= 1 - till.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getKeyStrOid(pg, r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            r = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (tree.compareByteArrays(till, pg, i) >= 1 - till.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == 0)
                            {
                                sp += 1;
                                gotoNextItem(pg, r);
                            }
                            else
                            {
                                posStack[sp++] = r - 1;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && from != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (tree.compareByteArrays(from, pg, posStack[sp - 1]) >= from.inclusion)
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
                                pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1);
                                end = OldBtreePage.getnItems(pg);
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
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (OldBtreePage.compare(from, pg, i) >= from.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            r = end = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (OldBtreePage.compare(from, pg, i) >= from.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == end)
                            {
                                sp += 1;
                                gotoNextItem(pg, r - 1);
                            }
                            else
                            {
                                posStack[sp++] = r;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && till != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (-OldBtreePage.compare(till, pg, posStack[sp - 1]) >= till.inclusion)
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
                                posStack[sp] = OldBtreePage.getnItems(pg);
                                pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - posStack[sp]);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            posStack[sp++] = OldBtreePage.getnItems(pg) - 1;
                            db.pool.unfix(pg);
                        }
                        else
                        {
                            while (--height > 0)
                            {
                                pageStack[sp] = pageId;
                                pg = db.getPage(pageId);
                                l = 0;
                                r = OldBtreePage.getnItems(pg);
                                while (l < r)
                                {
                                    i = (l + r) >> 1;
                                    if (OldBtreePage.compare(till, pg, i) >= 1 - till.inclusion)
                                        l = i + 1;
                                    else
                                        r = i;
                                }
                                Debug.Assert(r == l);
                                posStack[sp] = r;
                                pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - r);
                                db.pool.unfix(pg);
                                sp += 1;
                            }
                            pageStack[sp] = pageId;
                            pg = db.getPage(pageId);
                            l = 0;
                            r = OldBtreePage.getnItems(pg);
                            while (l < r)
                            {
                                i = (l + r) >> 1;
                                if (OldBtreePage.compare(till, pg, i) >= 1 - till.inclusion)
                                    l = i + 1;
                                else
                                    r = i;
                            }
                            Debug.Assert(r == l);
                            if (r == 0)
                            {
                                sp += 1;
                                gotoNextItem(pg, r);
                            }
                            else
                            {
                                posStack[sp++] = r - 1;
                                db.pool.unfix(pg);
                            }
                        }
                        if (sp != 0 && from != null)
                        {
                            pg = db.getPage(pageStack[sp - 1]);
                            if (OldBtreePage.compare(from, pg, posStack[sp - 1]) >= from.inclusion)
                            {
                                sp = 0;
                            }
                            db.pool.unfix(pg);
                        }
                    }
                }
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (updateCounter != tree.updateCounter)
                    throw new InvalidOperationException("B-Tree was modified");

                if (0 == sp)
                {
                    hasCurrent = false;
                    return false;
                }

                int pos = posStack[sp - 1];
                Page pg = db.getPage(pageStack[sp - 1]);
                hasCurrent = true;
                getCurrent(pg, pos);
                gotoNextItem(pg, pos);
                return true;
            }

            protected virtual void getCurrent(Page pg, int pos)
            {
                oid = (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    ? OldBtreePage.getKeyStrOid(pg, pos)
                    : OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - pos);
            }

            public virtual V Current
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return (V)db.lookupObject(oid, null);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (++pos <= OldBtreePage.getnItems(pg))
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        end = OldBtreePage.getnItems(pg);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = 0;
                                    } while (++sp < pageStack.Length);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
                        }
                        if (sp != 0 && till != null && -OldBtreePage.compareStr(till, pg, pos) >= till.inclusion)
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (--pos >= 0)
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = OldBtreePage.getnItems(pg);
                                    } while (++sp < pageStack.Length);
                                    posStack[sp - 1] = --pos;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
                        }
                        if (sp != 0 && from != null && OldBtreePage.compareStr(from, pg, pos) >= from.inclusion)
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (++pos <= OldBtreePage.getnItems(pg))
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        end = OldBtreePage.getnItems(pg);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = 0;
                                    } while (++sp < pageStack.Length);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (--pos >= 0)
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getKeyStrOid(pg, pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = OldBtreePage.getnItems(pg);
                                    } while (++sp < pageStack.Length);
                                    posStack[sp - 1] = --pos;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (++pos <= OldBtreePage.getnItems(pg))
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        end = OldBtreePage.getnItems(pg);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = 0;
                                    } while (++sp < pageStack.Length);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
                        }
                        if (sp != 0 && till != null && -OldBtreePage.compare(till, pg, pos) >= till.inclusion)
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
                                pos = posStack[sp - 1];
                                pg = db.getPage(pageStack[sp - 1]);
                                if (--pos >= 0)
                                {
                                    posStack[sp - 1] = pos;
                                    do
                                    {
                                        int pageId = OldBtreePage.getReference(pg, OldBtreePage.maxItems - 1 - pos);
                                        db.pool.unfix(pg);
                                        pg = db.getPage(pageId);
                                        pageStack[sp] = pageId;
                                        posStack[sp] = pos = OldBtreePage.getnItems(pg);
                                    } while (++sp < pageStack.Length);
                                    posStack[sp - 1] = --pos;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            posStack[sp - 1] = pos;
                        }
                        if (sp != 0 && from != null && OldBtreePage.compare(from, pg, pos) >= from.inclusion)
                        {
                            sp = 0;
                        }
                    }
                }
                db.pool.unfix(pg);
            }

            protected DatabaseImpl db;
            protected int[] pageStack;
            protected int[] posStack;
            protected OldBtree<K, V> tree;
            protected int sp;
            protected int end;
            protected int oid;
            protected Key from;
            protected Key till;
            protected bool hasCurrent;
            protected IterationOrder order;
            protected ClassDescriptor.FieldType type;
            protected int updateCounter;
        }

        class BtreeDictionarySelectionIterator : BtreeSelectionIterator, IDictionaryEnumerator
        {
            internal BtreeDictionarySelectionIterator(OldBtree<K, V> tree, Key from, Key till, IterationOrder order)
                : base(tree, from, till, order)
            { }

            protected override void getCurrent(Page pg, int pos)
            {
                base.getCurrent(pg, pos);
                key = tree.unpackKey(db, pg, pos);
            }

            public new virtual object Current
            {
                get
                {
                    return Entry;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public DictionaryEntry Entry
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return new DictionaryEntry(key, db.lookupObject(oid, null));
                }
            }

            public object Key
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return key;
                }
            }

            public object Value
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();

                    return db.lookupObject(oid, null);
                }
            }

            protected object key;
        }

        public IEnumerator<V> GetEnumerator(Key from, Key till, IterationOrder order)
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(K from, K till, IterationOrder order)
        {
            return Range(from, till, order).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(Key from, Key till)
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(K from, K till)
        {
            return Range(from, till).GetEnumerator();
        }

        public IEnumerator<V> GetEnumerator(string prefix)
        {
            return StartsWith(prefix).GetEnumerator();
        }

        public IEnumerable<V> Reverse()
        {
            return new BtreeSelectionIterator(this, null, null, IterationOrder.DescentOrder);
        }

        public virtual IEnumerable<V> Range(Key from, Key till, IterationOrder order)
        {
            return new BtreeSelectionIterator(this, checkKey(from), checkKey(till), order);
        }

        public virtual IEnumerable<V> Range(Key from, Key till)
        {
            return Range(from, till, IterationOrder.AscentOrder);
        }

        public IEnumerable<V> Range(K from, K till, IterationOrder order)
        {
            return Range(KeyBuilder.getKeyFromObject(from), KeyBuilder.getKeyFromObject(till), order);
        }

        public IEnumerable<V> Range(K from, K till)
        {
            return Range(KeyBuilder.getKeyFromObject(from), KeyBuilder.getKeyFromObject(till), IterationOrder.AscentOrder);
        }

        public IEnumerable<V> StartsWith(string prefix)
        {
            return Range(new Key(prefix.ToCharArray()),
                         new Key((prefix + Char.MaxValue).ToCharArray(), false), IterationOrder.AscentOrder);
        }

        public virtual IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order)
        {
            return new BtreeDictionarySelectionIterator(this, checkKey(from), checkKey(till), order);
        }
    }
}
#endif
