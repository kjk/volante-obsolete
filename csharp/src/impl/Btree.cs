namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    class Btree:Persistent, Index
    {
        internal int root;
        internal int height;
        internal ClassDescriptor.FieldType type;
        internal int nElems;
        internal bool unique;
		
        internal Btree()
        {
        }
		
        internal Btree(byte[] obj, int offs) {
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
            store();
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
            store();
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
                store();
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
    }
}