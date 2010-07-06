namespace Perst
{
    using System;
	
    /// <summary> Object storage
    /// </summary>
    public abstract class Storage
    {
        abstract public IPersistent Root{get;set;}
        /// <summary> Open the storage
        /// </summary>
        /// <param name="dbFile">path to the database file
        /// </param>
        /// <param name="pagePoolSize">size of page pool (in bytes). Page pool should contain
        /// at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
        /// But larger page pool ussually leads to better performance (unless it could not fit
        /// in memory and cause swapping).
        /// 
        /// </param>
        abstract public void  open(System.String dbFile, int pagePoolSize);
		
        /// <summary> Open the storage with default page pool size
        /// </summary>
        /// <param name="dbFile">path to the database file
        /// 
        /// </param>
        public virtual void  open(System.String dbFile)
        {
            open(dbFile, 4 * 1024 * 1024);
        }
		
        /// <summary> Get storage root. Storage can have exactly one root object. 
        /// If you need to have several root object and access them by name (as is is possible 
        /// in many other OODBMSes), you should create index and use it as root object.
        /// </summary>
        /// <returns>root object or <code>null</code> if root is not specified (storage is not yet initialized)
        /// 
        /// </returns>
		
        /// <summary> Set new storage root object.
        /// Previous reference to the root object is rewritten but old root is not automatically deallocated.
        /// </summary>
        /// <param name="root">object to become new storage root. If it is not persistent yet, it is made
        /// persistent and stored in the storage
        /// 
        /// </param>
		
        /// <summary> Commit changes done by the lat transaction. Transaction is started implcitlely with forst update
        /// opertation.
        /// </summary>
        abstract public void  commit();
		
        /// <summary> Rollback changes made by the last transaction
        /// </summary>
        abstract public void  rollback();
		
        /// <summary> Create new index
        /// </summary>
        /// <param name="type">type of the index key (you should path here <code>String.class</code>, 
        /// <code>int.class</code>, ...)
        /// </param>
        /// <param name="unique">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing index
        /// </returns>
        /// <exception cref="">StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE) exception if 
        /// specified key type is not supported by implementation.
        /// 
        /// </exception>
        abstract public Index createIndex(System.Type type, bool unique);
		
        /// <summary> Create one-to-many link.
        /// </summary>
        /// <returns>new empty link, new members can be added to the link later.
        /// 
        /// </returns>
        abstract public Link createLink();
		
        /// <summary> Create relation object. Unlike link which represent embedded relation and stored
        /// inside owner object, this Relation object is standalone persisitent object
        /// containing references to owner and members of the relation
        /// </summary>
        /// <param name="owner">owner of the relation
        /// </param>
        /// <returns>object representing empty relation (relation with specified owner and no members), 
        /// new members can be added to the link later.
        /// 
        /// </returns>
        abstract public Relation createRelation(IPersistent owner);
		
        /// <summary> Commit transaction (if neeeded) and close the storage
        /// </summary>
        abstract public void  close();
		
        // Internal methods
		
        abstract protected internal void  deallocateObject(IPersistent obj);
		
        abstract protected internal void  storeObject(IPersistent obj);
		
        abstract protected internal void  loadObject(IPersistent obj);
		
        protected internal void  setObjectOid(IPersistent obj, int oid, bool raw)
        {
            Persistent po = (Persistent) obj;
            po.oid = oid;
            po.storage = this;
            po.raw = raw;
        }
    }
}