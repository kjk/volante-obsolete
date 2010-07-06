package org.garret.perst;

/**
 * Object storage
 */
public abstract class Storage { 
    /**
     * Open the storage
     * @param dbFile path to the database file
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool ussually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     */
    abstract public void open(String dbFile, int pagePoolSize);

    /**
     * Open the storage with default page pool size
     * @param dbFile path to the database file
     */ 
    public void open(String dbFile) {
        open(dbFile, 4*1024*1024);
    }
    
    /**
     * Explicitly make object persistent (assign to the storage).
     * If object is already persistent, this method has no effect
     * @param obj object to be made persistent.
     */
    public void makeObjectPeristent(IPersistent obj) {
        if (obj.getOid() == 0) { 
            storeObject(obj);
        }
    }

    /**
     * Get storage root. Storage can have exactly one root object. 
     * If you need to have several root object and access them by name (as is is possible 
     * in many other OODBMSes), you should create index and use it as root object.
     * @return root object or <code>null</code> if root is not specified (storage is not yet initialized)
     */
    abstract public IPersistent getRoot();
    
    /**
     * Set new storage root object.
     * Previous reference to the root object is rewritten but old root is not automatically deallocated.
     * @param root object to become new storage root. If it is not persistent yet, it is made
     * persistent and stored in the storage
     */
    abstract public void setRoot(IPersistent root);

    

    /**
     * Commit changes done by the lat transaction. Transaction is started implcitlely with forst update
     * opertation.
     */
    abstract public void commit();

    /**
     * Rollback changes made by the last transaction
     */
    abstract public void rollback();

    /**
     * Create new index
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    abstract public Index createIndex(Class type, boolean unique);

    /**
     * Create new field index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    abstract public FieldIndex createFieldIndex(Class type, String fieldName, boolean unique);

    /**
     * Create one-to-many link.
     * @return new empty link, new members can be added to the link later.
     */
    abstract public Link createLink();
    
    /**
     * Create relation object. Unlike link which represent embedded relation and stored
     * inside owner object, this Relation object is standalone persisitent object
     * containing references to owner and members of the relation
     * @param owner owner of the relation
     * @return object representing empty relation (relation with specified owner and no members), 
     * new members can be added to the link later.
     */
    abstract public Relation createRelation(IPersistent owner);

    /**
     * Commit transaction (if needed) and close the storage
     */
    abstract public void close();

    /**
     * Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
     * Long.MAX_VALUE). If it is set to the value different from Long.MAX_VALUE, GC will be started each time when
     * delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator. 
     * @param allocatedDelta delta between total size of allocated and deallocated object since last GC 
     * or storage opening 
     */
    abstract public void setGcThreshold(long allocatedDelta);

    /**
     * Explicit start of garbage collector
     */
    abstract public void gc();

    // Internal methods

    abstract protected void deallocateObject(IPersistent obj);

    abstract protected void storeObject(IPersistent obj);

    abstract protected void loadObject(IPersistent obj);

    final protected void setObjectOid(IPersistent obj, int oid, boolean raw) { 
        Persistent po = (Persistent)obj;
        po.oid = oid;
        po.storage = this;
        po.raw = raw;
    }
}


