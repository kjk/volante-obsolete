package org.garret.perst;

/**
 * Object storage
 */
public abstract class Storage { 
    /**
     * Constant specifying that page pool should be dynamically extended 
     * to conatins all database file pages
     */
    public static final int INFINITE_PAGE_POOL = 0;
    /**
     * Constant specifying default pool size
     */
    public static final int DEFAULT_PAGE_POOL_SIZE = 4*1024*1024;

    /**
     * Open the storage
     * @param filePath path to the database file
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool usually leads to better performance (unless it could not fit
     * in memory and cause swapping). Value 0 of this paremeter corresponds to infinite
     * page pool (all pages are cashed in memory). It is especially useful for in-memory
     * database, when storage is created with NullFile.
     * 
     */
    abstract public void open(String filePath, int pagePoolSize);

    /**
     * Open the storage
     * @param file user specific implementation of IFile interface
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool ussually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     */
    abstract public void open(IFile file, int pagePoolSize);

    /**
     * Open the storage with default page pool size
     * @param file user specific implementation of IFile interface
     */ 
    public void open(IFile file) {
        open(file, DEFAULT_PAGE_POOL_SIZE);
    }

    /**
     * Open the storage with default page pool size
     * @param filePath path to the database file
     */ 
    public void open(String filePath) {
        open(filePath, DEFAULT_PAGE_POOL_SIZE);
    }

    /**
     * Check if database is opened
     * @return <code>true</code> if database was opened by <code>open</code> method, 
     * <code>false</code> otherwise
     */
    abstract public boolean isOpened();
    
    /**
     * Explicitly make object persistent (assign to the storage).
     * If object is already persistent, this method has no effect
     * @param obj object to be made persistent.
     */
    public void makeObjectPersistent(IPersistent obj) {
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
     * Create new peristent set
     * @return persistent object implementing set
     */
    abstract public java.util.Set createSet();

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
     * Create new spatial index
     * @return persistent object implementing spatial index
     */
    abstract public SpatialIndex createSpatialIndex();

    /**
     * Create new sorted collection
     * @param comparator comparator class specifying order in the collection
     * @param unique whether index is collection (members with the same key value are not allowed)
     * @return persistent object implementing sorted collection
     */
    abstract public SortedCollection createSortedCollection(PersistentComparator comparator, boolean unique);

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

    /**
     * Export database in XML format 
     * @param writer writer for generated XML document
     */
    abstract public void exportXML(java.io.Writer writer) throws java.io.IOException;

    /**
     * Import data from XML file
     * @param reader XML document reader
     */
    abstract public void importXML(java.io.Reader reader) throws XMLImportException;

    /**
     * Retrieve object by OID. This method should be used with care because
     * if object is deallocated, its OID can be reused. In this case
     * getObjectByOID will return reference to the new object with may be
     * different type.
     * @param oid object oid
     * @return reference to the object with specified OID
     */
    abstract public IPersistent getObjectByOID(int oid);

    /**
     * Set database property. This method should be invoked before opening database. 
     * Currently the following boolean properties are supported:
     * <TABLE><TR><TH>Property name</TH><TH>Parameter type</TH><TH>Default value</TH><TH>Description</TH></TR>
     * <TR><TD><code>perst.implicit.values</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Treate any class not derived from IPersistent as <i>value</i>. 
     * This object will be embedded inside persistent object containing reference to this object.
     * If this object is referenced from N persistent object, N instances of this object
     * will be stored in the database and after loading there will be N instances in memory. 
     * As well as persistent capable classes, value classes should have default constructor (constructor
     * with empty list of parameters) or has no constructors at all. For example <code>Integer</code>
     * class can not be stored as value in PERST because it has no such constructor. In this case 
     * serialization mechanism can be used (see below)
     * </TD></TR>
     * <TR><TD><code>perst.serialize.transient.objects</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Serialize any class not derived from IPersistent or IValue using standard Java serialization
     * mechanism. Packed object closure is stored in database as byte array. Latter the same mechanism is used
     * to unpack the objects. To be able to use this mechanism object and all objects referenced from it
     * should implement <code>java.io.Serializable</code> interface and should not contain references
     * to persistent objects. If such object is referenced from N persistent object, N instances of this object
     * will be stored in the database and after loading there will be N instances in memory.
     * </TD></TR>
     * <TR><TD><code>perst.object.cache.init.size</code></TD><TD>Integer</TD><TD>1319</TD>
     * <TD>Initial size of object cache
     * </TD></TR>
     * <TR><TD><code>perst.object.index.init.size</code></TD><TD>Integer</TD><TD>1024</TD>
     * <TD>Initial size of object index (specifying large value increase initial size of database, but reduce
     * number of index reallocations)
     * </TD></TR>
     * <TR><TD><code>perst.extension.quantum</code></TD><TD>Long</TD><TD>1048576</TD>
     * <TD>Object allocation bitmap extension quantum. Memory is allocate by scanning bitmap. If there is no
     * large enough hole, then database is extended by the value of dbDefaultExtensionQuantum. 
     * This parameter should not be smaller than 64Kb.
     * </TD></TR>
     * <TR><TD><code>perst.modification.list.limit</code></TD><TD>Integer</TD><TD>Integer.MAX_INT</TD>
     * <TD>Maximal size of modified object list. When this limit is reached, PERST will 
     * store and remove objects from the head of the list. Setting this parameter will help to 
     * prevent memory exhaustion if a lot of persistent objects are modified during transaction. 
     * When list is not limited, all modified objects are pinned in memory. 
     * To prevent loose of modifications in case of limited modification list, you should
     * invoke <code>Persistent.modify</code> method <b>after</b> object has been updated.
     * </TD></TR>
     * <TR><TD><code>perst.gc.threshold</code></TD><TD>Long</TD><TD>Long.MAX_VALUE</TD>
     * <TD>Threshold for initiation of garbage collection. 
     * If it is set to the value different from Long.MAX_VALUE, GC will be started each time 
     * when delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator.
     * </TD></TR>
     * </TABLE>
     * @param name name of the property
     * @param value value of the property (for boolean properties pass <code>java.lang.Boolean.TRUE</code>
     * and <code>java.lang.Boolean.FALSE</code>
     */
    abstract public void setProperty(String name, Object value);

   /**
     * Set database properties. This method should be invoked before opening database. 
     * For list of supported properties please see <code>setProperty</code> command. 
     * All not recognized properties are ignored.
     */
    abstract public void setProperties(java.util.Properties props);

    
    // Internal methods

    abstract protected void deallocateObject(IPersistent obj);

    abstract protected void storeObject(IPersistent obj);

    abstract protected void modifyObject(IPersistent obj);

    abstract protected void loadObject(IPersistent obj);

    final protected void setObjectOid(IPersistent obj, int oid, boolean raw) { 
        Persistent po = (Persistent)obj;
        po.oid = oid;
        po.storage = this;
        po.state = raw ? Persistent.RAW : 0;
    }
}


