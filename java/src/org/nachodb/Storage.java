package org.nachodb;

/**
 * Object storage
 */
public interface Storage { 
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
    public void open(String filePath, int pagePoolSize);

    /**
     * Open the storage
     * @param file user specific implementation of IFile interface
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool ussually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     */
    public void open(IFile file, int pagePoolSize);

    /**
     * Open the storage with default page pool size
     * @param file user specific implementation of IFile interface
     */ 
    public void open(IFile file);

    /**
     * Open the storage with default page pool size
     * @param filePath path to the database file
     */ 
    public void open(String filePath);

    /**
     * Open the encrypted storage
     * @param filePath path to the database file
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool ussually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     * @param cipherKey cipher key
     */
    public void open(String filePath, int pagePoolSize, String cipherKey);

    /**
     * Check if database is opened
     * @return <code>true</code> if database was opened by <code>open</code> method, 
     * <code>false</code> otherwise
     */
    public boolean isOpened();
    
    /**
     * Get storage root. Storage can have exactly one root object. 
     * If you need to have several root object and access them by name (as is is possible 
     * in many other OODBMSes), you should create index and use it as root object.
     * @return root object or <code>null</code> if root is not specified (storage is not yet initialized)
     */
    public IPersistent getRoot();
    
    /**
     * Set new storage root object.
     * Previous reference to the root object is rewritten but old root is not automatically deallocated.
     * @param root object to become new storage root. If it is not persistent yet, it is made
     * persistent and stored in the storage
     */
    public void setRoot(IPersistent root);

    

    /**
     * Commit changes done by the last transaction. Transaction is started implcitlely with forst update
     * opertation.
     */
    public void commit();

    /**
     * Rollback changes made by the last transaction
     */
    public void rollback();


    /**
     * Backup current state of database
     * @param out output stream to which backup is done
     */
    public void backup(java.io.OutputStream out) throws java.io.IOException;

    /**
     * Exclusive per-thread transaction: each thread access database in exclusive mode
     */
    public static final int EXCLUSIVE_TRANSACTION   = 0;
    /**
     * Cooperative mode; all threads share the same transaction. Commit will commit changes made
     * by all threads. To make this schema work correctly, it is necessary to ensure (using locking)
     * that no thread is performing update of the database while another one tries to perform commit.
     * Also please notice that rollback will undo the work of all threads. 
     */
    public static final int COOPERATIVE_TRANSACTION = 1;
    /**
     * Serializable per-thread transaction. Unlike exclusive mode, threads can concurrently access database, 
     * but effect will be the same as them work exclusively.
     * To provide such behavior, programmer should lock all access objects (or use hierarchical locking).
     * When object is updated, exclusive lock should be set, otherwise shared lock is enough.
     * Lock should be preserved until the end of transaction.
     */
    public static final int SERIALIZABLE_TRANSACTION = 2;

    /**
     * Read only transaction which can be started at replicastion slave node.
     * It runs concurrently with receiving updates from master node.
     */
    public static final int REPLICATION_SLAVE_TRANSACTION = 3;

    /** 
     * Begin per-thread transaction. Three types of per-thread transactions are supported: 
     * exclusive, cooperative and serializable. In case of exclusive transaction, only one 
     * thread can update the database. In cooperative mode, multiple transaction can work 
     * concurrently and commit() method will be invoked only when transactions of all threads
     * are terminated. Serializable transactions can also work concurrently. But unlike
     * cooperative transaction, the threads are isolated from each other. Each thread
     * has its own associated set of modified objects and committing the transaction will cause
     * saving only of these objects to the database. To synchronize access to the objects
     * in case of serializable transaction programmer should use lock methods
     * of IResource interface. Shared lock should be set before read access to any object, 
     * and exclusive lock - before write access. Locks will be automatically released when
     * transaction is committed (so programmer should not explicitly invoke unlock method)
     * In this case it is guaranteed that transactions are serializable.<br>
     * It is not possible to use <code>IPersistent.store()</code> method in
     * serializable transactions. That is why it is also not possible to use Index and FieldIndex
     * containers (since them are based on B-Tree and B-Tree directly access database pages
     * and use <code>store()</code> method to assign OID to inserted object. 
     * You should use <code>SortedCollection</code> based on T-Tree instead or alternative
     * B-Tree implemenataion (set "perst.alternative.btree" property).
     * @param mode <code>EXCLUSIVE_TRANSACTION</code>, <code>COOPERATIVE_TRANSACTION</code>, 
     * <code>SERIALIZABLE_TRANSACTION</code> or <code>REPLICATION_SLAVE_TRANSACTION</code>
     */
    public void beginThreadTransaction(int mode);
    
    /**
     * End per-thread transaction started by beginThreadTransaction method.<br>
     * If transaction is <i>exclusive</i>, this method commits the transaction and
     * allows other thread to proceed.<br>
     * If transaction is <i>serializable</i>, this method commits sll changes done by this thread
     * and release all locks set by this thread.<br>     
     * If transaction is <i>cooperative</i>, this method decrement counter of cooperative
     * transactions and if it becomes zero - commit the work
     */
    public void endThreadTransaction();

    /**
     * End per-thread cooperative transaction with specified maximal delay of transaction
     * commit. When cooperative transaction is ended, data is not immediately committed to the
     * disk (because other cooperative transaction can be active at this moment of time).
     * Instead of it cooperative transaction counter is decremented. Commit is performed
     * only when this counter reaches zero value. But in case of heavy load there can be a lot of
     * requests and so a lot of active cooperative transactions. So transaction counter never reaches zero value.
     * If system crash happens a large amount of work will be lost in this case. 
     * To prevent such scenario, it is possible to specify maximal delay of pending transaction commit.
     * In this case when such timeout is expired, new cooperative transaction will be blocked until
     * transaction is committed.
     * @param maxDelay maximal delay in milliseconds of committing transaction.  Please notice, that Perst could 
     * not force other threads to commit their cooperative transactions when this timeout is expired. It will only
     * block new cooperative transactions to make it possible to current transaction to complete their work.
     * If <code>maxDelay</code> is 0, current thread will be blocked until all other cooperative trasnaction are also finished
     * and changhes will be committed to the database.
     */
    public void endThreadTransaction(int maxDelay);
   
    /**
     * Rollback per-thread transaction. It is safe to use this method only for exclusive transactions.
     * In case of cooperative transactions, this method rollback results of all transactions.
     */
    public void rollbackThreadTransaction();

    /**
     * Create new persistent set
     * @return persistent object implementing set
     */
    public IPersistentSet createSet();

    /**
     * Create new scalable set references to persistent objects.
     * This container can effciently store small number of references as well as very large
     * number references. When number of memers is small, Link class is used to store 
     * set members. When number of members exceed some threshold, PersistentSet (based on B-Tree)
     * is used instead.
     * @return scalable set implementation
     */
    public IPersistentSet createScalableSet();

    /**
     * Create new scalable set references to persistent objects.
     * This container can effciently store small number of references as well as very large
     * number references. When number of memers is small, Link class is used to store 
     * set members. When number of members exceed some threshold, PersistentSet (based on B-Tree)
     * is used instead.
     * @param initialSize initial size of the set
     * @return scalable set implementation
     */
    public IPersistentSet createScalableSet(int initialSize);

    /**
     * Create new index
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public Index createIndex(Class type, boolean unique);

    /**
     * Create new thick index (index with large number of duplicated keys)
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public Index createThickIndex(Class type);

    /**
     * Create new bit index. Bit index is used to select object 
     * with specified set of (boolean) properties.
     * @return persistent object implementing bit index
     */
    public BitIndex createBitIndex();

    /**
     * Create new field index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public FieldIndex createFieldIndex(Class type, String fieldName, boolean unique);

    /**
     * Create new mutlifield index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldNames names of the index fields. Fields with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public FieldIndex createFieldIndex(Class type, String[] fieldNames, boolean unique);

    /**
     * Create new spatial index with integer coordinates
     * @return persistent object implementing spatial index
     */
    public SpatialIndex createSpatialIndex();

    /**
     * Create new R2 spatial index 
     * @return persistent object implementing spatial index
     */
    public SpatialIndexR2 createSpatialIndexR2();

    /**
     * Create new sorted collection with specified comparator
     * @param comparator comparator class specifying order in the collection
     * @param unique whether index is collection (members with the same key value are not allowed)
     * @return persistent object implementing sorted collection
     */
    public SortedCollection createSortedCollection(PersistentComparator comparator, boolean unique);

    /**
     * Create new sorted collection. Members of this collections should implement 
     * <code>java.lang.Comparable</code> interface and make it possible to compare collection members
     * with each other as well as with serch key.
     * @param unique whether index is collection (members with the same key value are not allowed)
     * @return persistent object implementing sorted collection
     */
    public SortedCollection createSortedCollection(boolean unique);

    /**
     * Create one-to-many link.
     * @return new empty link, new members can be added to the link later.
     */
    public Link createLink();
    
    /**
     * Create one-to-many link with specified initially allocated size.
     * @param initialSize initial size of array
     * @return new empty link, new members can be added to the link later.
     */
    public Link createLink(int initialSize);
    
    /**
     * Create relation object. Unlike link which represent embedded relation and stored
     * inside owner object, this Relation object is standalone persisitent object
     * containing references to owner and members of the relation
     * @param owner owner of the relation
     * @return object representing empty relation (relation with specified owner and no members), 
     * new members can be added to the link later.
     */
    public Relation createRelation(IPersistent owner);


    /**
     * Create new BLOB. Create object for storing large binary data.
     * @return empty BLOB
     */
    public Blob createBlob();

    /**
     * Create new time series object. 
     * @param blockClass class derived from TimeSeries.Block
     * @param maxBlockTimeInterval maximal difference in milliseconds between timestamps 
     * of the first and the last elements in a block. 
     * If value of this parameter is too small, then most blocks will contains less elements 
     * than preallocated. 
     * If it is too large, then searching of block will be inefficient, because index search 
     * will select a lot of extra blocks which do not contain any element from the 
     * specified range.
     * Usually the value of this parameter should be set as
     * (number of elements in block)*(tick interval)*2. 
     * Coefficient 2 here is used to compencate possible holes in time series.
     * For example, if we collect stocks data, we will have data only for working hours.
     * If number of element in block is 100, time series period is 1 day, then
     * value of maxBlockTimeInterval can be set as 100*(24*60*60*1000)*2
     * @return new empty time series
     */
    public TimeSeries createTimeSeries(Class blockClass, long maxBlockTimeInterval);


    /**
     * Create PATRICIA trie (Practical Algorithm To Retrieve Information Coded In Alphanumeric)
     * Tries are a kind of tree where each node holds a common part of one or more keys. 
     * PATRICIA trie is one of the many existing variants of the trie, which adds path compression 
     * by grouping common sequences of nodes together.<BR>
     * This structure provides a very efficient way of storing values while maintaining the lookup time 
     * for a key in O(N) in the worst case, where N is the length of the longest key. 
     * This structure has it's main use in IP routing software, but can provide an interesting alternative 
     * to other structures such as hashtables when memory space is of concern.
     * @return created PATRICIA trie
     */
    public PatriciaTrie createPatriciaTrie();

    /**
     * Commit transaction (if needed) and close the storage
     */
    public void close();

    /**
     * Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
     * Long.MAX_VALUE). If it is set to the value different from Long.MAX_VALUE, GC will be started each time when
     * delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator. 
     * @param allocatedDelta delta between total size of allocated and deallocated object since last GC 
     * or storage opening 
     */
    public void setGcThreshold(long allocatedDelta);

    /**
     * Explicit start of garbage collector
     * @return number of collected (deallocated) objects
     */
    public int gc();

    /**
     * Export database in XML format 
     * @param writer writer for generated XML document
     */
    public void exportXML(java.io.Writer writer) throws java.io.IOException;

    /**
     * Import data from XML file
     * @param reader XML document reader
     */
    public void importXML(java.io.Reader reader) throws XMLImportException;

    /**
     * Retrieve object by OID. This method should be used with care because
     * if object is deallocated, its OID can be reused. In this case
     * getObjectByOID will return reference to the new object with may be
     * different type.
     * @param oid object oid
     * @return reference to the object with specified OID
     */
    public IPersistent getObjectByOID(int oid);

    /**
     * Explicitely make object persistent. Usually objects are made persistent
     * implicitlely using "persistency on reachability apporach", but this
     * method allows to do it explicitly. If object is already persistent, execution of
     * this method has no effect.
     * @param obj object to be made persistent
     * @return OID assigned to the object  
     */
    public int makePersistent(IPersistent obj);

 
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
     * <TR><TD><code>perst.object.cache.kind</code></TD><TD>String</TD><TD>"lru"</TD>
     * <TD>Kind of object cache. The following values are supported:
     * "strong", "weak", "soft", "lru". <B>String</B> cache uses strong (normal) 
     * references to refer persistent objects. Thus none of loaded persistent objects
     * can be deallocated by GC. <B>Weak</B> cache use weak references and
     * soft cache - <B>soft</B> references. The main difference between soft and weak references is
     * that garbage collector is not required to remove soft referenced objects immediately
     * when object is detected to be <i>soft referenced</i>, so it may improve caching of objects. 
     * But it also may increase amount of memory
     * used  by application, and as far as persistent object requires finalization
     * it can cause memory overflow even though garbage collector is required
     * to clear all soft references before throwing OutOfMemoryException.<br>
     * But Java specification says nothing about the policy used by GC for soft references
     * (except the rule mentioned above). Unlike it <B>lru</B> cache provide determined behavior, 
     * pinning most recently used objects in memory. Number of pinned objects is determined 
     * for lru cache by <code>perst.object.index.init.size</code> parameter (it can be 0).      
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
     * <TR><TD><code>perst.gc.threshold</code></TD><TD>Long</TD><TD>Long.MAX_VALUE</TD>
     * <TD>Threshold for initiation of garbage collection. 
     * If it is set to the value different from Long.MAX_VALUE, GC will be started each time 
     * when delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator.
     * </TD></TR>
     * <TR><TD><code>perst.lock.file</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Lock database file to prevent concurrent access to the database by 
     * more than one application.
     * </TD></TR>
     * <TR><TD><code>perst.file.readonly</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Database file should be opened in read-only mode.
     * </TD></TR>
     * <TR><TD><code>perst.file.noflush</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Do not flush file during transaction commit. It will greatly increase performance because
     * eliminate synchronous write to the disk (when program has to wait until all changed
     * are actually written to the disk). But it can cause database corruption in case of 
     * OS or power failure (but abnormal termination of application itself should not cause
     * the problem, because all data which were written to the file, but is not yet saved to the disk is 
     * stored in OS file buffers and sooner or later them will be written to the disk)
     * </TD></TR>
     * <TR><TD><code>perst.alternative.btree</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Use aternative implementation of B-Tree (not using direct access to database
     * file pages). This implementation should be used in case of serialized per thread transctions.
     * New implementation of B-Tree will be used instead of old implementation
     * if "perst.alternative.btree" property is set. New B-Tree has incompatible format with 
     * old B-Tree, so you could not use old database or XML export file with new indices. 
     * Alternative B-Tree is needed to provide serializable transaction (old one could not be used).
     * Also it provides better performance (about 3 times comaring with old implementation) because
     * of object caching. And B-Tree supports keys of user defined types. 
     * </TD></TR>
     * <TR><TD><code>perst.background.gc</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Perform garbage collection in separate thread without blocking the main application.
     * </TD></TR>
     * <TR><TD><code>perst.string.encoding</code></TD><TD>String</TD><TD>null</TD>
     * <TD>Specifies encoding of storing strings in the database. By default Perst stores 
     * strings as sequence of chars (two bytes per char). If all strings in application are in 
     * the same language, then using encoding  can signifficantly reduce space needed
     * to store string (about two times). But please notice, that this option has influence
     * on all strings  stored in database. So if you already have some data in the storage
     * and then change encoding, then it can cause incorrect fetching of strings and even database crash.
     * </TD></TR>
     * <TR><TD><code>perst.replication.ack</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Request acknowledgement from slave that it receives all data before transaction
     * commit. If this option is not set, then replication master node just writes
     * data to the socket not warring whether it reaches slave node or not.
     * When this option is set to true, master not will wait during each transaction commit acknowledgement
     * from slave node. Please notice that this option should be either set or not set both
     * at slave and master node. If it is set only on one of this nodes then behavior of
     * the system is unpredicted. This option can be used both in synchronous and asynchronous replication
     * mode. The only difference is that in first case main application thread will be blocked waiting
     * for acknowledgment, while in the asynchronous mode special replication thread will be blocked
     * allowing thread performing commit to proceed.
     * </TD></TR>
     * </TABLE>
     * @param name name of the property
     * @param value value of the property (for boolean properties pass <code>java.lang.Boolean.TRUE</code>
     * and <code>java.lang.Boolean.FALSE</code>
     */
    public void setProperty(String name, Object value);

   /**
     * Set database properties. This method should be invoked before opening database. 
     * For list of supported properties please see <code>setProperty</code> command. 
     * All not recognized properties are ignored.
     */
    public void setProperties(java.util.Properties props);

    /**
     * Set storage listener.
     * @param listener new storage listener (may be null)
     * @return previous storage listener
     */
    public StorageListener setListener(StorageListener listener);

    /**
     * Get database memory dump. This function returns hashmap which key is classes
     * of stored objects and value - MemoryUsage object which specifies number of instances
     * of particular class in the storage and total size of memory used by these instance.
     * Size of internal database structures (object index,* memory allocation bitmap) is associated with 
     * <code>Storage</code> class. Size of class descriptors  - with <code>java.lang.Class</code> class.
     * <p>This method traverse the storage as garbage collection do - starting from the root object
     * and recursively visiting all reachable objects. So it reports statistic only for visible objects.
     * If total database size is significantly larger than total size of all instances reported
     * by this method, it means that there is garbage in the database. You can explicitly invoke
     * garbage collector in this case.</p> 
     */
    public java.util.HashMap getMemoryDump();
    

    /**
     * Get total size of all allocated objects in the database
     */
    public long getUsedSize();

    /**
     * Get size of the database
     */
    public long getDatabaseSize();

    /**
     * Set class loader. This class loader will be used to locate classes for 
     * loaded class descriptors. If class loader is not specified or
     * it did find the class, then <code>Thread.getContextClassLoader().loadClass()</code> method
     * will be used to get class for the specified name.
     * @param loader class loader
     * @return previous class loader or null if not specified
     */
    public ClassLoader setClassLoader(ClassLoader loader);

    /**
     * Get class loader used to locate classes for 
     * loaded class descriptors.
     * @return class loader previously set by <code>setClassLoader</code>
     * method or <code>null</code> if not specified. 
     */
    public ClassLoader getClassLoader();

    // Internal methods (I haev to made them public to be used by AspectJ API)

    public void deallocateObject(IPersistent obj);

    public void storeObject(IPersistent obj);

    public void storeFinalizedObject(IPersistent obj);

    public void modifyObject(IPersistent obj);

    public void loadObject(IPersistent obj);

    public void lockObject(IPersistent obj);
}


