namespace Perst
{
    using System;
	
    public enum TransactionMode
    { 
        Exclusive,
        Cooperative,
        Serializable
    };
	
	
    /// <summary> Object storage
    /// </summary>
    public abstract class Storage
    {
        /// <summary> Get/set storage root. Storage can have exactly one root object. 
        /// If you need to have several root object and access them by name (as is is possible 
        /// in many other OODBMSes), you should create index and use it as root object.
        /// Previous reference to the root object is rewritten but old root is not automatically deallocated.
        /// </summary>
        abstract public IPersistent Root {get; set;}
      
        /// <summary> 
        /// Constant specifying that page pool should be dynamically extended 
        /// to conatins all database file pages
        /// </summary>
        public const int INFINITE_PAGE_POOL = 0;

        /// <summary>
        /// Constant specifying default pool size
        /// </summary>
        public const int DEFAULT_PAGE_POOL_SIZE = 4*1024*1024;

        /// <summary> Open the storage
        /// </summary>
        /// <param name="filePath">path to the database file
        /// </param>
        /// <param name="pagePoolSize">size of page pool (in bytes). Page pool should contain
        /// at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
        /// But larger page pool ussually leads to better performance (unless it could not fit
        /// in memory and cause swapping).
        /// 
        /// </param>
        abstract public void  Open(String filePath, int pagePoolSize);
		
        /// <summary> Open the storage with default page pool size
        /// </summary>
        /// <param name="filePath">path to the database file
        /// 
        /// </param>
        public virtual void  Open(String filePath)
        {
            Open(filePath, DEFAULT_PAGE_POOL_SIZE);
        }
		
        /// <summary> Open the storage
        /// </summary>
        /// <param name="file">user specific implementation of IFile interface
        /// </param>
        /// <param name="pagePoolSize">size of page pool (in bytes). Page pool should contain
        /// at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
        /// But larger page pool ussually leads to better performance (unless it could not fit
        /// in memory and cause swapping).
        /// 
        /// </param>
        abstract public void  Open(IFile file, int pagePoolSize);
		
        /// <summary> Open the storage with default page pool size
        /// </summary>
        /// <param name="file">user specific implementation of IFile interface
        /// </param>
        public virtual void  Open(IFile file)
        {
            Open(file, DEFAULT_PAGE_POOL_SIZE);
        }
		
        /// <summary> Open the encrypted storage
        /// </summary>
        /// <param name="filePath">path to the database file
        /// </param>
        /// <param name="pagePoolSize">size of page pool (in bytes). Page pool should contain
        /// at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
        /// But larger page pool ussually leads to better performance (unless it could not fit
        /// in memory and cause swapping).
        /// </param>
        /// <param name="cipherKey">cipher key</param>
        abstract public void  Open(String filePath, int pagePoolSize, String cipherKey);

        
        /// <summary>Check if database is opened
        /// </summary>
        /// <returns><code>true</code> if database was opened by <code>open</code> method, 
        /// <code>false</code> otherwise
        /// </returns>        
        abstract public bool IsOpened();
		
        /// <summary> Set new storage root object.
        /// </summary>
        /// <param name="root">object to become new storage root. If it is not persistent yet, it is made
        /// persistent and stored in the storage
        /// 
        /// </param>
		
        /// <summary> Commit changes done by the last transaction. Transaction is started implcitlely with forst update
        /// opertation.
        /// </summary>
        abstract public void  Commit();
		
        /// <summary> Rollback changes made by the last transaction
        /// </summary>
        abstract public void  Rollback();
		
        /// <summary>
        /// Backup current state of database
        /// </summary>
        /// <param name="stream">output stream to which backup is done</param>
        abstract public void Backup( System.IO.Stream stream);

        /// <summary> Create new index
        /// </summary>
        /// <param name="type">type of the index key (you should path here <code>String.class</code>, 
        /// <code>int.class</code>, ...)
        /// </param>
        /// <param name="unique">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing index
        /// </returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE) exception if 
        /// specified key type is not supported by implementation.
        /// 
        /// </exception>
        abstract public Index CreateIndex(Type type, bool unique);
		
        /// <summary> 
        /// Create new field index
        /// </summary>
        /// <param name="type">objects of which type (or derived from which type) will be included in the index
        /// </param>
        /// <param name="fieldName">name of the index field. Field with such name should be present in specified class <code>type</code>
        /// </param>
        /// <param name="unique">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing field index
        /// </returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,
        /// StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
        /// </exception>
        abstract public FieldIndex CreateFieldIndex(Type type, string fieldName, bool unique);
		
        /// <summary> 
        /// Create new multi-field index
        /// </summary>
        /// <param name="type">objects of which type (or derived from which type) will be included in the index
        /// </param>
        /// <param name="fieldNames">array of names of the fields. Field with such name should be present in specified class <code>type</code>
        /// </param>
        /// <param name="unique">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing field index
        /// </returns>
        /// <exception cref="Perst.StorageError">StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,
        /// StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
        /// </exception>
        abstract public FieldIndex CreateFieldIndex(Type type, string[] fieldNames, bool unique);
		
        /// <summary>
        /// Create new spatial index with integer coordinates
        /// </summary>
        /// <returns>
        /// persistent object implementing spatial index
        /// </returns>
        abstract public SpatialIndex CreateSpatialIndex();

        /// <summary>
        /// Create new R2 spatial index
        /// </summary>
        /// <returns>
        /// persistent object implementing spatial index
        /// </returns>
        abstract public SpatialIndexR2 CreateSpatialIndexR2();

        /// <summary>
        /// Create new sorted collection with specified comparator
        /// </summary>
        /// <param name="comparator">comparator class specifying order in the collection</param>
        /// <param name="unique"> whether collection is unique (members with the same key value are not allowed)</param>
        /// <returns> persistent object implementing sorted collection</returns>
        abstract public SortedCollection CreateSortedCollection(PersistentComparator comparator, bool unique);

        /// <summary>
        /// Create new sorted collection. Members of this collections should implement 
        /// <code>System.IComparable</code> interface and make it possible to compare 
        /// collection members with each other as well as with serch key.
        /// </summary>
        /// <param name="unique"> whether collection is unique (members with the same key value are not allowed)</param>
        /// <returns> persistent object implementing sorted collection</returns>
        abstract public SortedCollection CreateSortedCollection(bool unique);

        /// <summary>
        /// Create new object set
        /// </summary>
        /// <returns>
        /// empty set of persistent objects
        /// </returns>
        abstract public ISet CreateSet();

        /// <summary> Create one-to-many link.
        /// </summary>
        /// <returns>new empty link, new members can be added to the link later.
        /// 
        /// </returns>
        abstract public Link CreateLink();
		
        /// <summary> Create one-to-many link with specified initial size.
        /// </summary>
        /// <param name="intialSize">initial size of the array</param>
        /// <returns>new empty link, new members can be added to the link later.
        /// 
        /// </returns>
        abstract public Link CreateLink(int initialSize);
		
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
        abstract public Relation CreateRelation(IPersistent owner);


        /// <summary>
        /// Create new BLOB. Create object for storing large binary data.
        /// </summary>
        /// <returns>empty BLOB</returns>
        abstract public Blob CreateBlob();

        /// <summary>
        /// Create new time series object. 
        /// </summary>
        /// <param name="blockClass">class derived from TimeSeriesBlock</param>
        /// <param name="maxBlockTimeInterval">maximal difference in system ticks (100 nanoseconds) between timestamps 
        /// of the first and the last elements in a block. 
        /// If value of this parameter is too small, then most blocks will contains less elements 
        /// than preallocated. 
        /// If it is too large, then searching of block will be inefficient, because index search 
        /// will select a lot of extra blocks which do not contain any element from the 
        /// specified range.
        /// Usually the value of this parameter should be set as
        /// (number of elements in block)*(tick interval)*2. 
        /// Coefficient 2 here is used to compencate possible holes in time series.
        /// For example, if we collect stocks data, we will have data only for working hours.
        /// If number of element in block is 100, time series period is 1 day, then
        /// value of maxBlockTimeInterval can be set as 100*(24*60*60*10000000L)*2
        /// </param>
        /// <returns>new empty time series</returns>
        abstract public TimeSeries CreateTimeSeries(Type blockClass, long maxBlockTimeInterval);
		
        /// <summary> Commit transaction (if needed) and close the storage
        /// </summary>
        abstract public void  Close();

        /// <summary> Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
        /// Int64.MaxValue). If it is set to the value different fro Long.MAX_VALUE, GC will be started each time when
        /// delta between total size of allocated and deallocated objects exceeds specified threashold OR
        /// after reaching end of allocation bitmap in allocator. 
        /// </summary>
        /// <param name="allocatedDelta"> delta between total size of allocated and deallocated object since last GC or storage opening
        /// </param>
        ///
        abstract public void SetGcThreshold(long allocatedDelta);

        /// <summary>Explicit start of garbage collector
        /// </summary>
        /// <returns>number of collected (deallocated) objects</returns>
        /// 
        abstract public int Gc();

        /// <summary> Export database in XML format 
        /// </summary>
        /// <param name="writer">writer for generated XML document
        /// 
        /// </param>
        abstract public void  ExportXML(System.IO.StreamWriter writer);
		
        /// <summary> Import data from XML file
        /// </summary>
        /// <param name="reader">XML document reader
        /// 
        /// </param>
        abstract public void  ImportXML(System.IO.StreamReader reader);
		
        		
        /// <summary> 
        /// Retrieve object by OID. This method should be used with care because
        /// if object is deallocated, its OID can be reused. In this case
        /// getObjectByOID will return reference to the new object with may be
        /// different type.
        /// </summary>
        /// <param name="oid">object oid</param>
        /// <returns>reference to the object with specified OID</returns>
        abstract public IPersistent GetObjectByOID(int oid);

        ///
        /// <summary>
        /// Set database property. This method should be invoked before opening database. 
        /// </summary>
        /// <remarks> 
        /// Currently the following boolean properties are supported:
        /// <TABLE><TR><TH>Property name</TH><TH>Parameter type</TH><TH>Default value</TH><TH>Description</TH></TR>
        /// <TR><TD><code>perst.serialize.transient.objects</code></TD><TD>bool</TD><TD>false</TD>
        /// <TD>Serialize any class not derived from IPersistent or IValue using standard Java serialization
        /// mechanism. Packed object closure is stored in database as byte array. Latter the same mechanism is used
        /// to unpack the objects. To be able to use this mechanism, object and all objects referenced from it
        /// should be marked with Serializable attribute and should not contain references
        /// to persistent objects. If such object is referenced from N persistent object, N instances of this object
        /// will be stored in the database and after loading there will be N instances in memory.
        /// </TD></TR>
        /// <TR><TD><code>perst.object.cache.init.size</code></TD><TD>int</TD><TD>1319</TD>
        /// <TD>Initial size of object cache
        /// </TD></TR>
        /// <TR><TD><code>perst.object.index.init.size</code></TD><TD>int</TD><TD>1024</TD>
        /// <TD>Initial size of object index (specifying large value increase initial size of database, but reduce
        /// number of index reallocations)
        /// </TD></TR>
        /// <TR><TD><code>perst.extension.quantum</code></TD><TD>long</TD><TD>1048576</TD>
        /// <TD>Object allocation bitmap extension quantum. Memory is allocate by scanning bitmap. If there is no
        /// large enough hole, then database is extended by the value of dbDefaultExtensionQuantum. 
        /// This parameter should not be smaller than 64Kb.
        /// </TD></TR>
        /// <TR><TD><code>perst.gc.threshold</code></TD><TD>long</TD><TD>long.MaxValue</TD>
        /// <TD>Threshold for initiation of garbage collection. 
        /// If it is set to the value different from long.MaxValue, GC will be started each time 
        /// when delta between total size of allocated and deallocated objects exceeds specified threashold OR                                                                                                                                                                                                                           
        /// after reaching end of allocation bitmap in allocator.
        /// </TD></TR>
        /// <TR><TD><code>perst.code.generation</code></TD><TD>bool</TD><TD>true</TD>
        /// <TD>enable or disable dynamic generation of pack/unpack methods for persistent 
        /// classes. Such methods can be generated only for classes with public fields.
        /// Using generated methods instead of .Net reflection API increase speed of
        /// object store/fetch operations, but generation itself takes additional time at 
        /// startup 
        /// </TD></TR>
        /// <TR><TD><code>perst.file.readonly</code></TD><TD>bool</TD><TD>false</TD>
        /// <TD>Database file should be opened in read-only mode.
        /// </TD></TR>
        /// <TR><TD><code>perst.file.noflush</code></TD><TD>bool</TD><TD>false</TD>
        /// <TD>To not flush file during transaction commit. It will greatly increase performance because
        /// eliminate synchronous write to the disk (when program has to wait until all changed
        /// are actually written to the disk). But it can cause database corruption in case of 
        /// OS or power failure (but abnormal termination of application itself should not cause
        /// the problem, because all data which were written to the file, but is not yet saved to the disk is 
        /// stored in OS file buffers and sooner or later them will be written to the disk)
        /// </TD></TR>
        /// <TR><TD><code>perst.alternative.btree</code></TD><TD>bool</TD><TD>false</TD>
        /// <TD>Use aternative implementation of B-Tree (not using direct access to database
        /// file pages). This implementation should be used in case of serialized per thread transctions.
        /// New implementation of B-Tree will be used instead of old implementation
        /// if "perst.alternative.btree" property is set. New B-Tree has incompatible format with 
        /// old B-Tree, so you could not use old database or XML export file with new indices. 
        /// Alternative B-Tree is needed to provide serializable transaction (old one could not be used).
        /// Also it provides better performance (about 3 times comaring with old implementation) because
        /// of object caching. And B-Tree supports keys of user defined types. 
        /// </TD></TR>
        /// <TR><TD><code>perst.background.gc</code></TD><TD>bool</TD><TD>false</TD>
        /// <TD>Perform garbage collection in separate thread without blocking the main application.                                                                                          
        /// </TD></TR>
        /// <TR><TD><code>perst.string.encoding</code></TD><TD>String</TD><TD>null</TD>
        /// <TD>Specifies encoding of storing strings in the database. By default Perst stores 
        /// strings as sequence of chars (two bytes per char). If all strings in application are in 
        /// the same language, then using encoding  can signifficantly reduce space needed
        /// to store string (about two times). But please notice, that this option has influence
        /// on all strings  stored in database. So if you already have some data in the storage
        /// and then change encoding, then it can cause incorrect fetching of strings and even database crash.
        /// </TD></TR>
        /// </TABLE>
        /// </remarks>
        /// <param name="name">name of the property</param>
        /// <param name="val">value of the property</param>
        ///
        abstract public void SetProperty(String name, Object val);

        ///
        /// <summary>Set database properties. This method should be invoked before opening database. 
        /// For list of supported properties please see <see cref="SetProperty">setProperty</see>. 
        /// All not recognized properties are ignored.
        /// </summary>
        /// <param name="props">collections with storage properties</param>
        ///
        abstract public void SetProperties(System.Collections.Specialized.NameValueCollection props);

        /// <summary>
        /// Set storage listener.
        /// </summary>summary>
        /// <param name="listener">new storage listener (may be null)</param>
        /// <returns>previous storage listener</returns>
        ///
        abstract public StorageListener SetListener(StorageListener listener);

        /// <summary>
        /// Set class loader. This class loader will be used to locate classes for 
        /// loaded class descriptors. If class loader is not specified or
        /// it did find the class, then class will be searched in all active assemblies
        /// </summary>
        public ClassLoader Loader
        {
       
            set 
            { 
                loader = value;
            }

            get 
            { 
                return loader;
            }
        }


#if COMPACT_NET_FRAMEWORK
        /// <summary>
        /// Compact.NET framework doesn;t allow to get list of assemblies loaded
        /// in application domain. Without it I do not know how to locate
        /// class from foreign assembly by name. 
        /// Assembly which creates Storare is automatically registered.
        /// Other assemblies has to explicitely registered by programmer.
        /// </summary>
        /// <param name="assembly">registered assembly</param>
        abstract public void RegisterAssembly(System.Reflection.Assembly assembly);
#else

        /// <summary>
        /// Begin per-thread transaction. Three types of per-thread transactions are supported: 
        /// exclusive, cooperative and serializable. In case of exclusive transaction, only one 
        /// thread can update the database. In cooperative mode, multiple transaction can work 
        /// concurrently and commit() method will be invoked only when transactions of all threads
        /// are terminated. Serializable transactions can also work concurrently. But unlike
        /// cooperative transaction, the threads are isolated from each other. Each thread
        /// has its own associated set of modified objects and committing the transaction will cause
        /// saving only of these objects to the database.To synchronize access to the objects
        /// in case of serializable transaction programmer should use lock methods
        /// of IResource interface. Shared lock should be set before read access to any object, 
        /// and exclusive lock - before write access. Locks will be automatically released when
        /// transaction is committed (so programmer should not explicitly invoke unlock method)
        /// In this case it is guaranteed that transactions are serializable.
        /// It is not possible to use <code>IPersistent.store()</code> method in
        /// serializable transactions. That is why it is also not possible to use Index and FieldIndex
        /// containers (since them are based on B-Tree and B-Tree directly access database pages
        /// and use <code>store()</code> method to assign OID to inserted object. 
        /// You should use <code>SortedCollection</code> based on T-Tree instead or alternative
        /// B-Tree implemenataion (set "perst.alternative.btree" property).
        /// </summary>
        /// <param name="mode"><code>TransactionMode.Exclusive</code>,  <code>TransactionMode.Cooperative</code> or <code>TransactionMode.Serializable</code>
        /// </param>
        abstract public void BeginThreadTransaction(TransactionMode mode);
    
        /// <summary>
        /// End per-thread transaction started by beginThreadTransaction method.
        /// <ul>
        /// <li>If transaction is <i>exclusive</i>, this method commits the transaction and
        /// allows other thread to proceed.</li><li>
        /// If transaction is <i>serializable</i>, this method commits sll changes done by this thread
        /// and release all locks set by this thread.</li><li>     
        /// If transaction is <i>cooperative</i>, this method decrement counter of cooperative
        /// transactions and if it becomes zero - commit the work</li></ul>
        /// </summary>
        public void EndThreadTransaction() 
        { 
            EndThreadTransaction(Int32.MaxValue);
        }

        /// <summary>
        /// End per-thread cooperative transaction with specified maximal delay of transaction
        /// commit. When cooperative transaction is ended, data is not immediately committed to the
        /// disk (because other cooperative transaction can be active at this moment of time).
        /// Instead of it cooperative transaction counter is decremented. Commit is performed
        /// only when this counter reaches zero value. But in case of heavy load there can be a lot of
        /// requests and so a lot of active cooperative transactions. So transaction counter never reaches zero value.
        /// If system crash happens a large amount of work will be lost in this case. 
        /// To prevent such scenario, it is possible to specify maximal delay of pending transaction commit.
        /// In this case when such timeout is expired, new cooperative transaction will be blocked until
        /// transaction is committed.
        /// </summary>
        /// <param name="maxDelay">maximal delay in milliseconds of committing transaction.  Please notice, that Perst could 
        /// not force other threads to commit their cooperative transactions when this timeout is expired. It will only
        /// block new cooperative transactions to make it possible to current transaction to complete their work.
        /// If <code>maxDelay</code> is 0, current thread will be blocked until all other cooperative trasnaction are also finished
        /// and changhes will be committed to the database.
        /// </param>
        abstract public void EndThreadTransaction(int maxDelay);
   
        /// <summary>
        /// Rollback per-thread transaction. It is safe to use this method only for exclusive transactions.
        /// In case of cooperative transactions, this method rollback results of all transactions.
        /// </summary>
        abstract public void RollbackThreadTransaction();
#endif

        /// <summary>
        /// Get database memory dump. This function returns hashmap which key is classes
        /// of stored objects and value - MemoryUsage object which specifies number of instances
        /// of particular class in the storage and total size of memory used by these instance.
        /// Size of internal database structures (object index, memory allocation bitmap) is associated with 
        /// <code>Storage</code> class. Size of class descriptors  - with <code>System.Type</code> class.
        /// <p>This method traverse the storage as garbage collection do - starting from the root object
        /// and recursively visiting all reachable objects. So it reports statistic only for visible objects.
        /// If total database size is significantly larger than total size of all instances reported
        /// by this method, it means that there is garbage in the database. You can explicitly invoke
        /// garbage collector in this case.</p> 
        /// </summary>
        abstract public System.Collections.Hashtable GetMemoryDump();

        /// <summary>
        /// Get total size of all allocated objects in the database
        /// </summary>
        abstract public long UsedSize {get;}

        /// <summary>
        /// Get size of the database
        /// </summary>
        abstract public long DatabaseSize {get;}


        // Internal methods
		
        abstract protected internal void  deallocateObject(IPersistent obj);
		
        abstract protected internal void  storeObject(IPersistent obj);
		
        abstract protected internal void  storeFinalizedObject(IPersistent obj);
		
        abstract protected internal void  loadObject(IPersistent obj);
		
        abstract protected internal void  modifyObject(IPersistent obj);

        abstract protected internal void  lockObject(IPersistent obj);
		
        private ClassLoader loader;
    }
}