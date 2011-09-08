namespace Volante
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public enum IndexType
    {
        Unique,
        NonUnique
    }

    public enum TransactionMode
    {
        /// <summary>
        /// Exclusive per-thread transaction: each thread accesses database in exclusive mode
        /// </summary>
        Exclusive,
        /// <summary>
        /// Cooperative mode; all threads share the same transaction. Commit will commit changes made
        /// by all threads. To make this schema work correctly, it is necessary to ensure (using locking)
        /// that no thread is performing update of the database while another one tries to perform commit.
        /// Rollback will undo the work of all threads. 
        /// </summary>
        Cooperative,
        /// <summary>
        /// Serializable per-thread transaction. Unlike exclusive mode, threads can concurrently access database, 
        /// but effect will be the same as them working exclusively.
        /// To provide such behavior, programmer should lock all access objects (or use hierarchical locking).
        /// When object is updated, exclusive lock should be set, otherwise shared lock is enough.
        /// Lock should be preserved until the end of transaction.
        /// </summary>
        Serializable,
#if WITH_REPLICATION
        /// <summary>
        /// Read only transaction which can be started at replication slave node.
        /// It runs concurrently with receiving updates from master node.
        /// </summary>
        ReplicationSlave
#endif
    }

    public enum CacheType
    {
        Lru,
        Strong,
        Weak
    }

    /// <summary> Object db
    /// </summary>
    public interface IDatabase
    {
        /// <summary>Get/set database root. Database can have exactly one root. 
        /// If you need several root objects and access them by name (as is possible 
        /// in many other OODBMSes), create an index and use it as root object.
        /// Previous reference to the root object is rewritten but old root is not
        /// automatically deallocated.
        /// </summary>
        IPersistent Root { get; set; }

        /// <summary>Open the database
        /// </summary>
        /// <param name="filePath">path to the database file
        /// </param>
        /// <param name="cacheSizeInBytes">size of database cache, in bytes.
        /// Minimum size of the cache should be 64kB (64*1024 bytes).
        /// Larger cache usually leads to better performance. If the size is 0
        /// the cache is unlimited - and will grow to the size of the database.
        /// </param>
        void Open(String filePath, int cacheSizeInBytes);

        /// <summary>Open the database with default page pool size (4 MB)
        /// </summary>
        /// <param name="filePath">path to the database file
        /// </param>
        void Open(String filePath);

        /// <summary>Open the db
        /// </summary>
        /// <param name="file">object implementing IFile interface
        /// </param>
        /// <param name="cacheSizeInBytes">size of database cache, in bytes.
        /// Minimum size of the cache should be 64kB (64*1024 bytes).
        /// Larger cache usually leads to better performance. If the size is 0
        /// the cache is unlimited - and will grow to the size of the database.
        /// </param>
        void Open(IFile file, int cacheSizeInBytes);

        /// <summary>Open the database with default cache size
        /// </summary>
        /// <param name="file">user specific implementation of IFile interface
        /// </param>
        void Open(IFile file);

        /// <summary>Check if database is opened
        /// </summary>
        /// <returns><code>true</code>if database was opened by <code>open</code> method, 
        /// <code>false</code> otherwise
        /// </returns>        
        bool IsOpened { get; }

        /// <summary> Commit changes done by the last transaction. Transaction is started implcitly with forst update
        /// opertation.
        /// </summary>
        void Commit();

        /// <summary> Rollback changes made by the last transaction
        /// </summary>
        void Rollback();

        /// <summary>
        /// Backup current state of database
        /// </summary>
        /// <param name="stream">output stream to which backup is done</param>
        void Backup(System.IO.Stream stream);

        /// <summary> Create new index. K parameter specifies key type, V - associated object type.
        /// </summary>
        /// <param name="indexType">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing index
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE) exception if 
        /// specified key type is not supported by implementation.
        /// </exception>
        IIndex<K, V> CreateIndex<K, V>(IndexType indexType) where V : class,IPersistent;

        /// <summary> Create new thick index (index with large number of duplicated keys).
        /// K parameter specifies key type, V - associated object type.
        /// </summary>
        /// <returns>persistent object implementing thick index
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE) exception if 
        /// specified key type is not supported by implementation.
        /// </exception>
        IIndex<K, V> CreateThickIndex<K, V>() where V : class,IPersistent;

        /// <summary> 
        /// Create new field index
        /// K parameter specifies key type, V - associated object type.
        /// </summary>
        /// <param name="fieldName">name of the index field. Field with such name should be present in specified class <code>type</code>
        /// </param>
        /// <param name="indexType">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing field index
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,
        /// DatabaseException(DatabaseException.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
        /// </exception>
        IFieldIndex<K, V> CreateFieldIndex<K, V>(string fieldName, IndexType indexType) where V : class,IPersistent;

        /// <summary> 
        /// Create new multi-field index
        /// </summary>
        /// <param name="fieldNames">array of names of the fields. Field with such name should be present in specified class <code>type</code>
        /// </param>
        /// <param name="indexType">whether index is unique (duplicate value of keys are not allowed)
        /// </param>
        /// <returns>persistent object implementing field index
        /// </returns>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,
        /// DatabaseException(DatabaseException.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
        /// </exception>
        IMultiFieldIndex<V> CreateFieldIndex<V>(string[] fieldNames, IndexType indexType) where V : class,IPersistent;

#if WITH_OLD_BTREE
        /// <summary>
        /// Create new bit index. Bit index is used to select object 
        /// with specified set of (boolean) properties.
        /// </summary>
        /// <returns>persistent object implementing bit index</returns>
        IBitIndex<T> CreateBitIndex<T>() where T : class,IPersistent;
#endif

        /// <summary>
        /// Create new spatial index with integer coordinates
        /// </summary>
        /// <returns>
        /// persistent object implementing spatial index
        /// </returns>
        ISpatialIndex<T> CreateSpatialIndex<T>() where T : class,IPersistent;

        /// <summary>
        /// Create new R2 spatial index
        /// </summary>
        /// <returns>
        /// persistent object implementing spatial index
        /// </returns>
        ISpatialIndexR2<T> CreateSpatialIndexR2<T>() where T : class,IPersistent;

        /// <summary>
        /// Create new sorted collection with specified comparator
        /// </summary>
        /// <param name="comparator">comparator class specifying order in the collection</param>
        /// <param name="indexType"> whether collection is unique (members with the same key value are not allowed)</param>
        /// <returns> persistent object implementing sorted collection</returns>
        ISortedCollection<K, V> CreateSortedCollection<K, V>(PersistentComparator<K, V> comparator, IndexType indexType) where V : class,IPersistent;

        /// <summary>
        /// Create new sorted collection. Members of this collections should implement 
        /// <code>System.IComparable</code> interface and make it possible to compare 
        /// collection members with each other as well as with serch key.
        /// </summary>
        /// <param name="indexType"> whether collection is unique (members with the same key value are not allowed)</param>
        /// <returns> persistent object implementing sorted collection</returns>
        ISortedCollection<K, V> CreateSortedCollection<K, V>(IndexType indexType) where V : class,IPersistent, IComparable<K>, IComparable<V>;

        /// <summary>Create set of references to persistent objects.
        /// </summary>
        /// <returns>empty set, members can be added to the set later.
        /// </returns>
        ISet<T> CreateSet<T>() where T : class,IPersistent;

        /// <summary>Create set of references to persistent objects.
        /// </summary>
        /// <param name="initialSize">initial size of the set</param>
        /// <returns>empty set, members can be added to the set later.
        /// </returns>
        ISet<T> CreateSet<T>(int initialSize) where T : class,IPersistent;

        /// <summary>Create one-to-many link.
        /// </summary>
        /// <returns>empty link, members can be added to the link later.
        /// </returns>
        ILink<T> CreateLink<T>() where T : class,IPersistent;

        /// <summary>Create one-to-many link with specified initial size.
        /// </summary>
        /// <param name="initialSize">initial size of the array</param>
        /// <returns>empty link with specified size
        /// </returns>
        ILink<T> CreateLink<T>(int initialSize) where T : class,IPersistent;

        /// <summary>Create dynamically extended array of referencess to persistent objects.
        /// It is intended to be used in classes using virtual properties to 
        /// access components of persistent objects.  
        /// </summary>
        /// <returns>new empty array, new members can be added to the array later.
        /// </returns>
        IPArray<T> CreateArray<T>() where T : class,IPersistent;

        /// <summary>Create dynamcially extended array of reference to persistent objects.
        /// It is inteded to be used in classes using virtual properties to 
        /// access components of persistent objects.  
        /// </summary>
        /// <param name="initialSize">initially allocated size of the array</param>
        /// <returns>new empty array, new members can be added to the array later.
        /// </returns>
        IPArray<T> CreateArray<T>(int initialSize) where T : class,IPersistent;

        /// <summary> Create relation object. Unlike link which represent embedded relation and stored
        /// inside owner object, this Relation object is standalone persisitent object
        /// containing references to owner and members of the relation
        /// </summary>
        /// <param name="owner">owner of the relation
        /// </param>
        /// <returns>object representing empty relation (relation with specified owner and no members), 
        /// new members can be added to the link later.
        /// </returns>
        Relation<M, O> CreateRelation<M, O>(O owner)
            where M : class,IPersistent
            where O : class,IPersistent;

        /// <summary>
        /// Create new BLOB. Create object for storing large binary data.
        /// </summary>
        /// <returns>empty BLOB</returns>
        IBlob CreateBlob();

        /// <summary>
        /// Create new time series object. 
        /// </summary>
        /// <param name="blockSize">number of elements in the block</param>
        /// <param name="maxBlockTimeInterval">maximal difference in system ticks (100 nanoseconds) between timestamps 
        /// of the first and the last elements in a block. 
        /// If value of this parameter is too small, then most blocks will contains less elements 
        /// than preallocated. 
        /// If it is too large, then searching of block will be inefficient, because index search 
        /// will select a lot of extra blocks which do not contain any element from the 
        /// specified range.
        /// Usually the value of this parameter should be set as
        /// (number of elements in block)*(tick interval)*2. 
        /// Coefficient 2 here is used to compact possible holes in time series.
        /// For example, if we collect stocks data, we will have data only for working hours.
        /// If number of element in block is 100, time series period is 1 day, then
        /// value of maxBlockTimeInterval can be set as 100*(24*60*60*10000000L)*2
        /// </param>
        /// <returns>new empty time series</returns>
        ITimeSeries<T> CreateTimeSeries<T>(int blockSize, long maxBlockTimeInterval) where T : ITimeSeriesTick;

#if WITH_PATRICIA
        /// <summary>
        /// Create PATRICIA trie (Practical Algorithm To Retrieve Information Coded In Alphanumeric)
        /// Tries are a kind of tree where each node holds a common part of one or more keys. 
        /// PATRICIA trie is one of the many existing variants of the trie, which adds path compression 
        /// by grouping common sequences of nodes together.
        /// This structure provides a very efficient way of storing values while maintaining the lookup time 
        /// for a key in O(N) in the worst case, where N is the length of the longest key. 
        /// This structure has it's main use in IP routing software, but can provide an interesting alternative 
        /// to other structures such as hashtables when memory space is of concern.
        /// </summary>
        /// <returns>created PATRICIA trie</returns>
        IPatriciaTrie<T> CreatePatriciaTrie<T>() where T : class,IPersistent;
#endif

        /// <summary> Commit transaction (if needed) and close the db
        /// </summary>
        void Close();

        /// <summary>Explicitly start garbage collection
        /// </summary>
        /// <returns>number of collected (deallocated) objects</returns>
        int Gc();

#if WITH_XML
        /// <summary> Export database in XML format 
        /// </summary>
        /// <param name="writer">writer for generated XML document
        /// </param>
        void ExportXML(System.IO.StreamWriter writer);

        /// <summary> Import data from XML file
        /// </summary>
        /// <param name="reader">XML document reader
        /// </param>
        void ImportXML(System.IO.StreamReader reader);
#endif

        /// <summary> 
        /// Retrieve object by oid. This method should be used with care because
        /// if object is deallocated, its oid can be reused. In this case
        /// GetObjectByOid() will return reference to the new object with may be
        /// different type.
        /// </summary>
        /// <param name="oid">object oid</param>
        /// <returns>reference to the object with specified oid</returns>
        IPersistent GetObjectByOid(int oid);

        /// <summary> 
        /// Explicitly make object peristent. Usually objects are made persistent
        /// implicitly using "persistency on reachability approach", but this
        /// method allows to do it explicitly. If object is already persistent, execution of
        /// this method has no effect.
        /// </summary>
        /// <param name="obj">object to be made persistent</param>
        /// <returns>oid assigned to the object</returns>
        int MakePersistent(IPersistent obj);

#if WITH_OLD_BTREE
        /// Use aternative implementation of B-Tree (not using direct access to database
        /// file pages). This implementation should be used in case of serialized per thread transctions.
        /// New implementation of B-Tree will be used instead of old implementation
        /// if AlternativeBtree property is set. New B-Tree has incompatible format with 
        /// old B-Tree, so you could not use old database or XML export file with new indices. 
        /// Alternative B-Tree is needed to provide serializable transaction (old one could not be used).
        /// Also it provides better performance (about 3 times comaring with old implementation) because
        /// of object caching. And B-Tree supports keys of user defined types. 
        /// Default value: false
        bool AlternativeBtree { get; set; }
#endif

        /// <summary>Set/get kind of object cache.
        /// If cache is CacheType.Strong none of the loaded persistent objects
        /// can be deallocated from memory by garbage collection.
        /// CacheType.Weak and CacheType.Lru both use weak references, so loaded
        /// objects can be deallocated. Lru cache can also pin some number of
        /// recently used objects for improved performance.
        /// Default value: CacheType.Lru
        /// </summary>
        CacheType CacheKind { get; set; }

        /// <summary>Set/get initial size of object index. Bigger values increase
        /// initial size of database but reduce number of index reallocations.
        /// Default value: 1024
        /// </summary>
        int ObjectIndexInitSize { get; set; }

        /// <summary>Set/get initial size of object cache. Default value: 1319
        /// </summary>
        int ObjectCacheInitSize { get; set; }

        /// <summary>Set/get object allocation bitmap extenstion quantum. Memory
        /// is allocated by scanning a bitmap. If there is no hole large enough,
        /// then database is extended by this value. It should not be smaller
        /// than 64 KB.
        /// Default value: 104857 bytes (1 MB)
        /// </summary>
        long ExtensionQuantum { get; set; }

        /// Threshold for initiation of garbage collection. 
        /// If it is set to the value different from long.MaxValue, GC will be started each time 
        /// when delta between total size of allocated and deallocated objects exceeds specified threashold OR
        /// after reaching end of allocation bitmap in allocator.
        /// <summary>Set threshold for initiation of garbage collection. By default garbage
        /// collection is disabled (threshold is set to
        /// Int64.MaxValue). If it is set to the value different fro
        /// Long.MAX_VALUE, GC will be started each time when
        /// delta between total size of allocated and deallocated
        /// objects exceeds specified threashold OR
        /// after reaching end of allocation bitmap in allocator. 
        /// </summary>
        /// <param>delta between total size of allocated and deallocated object since last GC or db opening
        /// </param>
        /// Default value: long.MaxValue
        long GcThreshold { get; set; }

        /// <summary>Set/get whether garbage collection is performed in a
        /// separate thread in order to not block main application.
        /// Default value: false
        /// </summary>
        bool BackgroundGc { get; set; }

        /// <summary>Set/get whether dynamic code generation is used to generate
        /// pack/unpack methods for persisted classes.
        /// If used, serialization/deserialization of classes that only have public
        /// fields will be faster. On the downside, those methods must be generated
        /// at startup, increasing startup time.
        /// Default value: false
        /// </summary>
        bool CodeGeneration { get; set; }

#if WITH_REPLICATION
        /// Request acknowledgement from slave that it receives all data before transaction
        /// commit. If this option is not set, then replication master node just writes
        /// data to the socket not warring whether it reaches slave node or not.
        /// When this option is set to true, master not will wait during each transaction commit acknowledgement
        /// from slave node. This option must be either set or not set at both
        /// slave and master nodes. If it is set only on one of this nodes then behavior of
        /// the system is unpredicted. This option can be used both in synchronous and asynchronous replication
        /// mode. The only difference is that in first case main application thread will be blocked waiting
        /// for acknowledgment, while in the asynchronous mode special replication thread will be blocked
        /// allowing thread performing commit to proceed.
        /// Default value: false
        bool ReplicationAck { get; set; }
#endif

        /// <summary>Get database file. Should only be used to set FileListener.</summary>
        IFile File { get; }

        /// <summary>Get/set db listener. You can set <code>null</code> listener.
        /// </summary>
        DatabaseListener Listener { get; set; }

        /// <summary>
        /// Set class loader. This class loader will be used to locate classes for 
        /// loaded class descriptors. If class loader is not specified or
        /// it did find the class, then class will be searched in all active assemblies
        /// </summary>
        IClassLoader Loader { get; set; }

#if CF
        /// <summary>
        /// Compact.NET framework doesn't allow to get list of assemblies loaded
        /// in application domain. Without it I do not know how to locate
        /// class from foreign assembly by name. 
        /// Assembly which creates Database is automatically registered.
        /// Other assemblies has to explicitely registered by programmer.
        /// </summary>
        /// <param name="assembly">registered assembly</param>
        void RegisterAssembly(System.Reflection.Assembly assembly);
#else
        /// <summary>
        /// Create persistent class wrapper. This wrapper will implement virtual properties
        /// defined in specified class or interface, performing transparent loading and storing of persistent object
        /// </summary>
        /// <param name="type">Class or interface type of instantiated object</param>
        /// <returns>Wrapper for the specified class, implementing all virtual properties defined
        /// in it
        /// </returns>
        IPersistent CreateClass(Type type);
#endif

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
        /// and use <code>Store()</code> method to assign oid to inserted object. 
        /// You should use <code>SortedCollection</code> based on T-Tree instead or alternative
        /// B-Tree implemenataion (set AlternativeBtree property).
        /// </summary>
        /// <param name="mode"><code>TransactionMode.Exclusive</code>,  <code>TransactionMode.Cooperative</code>,
        /// <code>TransactionMode.ReplicationSlave</code> or <code>TransactionMode.Serializable</code>
        /// </param>
        void BeginThreadTransaction(TransactionMode mode);

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
        void EndThreadTransaction();

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
        /// <param name="maxDelay">maximal delay in milliseconds of committing transaction.  Please notice, that Volante could 
        /// not force other threads to commit their cooperative transactions when this timeout is expired. It will only
        /// block new cooperative transactions to make it possible to current transaction to complete their work.
        /// If <code>maxDelay</code> is 0, current thread will be blocked until all other cooperative trasnaction are also finished
        /// and changhes will be committed to the database.
        /// </param>
        void EndThreadTransaction(int maxDelay);

        /// <summary>
        /// Rollback per-thread transaction. It is safe to use this method only for exclusive transactions.
        /// In case of cooperative transactions, this method rollback results of all transactions.
        /// </summary>
        void RollbackThreadTransaction();

        /// <summary>
        /// Get database memory dump. This function returns hashmap which key is classes
        /// of stored objects and value - TypeMemoryUsage object which specifies number of instances
        /// of particular class in the db and total size of memory used by these instance.
        /// Size of internal database structures (object index, memory allocation bitmap) is associated with 
        /// <code>IDatabase</code> class. Size of class descriptors  - with <code>System.Type</code> class.
        /// <p>This method traverse the db as garbage collection do - starting from the root object
        /// and recursively visiting all reachable objects. So it reports statistic only for visible objects.
        /// If total database size is significantly larger than total size of all instances reported
        /// by this method, it means that there is garbage in the database. You can explicitly invoke
        /// garbage collector in this case.</p> 
        /// </summary>
        Dictionary<Type, TypeMemoryUsage> GetMemoryUsage();

        /// <summary>
        /// Get total size of all allocated objects in the database
        /// </summary>
        long UsedSize { get; }

        /// <summary>
        /// Get size of the database
        /// </summary>
        long DatabaseSize { get; }

        // Internal methods
        void deallocateObject(IPersistent obj);

        void storeObject(IPersistent obj);

        void storeFinalizedObject(IPersistent obj);

        void loadObject(IPersistent obj);

        void modifyObject(IPersistent obj);

        void lockObject(IPersistent obj);
    }
}
