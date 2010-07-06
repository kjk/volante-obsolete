namespace Perst
{
    using System;
	
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
        abstract public void  open(String filePath, int pagePoolSize);
		
        /// <summary> Open the storage with default page pool size
        /// </summary>
        /// <param name="filePath">path to the database file
        /// 
        /// </param>
        public virtual void  open(String filePath)
        {
            open(filePath, 4 * 1024 * 1024);
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
        abstract public void  open(IFile file, int pagePoolSize);
		
        /// <summary> Open the storage with default page pool size
        /// </summary>
        /// <param name="file">user specific implementation of IFile interface
        /// </param>
        public virtual void  open(IFile file)
        {
            open(file, 4 * 1024 * 1024);
        }
		
        /// <summary>Check if database is opened
        /// </summary>
        /// <returns><code>true</code> if database was opened by <code>open</code> method, 
        /// <code>false</code> otherwise
        /// </returns>        
        abstract public bool isOpened();
		
        /// <summary> Set new storage root object.
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
        /// <exception cref="Perst.StorageError">StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE) exception if 
        /// specified key type is not supported by implementation.
        /// 
        /// </exception>
        abstract public Index createIndex(System.Type type, bool unique);
		
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
        abstract public FieldIndex createFieldIndex(System.Type type, String fieldName, bool unique);
		
        /// <summary>
        /// Create new spatial index
        /// </summary>
        /// <returns>
        /// persistent object implementing spatial index
        /// </returns>
        abstract public SpatialIndex createSpatialIndex();

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
		
        /// <summary> Commit transaction (if needed) and close the storage
        /// </summary>
        abstract public void  close();

        /// <summary> Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
        /// Int64.MaxValue). If it is set to the value different fro Long.MAX_VALUE, GC will be started each time when
        /// delta between total size of allocated and deallocated objects exceeds specified threashold OR
        /// after reaching end of allocation bitmap in allocator. 
        /// </summary>
        /// <param name="allocatedDelta"> delta between total size of allocated and deallocated object since last GC or storage opening
        /// </param>
        ///
        abstract public void setGcThreshold(long allocatedDelta);

        /// <summary>Explicit start of garbage collector
        /// </summary>
        /// 
        abstract public void gc();

        /// <summary> Export database in XML format 
        /// </summary>
        /// <param name="writer">writer for generated XML document
        /// 
        /// </param>
        abstract public void  exportXML(System.IO.StreamWriter writer);
		
        /// <summary> Import data from XML file
        /// </summary>
        /// <param name="reader">XML document reader
        /// 
        /// </param>
        abstract public void  importXML(System.IO.StreamReader reader);
		
        		
        /// <summary> 
        /// Retrieve object by OID. This method should be used with care because
        /// if object is deallocated, its OID can be reused. In this case
        /// getObjectByOID will return reference to the new object with may be
        /// different type.
        /// </summary>
        /// <param name="oid">object oid</param>
        /// <returns>reference to the object with specified OID</returns>
        abstract public IPersistent getObjectByOID(int oid);


        // Internal methods
		
        abstract protected internal void  deallocateObject(IPersistent obj);
		
        abstract protected internal void  storeObject(IPersistent obj);
		
        abstract protected internal void  loadObject(IPersistent obj);
		
        abstract protected internal void  modifyObject(IPersistent obj);
		
        protected internal void  setObjectOid(IPersistent obj, int oid, bool raw)
        {
            Persistent po = (Persistent) obj;
            po.oid = oid;
            po.storage = this;
            po.state = raw ? (int)Persistent.ObjectState.RAW : 0;
        }
    }
}