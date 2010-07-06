namespace Perst
{
    using System;
	
    /// <summary> Interface of all persistent capable objects
    /// </summary>
    public interface IPersistent
    {
        /// <summary> Get object identifier (OID)
        /// </summary>
        /// <returns>OID (0 if object is not persistent yet)
        /// 
        /// </returns>
        int Oid
        {
            get;
				
        }
        /// <summary> Get storage in which this object is stored
        /// </summary>
        /// <returns>storage containing this object (null if object is not persistent yet)
        /// 
        /// </returns>
        Storage Storage
        {
            get;				
        }
        /// <summary> Load object from the database (if needed)
        /// </summary>
        void  load();

        /// 
        /// <summary> Check if object is stub and has to be loaded from the database
        /// </summary>
        /// <param name="return"><code>true</code> if object has to be loaded from the database
        /// 
        /// </param>
        bool isRaw();

        /// <summary> Check if object is persistent 
        /// </summary>
        /// <returns><code>true</code> if object has assigned OID
        /// 
        /// </returns>
        bool isPersistent();

        /// <summary> Check if object was modified within current transaction
        /// </summary>
        /// <returns><code>true</code> if object is persistent and was modified within current transaction
        /// 
        /// </returns>
        bool isModified();

        /// <summary> Explicitely make object peristent. Usually objects are made persistent
        /// implicitlely using "persistency on reachability apporach", but this
        /// method allows to do it explicitly 
        /// </summary>
        /// <param name="storage">storage in which object should be stored
        /// 
        /// </param>
        void  makePersistent(Storage storage);

        /// <summary> Save object in the database
        /// </summary>
        void  store();

        /// <summary>
        /// Mark object as modified. Object will be saved to the database during transaction commit.
        /// </summary>
        void modify();
    
        /// <summary> Deallocate persistent object from the database
        /// </summary>
        void  deallocate();

        /// <summary> Specified whether object should be automatically loaded when it is referenced
        /// by other loaded peristent object. Default implementation of this method
        /// returns <code>true</code> making all cluster of referenced objects loaded together. 
        /// To avoid main memory overflow you should stop recursive loading of all objects
        /// from the database to main memory by redefining this method in some classes and returing
        /// <code>false</code> in it. In this case object has to be loaded explicitely 
        /// using Persistent.load method.
        /// </summary>
        /// <returns><code>true</code> if object is automatically loaded
        /// 
        /// </returns>
        bool recursiveLoading();

        /// <summary> This method is  called by the database after loading of the object.
        /// It can be used to initialize transient fields of the object. 
        /// Default implementation of this method do nothing 
        /// </summary>
        void onLoad();        
    }
}