namespace Perst
{
    using System;
	
    /// <summary> Interface of all persistent capable objects
    /// </summary>
    public interface IPersistent
    {
        int Oid
        {
            get;
				
        }
        Storage Storage
        {
            get;
				
        }
        /// <summary> Load obejct from the database (if needed)
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
        /// <summary> Explicitely make object peristent. Usually objects are made persistent
        /// implicitlely using "persistency on reachability apporach", but this
        /// method allows to do it explicitly 
        /// </summary>
        /// <param name="storage">storage in which object should be stored
        /// 
        /// </param>
        void  makePersistent(Storage storage);
        /// <summary> save object in the database
        /// </summary>
        void  store();
        /// <summary> Get object identifier (OID)
        /// </summary>
        /// <returns>OID (0 if object is not persistent yet)
        /// 
        /// </returns>
        /// <summary> Deallocate persistent object from the database
        /// </summary>
        void  deallocate();
        /// <summary> Specified whether object should be automatically loaded when it is referenced
        /// by other loaded peristent object. Default implementation of this method
        /// returns <code>true</code> making all cluster of referenced objects loaded together. 
        /// To avoid main memory overflow you should stop recursive loading of all objects
        /// from the database to main memory by redefining this method in some classes and returing
        /// <code>false</code> in it. In this case object has to be loaded explicitely 
        /// using Peristent.load method.
        /// </summary>
        /// <returns><code>true</code> if object is automatically loaded
        /// 
        /// </returns>
        bool recursiveLoading();
        /// <summary> Get storage in which this object is stored
        /// </summary>
        /// <returns>storage containing this object (null if object is not persistent yet)
        /// 
        /// </returns>
    }
}