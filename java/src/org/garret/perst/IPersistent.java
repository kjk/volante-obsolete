package org.garret.perst;

/**
 * Interface of all persistent capable objects
 */
public interface IPersistent {
    /**
     * Load obejct from the database (if needed)
     */
    public void load();

    /** 
     * Check if object is stub and has to be loaded from the database
     * @param return <code>true</code> if object has to be loaded from the database
     */
    public boolean isRaw();

    /**
     * Check if object is persistent 
     * @return <code>true</code> if object has assigned OID
     */
    public boolean isPersistent();

    /**
     * Explicitely make object peristent. Usually objects are made persistent
     * implicitlely using "persistency on reachability apporach", but this
     * method allows to do it explicitly 
     * @param storage storage in which object should be stored
     */
    public void makePersistent(Storage storage);

    /**
     * save object in the database
     */
    public void store();
    
    /**
     * Get object identifier (OID)
     * @return OID (0 if object is not persistent yet)
     */
    public int getOid();

    /**
     * Deallocate persistent object from the database
     */
    public void deallocate();

    /**
     * Specified whether object should be automatically loaded when it is referenced
     * by other loaded peristent object. Default implementation of this method
     * returns <code>true</code> making all cluster of referenced objects loaded together. 
     * To avoid main memory overflow you should stop recursive loading of all objects
     * from the database to main memory by redefining this method in some classes and returing
     * <code>false</code> in it. In this case object has to be loaded explicitely 
     * using Peristent.load method.
     * @return <code>true</code> if object is automatically loaded
     */
    public boolean recursiveLoading();

    /**
     * Get storage in which this object is stored
     * @object container (null if object is not persistent yet)
     */
    public Storage getStorage();
}

