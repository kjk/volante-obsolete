package org.nachodb;

/**
 * Listener of database events. Programmer should derive his own subclass and register
 * it using Storage.setListener method.
 */
public abstract class StorageListener {
    /**
     * This metod is called during database open when database was not
     * close normally and has to be recovered
     */
    public void databaseCorrupted() {}

    /**
     * This method is called after completion of recovery
     */
    public void recoveryCompleted() {}

    /**
     * This method is called when garbage collection is  started (ether explicitly
     * by invocation of Storage.gc() method, either implicitly  after allocation
     * of some amount of memory)).
     */
    public void gcStarted() {}

    /**
     * This method is called  when unreferenced object is deallocated from 
     * database. It is possible to get instance of the object using
     * <code>Storage.getObjectByOid()</code> method.
     * @param cls class of deallocated object
     * @param oid object identifier of deallocated object
     */
    public void deallocateObject(Class cls, int oid) {}

    /**
     * This method is called when garbage collection is completed
     * @param nDeallocatedObjects number of deallocated objects
     */
    public void gcCompleted(int nDeallocatedObjects) {}

    /**
     * Handle replication error 
     * @param host address of host replication to which is failed (null if error jappens at slave node)
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */
    public boolean replicationError(String host) {
        return false;
    }        
}
     
