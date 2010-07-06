using System;

namespace Perst
{
    /// <summary>
    /// Listener of database events. Programmer should derive his own subclass and register
    /// it using Storage.setListener method.
    /// </summary>
    public abstract class StorageListener 
    {
        /// <summary>
        /// This metod is called during database open when database was not
        /// close normally and has to be recovered
        /// </summary>
        public void DatabaseCorrupted() {}

        /// <summary>
        /// This method is called after completion of recovery
        /// </summary>
        public void RecoveryCompleted() {}

        /// <summary>
        /// This method is called when garbage collection is  started (ether explicitly
        /// by invocation of Storage.gc() method, either implicitly  after allocation
        /// of some amount of memory)).
        /// </summary>
        public void GcStarted() {}

        /// <summary>
        /// This method is called  when unreferenced object is deallocated from 
        /// database. It is possible to get instance of the object using
        /// <code>Storage.getObjectByOid()</code> method.
        /// </summary>
        /// <param name="cls">class of deallocated object</param>
        /// <param name="oid">object identifier of deallocated object</param>
        ///
        public void DeallocateObject(Type cls, int oid) {}

        /// <summary>
        /// This method is called when garbage collection is completed
        /// </summary>
        /// <param name="nDeallocatedObjects">number of deallocated objects</param>
        ///
        public void GcCompleted(int nDeallocatedObjects) {}
    }
}
