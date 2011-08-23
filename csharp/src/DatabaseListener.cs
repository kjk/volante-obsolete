using System;

namespace Volante
{
    /// <summary>
    /// Listener of database events. Programmer should derive his own subclass and register
    /// it using Database.SetListener method.
    /// </summary>
    public abstract class DatabaseListener
    {
        /// <summary>
        /// This metod is called during database open when database was not
        /// close normally and has to be recovered
        /// </summary>
        public void DatabaseCorrupted() { }

        /// <summary>
        /// This method is called after completion of recovery
        /// </summary>
        public void RecoveryCompleted() { }

        /// <summary>
        /// This method is called when garbage collection is  started (ether explicitly
        /// by invocation of Storage.gc() method, either implicitly  after allocation
        /// of some amount of memory)).
        /// </summary>
        public void GcStarted() { }

        /// <summary>
        /// This method is called  when unreferenced object is deallocated from 
        /// database. It is possible to get instance of the object using
        /// <code>Storage.getObjectByOid()</code> method.
        /// </summary>
        /// <param name="cls">class of deallocated object</param>
        /// <param name="oid">object identifier of deallocated object</param>
        ///
        public void DeallocateObject(Type cls, int oid) { }

        /// <summary>
        /// This method is called when garbage collection is completed
        /// </summary>
        /// <param name="nDeallocatedObjects">number of deallocated objects</param>
        ///
        public void GcCompleted(int nDeallocatedObjects) { }

        /// <summary>
        /// Handle replication error 
        /// </summary>
        /// <param name="host">address of host replication to which is failed (null if error jappens at slave node)</param>
        /// <returns><code>true</code> if host should be reconnected and attempt to send data to it should be 
        /// repeated, <code>false</code> if no more attmpts to communicate with this host should be performed
        /// </returns>
        public bool ReplicationError(string host)
        {
            return false;
        }
    }
}
