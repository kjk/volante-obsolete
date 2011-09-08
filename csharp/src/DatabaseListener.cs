using System;

namespace Volante
{
    /// <summary>
    /// Listener of database events. Programmer should derive his own subclass and register
    /// it using IDatabase.Listener property.
    /// </summary>
    public abstract class DatabaseListener
    {
        /// <summary>
        /// Called if database was detected to be corrupted during openinig
        /// (when database was not closed properly and has to be recovered)
        /// </summary>
        public virtual void DatabaseCorrupted() { }

        /// <summary>
        /// Called after database recovery has completed
        /// </summary>
        public virtual void RecoveryCompleted() { }

        /// <summary>
        /// Called when garbage collection is started, either explicitly
        /// (by calling IDatabase.Gc()) or implicitly (after allocating
        /// enough memory to trigger gc threshold)
        /// </summary>
        public virtual void GcStarted() { }

        /// <summary>
        /// Called when garbage collection is completed
        /// </summary>
        /// <param name="nDeallocatedObjects">number of deallocated objects</param>
        ///
        public virtual void GcCompleted(int nDeallocatedObjects) { }

        /// <summary>
        /// Called  when unreferenced object is deallocated from 
        /// database. It is possible to get instance of the object using
        /// <code>IDatabase.GetObjectByOid()</code> method.
        /// </summary>
        /// <param name="cls">class of deallocated object</param>
        /// <param name="oid">object identifier of deallocated object</param>
        ///
        public virtual void DeallocateObject(Type cls, int oid) { }

        /// <summary>
        /// Handle replication error 
        /// </summary>
        /// <param name="host">address of host replication to which is failed (null if error jappens at slave node)</param>
        /// <returns><code>true</code> if host should be reconnected and attempt to send data to it should be 
        /// repeated, <code>false</code> if no more attmpts to communicate with this host should be performed
        /// </returns>
        public virtual bool ReplicationError(string host)
        {
            return false;
        }
    }
}
