namespace NachoDB
{
    using System;
    using NachoDB.Impl;
	
    /// <summary> Storage factory
    /// </summary>
    public class StorageFactory
    {
        /// <summary> Create new instance of the storage
        /// </summary>
        /// <param name="new">instance of the storage (unopened,you should explicitely invoke open method)
        /// 
        /// </param>
        public static Storage CreateStorage()
        {
#if COMPACT_NET_FRAMEWORK
            return new StorageImpl(System.Reflection.Assembly.GetCallingAssembly());
#else
            return new StorageImpl();
#endif
        }
		
#if !COMPACT_NET_FRAMEWORK
        /// <summary>
        /// Create new instance of the master node of replicated storage
        /// </summary>
        /// <param name="replicationSlaveNodes">addresses of hosts to which replication will be performed. 
        /// Address as specified as NAME:PORT</param>
        /// <param name="asyncBufSize">if value of this parameter is greater than zero then replication will be 
        /// asynchronous, done by separate thread and not blocking main application. 
        /// Otherwise data is send to the slave nodes by the same thread which updates the database.
        /// If space asynchronous buffer is exhausted, then main thread willbe also blocked until the
        /// data is send.</param>
        /// <returns>new instance of the master storage (unopened, you should explicitely invoke open method)</returns>
        ///
        public static ReplicationMasterStorage CreateReplicationMasterStorage(string[] replicationSlaveNodes, int asyncBufSize) 
        {
            return new ReplicationMasterStorageImpl(replicationSlaveNodes, asyncBufSize);
        }

        /// <summary>
        /// Create new instance of the slave node of replicated storage
        /// </summary>
        /// <param name="port">socket port at which connection from master will be established</param>
        /// <returns>new instance of the slave storage (unopened, you should explicitely invoke open method)</returns>
        ////
        public static ReplicationSlaveStorage CreateReplicationSlaveStorage(int port) 
        {
            return new ReplicationSlaveStorageImpl(port);
        }
#endif
    }	
}