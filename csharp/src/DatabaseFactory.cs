namespace Volante
{
    using System;
    using Volante.Impl;

    /// <summary> Database factory
    /// </summary>
    public class DatabaseFactory
    {
        /// <summary> Create new instance of the storage
        /// </summary>
        public static IDatabase CreateDatabase()
        {
#if CF
            return new StorageImpl(System.Reflection.Assembly.GetCallingAssembly());
#else
            return new DatabaseImpl();
#endif
        }

#if !CF && WITH_REPLICATION
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
        public static ReplicationMasterDatabase CreateReplicationMasterDatabase(string[] replicationSlaveNodes, int asyncBufSize)
        {
            return new ReplicationMasterDatabaseImpl(replicationSlaveNodes, asyncBufSize);
        }

        /// <summary>
        /// Create new instance of the slave node of replicated storage
        /// </summary>
        /// <param name="port">socket port at which connection from master will be established</param>
        /// <returns>new instance of the slave storage (unopened, you should explicitely invoke open method)</returns>
        ////
        public static ReplicationSlaveDatabase CreateReplicationSlaveDatabase(int port)
        {
            return new ReplicationSlaveDatabaseImpl(port);
        }
#endif
    }
}