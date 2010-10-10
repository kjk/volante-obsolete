package org.nachodb;
import org.nachodb.impl.*;

/**
 * Storage factory
 */
public class StorageFactory {
    /**
     * Create new instance of the storage
     * @return new instance of the storage (unopened, you should explicitely invoke open method)
     */
    public Storage createStorage() {
        return new StorageImpl();
    }

    /**
     * Create new instance of the master node of replicated storage
     * @param replicationSlaveNodes addresses of hosts to which replication will be performed. 
     * Address as specified as NAME:PORT
     * @param asyncBufSize if value of this parameter is greater than zero then replication will be 
     * asynchronous, done by separate thread and not blocking main application. 
     * Otherwise data is send to the slave nodes by the same thread which updates the database.
     * If space asynchronous buffer is exhausted, then main thread willbe also blocked until the
     * data is send.
     * @return new instance of the master storage (unopened, you should explicitely invoke open method)
     */
    public ReplicationMasterStorage createReplicationMasterStorage(String[] replicationSlaveNodes, int asyncBufSize) {
        return new ReplicationMasterStorageImpl(replicationSlaveNodes, asyncBufSize);
    }

    /**
     * Create new instance of the slave node of replicated storage
     * @param port  socket port at which connection from master will be established
     * @return new instance of the slave storage (unopened, you should explicitely invoke open method)
     */
    public ReplicationSlaveStorage createReplicationSlaveStorage(int port) {
        return new ReplicationSlaveStorageImpl(port);
    }

    /**
     * Get instance of storage factory.
     * So new storages should be create in application in the following way:
     * <code>StorageFactory.getInstance().createStorage()</code>
     * @return instance of the storage factory
     */
    public static StorageFactory getInstance() { 
        return instance;
    }

    protected static final StorageFactory instance = new StorageFactory();
};
