package org.nachodb;

/**
 * Storage performing replication of changed pages to specified slave nodes.
 */
public interface ReplicationMasterStorage extends Storage { 
    /**
     * Get number of currently available slave nodes
     * @return number of online replication slaves
     */
    public int getNumberOfAvailableHosts();
}   
    