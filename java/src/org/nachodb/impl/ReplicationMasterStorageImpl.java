package org.nachodb.impl;

import java.io.*;
import java.net.*;

import org.nachodb.*;


public class ReplicationMasterStorageImpl extends StorageImpl implements ReplicationMasterStorage
{ 
    public ReplicationMasterStorageImpl(String[] hosts, int asyncBufSize) { 
        this.hosts = hosts;
        this.asyncBufSize = asyncBufSize;
    }
    
    public void open(IFile file, int pagePoolSize) {
        super.open(asyncBufSize != 0 
                   ? (ReplicationMasterFile)new AsyncReplicationMasterFile(this, file, asyncBufSize)
                   : new ReplicationMasterFile(this, file),
                   pagePoolSize);
    }

    public int getNumberOfAvailableHosts() { 
        return ((ReplicationMasterFile)pool.file).getNumberOfAvailableHosts();
    }

    String[] hosts;
    int      asyncBufSize;
}
