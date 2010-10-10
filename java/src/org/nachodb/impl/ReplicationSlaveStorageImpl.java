package org.nachodb.impl;

import java.io.*;
import java.net.*;

import org.nachodb.*;


public class ReplicationSlaveStorageImpl extends StorageImpl implements ReplicationSlaveStorage, Runnable
{ 
    public ReplicationSlaveStorageImpl(int port) { 
        this.port = port;
    }
    
    public void open(IFile file, int pagePoolSize) {
        try { 
            acceptor = new ServerSocket(port);
        } catch (IOException x) {
            return;
        }
        byte[] rootPage = new byte[Page.pageSize];
        int rc = file.read(0, rootPage);
        if (rc == Page.pageSize) { 
            prevIndex =  rootPage[DB_HDR_CURR_INDEX_OFFSET];
            initialized = rootPage[DB_HDR_INITIALIZED_OFFSET] != 0;
        } else { 
            initialized = false;
            prevIndex = -1;
        }
        this.file = file;
        lock = new PersistentResource();
        init = new Object();
        done = new Object();
        commit = new Object();
        listening = true;
        connect();
        pool = new PagePool(pagePoolSize/Page.pageSize);
        pool.open(file);
        thread = new Thread(this);
        thread.start();
        waitInitializationCompletion();
        super.open(file, pagePoolSize);
    }


    /**
     * Check if socket is connected to the master host
     * @return <code>true</code> if connection between slave and master is sucessfully established
     */
    public boolean isConnected() {
        return socket != null;
    }
    
    public void beginThreadTransaction(int mode)
    {
        if (mode != REPLICATION_SLAVE_TRANSACTION) {
            throw new IllegalArgumentException("Illegal transaction mode");
        }
        lock.sharedLock();
        Page pg = pool.getPage(0);
        header.unpack(pg.data);
        pool.unfix(pg);
        currIndex = 1-header.curr;
        currIndexSize = header.root[1-currIndex].indexUsed;
        committedIndexSize = currIndexSize;
        usedSize = header.root[currIndex].size;
    }
     
    public void endThreadTransaction(int maxDelay)
    {
        lock.unlock();
    }

    protected void waitInitializationCompletion() {
        try { 
            synchronized (init) { 
                while (!initialized) { 
                    init.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    /**
     * Wait until database is modified by master
     * This method blocks current thread until master node commits trasanction and
     * this transanction is completely delivered to this slave node
     */
    public void waitForModification() { 
        try { 
            synchronized (commit) { 
                if (socket != null) { 
                    commit.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    private static final int DB_HDR_CURR_INDEX_OFFSET  = 0;
    private static final int DB_HDR_DIRTY_OFFSET       = 1;
    private static final int DB_HDR_INITIALIZED_OFFSET = 2;
    private static final int PAGE_DATA_OFFSET          = 8;
    
    public static int LINGER_TIME = 10; // linger parameter for the socket

    private void connect()
    {
        try { 
            socket = acceptor.accept();
            try {
                socket.setSoLinger(true, LINGER_TIME);
            } catch (NoSuchMethodError er) {}
            try { 
                socket.setTcpNoDelay(true);
            } catch (Exception x) {}
            in = socket.getInputStream();
            if (replicationAck) { 
                out = socket.getOutputStream();
            }
        } catch (IOException x) { 
            socket = null;
            in = null;
        }
    }

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError() 
    {
        return (listener != null) ? listener.replicationError(null) : false;
    }

    public void run() { 
        byte[] buf = new byte[Page.pageSize+PAGE_DATA_OFFSET];
        byte[] page = new byte[Page.pageSize];

        while (listening) { 
            int offs = 0;
            do {
                int rc;
                try { 
                    rc = in.read(buf, offs, buf.length - offs);
                } catch (IOException x) { 
                    rc = -1;
                }
                synchronized(done) { 
                    if (!listening) { 
                        return;
                    }
                }
                if (rc < 0) { 
                    if (handleError()) { 
                        connect();
                    } else { 
                        return;
                    }
                } else { 
                    offs += rc;
                }
            } while (offs < buf.length);
            
            long pos = Bytes.unpack8(buf, 0);
            boolean transactionCommit = false;
            if (pos == 0) { 
                if (replicationAck) { 
                    try { 
                        out.write(buf, 0, 1);
                    } catch (IOException x) {
                        handleError();
                    }
                }
                if (buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != prevIndex) { 
                    prevIndex = buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                    lock.exclusiveLock();
                    transactionCommit = true;
                }
            } else if (pos < 0) { 
                synchronized(commit) { 
                    hangup();
                    commit.notifyAll();
                }     
                return;
            }
            
            Page pg = pool.putPage(pos);
            System.arraycopy(buf, PAGE_DATA_OFFSET, pg.data, 0, Page.pageSize);
            pool.unfix(pg);
            
            if (pos == 0) { 
                if (!initialized && buf[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0) { 
                    synchronized(init) { 
                        initialized = true;
                        init.notify();
                    }
                }
                if (transactionCommit) { 
                    lock.unlock();
                    synchronized(commit) { 
                        commit.notifyAll();
                    }
                    pool.flush();
                }
            }
        }            
    }

    public void close() {
        synchronized (done) {
            listening = false;
        }
        try { 
            thread.interrupt();
            thread.join();
        } catch (InterruptedException x) {}

        hangup();

        pool.flush();
        super.close();
    }

    private void hangup() { 
        if (socket != null) { 
            try { 
                in.close();
                if (out != null) { 
                    out.close();
                }
                socket.close();
            } catch (IOException x) {}
            in = null;
            socket = null;
        }
    }

    protected boolean isDirty() { 
        return false;
    }

    protected InputStream  in;
    protected OutputStream out;
    protected Socket       socket;
    protected int          port;
    protected IFile        file;
    protected boolean      initialized;
    protected boolean      listening;
    protected Object       init;
    protected Object       done;
    protected Object       commit;
    protected int          prevIndex;
    protected IResource    lock;
    protected ServerSocket acceptor;
    protected Thread       thread;
}
