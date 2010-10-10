package org.nachodb.impl;

import java.io.*;
import java.net.*;

import org.nachodb.*;


/**
 * File performing replication of changed pages to specified slave nodes.
 */
public class ReplicationMasterFile implements IFile 
{ 
    /**
     * Constructor of replication master file
     * @param storage replication storage
     * @param file local file used to store data locally
     */
    public ReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file) { 
        this(file, storage.hosts, storage.replicationAck);
        this.storage = storage;
    }

    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     */
    public ReplicationMasterFile(IFile file, String[] hosts, boolean ack) {         
        this.file = file;
        this.hosts = hosts;
        this.ack = ack;
        sockets = new Socket[hosts.length];
        out = new OutputStream[hosts.length];
        if (ack) { 
            in = new InputStream[hosts.length];
            rcBuf = new byte[1];
        }
        txBuf = new byte[8 + Page.pageSize];
        nHosts = 0;
        for (int i = 0; i < hosts.length; i++) { 
            connect(i);
        }
    }

    public int getNumberOfAvailableHosts() { 
        return nHosts;
    }

    protected void connect(int i)
    {
        String host = hosts[i];
        int colon = host.indexOf(':');
        int port = Integer.parseInt(host.substring(colon+1));
        host = host.substring(0, colon);
        Socket socket = null; 
        try { 
            for (int j = 0; j < MAX_CONNECT_ATTEMPTS; j++) { 
                try { 
                    socket = new Socket(InetAddress.getByName(host), port);
                    if (socket != null) { 
                        break;
                    }
                    Thread.sleep(CONNECTION_TIMEOUT);
                } catch (IOException x) {}
            }
        } catch (InterruptedException x) {}
            
        if (socket != null) { 
            try { 
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (NoSuchMethodError er) {}
                try { 
                    socket.setTcpNoDelay(true);
                } catch (Exception x) {}
                sockets[i] = socket;
                out[i] = socket.getOutputStream();
                if (ack) { 
                    in[i] = socket.getInputStream();
                }
                nHosts += 1;
            } catch (IOException x) { 
                handleError(hosts[i]);
                sockets[i] = null;
                out[i] = null;
            }
        } 
    }

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError(String host) 
    {
        System.err.println("Failed to establish connection with host " + host);
        return (storage != null && storage.listener != null) 
            ? storage.listener.replicationError(host) 
            : false;
    }


    public void write(long pos, byte[] buf) {
        for (int i = 0; i < out.length; i++) { 
            while (out[i] != null) {                 
                try { 
                    Bytes.pack8(txBuf, 0, pos);
                    System.arraycopy(buf, 0, txBuf, 8, buf.length);
                    out[i].write(txBuf);
                    if (!ack || pos != 0 || in[i].read(rcBuf) == 1) { 
                        break;
                    }
                } catch (IOException x) {} 

                out[i] = null;
                sockets[i] = null;
                nHosts -= 1;
                if (handleError(hosts[i])) { 
                    connect(i);
                } else { 
                    break;
                }
            }
        }
        file.write(pos, buf);
    }

    public int read(long pos, byte[] buf) {
        return file.read(pos, buf);
    }

    public void sync() {
        file.sync();
    }

    public boolean lock() { 
        return file.lock();
    }

    public void close() {
        file.close();
        Bytes.pack8(txBuf, 0, -1);
        for (int i = 0; i < out.length; i++) {  
            if (sockets[i] != null) { 
                try {  
                    out[i].write(txBuf);
                    out[i].close();
                    if (in != null) { 
                        in[i].close();
                    }
                    sockets[i].close();
                } catch (IOException x) {}
            }
        }
    }

    public long length() {
        return file.length();
    }

    public static int LINGER_TIME = 10; // linger parameter for the socket
    public static int MAX_CONNECT_ATTEMPTS = 10; // attempts to establish connection with slave node
    public static int CONNECTION_TIMEOUT = 1000; // timeout between attempts to conbbect to the slave

    OutputStream[] out;
    InputStream[]  in;
    Socket[]       sockets;
    byte[]         txBuf;
    byte[]         rcBuf;
    IFile          file;
    String[]       hosts;
    int            nHosts;
    boolean        ack;
    
    ReplicationMasterStorageImpl storage;
}
