#if WITH_REPLICATION
namespace Volante.Impl
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using Volante;

    public class ReplicationSlaveDatabaseImpl : DatabaseImpl, ReplicationSlaveDatabase
    {
        public ReplicationSlaveDatabaseImpl(int port)
        {
            this.port = port;
        }

        public override void Open(IFile file, int cacheSizeInBytes)
        {
            acceptor = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            acceptor.Bind(new IPEndPoint(IPAddress.Any, port));
            acceptor.Listen(ListenQueueSize);
            if (file.Length > 0)
            {
                byte[] rootPage = new byte[Page.pageSize];
                try
                {
                    file.Read(0, rootPage);
                    prevIndex = rootPage[DB_HDR_CURR_INDEX_OFFSET];
                    initialized = rootPage[DB_HDR_INITIALIZED_OFFSET] != 0;
                }
                catch (DatabaseException)
                {
                    initialized = false;
                    prevIndex = -1;
                }
            }
            else
            {
                prevIndex = -1;
                initialized = false;
            }
            this.file = file;
            lck = new PersistentResource();
            init = new object();
            done = new object();
            commit = new object();
            listening = true;
            connect();
            pool = new PagePool(cacheSizeInBytes / Page.pageSize);
            pool.open(file);
            thread = new Thread(new ThreadStart(run));
            thread.Name = "ReplicationSlaveStorageImpl";
            thread.Start();
            WaitInitializationCompletion();
            base.Open(file, cacheSizeInBytes);
        }


        /// <summary>
        /// Check if socket is connected to the master host
        /// @return <code>true</code> if connection between slave and master is sucessfully established
        /// </summary>
        public bool IsConnected()
        {
            return socket != null;
        }

        public override void BeginThreadTransaction(TransactionMode mode)
        {
            if (mode != TransactionMode.ReplicationSlave)
            {
                throw new ArgumentException("Illegal transaction mode");
            }
            lck.SharedLock();
            Page pg = pool.getPage(0);
            header.unpack(pg.data);
            pool.unfix(pg);
            currIndex = 1 - header.curr;
            currIndexSize = header.root[1 - currIndex].indexUsed;
            committedIndexSize = currIndexSize;
            usedSize = header.root[currIndex].size;
        }

        public override void EndThreadTransaction(int maxDelay)
        {
            lck.Unlock();
        }

        protected void WaitInitializationCompletion()
        {
            lock (init)
            {
                while (!initialized)
                {
                    Monitor.Wait(init);
                }
            }
        }

        /// <summary>
        /// Wait until database is modified by master
        /// This method blocks current thread until master node commits trasanction and
        /// this transanction is completely delivered to this slave node
        /// </summary>
        public void WaitForModification()
        {
            lock (commit)
            {
                if (socket != null)
                {
                    Monitor.Wait(commit);
                }
            }
        }

        const int DB_HDR_CURR_INDEX_OFFSET = 0;
        const int DB_HDR_DIRTY_OFFSET = 1;
        const int DB_HDR_INITIALIZED_OFFSET = 2;
        const int PAGE_DATA_OFFSET = 8;

        public static int ListenQueueSize = 10;
        public static int LingerTime = 10; // linger parameter for the socket

        private void connect()
        {
            try
            {
                socket = acceptor.Accept();
            }
            catch (SocketException)
            {
                socket = null;
            }
        }

        /// <summary>
        /// When overriden by base class this method perfroms socket error handling
        /// @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
        /// repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
        /// </summary>     
        public virtual bool HandleError()
        {
            return (Listener != null) ? Listener.ReplicationError(null) : false;
        }

        public void run()
        {
            byte[] buf = new byte[Page.pageSize + PAGE_DATA_OFFSET];

            while (listening)
            {
                int offs = 0;
                do
                {
                    int rc;
                    try
                    {
                        rc = socket.Receive(buf, offs, buf.Length - offs, SocketFlags.None);
                    }
                    catch (SocketException)
                    {
                        rc = -1;
                    }
                    lock (done)
                    {
                        if (!listening)
                        {
                            return;
                        }
                    }
                    if (rc < 0)
                    {
                        if (HandleError())
                        {
                            connect();
                        }
                        else
                        {
                            return;
                        }
                    }
                    else
                    {
                        offs += rc;
                    }
                } while (offs < buf.Length);

                long pos = Bytes.unpack8(buf, 0);
                bool transactionCommit = false;
                if (pos == 0)
                {
                    if (replicationAck)
                    {
                        try
                        {
                            socket.Send(buf, 0, 1, SocketFlags.None);
                        }
                        catch (SocketException)
                        {
                            HandleError();
                        }
                    }
                    if (buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != prevIndex)
                    {
                        prevIndex = buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                        lck.ExclusiveLock();
                        transactionCommit = true;
                    }
                }
                else if (pos < 0)
                {
                    lock (commit)
                    {
                        hangup();
                        Monitor.PulseAll(commit);
                    }
                    return;
                }

                Page pg = pool.putPage(pos);
                Array.Copy(buf, PAGE_DATA_OFFSET, pg.data, 0, Page.pageSize);
                pool.unfix(pg);

                if (pos == 0)
                {
                    if (!initialized && buf[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0)
                    {
                        lock (init)
                        {
                            initialized = true;
                            Monitor.Pulse(init);
                        }
                    }
                    if (transactionCommit)
                    {
                        lck.Unlock();
                        lock (commit)
                        {
                            Monitor.PulseAll(commit);
                        }
                        pool.flush();
                    }
                }
            }
        }

        public override void Close()
        {
            lock (done)
            {
                listening = false;
            }
            thread.Interrupt();
            thread.Join();
            hangup();

            pool.flush();
            base.Close();
        }

        private void hangup()
        {
            if (socket != null)
            {
                try
                {
                    socket.Close();
                }
                catch (SocketException) { }
                socket = null;
            }
        }

        protected override bool isDirty()
        {
            return false;
        }

        protected Socket socket;
        protected int port;
        protected IFile file;
        protected bool initialized;
        protected bool listening;
        protected object init;
        protected object done;
        protected object commit;
        protected int prevIndex;
        protected IResource lck;
        protected Socket acceptor;
        protected Thread thread;
    }
}
#endif
