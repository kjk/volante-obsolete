#if WITH_REPLICATION
namespace Volante.Impl
{
    using System;
    using System.Net.Sockets;
    using System.Net;
    using System.Threading;
    using Volante;

    /// <summary>
    /// File performing replication of changed pages to specified slave nodes.
    /// </summary>
    public class ReplicationMasterFile : IFile
    {
        public FileListener Listener { get; set; }

        /// <summary>
        /// Constructor of replication master file
        /// </summary>
        /// <param name="db">replication database</param>
        /// <param name="file">local file used to store data locally</param>
        public ReplicationMasterFile(ReplicationMasterDatabaseImpl db, IFile file)
            : this(file, db.hosts, db.replicationAck)
        {
            this.db = db;
        }

        /// <summary>
        /// Constructor of replication master file
        /// </summary>
        /// <param name="file">local file used to store data locally</param>
        /// <param name="hosts">slave node hosts to which replication will be performed</param>
        /// <param name="ack">whether master should wait acknowledgment from slave node during trasanction commit</param>
        public ReplicationMasterFile(IFile file, string[] hosts, bool ack)
        {
            this.file = file;
            this.hosts = hosts;
            this.ack = ack;
            sockets = new Socket[hosts.Length];
            rcBuf = new byte[1];
            txBuf = new byte[8 + Page.pageSize];
            nHosts = 0;
            for (int i = 0; i < hosts.Length; i++)
            {
                connect(i);
            }
        }

        public int GetNumberOfAvailableHosts()
        {
            return nHosts;
        }

        protected void connect(int i)
        {
            String host = hosts[i];
            int colon = host.IndexOf(':');
            int port = int.Parse(host.Substring(colon + 1));
            host = host.Substring(0, colon);
            Socket socket = null;
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            for (int j = 0; j < MaxConnectionAttempts; j++)
            {
                foreach (IPAddress ip in Dns.GetHostEntry(host).AddressList)
                {
                    try
                    {
                        socket.Connect(new IPEndPoint(ip, port));
                        sockets[i] = socket;
                        nHosts += 1;
                        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, 1);
                        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger,
                            new System.Net.Sockets.LingerOption(true, LingerTime));
                        return;
                    }
                    catch (SocketException) { }
                }
                Thread.Sleep(ConnectionTimeout);
            }
            HandleError(hosts[i]);
        }

        /// <summary>
        /// When overriden by base class this method perfroms socket error handling
        /// </summary>     
        /// <returns><code>true</code> if host should be reconnected and attempt to send data to it should be 
        /// repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
        /// </returns>
        public bool HandleError(string host)
        {
            return (db != null && db.Listener != null)
                ? db.Listener.ReplicationError(host)
                : false;
        }

        public virtual void Write(long pos, byte[] buf)
        {
            for (int i = 0; i < sockets.Length; i++)
            {
                while (sockets[i] != null)
                {
                    try
                    {
                        Bytes.pack8(txBuf, 0, pos);
                        Array.Copy(buf, 0, txBuf, 8, buf.Length);
                        sockets[i].Send(txBuf);
                        if (!ack || pos != 0 || sockets[i].Receive(rcBuf) == 1)
                        {
                            break;
                        }
                    }
                    catch (SocketException) { }

                    sockets[i] = null;
                    nHosts -= 1;
                    if (HandleError(hosts[i]))
                    {
                        connect(i);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            file.Write(pos, buf);
        }

        public int Read(long pos, byte[] buf)
        {
            return file.Read(pos, buf);
        }

        public void Sync()
        {
            file.Sync();
        }

        public void Lock()
        {
            file.Lock();
        }

        public bool NoFlush
        {
            get { return file.NoFlush; }
            set { file.NoFlush = value; }
        }

        public virtual void Close()
        {
            file.Close();
            Bytes.pack8(txBuf, 0, -1);
            for (int i = 0; i < sockets.Length; i++)
            {
                if (sockets[i] != null)
                {
                    try
                    {
                        sockets[i].Send(txBuf);
                        sockets[i].Close();
                    }
                    catch (SocketException) { }
                }
            }
        }

        public long Length
        {
            get { return file.Length; }
        }

        public static int LingerTime = 10; // linger parameter for the socket
        public static int MaxConnectionAttempts = 10; // attempts to establish connection with slave node
        public static int ConnectionTimeout = 1000; // timeout between attempts to conbbect to the slave

        protected Socket[] sockets;
        protected byte[] txBuf;
        protected byte[] rcBuf;
        protected IFile file;
        protected string[] hosts;
        protected int nHosts;
        protected bool ack;

        protected ReplicationMasterDatabaseImpl db;
    }
}
#endif
