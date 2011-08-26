#if WITH_REPLICATION
namespace Volante.Impl
{
    using System;
    using System.Threading;
    using System.Net;
    using System.Net.Sockets;
    using Volante;

    /// <summary>
    /// File performing asynchronous replication of changed pages to specified slave nodes.
    /// </summary>
    public class AsyncReplicationMasterFile : ReplicationMasterFile
    {
        /// <summary>
        /// Constructor of replication master file
        /// <param name="db">replication database</param>
        /// <param name="file">local file used to store data locally</param>
        /// <param name="asyncBufSize">size of asynchronous buffer</param>
        /// </summary>
        public AsyncReplicationMasterFile(ReplicationMasterDatabaseImpl db, IFile file, int asyncBufSize)
            : base(db, file)
        {
            this.asyncBufSize = asyncBufSize;
            start();
        }

        /// <summary>
        /// Constructor of replication master file
        /// <param name="file">local file used to store data locally</param>
        /// <param name="hosts">slave node hosts to which replication will be performed</param>
        /// <param name="asyncBufSize">size of asynchronous buffer</param>
        /// <param name="ack">whether master should wait acknowledgment from slave node during trasanction commit</param>
        /// </summary>
        public AsyncReplicationMasterFile(IFile file, String[] hosts, int asyncBufSize, bool ack)
            : base(file, hosts, ack)
        {
            this.asyncBufSize = asyncBufSize;
            start();
        }

        private void start()
        {
            go = new object();
            async = new object();
            thread = new Thread(new ThreadStart(run));
            thread.Start();
        }

        class Parcel
        {
            public byte[] data;
            public long pos;
            public int host;
            public Parcel next;
        }

        public override void Write(long pos, byte[] buf)
        {
            file.Write(pos, buf);
            for (int i = 0; i < sockets.Length; i++)
            {
                if (sockets[i] != null)
                {
                    byte[] data = new byte[8 + buf.Length];
                    Bytes.pack8(data, 0, pos);
                    Array.Copy(buf, 0, data, 8, buf.Length);
                    Parcel p = new Parcel();
                    p.data = data;
                    p.pos = pos;
                    p.host = i;

                    lock (async)
                    {
                        buffered += data.Length;
                        while (buffered > asyncBufSize)
                        {
                            Monitor.Wait(async);
                        }
                    }

                    lock (go)
                    {
                        if (head == null)
                        {
                            head = tail = p;
                        }
                        else
                        {
                            tail = tail.next = p;
                        }
                        Monitor.Pulse(go);
                    }
                }
            }
        }

        public void run()
        {
            while (true)
            {
                Parcel p;
                lock (go)
                {
                    while (head == null)
                    {
                        if (closed)
                        {
                            return;
                        }
                        Monitor.Wait(go);
                    }
                    p = head;
                    head = p.next;
                }

                lock (async)
                {
                    if (buffered > asyncBufSize)
                    {
                        Monitor.PulseAll(async);
                    }
                    buffered -= p.data.Length;
                }
                int i = p.host;
                while (sockets[i] != null)
                {
                    try
                    {
                        sockets[i].Send(p.data);
                        if (!ack || p.pos != 0 || sockets[i].Receive(rcBuf) == 1)
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
        }

        public override void Close()
        {
            lock (go)
            {
                closed = true;
                Monitor.Pulse(go);
            }
            thread.Join();
            base.Close();
        }

        private int asyncBufSize;
        private int buffered;
        private bool closed;
        private object go;
        private object async;
        private Parcel head;
        private Parcel tail;
        private Thread thread;
    }
}
#endif
