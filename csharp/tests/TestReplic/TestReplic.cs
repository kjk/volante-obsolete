using System;
using Volante;
using System.Diagnostics;

#if !WITH_REPLICATION
public class TestReplic 
{ 
    static public void Main(string[] args)
    {
        Console.WriteLine("Replication not present in this build");
    }
    
}
#else
public class TestReplic
{
    class Record : Persistent
    {
        public int key;
    }

    const int nIterations = 1000000;
    const int count = 1000;
    const int transSize = 100;
    const int defaultPort = 6000;
    const int asyncBufSize = 1024 * 1024;
    const int pagePoolSize = 32 * 1024 * 1024;

    private static void usage()
    {
        Console.WriteLine("Usage: TestReplic (master|slave) [port] [-async] [-ack]");
    }

    static public void Main(string[] args)
    {
        int i;
        if (args.Length < 1)
        {
            usage();
            return;
        }
        int port = defaultPort;
        bool ack = false;
        bool async = false;
        for (i = 1; i < args.Length; i++)
        {
            if (args[i].StartsWith("-"))
            {
                if (args[i] == "-async")
                {
                    async = true;
                }
                else if (args[i] == "-ack")
                {
                    ack = true;
                }
                else
                {
                    usage();
                }
            }
            else
            {
                port = int.Parse(args[i]);
            }
        }
        if ("master" == args[0])
        {
            ReplicationMasterDatabase db =
                DatabaseFactory.CreateReplicationMasterDatabase(new string[] { "localhost:" + port },
                                                                       async ? asyncBufSize : 0);
            db.ReplicationAck = ack;
            var dbFile = new OsFile("master.dbs");
            dbFile.NoFlush = true;
            db.Open(dbFile, pagePoolSize);

            IFieldIndex<int, Record> root = (IFieldIndex<int, Record>)db.Root;
            if (root == null)
            {
                root = db.CreateFieldIndex<int, Record>("key", IndexType.Unique);
                db.Root = root;
            }
            DateTime start = DateTime.Now;
            for (i = 0; i < nIterations; i++)
            {
                if (i >= count)
                {
                    root.Remove(new Key(i - count));
                }
                Record rec = new Record();
                rec.key = i;
                root.Put(rec);
                if (i >= count && i % transSize == 0)
                {
                    db.Commit();
                }
            }
            db.Close();
            Console.WriteLine("Elapsed time for " + nIterations + " iterations: "
                               + (DateTime.Now - start));
        }
        else if ("slave" == args[0])
        {
            ReplicationSlaveDatabase db =
                DatabaseFactory.CreateReplicationSlaveDatabase(port);

            db.ReplicationAck = ack;
            var dbFile = new OsFile("slave.dbs");
            dbFile.NoFlush = true;
            db.Open(dbFile, pagePoolSize);

            DateTime total = new DateTime(0);
            int n = 0;
            while (db.IsConnected())
            {
                db.WaitForModification();
                db.BeginThreadTransaction(TransactionMode.ReplicationSlave);
                IFieldIndex<int, Record> root = (IFieldIndex<int, Record>)db.Root;
                if (root != null && root.Count == count)
                {
                    DateTime start = DateTime.Now;
                    int prevKey = -1;
                    i = 0;
                    foreach (Record rec in root)
                    {
                        int key = rec.key;
                        Debug.Assert(prevKey < 0 || key == prevKey + 1);
                        prevKey = key;
                        i += 1;
                    }
                    Debug.Assert(i == count);
                    n += 1;
                    total += (DateTime.Now - start);
                }
                db.EndThreadTransaction();
            }
            db.Close();
            Console.WriteLine("Elapsed time for " + n + " iterations: " + total);
        }
        else
        {
            usage();
        }
    }
}
#endif

