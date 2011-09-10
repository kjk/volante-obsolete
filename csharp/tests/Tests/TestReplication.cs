#if WITH_REPLICATION
namespace Volante
{
    using System;
    using System.Diagnostics;
    using System.Threading;

    public class TestReplication : ITest
    {
        class Record : Persistent
        {
            public int key;
        }

        bool log = false;
        const int count = 1000;
        const int transSize = 100;
        const int defaultPort = 6000;
        const int asyncBufSize = 1024 * 1024;
        const int cacheSizeInBytes = 32 * 1024 * 1024;

        long slaveCurrKey = 0;

        void Master(int port, bool async, bool ack, int nIterations)
        {
            Console.WriteLine("Starting a replication master");
            ReplicationMasterDatabase db =
                DatabaseFactory.CreateReplicationMasterDatabase(new string[] { "localhost:" + port },
                                                                       async ? asyncBufSize : 0);
            string dbName = "replicmaster.dbs";
            Tests.TryDeleteFile(dbName);
            db.ReplicationAck = ack;
            var dbFile = new OsFile(dbName);
            dbFile.NoFlush = true;
            db.Open(dbFile, cacheSizeInBytes);

            IFieldIndex<int, Record> root = (IFieldIndex<int, Record>)db.Root;
            if (root == null)
            {
                root = db.CreateFieldIndex<int, Record>("key", IndexType.Unique);
                db.Root = root;
            }
            DateTime start = DateTime.Now;
            int i;
            int lastKey = 0;
            for (i = 0; i < nIterations; i++)
            {
                if (i >= count)
                    root.Remove(new Key(i - count));

                Record rec = new Record();
                rec.key = i;
                lastKey = rec.key;
                root.Put(rec);
                if (i >= count && i % transSize == 0)
                    db.Commit();
                if (log && i % 1000 == 0)
                    Console.WriteLine("Master processed {0} rounds", i);
            }
            db.Commit();
            while (true)
            {
                long slaveKey = Interlocked.Read(ref slaveCurrKey);
                if (slaveKey == lastKey)
                    break;
                Thread.Sleep(100); // 1/10th sec
            }
            db.Close();

            Console.WriteLine("Replication master finished", i);
        }

        void Slave(int port, bool async, bool ack)
        {
            Console.WriteLine("Starting a replication slave");
            int i;
            ReplicationSlaveDatabase db = DatabaseFactory.CreateReplicationSlaveDatabase(port);

            db.ReplicationAck = ack;
            string dbName = "replicslave.dbs";
            Tests.TryDeleteFile(dbName);
            var dbFile = new OsFile(dbName);
            dbFile.NoFlush = true;
            db.Open(dbFile, cacheSizeInBytes);

            DateTime total = new DateTime(0);
            int n = 0;
            long lastKey = 0;
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
                        lastKey = rec.key;
                        Debug.Assert(prevKey < 0 || key == prevKey + 1);
                        prevKey = key;
                        i += 1;
                    }
                    Debug.Assert(i == count);
                    n += i;
                    total += (DateTime.Now - start);
                }
                db.EndThreadTransaction();
                Interlocked.Exchange(ref slaveCurrKey, lastKey);
                if (log && n % 1000 == 0)
                    Console.WriteLine("Slave processed {0} transactions", n);
            }
            db.Close();
            Console.WriteLine("Replication slave finished", n);
        }

        // TODO: use more databases from TestConfig
        public void Run(TestConfig config)
        {
            config.Result = new TestResult();

            bool ack = false;
            bool async = true;
            int port = defaultPort;
            // start the master thread
            int nIterations = config.Count;
            Thread t1 = new Thread(() => { Master(port, async, ack, nIterations); });
            t1.Name = "ReplicMaster";
            t1.Start();

            Thread t2 = new Thread(() => { Slave(port, async, ack); });
            t2.Name = "ReplicSlave";
            t2.Start();
            t1.Join();
            t2.Join();
        }
    }
}
#endif
