using System;
using NachoDB;
using System.Diagnostics;

public class TestReplic 
{ 
    class Record : Persistent { 
        public int key;
    }
    
    const int nIterations = 1000000;
    const int nRecords = 1000;
    const int transSize = 100;
    const int defaultPort = 6000;
    const int asyncBufSize = 1024*1024;
    const int pagePoolSize = 32*1024*1024;

    private static void usage() { 
        Console.WriteLine("Usage: java TestReplic (master|slave) [port] [-async] [-ack]");
    }

    static public void Main(string[] args) {    
        int i;
        if (args.Length < 1) {
            usage();
            return;
        }
        int port = defaultPort;
        bool ack = false;
        bool async = false;
        for (i = 1; i < args.Length; i++) { 
            if (args[i].StartsWith("-")) { 
                if (args[i] == "-async") { 
                    async = true;
                } else if (args[i] == "-ack") { 
                    ack = true;
                } else { 
                    usage();
                }
            } else { 
                port = int.Parse(args[i]);
            }
        }
        if ("master" == args[0]) { 
            ReplicationMasterStorage db = 
                StorageFactory.Instance.CreateReplicationMasterStorage(new string[]{"localhost:" + port},
                                                                       async ? asyncBufSize : 0);
            db.SetProperty("perst.file.noflush", true);            
            db.SetProperty("perst.replication.ack", ack);
            db.Open("master.dbs", pagePoolSize);

#if USE_GENERICS
            FieldIndex<int,Record> root = (FieldIndex<int,Record>)db.Root;
            if (root == null) { 
                root = db.CreateFieldIndex<int,Record>("key", true);
#else
            FieldIndex root = (FieldIndex)db.Root;
            if (root == null) { 
                root = db.CreateFieldIndex(typeof(Record), "key", true);
#endif
                db.Root = root;
            }
            DateTime start = DateTime.Now;
            for (i = 0; i < nIterations; i++) {
                if (i >= nRecords) { 
                    root.Remove(new Key(i-nRecords));
                }
                Record rec = new Record();
                rec.key = i;
                root.Put(rec);
                if (i >= nRecords && i % transSize == 0) {
                    db.Commit();
                }
            }
            db.Close();
            Console.WriteLine("Elapsed time for " + nIterations + " iterations: " 
                               + (DateTime.Now - start));
        } else if ("slave" == args[0]) { 
            ReplicationSlaveStorage db = 
                StorageFactory.Instance.CreateReplicationSlaveStorage(port); 
            db.SetProperty("perst.file.noflush", true);
            db.SetProperty("perst.replication.ack", ack);
            db.Open("slave.dbs", pagePoolSize);         
            DateTime total = new DateTime(0);
            int n = 0;
            while (db.IsConnected()) { 
                db.WaitForModification();
                db.BeginThreadTransaction(TransactionMode.ReplicationSlave);
#if USE_GENERICS
                FieldIndex<int,Record> root = (FieldIndex<int,Record>)db.Root;
#else
                FieldIndex root = (FieldIndex)db.Root;
#endif
                if (root != null && root.Count == nRecords) {
                    DateTime start = DateTime.Now;
                    int prevKey = -1;
                    i = 0;
                    foreach (Record rec in root) { 
                        int key = rec.key;
                        Debug.Assert(prevKey < 0 || key == prevKey+1);
                        prevKey = key;
                        i += 1;
                    }
                    Debug.Assert(i == nRecords);
                    n += 1;
                    total += (DateTime.Now - start);
                }
                db.EndThreadTransaction();
            }
            db.Close();
            Console.WriteLine("Elapsed time for " + n + " iterations: " + total);
        } else {
            usage();
        }
    }
}
