import org.garret.perst.*;

import java.util.*;

public class TestReplic { 
    static class Record extends Persistent { 
        int key;
    }
    
    final static int nIterations = 1000000;
    final static int nRecords = 1000;
    final static int transSize = 100;
    final static int defaultPort = 6000;
    final static int asyncBufSize = 1024*1024;
    static int pagePoolSize = 32*1024*1024;

    private static void usage() { 
        System.err.println("Usage: java TestReplic (master|slave) [port] [-async] [-ack]");
    }

    static public void main(String[] args) {    
        int i;
        if (args.length < 1) {
            usage();
            return;
        }
        int port = defaultPort;
        boolean ack = false;
        boolean async = false;
        for (i = 1; i < args.length; i++) { 
            if (args[i].startsWith("-")) { 
                if (args[i].equals("-async")) { 
                    async = true;
                } else if (args[i].equals("-ack")) { 
                    ack = true;
                } else { 
                    usage();
                }
            } else { 
                port = Integer.parseInt(args[i]);
            }
        }
        if ("master".equals(args[0])) { 
            ReplicationMasterStorage db = 
                StorageFactory.getInstance().createReplicationMasterStorage(new String[]{"localhost:" + port},
                                                                            async ? asyncBufSize : 0);
            db.setProperty("perst.file.noflush", Boolean.TRUE);            
            db.setProperty("perst.replication.ack", Boolean.valueOf(ack));
            db.open("master.dbs", pagePoolSize);

            FieldIndex root = (FieldIndex)db.getRoot();
            if (root == null) { 
                root = db.createFieldIndex(Record.class, "key", true);
                db.setRoot(root);
            }
            long start = System.currentTimeMillis();
            for (i = 0; i < nIterations; i++) {
                if (i >= nRecords) { 
                    root.remove(new Key(i-nRecords));
                }
                Record rec = new Record();
                rec.key = i;
                root.put(rec);
                if (i >= nRecords && i % transSize == 0) {
                    db.commit();
                }
            }
            db.close();
            System.out.println("Elapsed time for " + nIterations + " iterations: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        } else if ("slave".equals(args[0])) { 
            ReplicationSlaveStorage db = 
                StorageFactory.getInstance().createReplicationSlaveStorage(port); 
            db.setProperty("perst.file.noflush", Boolean.TRUE);
            db.setProperty("perst.replication.ack", Boolean.valueOf(ack));
            db.open("slave.dbs", pagePoolSize);         
            long total = 0;
            int n = 0;
            while (db.isConnected()) { 
                db.waitForModification();
                db.beginThreadTransaction(Storage.REPLICATION_SLAVE_TRANSACTION);
                FieldIndex root = (FieldIndex)db.getRoot();
                if (root != null && root.size() == nRecords) {
                    long start = System.currentTimeMillis();
                    Iterator iterator = root.iterator();
                    int prevKey = ((Record)iterator.next()).key;
                    for (i = 1; iterator.hasNext(); i++) { 
                        int key = ((Record)iterator.next()).key;
                        Assert.that(key == prevKey+1);
                        prevKey = key;
                    }
                    Assert.that(i == nRecords);
                    n += 1;
                    total += (System.currentTimeMillis() - start);
                }
                db.endThreadTransaction();
            }
            db.close();
            System.out.println("Elapsed time for " + n + " iterations: " + total + " milliseconds");
        } else {
            usage();
        }
    }
}
