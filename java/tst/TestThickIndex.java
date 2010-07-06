import org.garret.perst.*;

import java.util.*;

public class TestThickIndex { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
    }
    
    static class Indices extends Persistent {
        Index strIndex;
        Index intIndex;
    }

    final static int nRecords = 1000;
    final static int maxDuplicates = 1000;
    static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testthick.dbs", pagePoolSize);

        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createThickIndex(String.class);
            root.intIndex = db.createThickIndex(long.class);
            db.setRoot(root);
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;        
        int n = 0;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int d = (int)(key % maxDuplicates);            
            for (int j = 0; j < d; j++) { 
                Record rec = new Record();
                rec.intKey = key;
                rec.strKey = Long.toString(key);
                intIndex.put(new Key(rec.intKey), rec);                
                strIndex.put(new Key(rec.strKey), rec);                
                n += 1;
            }
        }
                
        db.commit();
        System.out.println("Elapsed time for inserting " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            IPersistent[] res1 = intIndex.get(new Key(key), new Key(key));
            IPersistent[] res2 = strIndex.get(new Key(Long.toString(key)), new Key(Long.toString(key)));
            int d = (int)(key % maxDuplicates);            
            Assert.that(res1.length == res2.length && res1.length == d);
            for (int j = 0; j < d; j++) { 
                Assert.that(((Record)res1[j]).intKey == key && ((Record)res2[j]).intKey == key);
            }
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        Iterator iterator = intIndex.iterator();
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
        }
        Assert.that(i == n);
        iterator = strIndex.iterator();
        String strKey = "";
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
        }
        Assert.that(i == n);
        System.out.println("Elapsed time for iterating through " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            IPersistent[] res = intIndex.get(new Key(key), new Key(key));
            int d = (int)(key % maxDuplicates);            
            Assert.that(res.length == d);
            for (int j = 0; j < d; j++) { 
                intIndex.remove(new Key(key), res[j]);
                strIndex.remove(new Key(Long.toString(key)), res[j]);
                res[j].deallocate();
            }
        }
        Assert.that(!intIndex.iterator().hasNext());
        Assert.that(!strIndex.iterator().hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        System.out.println("Elapsed time for deleting " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
