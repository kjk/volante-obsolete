import org.nachodb.*;

import java.util.*;

enum Color { 
    RED, 
    GREEN,
    BLUE
}

class Record extends Persistent { 
    String strKey;
    long   intKey;
    Color  c;
};

class Indices extends Persistent {
    Index<Record> strIndex;
    Index<Record> intIndex;
}

public class TestIndex { 
    final static int nRecords = 100000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        //db.open("testidx.dbs", pagePoolSize);
        db.open(new MappedFile("testidx.dbs", pagePoolSize, false), pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.<Record>createIndex(String.class, true);
            root.intIndex = db.<Record>createIndex(long.class, true);
            db.setRoot(root);
        }
        Index<Record> intIndex = root.intIndex;
        Index<Record> strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;        
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            rec.c = Color.GREEN;
            intIndex.put(rec.intKey, rec);                
            strIndex.put(rec.strKey, rec);                
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intIndex.get(key);
            Record rec2 = (Record)strIndex.get(Long.toString(key));
            Assert.that(rec1 != null && rec1 == rec2);
            assert(rec1.c == Color.GREEN);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        key = Long.MIN_VALUE;
        i = 0;
        for (Record rec : intIndex) { 
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
            i += 1;
        }
        Assert.that(i == nRecords);
        String strKey = "";
        i = 0;
        for (Record rec : strIndex) { 
            Assert.that(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
            i += 1;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + (nRecords*2) + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        System.out.println("Memory usage");
        start = System.currentTimeMillis();
        for (MemoryUsage usage : db.getMemoryDump().values()) { 
            System.out.println(" " + usage.cls.getName() + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
        }
        System.out.println("Elapsed time for memory dump: " + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec = (Record)intIndex.get(key);
            intIndex.remove(key, rec);
            strIndex.remove(Long.toString(key));
            rec.deallocate();
        }
        Assert.that(!intIndex.iterator().hasNext());
        Assert.that(!strIndex.iterator().hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
