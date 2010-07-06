import org.garret.perst.*;

class Record extends Persistent { 
    String strKey;
    long   intKey;
};

class Root extends Persistent {
    Index strIndex;
    Index intIndex;
}

public class TestIndex extends Thread { 
    final static int nRecords = 200000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {	
        Storage db = StorageFactory.getInstance().createStorage();

	db.open("testidx.dbs", pagePoolSize);
        Root root = (Root)db.getRoot();
        if (root == null) { 
            root = new Root();
            root.strIndex = db.createIndex(String.class, true);
            root.intIndex = db.createIndex(long.class, true);
            db.setRoot(root);
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            intIndex.put(new Key(rec.intKey), rec);                
            strIndex.put(new Key(rec.strKey), rec);                
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intIndex.get(new Key(key));
            Record rec2 = (Record)strIndex.get(new Key(Long.toString(key)));
            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec = (Record)intIndex.get(new Key(key));
            intIndex.remove(new Key(key));
            strIndex.remove(new Key(Long.toString(key)), rec);
            rec.deallocate();
        }
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
