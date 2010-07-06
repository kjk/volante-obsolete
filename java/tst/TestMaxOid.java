import org.garret.perst.*;

import java.util.*;

public class TestMaxOid { 
    final static int nRecords = 1000000000;
    static int pagePoolSize = 256*1024*1024;

    static class Record extends Persistent { 
        int key;

        Record() {}
        Record(int key) {
            this.key = key;
        }
    };

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testmaxoid.dbs", pagePoolSize);
        int i;
        FieldIndex root = (FieldIndex)db.getRoot();
        if (root == null) { 
            root = db.createFieldIndex(Record.class, "key", true);
            db.setRoot(root);
        }
        long start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record(i);
            root.put(rec);                
            if (i % 1000000 == 0) { 
                System.out.print("Insert " + i + " records\r");
                db.commit();
            }
        }
        
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.get(new Key(i));
            Assert.that(rec != null && rec.key == i);
        }
        System.out.println("Elapsed time for performing " + nRecords + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        Iterator iterator = root.iterator();
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.key == i);
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.remove(new Key(i));
            Assert.that(rec.key == i);
            rec.deallocate();
        }
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
