import org.garret.perst.*;

import java.io.*;

public class TestBackup { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
        double realKey;
    };
    
    static class Indices extends Persistent {
        Index      strIndex;
        FieldIndex intIndex;
        FieldIndex compoundIndex;
    }

    final static int nRecords = 100000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) throws Exception {   
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("testbck1.dbs", pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createIndex(String.class, true);
            root.intIndex = db.createFieldIndex(Record.class, "intKey", true);
            root.compoundIndex = db.createFieldIndex(Record.class, new String[]{"strKey", "intKey"}, true);
            db.setRoot(root);
        }
        FieldIndex intIndex = root.intIndex;
        FieldIndex compoundIndex = root.compoundIndex;
        Index strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            rec.realKey = (double)key;
            intIndex.put(rec);                
            strIndex.put(new Key(rec.strKey), rec);                
            compoundIndex.put(rec);                
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        OutputStream out = new FileOutputStream("testbck2.dbs");
        db.backup(out);
        out.close();

        System.out.println("Elapsed time for backup " + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
        db.open("testbck2.dbs", pagePoolSize);

        root = (Indices)db.getRoot();
        intIndex = root.intIndex;
        strIndex = root.strIndex;
        compoundIndex = root.compoundIndex;
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            String strKey = Long.toString(key);
            Record rec1 = (Record)intIndex.get(new Key(key));
            Record rec2 = (Record)strIndex.get(new Key(strKey));
            Record rec3 = (Record)compoundIndex.get(new Key(strKey, new Long(key)));
            Assert.that(rec1 != null);
            Assert.that(rec1 == rec2);
            Assert.that(rec1 == rec3);
            Assert.that(rec1.intKey == key);
            Assert.that(rec1.realKey == (double)key);
            Assert.that(strKey.equals(rec1.strKey));
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}




