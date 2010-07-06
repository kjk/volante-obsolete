import org.garret.perst.*;

import java.util.*;

public class TestIndex2 { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
    };
    
    static class Indices extends Persistent {
        SortedCollection strIndex;
        SortedCollection intIndex;
    }

    static class IntRecordComparator extends PersistentComparator 
    {
        public int compareMembers(IPersistent m1, IPersistent m2) {
            long diff = ((Record)m1).intKey - ((Record)m2).intKey;
            return diff < 0 ? -1 : diff == 0 ? 0 : 1;
        }
        
        public int compareMemberWithKey(IPersistent mbr, Object key) { 
            long diff = ((Record)mbr).intKey - ((Long)key).longValue();
            return diff < 0 ? -1 : diff == 0 ? 0 : 1;
        }
    }

    static class StrRecordComparator extends PersistentComparator {
        public int compareMembers(IPersistent m1, IPersistent m2) {
            return ((Record)m1).strKey.compareTo(((Record)m2).strKey);
        }
        
        public int compareMemberWithKey(IPersistent mbr, Object key) {
            return ((Record)mbr).strKey.compareTo((String)key);
        }
    }

    final static int nRecords = 100000;
    final static int pagePoolSize = Storage.INFINITE_PAGE_POOL; // 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("testidx2.dbs", pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createSortedCollection(new StrRecordComparator(), true);
            root.intIndex = db.createSortedCollection(new IntRecordComparator(), true);
            db.setRoot(root);
        }
        SortedCollection intIndex = root.intIndex;
        SortedCollection strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;        
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            intIndex.add(rec);                
            strIndex.add(rec);                
        }
        db.commit();
        db.gc();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intIndex.get(new Long(key));
            Record rec2 = (Record)strIndex.get(Long.toString(key));
            Assert.that(rec1 != null && rec1 == rec2);
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
        Assert.that(i == nRecords);
        iterator = strIndex.iterator();
        String strKey = "";
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + (nRecords*2) + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        HashMap map = db.getMemoryDump();
        iterator = map.values().iterator();
        System.out.println("Memory usage");
        start = System.currentTimeMillis();
        while (iterator.hasNext()) { 
            MemoryUsage usage = (MemoryUsage)iterator.next();
            System.out.println(" " + usage.cls.getName() + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
        }
        System.out.println("Elapsed time for memory dump: " + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec = (Record)intIndex.get(new Long(key));
            intIndex.remove(rec);            
            strIndex.remove(rec);
            rec.deallocate();
        }
        Assert.that(!intIndex.iterator().hasNext());
        Assert.that(!strIndex.iterator().hasNext());
        Assert.that(!intIndex.iterator(null, null).hasNext());
        Assert.that(!strIndex.iterator(null, null).hasNext());
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
