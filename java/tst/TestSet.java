import org.garret.perst.*;

import java.util.*;

public class TestSet { 
    static class Indices extends Persistent {
        IPersistentSet set;
        Index          index;
    }

    static class Record extends Persistent { 
        int id;

        Record() {}
        Record(int id) { this.id = id; }
    }

    final static int nRecords = 1000;
    final static int maxInitSize = 500;
    static int pagePoolSize = 32*1024*1024;
    
    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testset.dbs", pagePoolSize);

        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.set = db.createSet();
            root.index = db.createIndex(long.class, true);
            db.setRoot(root);
        }
        int i, n, m;    
        long key = 1999;
        long start = System.currentTimeMillis();    
        for (i = 0, n = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int r = (int)(key % maxInitSize);            
            IPersistentSet ps = db.createScalableSet(r);
            for (int j = 0; j < r; j++) { 
                ps.add(new Record(j));
                n += 1;
            }
            root.set.add(ps);
            root.index.put(new Key(key), ps);
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int r = (int)(key % maxInitSize);            
            IPersistentSet ps = (IPersistentSet)root.index.get(new Key(key));
            Assert.that(root.set.contains(ps));
            Assert.that(ps.size() == r);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        Iterator iterator = root.set.iterator();
        for (i = 0; iterator.hasNext();) { 
            IPersistentSet ps = (IPersistentSet)iterator.next();
            Iterator si = ps.iterator();
            int sum = 0;
            while (si.hasNext()) { 
                sum += ((Record)si.next()).id;
            }
            Assert.that(ps.size()*(ps.size()-1)/2 == sum);
            i += ps.size();
        }
        Assert.that(i == n);
        System.out.println("Elapsed time for iterating through " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int r = (int)(key % maxInitSize);            
            IPersistentSet ps = (IPersistentSet)root.index.get(new Key(key));
            Assert.that(ps.size() == r);
            for (int j = r; j < r*2; j++) { 
                Record rec = new Record(j);
                ps.add(rec);
                ps.add(rec);
            }
        }
        db.commit();
        System.out.println("Elapsed time for adding " + n + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        iterator = root.set.iterator();
        for (i = 0; iterator.hasNext();) { 
            IPersistentSet ps = (IPersistentSet)iterator.next();
            Iterator si = ps.iterator();
            int sum = 0;
            while (si.hasNext()) { 
                sum += ((Record)si.next()).id;
            }
            Assert.that(ps.size()*(ps.size()-1)/2 == sum);
            i += ps.size();
        }
        Assert.that(i == n*2);
        System.out.println("Elapsed time for iterating through " + n*2 + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            IPersistentSet ps = (IPersistentSet)root.index.remove(new Key(key));
            int r = (int)(key % maxInitSize)*2;
            Assert.that(ps.size() == r);
            root.set.remove(ps);
            for (iterator = ps.iterator(); iterator.hasNext(); ((IPersistent)iterator.next()).deallocate());
            ps.deallocate();
        }
        Assert.that(root.set.size() == 0);
        Assert.that(root.index.size() == 0);
        Assert.that(!root.set.iterator().hasNext());
        Assert.that(!root.index.iterator().hasNext());
        System.out.println("Elapsed time for deleting " + n*2 + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
