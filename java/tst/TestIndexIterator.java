import org.garret.perst.*;

import java.util.Iterator;

public class TestIndexIterator { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
    };
    
    static class Indices extends Persistent {
        Index strIndex;
        Index intIndex;
    }

    final static int nRecords = 1000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        if (args.length > 0) { 
            if ("altbtree".equals(args[0])) { 
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
            } else { 
                System.err.println("Unrecognized option: " + args[0]);
            }
        }
        db.open("testiter.dbs", pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createIndex(String.class, false);
            root.intIndex = db.createIndex(long.class, false);
            db.setRoot(root);
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i, j;
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            for (j = (int)(key % 10); --j >= 0;) {  
                intIndex.put(new Key(rec.intKey), rec);                
                strIndex.put(new Key(rec.strKey), rec);        
            }        
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i+=2) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Key fromInclusive = new Key(key);
            Key fromInclusiveStr = new Key(Long.toString(key));
            Key fromExclusive = new Key(key, false);
            Key fromExclusiveStr = new Key(Long.toString(key), false);
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Key tillInclusive = new Key(key);
            Key tillInclusiveStr = new Key(Long.toString(key));
            Key tillExclusive = new Key(key, false);
            Key tillExclusiveStr = new Key(Long.toString(key), false);
            
            IPersistent[] records;
            Iterator iterator;

            records = intIndex.get(fromInclusive, tillInclusive);
            iterator = intIndex.iterator(fromInclusive, tillInclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(fromInclusive, tillExclusive);
            iterator = intIndex.iterator(fromInclusive, tillExclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(fromExclusive, tillInclusive);
            iterator = intIndex.iterator(fromExclusive, tillInclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(fromExclusive, tillExclusive);
            iterator = intIndex.iterator(fromExclusive, tillExclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);



            records = intIndex.get(fromInclusive, null);
            iterator = intIndex.iterator(fromInclusive, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(fromExclusive, null);
            iterator = intIndex.iterator(fromExclusive, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(null, tillInclusive);
            iterator = intIndex.iterator(null, tillInclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(null, tillExclusive);
            iterator = intIndex.iterator(null, tillExclusive, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = intIndex.get(null, null);
            iterator = intIndex.iterator(null, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);



            records = intIndex.get(fromInclusive, tillInclusive);
            iterator = intIndex.iterator(fromInclusive, tillInclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromInclusive, tillExclusive);
            iterator = intIndex.iterator(fromInclusive, tillExclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, tillInclusive);
            iterator = intIndex.iterator(fromExclusive, tillInclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, tillExclusive);
            iterator = intIndex.iterator(fromExclusive, tillExclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);


            records = intIndex.get(fromInclusive, null);
            iterator = intIndex.iterator(fromInclusive, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, null);
            iterator = intIndex.iterator(fromExclusive, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, tillInclusive);
            iterator = intIndex.iterator(null, tillInclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, tillExclusive);
            iterator = intIndex.iterator(null, tillExclusive, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, null);
            iterator = intIndex.iterator(null, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);


            records = strIndex.get(fromInclusiveStr, tillInclusiveStr);
            iterator = strIndex.iterator(fromInclusiveStr, tillInclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(fromInclusiveStr, tillExclusiveStr);
            iterator = strIndex.iterator(fromInclusiveStr, tillExclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(fromExclusiveStr, tillInclusiveStr);
            iterator = strIndex.iterator(fromExclusiveStr, tillInclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(fromExclusiveStr, tillExclusiveStr);
            iterator = strIndex.iterator(fromExclusiveStr, tillExclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);


            records = strIndex.get(fromInclusiveStr, null);
            iterator = strIndex.iterator(fromInclusiveStr, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(fromExclusiveStr, null);
            iterator = strIndex.iterator(fromExclusiveStr, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(null, tillInclusiveStr);
            iterator = strIndex.iterator(null, tillInclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(null, tillExclusiveStr);
            iterator = strIndex.iterator(null, tillExclusiveStr, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);

            records = strIndex.get(null, null);
            iterator = strIndex.iterator(null, null, Index.ASCENT_ORDER);
            for (j = 0; iterator.hasNext(); j++) { 
                Assert.that(iterator.next() == records[j]);
            }
            Assert.that(j == records.length);



            records = strIndex.get(fromInclusiveStr, tillInclusiveStr);
            iterator = strIndex.iterator(fromInclusiveStr, tillInclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromInclusiveStr, tillExclusiveStr);
            iterator = strIndex.iterator(fromInclusiveStr, tillExclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, tillInclusiveStr);
            iterator = strIndex.iterator(fromExclusiveStr, tillInclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, tillExclusiveStr);
            iterator = strIndex.iterator(fromExclusiveStr, tillExclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);



            records = strIndex.get(fromInclusiveStr, null);
            iterator = strIndex.iterator(fromInclusiveStr, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, null);
            iterator = strIndex.iterator(fromExclusiveStr, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, tillInclusiveStr);
            iterator = strIndex.iterator(null, tillInclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, tillExclusiveStr);
            iterator = strIndex.iterator(null, tillExclusiveStr, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, null);
            iterator = strIndex.iterator(null, null, Index.DESCENT_ORDER);
            for (j = records.length; iterator.hasNext();) { 
                Assert.that(iterator.next() == records[--j]);
            }
            Assert.that(j == 0);

            if (i % 100 == 0) { 
                System.out.print("Iteration " + i + "\r");
            }
        }
        System.out.println("\nElapsed time for performing " + nRecords*36 + " index range searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        strIndex.clear();
        intIndex.clear();

        Assert.that(!strIndex.iterator().hasNext());
        Assert.that(!intIndex.iterator().hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        Assert.that(!strIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        db.commit();
        db.gc();
        db.close();
    }
}
