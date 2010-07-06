using System;
using Perst;

public class TestEnumerator
{ 
    const int nRecords = 1000;
    const int pagePoolSize = 32*1024*1024;

    class Record : Persistent 
    { 
        internal String strKey;
        internal long    intKey;
    }

    class Indices : Persistent 
    {
        internal Index strIndex;
        internal Index intIndex;
    }

    static public void Main(string[] args) 
    {	
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("testidx2.dbs", pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) 
        { 
            root = new Indices();
            root.strIndex = db.createIndex(typeof(string), false);
            root.intIndex = db.createIndex(typeof(long), false);
            db.setRoot(root);
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        DateTime start = DateTime.Now;
        long key = 1999;
        int i, j;
        IPersistent[] records;

        for (i = 0; i < nRecords; i++) 
        { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Convert.ToString(key);
            for (j = (int)(key % 10); --j >= 0;) 
            {  
                intIndex.put(rec.intKey, rec);                
                strIndex.put(rec.strKey, rec);        
            }        
        }
        db.commit();
        Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
        
        start = DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++) 
        { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Key fromInclusive = new Key(key);
            Key fromInclusiveStr = new Key(Convert.ToString(key));
            Key fromExclusive = new Key(key, false);
            Key fromExclusiveStr = new Key(Convert.ToString(key), false);
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Key tillInclusive = new Key(key);
            Key tillInclusiveStr = new Key(Convert.ToString(key));
            Key tillExclusive = new Key(key, false);
            Key tillExclusiveStr = new Key(Convert.ToString(key), false);
            
            // int key ascent order
            records = intIndex.get(fromInclusive, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.range(fromInclusive, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(fromInclusive, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.range(fromInclusive, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(fromExclusive, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.range(fromExclusive, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(fromExclusive, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.range(fromExclusive, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);



            records = intIndex.get(fromInclusive, null);
            j = 0;
            foreach (Record rec in intIndex.range(fromInclusive, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(fromExclusive, null);
            j = 0;
            foreach (Record rec in intIndex.range(fromExclusive, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(null, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.range(null, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(null, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.range(null, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = intIndex.get(null, null);
            j = 0;
            foreach (Record rec in intIndex.range(null, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);



            // int key descent order
            records = intIndex.get(fromInclusive, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromInclusive, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromInclusive, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromInclusive, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromExclusive, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromExclusive, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);



            records = intIndex.get(fromInclusive, null);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromInclusive, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(fromExclusive, null);
            j = records.Length;
            foreach (Record rec in intIndex.range(fromExclusive, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(null, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.range(null, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = intIndex.get(null, null);
            j = records.Length;
            foreach (Record rec in intIndex.range(null, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);


            // str key ascent order
            records = strIndex.get(fromInclusiveStr, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(fromInclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(fromInclusiveStr, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(fromInclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(fromExclusiveStr, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(fromExclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(fromExclusiveStr, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(fromExclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);



            records = strIndex.get(fromInclusiveStr, null);
            j = 0;
            foreach (Record rec in strIndex.range(fromInclusiveStr, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(fromExclusiveStr, null);
            j = 0;
            foreach (Record rec in strIndex.range(fromExclusiveStr, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(null, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(null, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(null, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.range(null, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);

            records = strIndex.get(null, null);
            j = 0;
            foreach (Record rec in strIndex.range(null, null, IterationOrder.AscentOrder)) 
            {
                Assert.that(rec == records[j++]);
            }
            Assert.that(j == records.Length);



            // str key descent order
            records = strIndex.get(fromInclusiveStr, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromInclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromInclusiveStr, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromInclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromExclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromExclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);



            records = strIndex.get(fromInclusiveStr, null);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromInclusiveStr, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(fromExclusiveStr, null);
            j = records.Length;
            foreach (Record rec in strIndex.range(fromExclusiveStr, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(null, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.range(null, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

            records = strIndex.get(null, null);
            j = records.Length;
            foreach (Record rec in strIndex.range(null, null, IterationOrder.DescentOrder)) 
            {
                Assert.that(rec == records[--j]);
            }
            Assert.that(j == 0);

           if (i % 100 == 0) { 
                Console.Write("Iteration " + i + "\n");
            }
        }
        Console.WriteLine("\nElapsed time for performing " + nRecords*36 + " index range searches: " 
                           + (DateTime.Now - start));
        
        strIndex.clear();
        intIndex.clear();

        Assert.that(!strIndex.GetEnumerator().MoveNext());
        Assert.that(!intIndex.GetEnumerator().MoveNext());
        Assert.that(!strIndex.GetEnumerator(null, null, IterationOrder.AscentOrder).MoveNext());
        Assert.that(!intIndex.GetEnumerator(null, null, IterationOrder.AscentOrder).MoveNext());
        Assert.that(!strIndex.GetEnumerator(null, null, IterationOrder.DescentOrder).MoveNext());
        Assert.that(!intIndex.GetEnumerator(null, null, IterationOrder.DescentOrder).MoveNext());
        db.commit();
        db.gc();
        db.close();
    }
}

