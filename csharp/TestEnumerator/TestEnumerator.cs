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
        Storage db = StorageFactory.Instance.CreateStorage();

        db.Open("testidx2.dbs", pagePoolSize);
        Indices root = (Indices)db.Root;
        if (root == null) 
        { 
            root = new Indices();
            root.strIndex = db.CreateIndex(typeof(string), false);
            root.intIndex = db.CreateIndex(typeof(long), false);
            db.Root = root;
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
                intIndex.Put(rec.intKey, rec);                
                strIndex.Put(rec.strKey, rec);        
            }        
        }
        db.Commit();
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
            records = intIndex.Get(fromInclusive, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(fromInclusive, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(fromExclusive, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(fromExclusive, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);



            records = intIndex.Get(fromInclusive, null);
            j = 0;
            foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(fromExclusive, null);
            j = 0;
            foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(null, tillInclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(null, tillExclusive);
            j = 0;
            foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = intIndex.Get(null, null);
            j = 0;
            foreach (Record rec in intIndex.Range(null, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);



            // int key descent order
            records = intIndex.Get(fromInclusive, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(fromInclusive, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(fromExclusive, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(fromExclusive, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);



            records = intIndex.Get(fromInclusive, null);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(fromExclusive, null);
            j = records.Length;
            foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(null, tillInclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(null, tillExclusive);
            j = records.Length;
            foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = intIndex.Get(null, null);
            j = records.Length;
            foreach (Record rec in intIndex.Range(null, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);


            // str key ascent order
            records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);



            records = strIndex.Get(fromInclusiveStr, null);
            j = 0;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(fromExclusiveStr, null);
            j = 0;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(null, tillInclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(null, tillExclusiveStr);
            j = 0;
            foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);

            records = strIndex.Get(null, null);
            j = 0;
            foreach (Record rec in strIndex.Range(null, null, IterationOrder.AscentOrder)) 
            {
                Assert.That(rec == records[j++]);
            }
            Assert.That(j == records.Length);



            // str key descent order
            records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);



            records = strIndex.Get(fromInclusiveStr, null);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(fromExclusiveStr, null);
            j = records.Length;
            foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(null, tillInclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(null, tillExclusiveStr);
            j = records.Length;
            foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

            records = strIndex.Get(null, null);
            j = records.Length;
            foreach (Record rec in strIndex.Range(null, null, IterationOrder.DescentOrder)) 
            {
                Assert.That(rec == records[--j]);
            }
            Assert.That(j == 0);

           if (i % 100 == 0) { 
                Console.Write("Iteration " + i + "\n");
            }
        }
        Console.WriteLine("\nElapsed time for performing " + nRecords*36 + " index range searches: " 
                           + (DateTime.Now - start));
        
        strIndex.Clear();
        intIndex.Clear();

        Assert.That(!strIndex.GetEnumerator().MoveNext());
        Assert.That(!intIndex.GetEnumerator().MoveNext());
        Assert.That(!strIndex.GetEnumerator(null, null, IterationOrder.AscentOrder).MoveNext());
        Assert.That(!intIndex.GetEnumerator(null, null, IterationOrder.AscentOrder).MoveNext());
        Assert.That(!strIndex.GetEnumerator(null, null, IterationOrder.DescentOrder).MoveNext());
        Assert.That(!intIndex.GetEnumerator(null, null, IterationOrder.DescentOrder).MoveNext());
        db.Commit();
        db.Gc();
        db.Close();
    }
}

