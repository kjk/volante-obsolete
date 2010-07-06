using System;
using Perst;

class Record:Persistent
{
    internal System.String strKey;
    internal long intKey;
}


class Root:Persistent
{
    internal Index strIndex;
    internal Index intIndex;
}

public class TestIndex
{
    internal const int nRecords = 100000;
    internal static int pagePoolSize = 32 * 1024 * 1024;
	
    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.Instance.createStorage();
		
        db.open("testidx.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.createIndex(typeof(String), true);
            root.intIndex = db.createIndex(typeof(long), true);
            db.Root = root;
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        DateTime start = DateTime.Now;
        long key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            Record rec = new Record();
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = System.Convert.ToString(key);
            intIndex.put(new Key(rec.intKey), rec);
            strIndex.put(new Key(rec.strKey), rec);
        }
        db.commit();
        System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
		
        start = System.DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec1 = (Record) intIndex.get(new Key(key));
            Record rec2 = (Record) strIndex.get(new Key(Convert.ToString(key)));
            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));

        start = System.DateTime.Now;
        key = Int64.MinValue;
        i = 0;
        foreach (Record rec in intIndex) 
        {
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
            i += 1;
        }
        Assert.that(i == nRecords);
        i = 0;
        String strKey = "";
        foreach (Record rec in strIndex) 
        {
            Assert.that(rec.strKey.CompareTo(strKey) >= 0);
            strKey = rec.strKey;
            i += 1;
        }
        Assert.that(i == nRecords);
        System.Console.WriteLine("Elapsed time for iteration through " + (nRecords * 2) + " records: " + (DateTime.Now - start));

        start = System.DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec = (Record) intIndex.get(new Key(key));
            intIndex.remove(new Key(key));
            strIndex.remove(new Key(System.Convert.ToString(key)), rec);
            rec.deallocate();
        }
        System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
        db.close();
    }
}