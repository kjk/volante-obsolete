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
    internal const int nRecords = 200000;
    internal static int pagePoolSize = 32 * 1024 * 1024;
	
    static public void  Main(System.String[] args)
    {
        Storage db = StorageFactory.Instance.createStorage();
		
        db.open("testidx.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.createIndex(typeof(System.String), true);
            root.intIndex = db.createIndex(typeof(long), true);
            db.Root = root;
        }
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        DateTime start = DateTime.Now;
        long key = 1999;
        for (int i = 0; i < nRecords; i++)
        {
            Record rec = new Record();
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = System.Convert.ToString(key);
            intIndex.put(new Key(rec.intKey), rec);
            strIndex.put(new Key(rec.strKey), rec);
        }
        db.commit();
        System.Console.Out.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
		
        start = System.DateTime.Now;
        key = 1999;
        for (int i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec1 = (Record) intIndex.get(new Key(key));
            Record rec2 = (Record) strIndex.get(new Key(System.Convert.ToString(key)));
            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.Console.Out.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
		
        start = System.DateTime.Now;
        key = 1999;
        for (int i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec = (Record) intIndex.get(new Key(key));
            intIndex.remove(new Key(key));
            strIndex.remove(new Key(System.Convert.ToString(key)), rec);
            rec.deallocate();
        }
        System.Console.Out.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
        db.close();
    }
}