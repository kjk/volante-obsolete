using System;
using System.Collections;
using Perst;
using System.Diagnostics;

public class Record:Persistent
{
    public string strKey;
    public long intKey;
}


public class Root:Persistent
{
    public Index strIndex;
    public Index intIndex;
}

public class TestIndex
{
    internal const int nRecords = 100000;
    internal static int pagePoolSize = 32 * 1024 * 1024;
	
    static public void  Main(string[] args)
    {
        int i;
        Storage db = StorageFactory.Instance.CreateStorage();
		
        bool serializableTransaction = false;
        for (i = 0; i < args.Length; i++) 
        { 
            if ("inmemory" == args[i]) 
            { 
                pagePoolSize = Storage.INFINITE_PAGE_POOL;
            } 
            else if ("altbtree" == args[i]) 
            { 
                db.SetProperty("perst.alternative.btree", true);
            } 
            else if ("serializable" == args[i]) 
            { 
                db.SetProperty("perst.alternative.btree", true);
                serializableTransaction = true;
            } 
            else 
            { 
                Console.WriteLine("Unrecognized option: " + args[i]);
            }
        }
        db.Open("testidx.dbs", pagePoolSize);

#if !COMPACT_NET_FRAMEWORK
        if (serializableTransaction) 
        { 
            db.BeginThreadTransaction(TransactionMode.Serializable);
        }
#endif

        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.CreateIndex(typeof(String), true);
            root.intIndex = db.CreateIndex(typeof(long), true);
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
            intIndex.Put(new Key(rec.intKey), rec);
            strIndex.Put(new Key(rec.strKey), rec);
        }

#if !COMPACT_NET_FRAMEWORK
        if (serializableTransaction) 
        { 
            db.EndThreadTransaction();
            db.BeginThreadTransaction(TransactionMode.Serializable);
        } 
        else 
        {
            db.Commit();
        }
#else 
        db.Commit();
#endif
        System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
		
        start = System.DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec1 = (Record) intIndex.Get(new Key(key));
            Record rec2 = (Record) strIndex.Get(new Key(Convert.ToString(key)));
            Debug.Assert(rec1 != null && rec1 == rec2);
        }     
        System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));

        start = System.DateTime.Now;
        key = Int64.MinValue;
        i = 0;
        foreach (Record rec in intIndex) 
        {
            Debug.Assert(rec.intKey >= key);
            key = rec.intKey;
            i += 1;
        }
        Debug.Assert(i == nRecords);
        i = 0;
        String strKey = "";
        foreach (Record rec in strIndex) 
        {
            Debug.Assert(rec.strKey.CompareTo(strKey) >= 0);
            strKey = rec.strKey;
            i += 1;
        }
        Debug.Assert(i == nRecords);
        System.Console.WriteLine("Elapsed time for iteration through " + (nRecords * 2) + " records: " + (DateTime.Now - start));


        Hashtable map = db.GetMemoryDump();
        Console.WriteLine("Memory usage");
        start = DateTime.Now;
        foreach (MemoryUsage usage in db.GetMemoryDump().Values) 
        { 
            Console.WriteLine(" " + usage.type.Name + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
        }
        Console.WriteLine("Elapsed time for memory dump: " + (DateTime.Now - start));
 

        start = System.DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec = (Record) intIndex.Get(new Key(key));
            Record removed = (Record)intIndex.Remove(new Key(key));
            Debug.Assert(removed == rec);
            strIndex.Remove(new Key(System.Convert.ToString(key)), rec);
            rec.Deallocate();
        }
        System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
        db.Close();
    }
}