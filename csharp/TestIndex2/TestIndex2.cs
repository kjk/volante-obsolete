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
    public SortedCollection strIndex;
    public SortedCollection intIndex;
}

public class IntRecordComparator:PersistentComparator 
{
    public override int CompareMembers(IPersistent m1, IPersistent m2) 
    {
        long diff = ((Record)m1).intKey - ((Record)m2).intKey;
        return diff < 0 ? -1 : diff == 0 ? 0 : 1;
    }

    public override int CompareMemberWithKey(IPersistent mbr, object key) 
    {
        long diff = ((Record)mbr).intKey - (long)key;
        return diff < 0 ? -1 : diff == 0 ? 0 : 1;
    }
}

public class StrRecordComparator:PersistentComparator 
{
    public override int CompareMembers(IPersistent m1, IPersistent m2) 
    {
        return ((Record)m1).strKey.CompareTo(((Record)m2).strKey);
    }

    public override int CompareMemberWithKey(IPersistent mbr, object key) 
    {
        return ((Record)mbr).strKey.CompareTo((string)key);
    }
}

public class TestIndex
{
    internal const int nRecords = 100000;
    internal static int pagePoolSize = Storage.INFINITE_PAGE_POOL; // 32 * 1024 * 1024;
	
    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.Instance.CreateStorage();
		
        db.Open("testidx2.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.CreateSortedCollection(new StrRecordComparator(), true);
            root.intIndex = db.CreateSortedCollection(new IntRecordComparator(), true);
            db.Root = root;
        }
        SortedCollection intIndex = root.intIndex;
        SortedCollection strIndex = root.strIndex;
        DateTime start = DateTime.Now;
        long key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            Record rec = new Record();
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = System.Convert.ToString(key);
            intIndex.Add(rec);
            strIndex.Add(rec);
        }
        db.Commit();
        System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
        start = System.DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            Record rec1 = (Record) intIndex[key];
            Record rec2 = (Record) strIndex[Convert.ToString(key)];
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
            Record rec = (Record) intIndex[key];
            intIndex.Remove(rec);
            strIndex.Remove(rec);
            rec.Deallocate();
        }
        System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
        db.Close();
    }
}