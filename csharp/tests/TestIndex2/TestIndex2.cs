using System;
using System.Collections;
using NachoDB;
using System.Diagnostics;

public class Record : Persistent
{
    public string strKey;
    public long intKey;
}

public class Root : Persistent
{
#if USE_GENERICS
    public SortedCollection<string,Record> strIndex;
    public SortedCollection<long,Record>   intIndex;
#else
    public SortedCollection strIndex;
    public SortedCollection intIndex;
#endif
}

#if USE_GENERICS
public class IntRecordComparator : PersistentComparator<long,Record> 
{
    public override int CompareMembers(Record m1, Record m2) 
    {
        long diff = m1.intKey - m2.intKey;
        return diff < 0 ? -1 : diff == 0 ? 0 : 1;
    }

    public override int CompareMemberWithKey(Record mbr, long key) 
    {
        long diff = mbr.intKey - key;
        return diff < 0 ? -1 : diff == 0 ? 0 : 1;
    }
}
#else
public class IntRecordComparator : PersistentComparator 
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
#endif

#if USE_GENERICS
public class StrRecordComparator : PersistentComparator<string,Record> 
{
    public override int CompareMembers(Record m1, Record m2) 
    {
        return m1.strKey.CompareTo(m2.strKey);
    }

    public override int CompareMemberWithKey(Record mbr, string key) 
    {
        return mbr.strKey.CompareTo(key);
    }
}
#else
public class StrRecordComparator : PersistentComparator 
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
#endif

public class TestIndex
{
    internal const int nRecords = 100000;
    internal static int pagePoolSize = 0; // infine page pool

    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.CreateStorage();

        db.Open("testidx2.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
#if USE_GENERICS
            root.strIndex = db.CreateSortedCollection<string,Record>(new StrRecordComparator(), true);
            root.intIndex = db.CreateSortedCollection<long,Record>(new IntRecordComparator(), true);
#else
            root.strIndex = db.CreateSortedCollection(new StrRecordComparator(), true);
            root.intIndex = db.CreateSortedCollection(new IntRecordComparator(), true);
#endif
            db.Root = root;
        }
#if USE_GENERICS
        SortedCollection<long,Record> intIndex = root.intIndex;
        SortedCollection<string,Record> strIndex = root.strIndex;
#else
        SortedCollection intIndex = root.intIndex;
        SortedCollection strIndex = root.strIndex;
#endif
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
#if USE_GENERICS
            Record rec1 = intIndex[key];
            Record rec2 = strIndex[Convert.ToString(key)];
#else
            Record rec1 = (Record) intIndex[key];
            Record rec2 = (Record) strIndex[Convert.ToString(key)];
#endif
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
#if USE_GENERICS
            Record rec = intIndex[key];
#else
            Record rec = (Record) intIndex[key];
#endif
            intIndex.Remove(rec);
            strIndex.Remove(rec);
            rec.Deallocate();
        }
        System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
        db.Close();
    }
}