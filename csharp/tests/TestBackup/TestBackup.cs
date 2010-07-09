using System;
using System.IO;
using NachoDB;
using System.Diagnostics;

public class TestBackup
{
    class Record : Persistent
    {
        internal String strKey;
        internal long   intKey;
        internal double realKey;
    }

    class Root : Persistent
    {
#if USE_GENERICS
        internal Index<string,Record> strIndex;
        internal FieldIndex<long,Record> intIndex;
        internal MultiFieldIndex<Record> compoundIndex;
#else
        internal Index strIndex;
        internal FieldIndex intIndex;
        internal FieldIndex compoundIndex;
#endif
    }

    internal const int nRecords = 100000;
    internal static int pagePoolSize = 32 * 1024 * 1024;

    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.CreateStorage();

        db.Open("testbck1.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
#if USE_GENERICS
            root.strIndex = db.CreateIndex<string,Record>(true);
            root.intIndex = db.CreateFieldIndex<long,Record>("intKey", true);
            root.compoundIndex = db.CreateFieldIndex<Record>(new String[]{"strKey", "intKey"}, true);
#else
            root.strIndex = db.CreateIndex(typeof(string), true);
            root.intIndex = db.CreateFieldIndex(typeof(Record), "intKey", true);
            root.compoundIndex = db.CreateFieldIndex(typeof(Record), new String[]{"strKey", "intKey"}, true);
#endif
            db.Root = root;
        }
#if USE_GENERICS
        FieldIndex<long,Record> intIndex = root.intIndex;
        MultiFieldIndex<Record> compoundIndex = root.compoundIndex;
        Index<string,Record> strIndex = root.strIndex;
#else
        FieldIndex intIndex = root.intIndex;
        FieldIndex compoundIndex = root.compoundIndex;
        Index strIndex = root.strIndex;
#endif        
        DateTime start = DateTime.Now;
        long key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            Record rec = new Record();
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = System.Convert.ToString(key);
            rec.realKey = (double)key;
            intIndex.Put(rec);
            strIndex.Put(new Key(rec.strKey), rec);
            compoundIndex.Put(rec);                
        }
        db.Commit();
        System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

        start = DateTime.Now;
        System.IO.FileStream stream = new System.IO.FileStream("testbck2.dbs", FileMode.Create, FileAccess.Write);
        db.Backup(stream);
        stream.Close();
        System.Console.WriteLine("Elapsed time for backup completion: " + (DateTime.Now - start));
        
        db.Close();
        db.Open("testbck2.dbs", pagePoolSize);

        root = (Root)db.Root;
        intIndex = root.intIndex;
        strIndex = root.strIndex;
        compoundIndex = root.compoundIndex;

        start = DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            String strKey = System.Convert.ToString(key);
#if USE_GENERICS
            Record rec1 = intIndex.Get(key);
            Record rec2 = strIndex.Get(strKey);
            Record rec3 = compoundIndex.Get(new Key(strKey, key));
#else
            Record rec1 = (Record)intIndex.Get(new Key(key));
            Record rec2 = (Record)strIndex.Get(new Key(strKey));
            Record rec3 = (Record)compoundIndex.Get(new Key(strKey, key));
#endif
            Debug.Assert(rec1 != null);
            Debug.Assert(rec1 == rec2);
            Debug.Assert(rec1 == rec3);
            Debug.Assert(rec1.intKey == key);
            Debug.Assert(rec1.realKey == (double)key);
            Debug.Assert(strKey.Equals(rec1.strKey));
        }
        System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
        db.Close();
    }
}