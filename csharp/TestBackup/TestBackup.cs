using System;
using System.IO;
using Perst;

public class TestBackup
{
    class Record:Persistent
    {
        internal String strKey;
        internal long   intKey;
        internal double realKey;
    }


    class Root:Persistent
    {
        internal Index strIndex;
        internal FieldIndex intIndex;
        internal FieldIndex compoundIndex;
    }

    internal const int nRecords = 100000;
    internal static int pagePoolSize = 32 * 1024 * 1024;
	
    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.Instance.CreateStorage();
		
        db.Open("testbck1.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.CreateIndex(typeof(System.String), true);
            root.intIndex = db.CreateFieldIndex(typeof(Record), "intKey", true);
            root.compoundIndex = db.CreateFieldIndex(typeof(Record), new String[]{"strKey", "intKey"}, true);
            db.Root = root;
        }
        FieldIndex intIndex = root.intIndex;
        FieldIndex compoundIndex = root.compoundIndex;
        Index strIndex = root.strIndex;
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
            Record rec1 = (Record) intIndex.Get(new Key(key));
            Record rec2 = (Record) strIndex.Get(new Key(strKey));
            Record rec3 = (Record)compoundIndex.Get(new Key(strKey, key));
            Assert.That(rec1 != null);
            Assert.That(rec1 == rec2);
            Assert.That(rec1 == rec3);
            Assert.That(rec1.intKey == key);
            Assert.That(rec1.realKey == (double)key);
            Assert.That(strKey.Equals(rec1.strKey));
        }
        System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
        db.Close();
    }
}