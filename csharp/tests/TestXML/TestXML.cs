using System;
using NachoDB;
using System.Diagnostics;

public class TestXML
{
    class Record : Persistent
    {
        internal String strKey;
        internal long   intKey;
        internal double realKey;
    }

    struct Point {
        public int x;
        public int y;
    }

    class Root : Persistent
    {
#if USE_GENERICS
        internal Index<string,Record>    strIndex;
        internal FieldIndex<long,Record> intIndex;
        internal MultiFieldIndex<Record> compoundIndex;
#else
        internal Index strIndex;
        internal FieldIndex intIndex;
        internal FieldIndex compoundIndex;
#endif
        internal Point      point;
    }

    internal const int nRecords = 100000;
    internal static int pagePoolSize = 32 * 1024 * 1024;

    static public void  Main(System.String[] args)
    {
        int i;
        Storage db = StorageFactory.CreateStorage();

        db.Open("test1.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
#if USE_GENERICS
            root.strIndex = db.CreateIndex<string,Record>(true);
            root.intIndex = db.CreateFieldIndex<long,Record>("intKey", true);
            root.compoundIndex = db.CreateFieldIndex<Record>(new string[]{"strKey", "intKey"}, true);
#else
            root.strIndex = db.CreateIndex(typeof(System.String), true);
            root.intIndex = db.CreateFieldIndex(typeof(Record), "intKey", true);
            root.compoundIndex = db.CreateFieldIndex(typeof(Record), new String[]{"strKey", "intKey"}, true);
#endif
            root.point.x = 1;
            root.point.y = 2;
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
        System.IO.StreamWriter writer = new System.IO.StreamWriter("test.xml");
        db.ExportXML(writer);
        writer.Close();
        System.Console.WriteLine("Elapsed time for XML export: " + (DateTime.Now - start));
        
        db.Close();
        db.Open("test2.dbs", pagePoolSize);

        start = DateTime.Now;
        System.IO.StreamReader reader = new System.IO.StreamReader("test.xml");
        db.ImportXML(reader);
        reader.Close();
        System.Console.WriteLine("Elapsed time for XML import: " + (DateTime.Now - start));

        root = (Root)db.Root;
        intIndex = root.intIndex;
        strIndex = root.strIndex;
        compoundIndex = root.compoundIndex;
        Debug.Assert(root.point.x == 1 && root.point.y == 2);

        start = DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            String strKey = System.Convert.ToString(key);
#if USE_GENERICS
            Record rec1 = intIndex[key];
            Record rec2 = strIndex[strKey];
            Record rec3 = compoundIndex.Get(new Key(strKey, key));
#else
            Record rec1 = (Record) intIndex.Get(new Key(key));
            Record rec2 = (Record) strIndex.Get(new Key(strKey));
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