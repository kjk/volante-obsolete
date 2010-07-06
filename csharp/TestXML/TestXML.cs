using System;
using Perst;

public class TestIndex
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
        Storage db = StorageFactory.Instance.createStorage();
		
        db.open("test1.dbs", pagePoolSize);
        Root root = (Root) db.Root;
        if (root == null)
        {
            root = new Root();
            root.strIndex = db.createIndex(typeof(System.String), true);
            root.intIndex = db.createFieldIndex(typeof(Record), "intKey", true);
            root.compoundIndex = db.createFieldIndex(typeof(Record), new String[]{"strKey", "intKey"}, true);
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
            intIndex.put(rec);
            strIndex.put(new Key(rec.strKey), rec);
            compoundIndex.put(rec);                
        }
        db.commit();
        System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

        start = DateTime.Now;
        System.IO.StreamWriter writer = new System.IO.StreamWriter("test.xml");
        db.exportXML(writer);
        writer.Close();
        System.Console.WriteLine("Elapsed time for for XML export: " + (DateTime.Now - start));
        
        db.close();
        db.open("test2.dbs", pagePoolSize);

        start = DateTime.Now;
        System.IO.StreamReader reader = new System.IO.StreamReader("test.xml");
        db.importXML(reader);
        reader.Close();
        System.Console.WriteLine("Elapsed time for for XML import: " + (DateTime.Now - start));

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
            Record rec1 = (Record) intIndex.get(new Key(key));
            Record rec2 = (Record) strIndex.get(new Key(strKey));
            Record rec3 = (Record)compoundIndex.get(new Key(strKey, key));
            Assert.that(rec1 != null);
            Assert.that(rec1 == rec2);
            Assert.that(rec1 == rec3);
            Assert.that(rec1.intKey == key);
            Assert.that(rec1.realKey == (double)key);
            Assert.that(strKey.Equals(rec1.strKey));
        }
        System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
        db.close();
    }
}