namespace Volante
{
#if !OMIT_XML
    using System;
    using System.Diagnostics;

    public class TestXml
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
            internal double realKey;
        }

        struct Point
        {
            public int x;
            public int y;
        }

        class Root : Persistent
        {
            internal Index<string, Record> strIndex;
            internal FieldIndex<long, Record> intIndex;
            internal MultiFieldIndex<Record> compoundIndex;
            internal Point point;
        }

        internal static int pagePoolSize = 32 * 1024 * 1024;

        public static void Run(int nRecords, bool useAltBtree)
        {
            string dbName1 = @"testxml1.dbs";
            string dbName2 = @"testxml2.dbs";
            Tests.SafeDeleteFile(dbName1);
            Tests.SafeDeleteFile(dbName2);

            string xmlName = useAltBtree ? @"testalt.xml" : @"test.xml";
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName1, pagePoolSize);

            Root root = (Root)db.Root;
            Tests.AssertThat(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            root.intIndex = db.CreateFieldIndex<long, Record>("intKey", true);
            root.compoundIndex = db.CreateFieldIndex<Record>(new string[] { "strKey", "intKey" }, true);
            root.point.x = 1;
            root.point.y = 2;
            db.Root = root;

            //DateTime start = DateTime.Now;
            Index<string, Record> strIndex = root.strIndex;
            FieldIndex<long, Record> intIndex = root.intIndex;
            MultiFieldIndex<Record> compoundIndex = root.compoundIndex;

            long key = 1999;
            int i;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                rec.realKey = (double)key;
                strIndex.Put(new Key(rec.strKey), rec);
                intIndex.Put(rec);
                compoundIndex.Put(rec);
            }
            db.Commit();
            //System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

            // start = DateTime.Now
            System.IO.StreamWriter writer = new System.IO.StreamWriter(xmlName);
            db.ExportXML(writer);
            writer.Close();
            db.Close();
            //System.Console.WriteLine("Elapsed time for XML export: " + (DateTime.Now - start));

            //start = DateTime.Now;
            db.Open(dbName2, pagePoolSize);
            System.IO.StreamReader reader = new System.IO.StreamReader(xmlName);
            db.ImportXML(reader);
            reader.Close();
            //System.Console.WriteLine("Elapsed time for XML import: " + (DateTime.Now - start));

            root = (Root)db.Root;
            strIndex = root.strIndex;
            intIndex = root.intIndex;
            compoundIndex = root.compoundIndex;
            Tests.AssertThat(root.point.x == 1 && root.point.y == 2);

            //start = DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                String strKey = System.Convert.ToString(key);
                Record rec1 = strIndex[strKey];
                Record rec2 = intIndex[key];
                Record rec3 = compoundIndex.Get(new Key(strKey, key));
                Tests.AssertThat(rec1 != null);
                Tests.AssertThat(rec1 == rec2);
                Tests.AssertThat(rec1 == rec3);
                Tests.AssertThat(rec1.intKey == key);
                Tests.AssertThat(rec1.realKey == (double)key);
                Tests.AssertThat(strKey.Equals(rec1.strKey));
            }
            db.Close();
            //System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
        }
    }
#endif

}
