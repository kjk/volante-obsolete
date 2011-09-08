#if WITH_XML
namespace Volante
{
    using System;

    public class TestXmlResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan ExportTime;
        public TimeSpan ImportTime;
        public TimeSpan IndexSearchTime;
    }

    public class TestXml : ITest
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
            internal IIndex<string, Record> strIndex;
            internal IFieldIndex<long, Record> intIndex;
            internal IMultiFieldIndex<Record> compoundIndex;
            internal Point point;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestXmlResult();
            config.Result = res;

            DateTime start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            string xmlName = config.DatabaseName + ".xml";
            string dbNameImported = config.DatabaseName + ".imported.dbs";
            Tests.TryDeleteFile(xmlName);
            Tests.TryDeleteFile(dbNameImported);

            Root root = (Root)db.Root;
            Tests.Assert(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(IndexType.Unique);
            root.intIndex = db.CreateFieldIndex<long, Record>("intKey", IndexType.Unique);
            root.compoundIndex = db.CreateFieldIndex<Record>(new string[] { "strKey", "intKey" }, IndexType.Unique);
            root.point.x = 1;
            root.point.y = 2;
            db.Root = root;

            IIndex<string, Record> strIndex = root.strIndex;
            IFieldIndex<long, Record> intIndex = root.intIndex;
            IMultiFieldIndex<Record> compoundIndex = root.compoundIndex;

            long key = 1999;
            int i;
            for (i = 0; i < count; i++)
            {
                Record rec = new Record();
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                rec.realKey = (double)key;
                strIndex.Put(new Key(rec.strKey), rec);
                intIndex.Put(rec);
                compoundIndex.Put(rec);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            System.IO.StreamWriter writer = new System.IO.StreamWriter(xmlName);
            db.ExportXML(writer);
            writer.Close();
            db.Close();
            res.ExportTime = DateTime.Now - start;

            start = DateTime.Now;
            db.Open(dbNameImported);
            System.IO.StreamReader reader = new System.IO.StreamReader(xmlName);
            db.ImportXML(reader);
            reader.Close();
            res.ImportTime = DateTime.Now - start;

            start = DateTime.Now;

            root = (Root)db.Root;
            strIndex = root.strIndex;
            intIndex = root.intIndex;
            compoundIndex = root.compoundIndex;
            Tests.Assert(root.point.x == 1 && root.point.y == 2);

            key = 1999;
            for (i = 0; i < count; i++)
            {
                String strKey = System.Convert.ToString(key);
                Record rec1 = strIndex[strKey];
                Record rec2 = intIndex[key];
                Record rec3 = compoundIndex.Get(new Key(strKey, key));
                Tests.Assert(rec1 != null);
                Tests.Assert(rec1 == rec2);
                Tests.Assert(rec1 == rec3);
                Tests.Assert(rec1.intKey == key);
                Tests.Assert(rec1.realKey == (double)key);
                Tests.Assert(strKey.Equals(rec1.strKey));
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            res.IndexSearchTime = DateTime.Now - start;
            db.Close();
        }
    }
}
#endif
