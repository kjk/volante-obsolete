namespace Volante
{
    using System;
    using System.IO;

    public class TestBackupResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan BackupTime;
    }

    public class TestBackup
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
            internal double realKey;
        }

        class Root : Persistent
        {
            internal Index<string, Record> strIndex;
            internal FieldIndex<long, Record> intIndex;
            internal IMultiFieldIndex<Record> compoundIndex;
        }

        internal static int pagePoolSize = 32 * 1024 * 1024;

        static string DbName1 = "testbck1.dbs";
        static string DbName2 = "testbck2.dbs";

        static public void Init()
        {
            Tests.SafeDeleteFile(DbName1);
            Tests.SafeDeleteFile(DbName2);
        }

        static public TestBackupResult Run(int nRecords)
        {
            int i;

            var res = new TestBackupResult()
            {
                Count = nRecords,
                TestName = "TestBackup"
            };

            DateTime tStart = DateTime.Now;
            DateTime start = DateTime.Now;

            IStorage db = StorageFactory.CreateStorage();
            db.Open(DbName1, pagePoolSize);
            Root root = (Root)db.Root;
            if (root == null)
            {
                root = new Root();
                root.strIndex = db.CreateIndex<string, Record>(true);
                root.intIndex = db.CreateFieldIndex<long, Record>("intKey", true);
                root.compoundIndex = db.CreateFieldIndex<Record>(new String[] { "strKey", "intKey" }, true);
                db.Root = root;
            }
            FieldIndex<long, Record> intIndex = root.intIndex;
            IMultiFieldIndex<Record> compoundIndex = root.compoundIndex;
            Index<string, Record> strIndex = root.strIndex;
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
            Tests.Assert(intIndex.Count == nRecords);
            Tests.Assert(strIndex.Count == nRecords);
            Tests.Assert(compoundIndex.Count == nRecords);
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            System.IO.FileStream stream = new System.IO.FileStream(DbName2, FileMode.Create, FileAccess.Write);
            db.Backup(stream);
            stream.Close();
            db.Close();
            res.BackupTime = DateTime.Now - start;

            start = DateTime.Now;
            db.Open(DbName2, pagePoolSize);
            root = (Root)db.Root;
            intIndex = root.intIndex;
            strIndex = root.strIndex;
            compoundIndex = root.compoundIndex;

            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                String strKey = System.Convert.ToString(key);
                Record rec1 = intIndex.Get(key);
                Record rec2 = strIndex.Get(strKey);
                Record rec3 = compoundIndex.Get(new Key(strKey, key));

                Tests.Assert(rec1 != null);
                Tests.Assert(rec1 == rec2);
                Tests.Assert(rec1 == rec3);
                Tests.Assert(rec1.intKey == key);
                Tests.Assert(rec1.realKey == (double)key);
                Tests.Assert(strKey.Equals(rec1.strKey));
            }
            db.Close();
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}
