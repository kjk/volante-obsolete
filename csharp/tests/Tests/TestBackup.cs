namespace Volante
{
    using System;
    using System.IO;

    public class TestBackupResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan BackupTime;
    }

    public class TestBackup : ITest
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
            internal double realKey;
        }

        class Root : Persistent
        {
            internal IIndex<string, Record> strIndex;
            internal IFieldIndex<long, Record> intIndex;
            internal IMultiFieldIndex<Record> compoundIndex;
        }

        public void Run(TestConfig config)
        {
            int i;
            int count = config.Count;
            var res = new TestBackupResult();

            DateTime start = DateTime.Now;

            string dbNameBackup = config.DatabaseName + ".backup.dbs";
            IDatabase db = config.GetDatabase();
            Root root = (Root)db.Root;
            Tests.Assert(root == null);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(IndexType.Unique);
            root.intIndex = db.CreateFieldIndex<long, Record>("intKey", IndexType.Unique);
            root.compoundIndex = db.CreateFieldIndex<Record>(new String[] { "strKey", "intKey" }, IndexType.Unique);
            db.Root = root;
            IFieldIndex<long, Record> intIndex = root.intIndex;
            IMultiFieldIndex<Record> compoundIndex = root.compoundIndex;
            IIndex<string, Record> strIndex = root.strIndex;
            long key = 1999;
            for (i = 0; i < count; i++)
            {
                Record rec = new Record();
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                rec.realKey = (double)key;
                intIndex.Put(rec);
                strIndex.Put(new Key(rec.strKey), rec);
                compoundIndex.Put(rec);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            db.Commit();
            Tests.Assert(intIndex.Count == count);
            Tests.Assert(strIndex.Count == count);
            Tests.Assert(compoundIndex.Count == count);
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            System.IO.FileStream stream = new System.IO.FileStream(dbNameBackup, FileMode.Create, FileAccess.Write);
            db.Backup(stream);
            stream.Close();
            db.Close();
            res.BackupTime = DateTime.Now - start;

            start = DateTime.Now;
            db.Open(dbNameBackup);
            root = (Root)db.Root;
            intIndex = root.intIndex;
            strIndex = root.strIndex;
            compoundIndex = root.compoundIndex;

            key = 1999;
            for (i = 0; i < count; i++)
            {
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
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            db.Close();
        }
    }
}
