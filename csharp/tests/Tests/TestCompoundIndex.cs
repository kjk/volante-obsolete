namespace Volante
{
    using System;

    public class TestCompoundResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan IndexSearchTime;
        public TimeSpan IterationTime;
        public TimeSpan RemoveTime;
    }

    public class TestCompoundIndex
    {
        const int pagePoolSize = 32 * 1024 * 1024;

        class Record : Persistent
        {
            internal String strKey;
            internal int intKey;
        }

        public static TestCompoundResult Run(int nRecords, bool altBtree)
        {
            int i;
            string dbName = "testcidx.dbs";
            var res = new TestCompoundResult()
            {
                Count = nRecords,
                TestName = String.Format("TestCompoundResult(altBtree={0})", altBtree)
            };
            Tests.SafeDeleteFile(dbName);

            DateTime tStart = DateTime.Now;
            DateTime start = DateTime.Now;

            IStorage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.Open(dbName, pagePoolSize);
            IMultiFieldIndex<Record> root = (IMultiFieldIndex<Record>)db.Root;
            if (root == null)
            {
                root = db.CreateFieldIndex<Record>(new string[] { "intKey", "strKey" }, true);
                db.Root = root;
            }
            long key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = (int)((ulong)key >> 32);
                rec.strKey = Convert.ToString((int)key);
                root.Put(rec);
            }
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            key = 1999;
            int minKey = Int32.MaxValue;
            int maxKey = Int32.MinValue;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int intKey = (int)((ulong)key >> 32);
                String strKey = Convert.ToString((int)key);
                Record rec = root.Get(new Key(new Object[] { intKey, strKey }));
                Tests.Assert(rec != null && rec.intKey == intKey && rec.strKey.Equals(strKey));
                if (intKey < minKey)
                {
                    minKey = intKey;
                }
                if (intKey > maxKey)
                {
                    maxKey = intKey;
                }
            }
            res.IndexSearchTime = DateTime.Now - start;

            start = DateTime.Now;
            int n = 0;
            string prevStr = "";
            int prevInt = minKey;
            foreach (Record rec in root.Range(new Key(minKey, ""),
                                              new Key(maxKey + 1, "???"),
                                              IterationOrder.AscentOrder))
            {
                Tests.Assert(rec.intKey > prevInt || rec.intKey == prevInt && rec.strKey.CompareTo(prevStr) > 0);
                prevStr = rec.strKey;
                prevInt = rec.intKey;
                n += 1;
            }
            Tests.Assert(n == nRecords);

            n = 0;
            prevInt = maxKey + 1;
            foreach (Record rec in root.Range(new Key(minKey, "", false),
                                              new Key(maxKey + 1, "???", false),
                                              IterationOrder.DescentOrder))
            {
                Tests.Assert(rec.intKey < prevInt || rec.intKey == prevInt && rec.strKey.CompareTo(prevStr) < 0);
                prevStr = rec.strKey;
                prevInt = rec.intKey;
                n += 1;
            }
            Tests.Assert(n == nRecords);
            res.IterationTime = DateTime.Now - start;
            start = DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int intKey = (int)((ulong)key >> 32);
                String strKey = Convert.ToString((int)key);
                Record rec = root.Get(new Key(new Object[] { intKey, strKey }));
                Tests.Assert(rec != null && rec.intKey == intKey && rec.strKey.Equals(strKey));
                Tests.Assert(root.Contains(rec));
                root.Remove(rec);
                rec.Deallocate();
            }
            Tests.Assert(!root.GetEnumerator().MoveNext());
            Tests.Assert(!root.Reverse().GetEnumerator().MoveNext());
            res.RemoveTime = DateTime.Now - start;
            db.Close();

            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }

}
