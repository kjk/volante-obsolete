namespace Volante
{
    using System;
    using System.Collections;

    public class TestIndexResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan IndexSearchTime;
        public TimeSpan IterationTime;
        public TimeSpan RemoveTime;
        public ICollection MemoryUsage; // values are of MemoryUsage type
    }

    public class TestIndex
    {
        public class Record : Persistent
        {
            public string strKey;
            public long intKey;
        }

        public class Root : Persistent
        {
            public IIndex<string, Record> strIndex;
            public IIndex<long, Record> intIndex;
        }

        public void Run(TestConfig config)
        {
            int i;
            int count = config.Count;
            var res = new TestIndexResult();
            config.Result = res;
            IStorage db = config.GetDatabase();
            if (config.Serializable)
                db.BeginThreadTransaction(TransactionMode.Serializable);

            Root root = (Root)db.Root;
            Tests.Assert(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            root.intIndex = db.CreateIndex<long, Record>(true);
            db.Root = root;
            var strIndex = root.strIndex;
            Tests.Assert(typeof(string) == strIndex.KeyType);
            var intIndex = root.intIndex;
            Tests.Assert(typeof(long) == intIndex.KeyType);

            DateTime start = DateTime.Now;
            long key = 1999;
            for (i = 0; i < count; i++)
            {
                Record rec = new Record();
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                intIndex[rec.intKey] = rec;
                strIndex[rec.strKey] = rec;
                if (i % 100 == 0)
                    db.Commit();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }

            if (config.Serializable)
            {
                db.EndThreadTransaction();
                db.BeginThreadTransaction(TransactionMode.Serializable);
            }
            else
            {
                db.Commit();
            }

            res.InsertTime = DateTime.Now - start;
            start = System.DateTime.Now;

            key = 1999;
            for (i = 0; i < count; i++)
            {
                Record rec1 = intIndex[key];
                Record rec2 = strIndex[Convert.ToString(key)];
                Tests.Assert(rec1 != null && rec1 == rec2);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            res.IndexSearchTime = DateTime.Now - start;
            start = System.DateTime.Now;

            key = Int64.MinValue;
            i = 0;
            foreach (Record rec in intIndex)
            {
                Tests.Assert(rec.intKey >= key);
                key = rec.intKey;
                i += 1;
            }
            Tests.Assert(i == count);

            String strKey = "";
            i = 0;
            foreach (Record rec in strIndex)
            {
                Tests.Assert(rec.strKey.CompareTo(strKey) >= 0);
                strKey = rec.strKey;
                i += 1;
            }
            Tests.Assert(i == count);
            res.IterationTime = DateTime.Now - start;
            start = System.DateTime.Now;

            IDictionaryEnumerator de = intIndex.GetDictionaryEnumerator();
            i = VerifyDictionaryEnumerator(de, IterationOrder.AscentOrder);
            Tests.Assert(i == count);

            long mid = 0;
            long max = long.MaxValue;
            de = intIndex.GetDictionaryEnumerator(new Key(mid), new Key(max), IterationOrder.DescentOrder);
            VerifyDictionaryEnumerator(de, IterationOrder.DescentOrder);

            key = 1999;
            for (i = 0; i < count; i++)
            {
                Record rec = intIndex.Get(key);
                Record removed = intIndex.RemoveKey(key);
                Tests.Assert(removed == rec);
                strIndex.Remove(new Key(System.Convert.ToString(key)), rec);
                rec.Deallocate();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            res.RemoveTime = DateTime.Now - start;
            db.Close();
        }

        static int VerifyDictionaryEnumerator(IDictionaryEnumerator de, IterationOrder order)
        {
            long prev = long.MinValue;
            if (order == IterationOrder.DescentOrder)
                prev = long.MaxValue;
            int i = 0;
            while (de.MoveNext())
            {
                DictionaryEntry e1 = (DictionaryEntry)de.Current;
                DictionaryEntry e2 = de.Entry;
                Tests.Assert(e1.Equals(e2));
                long k = (long)e1.Key;
                long k2 = (long)de.Key;
                Tests.Assert(k == k2);
                Record v1 = (Record)e1.Value;
                Record v2 = (Record)de.Value;
                Tests.Assert(v1.Equals(v2));
                Tests.Assert(v1.intKey == k);
                if (order == IterationOrder.AscentOrder)
                    Tests.Assert(k >= prev);
                else
                    Tests.Assert(k <= prev);
                prev = k;
                i++;
            }
            Tests.VerifyDictionaryEnumeratorDone(de);
            return i;
        }
    }
}
