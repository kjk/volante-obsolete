// Copyright: Krzysztof Kowalczyk
// License: BSD

namespace Volante
{
    using System;

    public class TestEncoding
    {
        public class Record : Persistent
        {
            public string valStr;
            public long val;
        }

        public class Root : Persistent
        {
            public IIndex<long, Record> longIndex;
        }

        public static string GenStrFromKey(long val)
        {
            string s = val.ToString();
            long count = (val % 4) + 1;
            if (count == 2)
                return s + s;
            else if (count == 3)
                return s + s + s;
            else if (count == 4)
                return s + s + s + s;
            return s;
        }

        public void Run(TestConfig config)
        {
            int i;
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;
            var start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            Root root = new Root();
            root.longIndex = db.CreateIndex<long, Record>(IndexType.NonUnique);
            db.Root = root;
            long key = 1999;
            for (i = 0; i < count; i++)
            {
                Record rec = new Record() { val = key };
                rec.valStr = GenStrFromKey(key);
                root.longIndex.Put(key, rec);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            db.Commit();
            db.Close();

            db = config.GetDatabase(false);
            root = (Root)db.Root;
            foreach (Record rec in root.longIndex)
            {
                long v = rec.val;
                var str = GenStrFromKey(v);
                Tests.Assert(rec.valStr == str);
            }

            var memUsage = db.GetMemoryDump().Values;
            db.Close();
            //Console.WriteLine(config.DatabaseName);
            //Tests.DumpMemoryUsage(memUsage);
        }
    }
}