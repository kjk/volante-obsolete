namespace Volante
{
    using System;
    using System.Collections;

    public class TestIndex4
    {
        public class StringInt : Persistent
        {
            public string s;
            public int no;
            public StringInt()
            {
            }
            public StringInt(string s, int no)
            {
                this.s = s;
                this.no = no;
            }
        }

        public static void CheckStrings(Index<string, StringInt> root, string[] strs, int count)
        {
            int no = 1;
            for (var i = 0; i < count; i++)
            {
                foreach (string s in strs)
                {
                    var s2 = String.Format("{0}-{1}", s, i);
                    StringInt o = root[s2];
                    Tests.Assert(o.no == no++);
                }
            }
        }

        public static TestResult Run(int count, bool useAltBtree)
        {
            var res = new TestResult()
            {
                Count = count,
                TestName = String.Format("TestIndex4 altBtree={0}", useAltBtree)
            };

            string dbName = @"testidx4.dbs";
            Tests.SafeDeleteFile(dbName);

            var tStart = DateTime.Now;
            IStorage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName);
            Index<string, StringInt> root = (Index<string, StringInt>)db.Root;
            Tests.Assert(null == root);
            root = db.CreateIndex<string, StringInt>(true);
            db.Root = root;

            string[] strs = new string[] { "one", "two", "three", "four" };
            int no = 1;
            for (var i = 0; i < count; i++)
            {
                foreach (string s in strs)
                {
                    var s2 = String.Format("{0}-{1}", s, i);
                    var o = new StringInt(s, no++);
                    root[s2] = o;
                }
            }

            CheckStrings(root, strs, count);
            db.Close();

            db = StorageFactory.CreateStorage();
            db.Open(dbName);
            root = (Index<string, StringInt>)db.Root;
            Tests.Assert(null != root);
            CheckStrings(root, strs, count);
            db.Close();

            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}
