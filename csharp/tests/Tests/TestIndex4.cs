namespace Volante
{
    using System;
    using System.Collections;

    public class TestIndex4 : ITest
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

        public static void CheckStrings(IIndex<string, StringInt> root, string[] strs, int count)
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

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;
            IDatabase db = config.GetDatabase();
            IIndex<string, StringInt> root = (IIndex<string, StringInt>)db.Root;
            Tests.Assert(null == root);
            root = db.CreateIndex<string, StringInt>(IndexType.Unique);
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

            db = config.GetDatabase(false);
            root = (IIndex<string, StringInt>)db.Root;
            Tests.Assert(null != root);
            CheckStrings(root, strs, count);
            db.Close();
        }
    }
}
