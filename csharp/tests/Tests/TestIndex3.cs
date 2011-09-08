namespace Volante
{
    using System;
    using System.Collections;

    public class TestIndex3 : ITest
    {
        public class Record : Persistent
        {
            public string strKey;
            public long intKey;
        }

        public class Root : Persistent
        {
            public IIndex<string, Record> strIndex;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;
            IDatabase db = config.GetDatabase();
            Root root = (Root)db.Root;
            Tests.Assert(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(IndexType.Unique);
            db.Root = root;
            string[] strs = new string[] { "one", "two", "three", "four" };
            int no = 0;
            for (var i = 0; i < count; i++)
            {
                foreach (string s in strs)
                {
                    var s2 = String.Format("{0}-{1}", s, i);
                    Record o = new Record();
                    o.strKey = s2;
                    o.intKey = no++;
                    root.strIndex[s2] = o;
                }
            }
            db.Commit();

            // Test that modyfing an index while traversing it throws an exception
            // Tests Btree.BtreeEnumerator
            long n = -1;
            Tests.AssertException<InvalidOperationException>(
                () => {
                    foreach (Record r in root.strIndex)
                    {
                        n = r.intKey;
                        var i = n % strs.Length;
                        var j = n / strs.Length;
                        var sBase = strs[i];
                        var expectedStr = String.Format("{0}-{1}", sBase, j);
                        string s = r.strKey;
                        Tests.Assert(s == expectedStr);

                        if (n == 0)
                        {
                            Record o = new Record();
                            o.strKey = "five";
                            o.intKey = 5;
                            root.strIndex[o.strKey] = o;
                        }
                    }
                });
            Tests.Assert(n == 0);

            // Test that modyfing an index while traversing it throws an exception
            // Tests Btree.BtreeSelectionIterator

            Key keyStart = new Key("four", true);
            Key keyEnd = new Key("three", true);
            Tests.AssertException<InvalidOperationException>(() =>
                {
                    foreach (Record r in root.strIndex.Range(keyStart, keyEnd, IterationOrder.AscentOrder))
                    {
                        n = r.intKey;
                        var i = n % strs.Length;
                        var j = n / strs.Length;
                        var sBase = strs[i];
                        var expectedStr = String.Format("{0}-{1}", sBase, j);
                        string s = r.strKey;
                        Tests.Assert(s == expectedStr);

                        Record o = new Record();
                        o.strKey = "six";
                        o.intKey = 6;
                        root.strIndex[o.strKey] = o;
                    }
                });
            db.Close();
        }
    }
}
