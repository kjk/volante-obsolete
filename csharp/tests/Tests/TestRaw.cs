namespace Volante
{
    using System;
    using System.Collections;
    using System.Diagnostics;

    public class TestRawResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan TraverseTime;
    }

    [Serializable()]
    class L1List
    {
        internal L1List next;
        internal Object obj;

        internal L1List(Object val, L1List list)
        {
            obj = val;
            next = list;
        }
    }

    public class TestRaw : Persistent, ITest
    {
        L1List list;
        Hashtable map;
        Object nil;

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestRawResult();
            config.Result = res;

            int nHashMembers = count * 10;
            var start = DateTime.Now;

            IDatabase db = config.GetDatabase();
            TestRaw root = (TestRaw)db.Root;
            if (count % 2 != 0)
            {
                // Silence compiler about unused nil variable.
                // This shouldn't happen since we never pass count
                // that is an odd number
                Debug.Assert(false);
                root.nil = new object();
            }

            root = new TestRaw();
            Tests.Assert(root.nil == null);
            L1List list = null;
            for (int i = 0; i < count; i++)
            {
                list = new L1List(i, list);
            }
            root.list = list;
            root.map = new Hashtable();
            for (int i = 0; i < nHashMembers; i++)
            {
                root.map["key-" + i] = "value-" + i;
            }
            db.Root = root;
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            L1List elem = root.list;
            for (int i = count; --i >= 0; )
            {
                Tests.Assert(elem.obj.Equals(i));
                elem = elem.next;
            }
            for (int i = nHashMembers; --i >= 0; )
            {
                Tests.Assert(root.map["key-" + i].Equals("value-" + i));
            }
            res.TraverseTime = DateTime.Now - start;
            db.Close();
        }
    }
}
