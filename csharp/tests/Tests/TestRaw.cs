namespace Volante
{
    using System;
    using System.Collections;

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

    public class TestRaw : Persistent
    {
        L1List list;
        Hashtable map;
        Object nil;

        static public string dbName = "testraw.dbs";

        public static TestRawResult Run(int nListMembers)
        {
            var res = new TestRawResult()
            {
                Count = nListMembers,
                TestName = "TestRaw"
            };

            int nHashMembers = nListMembers * 10;

            var tStart = DateTime.Now;
            var start = DateTime.Now;

            IStorage db = StorageFactory.CreateStorage();
            db.SerializeTransientObjects = true;
            db.Open(dbName);
            TestRaw root = (TestRaw)db.Root;
            if (root == null)
            {
                root = new TestRaw();
                L1List list = null;
                for (int i = 0; i < nListMembers; i++)
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
            }

            start = DateTime.Now;
            L1List elem = root.list;
            for (int i = nListMembers; --i >= 0; )
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
            // shutup the compiler about TestRaw.nil not being used
            Tests.Assert(root.nil == null);
            root.nil = 3;
            Tests.Assert(root.nil != null);

            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }


}
