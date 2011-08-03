namespace Volante
{
    using System;
    using System.Diagnostics;
    using System.Collections;

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

        public static void Run(int nListMembers)
        {
            int nHashMembers = nListMembers * 10;

            Storage db = StorageFactory.CreateStorage();
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
                Console.WriteLine("Initialization of database completed");
            }
            L1List elem = root.list;
            for (int i = nListMembers; --i >= 0; )
            {
                Debug.Assert(elem.obj.Equals(i));
                elem = elem.next;
            }
            for (int i = nHashMembers; --i >= 0; )
            {
                Debug.Assert(root.map["key-" + i].Equals("value-" + i));
            }
            Console.WriteLine("Database is OK");
            db.Close();
            // shutup the compiler about TestRaw.nil not being used
            Tests.AssertThat(root.nil == null);
            root.nil = 3;
            Tests.AssertThat(root.nil != null);
        }
    }


}
