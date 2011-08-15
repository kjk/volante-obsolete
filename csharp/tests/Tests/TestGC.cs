namespace Volante
{
    using System;

    public class TestGCResult : TestResult
    {
    }

    public class TestGC
    {
        class PObject : Persistent
        {
            internal long intKey;
            internal PObject next;
            internal String strKey;
        }

        class StorageRoot : Persistent
        {
            internal PObject list;
            internal Index<string, PObject> strIndex;
            internal Index<long, PObject> intIndex;
        }

        const int nObjectsInTree = 10000;

        static public TestGCResult Run(int nIterations, bool altBtree, bool backgroundGc)
        {
            var res = new TestGCResult()
            {
                Count = nIterations,
                TestName = String.Format("TestGC(altBtree={0}, bakcgroundGC={1}", altBtree, backgroundGc)
            };

            string dbName = "testgc.dbs";
            Tests.SafeDeleteFile(dbName);
            var tStart = DateTime.Now;

            IStorage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.BackgroundGc = backgroundGc;
            db.Open(dbName);
            db.GcThreshold = 1000000;
            StorageRoot root = new StorageRoot();
            Index<string, PObject> strIndex = root.strIndex = db.CreateIndex<string, PObject>(true);
            Index<long, PObject> intIndex = root.intIndex = db.CreateIndex<long, PObject>(true);
            db.Root = root;
            long insKey = 1999;
            long remKey = 1999;

            for (int i = 0; i < nIterations; i++)
            {
                if (i > nObjectsInTree)
                {
                    remKey = (3141592621L * remKey + 2718281829L) % 1000000007L;
                    intIndex.Remove(new Key(remKey));
                    strIndex.Remove(new Key(remKey.ToString()));
                }
                PObject obj = new PObject();
                insKey = (3141592621L * insKey + 2718281829L) % 1000000007L;
                obj.intKey = insKey;
                obj.strKey = insKey.ToString();
                obj.next = new PObject();
                intIndex[obj.intKey] = obj;
                strIndex[obj.strKey] = obj;
                if (i > 0)
                {
                    Tests.Assert(root.list.intKey == i - 1);
                }
                root.list = new PObject();
                root.list.intKey = i;
                root.Store();
                if (i % 1000 == 0)
                {
                    db.Commit();
                }
            }
            db.Close();
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }

}
