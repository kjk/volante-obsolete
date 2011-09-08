namespace Volante
{
    using System;
    using System.Collections.Generic;

    public class TestGcResult : TestResult
    {
    }

    public class TestGc : ITest
    {
        class PObject : Persistent
        {
            internal long intKey;
            internal PObject next;
            internal String strKey;
        }

        class Root : Persistent
        {
            internal PObject list;
            internal IIndex<string, PObject> strIndex;
            internal IIndex<long, PObject> intIndex;
        }

        const int nObjectsInTree = 10000;

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestGcResult();
            config.Result = res;
            IDatabase db = config.GetDatabase();
            Root root = new Root();
            IIndex<string, PObject> strIndex = root.strIndex = db.CreateIndex<string, PObject>(IndexType.Unique);
            IIndex<long, PObject> intIndex = root.intIndex = db.CreateIndex<long, PObject>(IndexType.Unique);
            db.Root = root;
            long insKey = 1999;
            long remKey = 1999;

            for (int i = 0; i < count; i++)
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
                    db.Commit();
            }
            db.Close();
        }
    }
}
