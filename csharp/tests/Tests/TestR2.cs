namespace Volante
{
    using System;

    public class TestR2Result : TestResult
    {
    }

    public class TestR2 : Persistent
    {
        class SpatialObject : Persistent
        {
            public RectangleR2 rect;
        }

        ISpatialIndexR2<SpatialObject> index;

        const int nObjectsInTree = 1000;

        public void Run(TestConfig config)
        {
            SpatialObject so;
            RectangleR2 r;
            int count = config.Count;
            bool noFlush = config.AltBtree; // a hack
            var res = new TestR2Result();
            config.Result = res;
            IDatabase db = config.GetDatabase();
            TestR2 root = (TestR2)db.Root;
            Tests.Assert(root == null);
            root = new TestR2();
            root.index = db.CreateSpatialIndexR2<SpatialObject>();
            db.Root = root;

            RectangleR2[] rectangles = new RectangleR2[nObjectsInTree];
            long key = 1999;
            for (int i = 0; i < count; i++)
            {
                int j = i % nObjectsInTree;
                if (i >= nObjectsInTree)
                {
                    r = rectangles[j];
                    SpatialObject[] sos = root.index.Get(r);
                    SpatialObject po = null;
                    int n = 0;
                    for (int k = 0; k < sos.Length; k++)
                    {
                        so = sos[k];
                        if (r.Equals(so.rect))
                            po = so;
                        else
                            Tests.Assert(r.Intersects(so.rect));
                    }
                    Tests.Assert(po != null);
                    for (int k = 0; k < nObjectsInTree; k++)
                    {
                        if (r.Intersects(rectangles[k]))
                            n += 1;
                    }
                    Tests.Assert(n == sos.Length);

                    n = 0;
                    foreach (SpatialObject o in root.index.Overlaps(r))
                    {
                        Tests.Assert(o == sos[n++]);
                    }
                    Tests.Assert(n == sos.Length);

                    root.index.Remove(r, po);
                    po.Deallocate();
                }
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int top = (int)(key % 1000);
                int left = (int)(key / 1000 % 1000);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int bottom = top + (int)(key % 100);
                int right = left + (int)(key / 100 % 100);
                so = new SpatialObject();
                r = new RectangleR2(top, left, bottom, right);
                so.rect = r;
                rectangles[j] = r;
                root.index.Put(r, so);

                if (i % 100 == 0)
                    db.Commit();
            }
            root.index.Clear();
            db.Close();
        }
    }
}
