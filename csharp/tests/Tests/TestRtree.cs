namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestRtreeResult : TestResult
    {
    }

    class SpatialObject : Persistent
    {
        public Rectangle rect;
    }

    public class TestRtree : Persistent, ITest
    {
        ISpatialIndex<SpatialObject> index;

        const int nObjectsInTree = 1000;

        public void Run(TestConfig config)
        {
            SpatialObject so;
            Rectangle r;
            int count = config.Count;
            var res = new TestRtreeResult();
            config.Result = res;

            IDatabase db = config.GetDatabase();
            TestRtree root = (TestRtree)db.Root;
            Tests.Assert(root == null);
            root = new TestRtree();
            root.index = db.CreateSpatialIndex<SpatialObject>();
            db.Root = root;

            Rectangle[] rectangles = new Rectangle[nObjectsInTree];
            long key = 1999;
            for (int i = 0; i < count; i++)
            {
                int j = i % nObjectsInTree;
                if (i >= nObjectsInTree)
                {
                    r = rectangles[j];
                    SpatialObject[] sos = root.index.Get(r);
                    SpatialObject po = null;
                    for (int k = 0; k < sos.Length; k++)
                    {
                        so = sos[k];
                        if (r.Equals(so.rect))
                            po = so;
                        else
                            Tests.Assert(r.Intersects(so.rect));
                    }
                    Tests.Assert(po != null);

                    int n = 0;
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
                r = new Rectangle(top, left, bottom, right);
                so.rect = r;
                rectangles[j] = r;
                root.index.Put(r, so);

                if (i % 100 == 0)
                    db.Commit();
            }
            db.Commit();
            Rectangle wrappingRect = root.index.WrappingRectangle;
            SpatialObject[] objsTmp = root.index.Get(wrappingRect);
            Tests.Assert(root.index.Count == objsTmp.Length);
            var objs = new List<SpatialObject>();
            objs.AddRange(objsTmp);

            foreach (var spo in root.index)
            {
                Tests.Assert(objs.Contains(spo));
            }

            IDictionaryEnumerator de = root.index.GetDictionaryEnumerator();
            while (de.MoveNext())
            {
                var spo = (SpatialObject)de.Value;
                var rect = (Rectangle)de.Key;
                Tests.Assert(spo.rect.EqualsTo(rect));
                Tests.Assert(objs.Contains(spo));
            }

            var rand = new Random();
            while (root.index.Count > 5)
            {
                int idx = rand.Next(root.index.Count);
                SpatialObject o = objs[idx];
                if (rand.Next(10) > 5)
                    root.index.Remove(o.rect, o);
                else
                    root.index.Remove(wrappingRect, o);
                objs.RemoveAt(idx);
            }

            root.index.Clear();
            db.Close();
        }
    }
}
