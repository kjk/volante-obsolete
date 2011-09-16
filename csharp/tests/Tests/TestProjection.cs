namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestProjection : ITest
    {
        public class ToRec : Persistent
        {
            int v;

            public ToRec()
            {
            }

            public ToRec(int v)
            {
                this.v = v;
            }
        }

        public class FromRec : Persistent
        {
            public long v;
            public ILink<ToRec> list;
            public ToRec[] list2;
            public ToRec toEl;

            public FromRec()
            {
            }

            public FromRec(IDatabase db, long v)
            {
                this.v = v;
                int n = 5;
                list = db.CreateLink<ToRec>(n);
                list.Length = n;
                list2 = new ToRec[n];
                for (int i = 0; i < n; i++)
                {
                    list[i] = new ToRec(i);
                    list2[i] = new ToRec(i);
                }
                toEl = new ToRec(n);
            }
        }

        public class Root : Persistent
        {
            public IPArray<FromRec> arr;
        }

        public void Run(TestConfig config)
        {
            int i;
            IDatabase db = config.GetDatabase();
            config.Result = new TestResult();
            Root root = new Root();
            var arr = db.CreateArray<FromRec>(45);
            root.arr = arr;
            db.Root = root;
            db.Commit();
            int projectedEls = 45;
            for (i = 0; i < projectedEls; i++)
            {
                arr.Add(new FromRec(db, i));
            }
            arr[0].toEl = null;

            db.Commit();
            var p1 = new Projection<FromRec, ToRec>("list");
            Tests.Assert(p1.Count == 0);
            Tests.Assert(p1.Length == 0);
            Tests.Assert(!p1.IsReadOnly);
            Tests.Assert(!p1.IsSynchronized);
            Tests.Assert(null == p1.SyncRoot);
            p1.Project(arr);
            Tests.Assert(p1.Count == projectedEls * 5);
            var arrTmp = p1.ToArray();
            Tests.Assert(arrTmp.Length == p1.Length);
            p1.Reset();

            p1.Project(arr[0]);
            Tests.Assert(p1.Length == 5);
            p1.Reset();

            var arr3 = arr.ToArray();
            p1.Project(arr3);
            Tests.Assert(p1.Length == projectedEls * 5);
            p1.Clear();

            IEnumerator<FromRec> e1 = arr.GetEnumerator();
            p1.Project(e1);
            Tests.Assert(p1.Length == projectedEls * 5);

            var p2 = new Projection<FromRec, ToRec>("list2");
            p2.Project(arr);
            Tests.Assert(p2.Length == projectedEls * 5);

            var p3 = new Projection<FromRec, ToRec>("toEl");
            p3.Project(arr);
            Tests.Assert(p2.Length == projectedEls * 5);

            p1.Join<FromRec>(p2);

            Tests.Assert(p1.GetEnumerator() != null);
            IEnumerator eTmp = ((IEnumerable)p1).GetEnumerator();
            Tests.Assert(eTmp != null);

            ToRec[] res = new ToRec[p3.Count];
            p3.CopyTo(res, 0);
            foreach (var tmp in res)
            {
                Tests.Assert(p3.Contains(tmp));
                p3.Remove(tmp);
                Tests.Assert(!p3.Contains(tmp));
            }
            Tests.Assert(0 == p3.Length);
            db.Commit();
            db.Close();
        }
    }
}
