// Copyright: Krzysztof Kowalczyk
// License: BSD

namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestLinkPArray : ITest
    {
        public class Root : Persistent
        {
            public IPArray<RecordFull> arr;
            public ILink<RecordFull> link;
        }

        public void Run(TestConfig config)
        {
            RecordFull r;
            RecordFull[] recs;
            RecordFull notInArr1;
            IDatabase db = config.GetDatabase();
            config.Result = new TestResult();
            Root root = new Root();
            var arr = db.CreateArray<RecordFull>(256);
            arr = db.CreateArray<RecordFull>();
            var link = db.CreateLink<RecordFull>(256);
            link = db.CreateLink<RecordFull>();
            root.arr = arr;
            root.link = link;
            db.Root = root;
            Tests.Assert(arr.Count == 0);
            Tests.Assert(((IGenericPArray)arr).Size() == 0);

            var inMem = new List<RecordFull>();
            for (long i = 0; i < 256; i++)
            {
                r = new RecordFull(i);
                inMem.Add(r);
                arr.Add(r);
                Tests.Assert(arr.Count == i + 1);
                link.Add(r);
                Tests.Assert(link.Count == i + 1);
            }
            recs = arr.ToArray();
            Tests.Assert(recs.Length == arr.Length);
            for (int j = 0; j < recs.Length; j++)
            {
                Tests.Assert(recs[j] == arr[j]);
            }
            recs = inMem.ToArray();

            arr.AddAll(recs);

            notInArr1 = new RecordFull(256);
            inMem.Add(notInArr1);
            db.Commit();

            var e = link.GetEnumerator();
            int idx = 0;
            while (e.MoveNext())
            {
                Tests.Assert(e.Current == inMem[idx++]);
            }
            e.Reset();
            idx = 0;
            int nullCount = 0;
            while (e.MoveNext())
            {
                Tests.Assert(e.Current == inMem[idx++]);
                IEnumerator e2 = (IEnumerator)e;
                if (e2.Current == null)
                    nullCount++;
            }

            nullCount = 0;
            foreach (var r2 in link)
            {
                if (null == r2)
                    nullCount++;
            }

            Tests.Assert(arr.Length == 512);
            Array a = arr.ToArray();
            Tests.Assert(a.Length == 512);
            a = arr.ToRawArray();
            Tests.Assert(a.Length == 512);

            arr.RemoveAt(0);
            db.Commit();

            Tests.Assert(arr.Count == 511);
            arr.RemoveAt(arr.Count - 1);
            db.Commit();
            Tests.Assert(arr.Count == 510);
            r = arr[5];
            Tests.Assert(arr.Contains(r));
            Tests.Assert(!arr.Contains(null));
            Tests.Assert(!arr.Contains(notInArr1));
            Tests.Assert(arr.ContainsElement(5, r));
            Tests.Assert(!arr.IsReadOnly);
            Tests.Assert(!link.IsReadOnly);
            Tests.Assert(5 == arr.IndexOf(r));
            Tests.Assert(-1 == arr.IndexOf(notInArr1));
            Tests.Assert(-1 == arr.IndexOf(null));
            Tests.Assert(r.Oid == arr.GetOid(5));
            arr[5] = new RecordFull(17);
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { r = arr[1024]; });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.Insert(9999, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { link.Insert(9999, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.Insert(-1, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { link.Insert(-1, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.RemoveAt(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.RemoveAt(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.GetOid(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.GetOid(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.GetRaw(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.GetRaw(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.Set(9999, new RecordFull(9988)); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr.Set(-1, new RecordFull(9988)); });

            Tests.Assert(null != arr.GetRaw(8));

            arr.Set(25, arr[24]);
            arr.Pin();
            arr.Unpin();
            Tests.Assert(arr.Remove(arr[12]));
            Tests.Assert(!arr.Remove(notInArr1));
            Tests.Assert(link.Remove(link[3]));
            Tests.Assert(!link.Remove(notInArr1));
            Tests.Assert(arr.Length == 509);
            arr.Insert(5, new RecordFull(88));
            Tests.Assert(arr.Length == 510);
            link.Insert(5, new RecordFull(88));
            int expectedCount = arr.Count + link.Count;
            arr.AddAll(link);
            Tests.Assert(arr.Count == expectedCount);
            Array aTmp = arr.ToArray(typeof(RecordFull));
            Tests.Assert(aTmp.Length == arr.Count);
            aTmp = link.ToArray(typeof(RecordFull));
            Tests.Assert(aTmp.Length == link.Count);

            Tests.Assert(null != arr.GetEnumerator());
            Tests.Assert(null != link.GetEnumerator());

            link.Length = 1024;
            Tests.Assert(link.Length == 1024);
            link.Length = 128;
            Tests.Assert(link.Length == 128);
            link.AddAll(arr);
            arr.Clear();
            Tests.Assert(0 == arr.Length);
            db.Commit();
            arr.AddAll(link);
            arr.AddAll(arr);
            recs = arr.ToArray();
            link.AddAll(new RecordFull[1] { recs[0] });
            link.AddAll(recs, 1, 1);
            db.Commit();
            recs = link.ToArray();
            Tests.Assert(recs.Length == link.Length);
            link.Length = link.Length - 2;
            db.Close();
        }
    }

}