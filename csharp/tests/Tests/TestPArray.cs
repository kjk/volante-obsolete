// Copyright: Krzysztof Kowalczyk
// License: BSD

namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestPArray
    {
        public class Root : Persistent
        {
            public IPArray<RecordFull> arr1;
            public IPArray<RecordFull> arr2;
        }

        public void Run(TestConfig config)
        {
            RecordFull r;
            RecordFull[] recs;
            RecordFull notInArr1;
            IDatabase db = config.GetDatabase();
            config.Result = new TestResult();
            Root root = new Root();
            var arr1 = db.CreateArray<RecordFull>();
            var arr2 = db.CreateArray<RecordFull>(256);
            root.arr1 = arr1;
            root.arr2 = arr2;
            db.Root = root;
            Tests.Assert(arr1.Count == 0);
            Tests.Assert(((IGenericPArray)arr1).Size() == 0);

            var inMem = new List<RecordFull>();
            for (long i = 0; i < 256; i++)
            {
                r = new RecordFull(i);
                inMem.Add(r);
                arr1.Add(r);
                Tests.Assert(arr1.Count == i + 1);
                arr2.Add(r);
                Tests.Assert(arr2.Count == i + 1);
            }
            recs = arr1.ToArray();
            Tests.Assert(recs.Length == arr1.Length);
            for (int j = 0; j < recs.Length; j++)
            {
                Tests.Assert(recs[j] == arr1[j]);
            }
            recs = inMem.ToArray();

            arr1.AddAll(recs);

            notInArr1 = new RecordFull(256);
            inMem.Add(notInArr1);
            arr2.Add(notInArr1);
            db.Commit();

            var e = arr2.GetEnumerator();
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
            foreach (var r2 in arr2)
            {
                if (null == r2)
                    nullCount++;
            }

            Tests.Assert(arr1.Length == 512);
            Array a = arr1.ToArray();
            Tests.Assert(a.Length == 512);
            a = arr1.ToRawArray();
            Tests.Assert(a.Length == 512);

            arr1.RemoveAt(0);
            db.Commit();

            Tests.Assert(arr1.Count == 511);
            arr1.RemoveAt(arr1.Count - 1);
            db.Commit();
            Tests.Assert(arr1.Count == 510);
            r = arr1[5];
            Tests.Assert(arr1.Contains(r));
            Tests.Assert(!arr1.Contains(null));
            Tests.Assert(!arr1.Contains(notInArr1));
            Tests.Assert(arr1.ContainsElement(5, r));
            Tests.Assert(!arr1.IsReadOnly);
            Tests.Assert(5 == arr1.IndexOf(r));
            Tests.Assert(-1 == arr1.IndexOf(notInArr1));
            Tests.Assert(-1 == arr1.IndexOf(null));
            Tests.Assert(r.Oid == arr1.GetOid(5));
            arr1[5] = new RecordFull(17);
            Tests.AssertException<IndexOutOfRangeException>(() =>
                { r = arr1[1024]; });
            Tests.AssertException<IndexOutOfRangeException>(() =>
                { arr1.Insert(9999, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.Insert(-1, null); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.RemoveAt(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.RemoveAt(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.GetOid(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.GetOid(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.GetRaw(9999); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.GetRaw(-1); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.Set(9999, new RecordFull(9988)); });
            Tests.AssertException<IndexOutOfRangeException>(() =>
            { arr1.Set(-1, new RecordFull(9988)); });

            Tests.Assert(null != arr1.GetRaw(8));

            arr1.Set(25, arr1[24]);
            arr1.Pin();
            arr1.Unpin();
            bool ok = arr1.Remove(arr1[12]);
            Tests.Assert(ok);
            ok = arr1.Remove(notInArr1);
            Tests.Assert(!ok);
            Tests.Assert(arr1.Length == 509);
            arr1.Insert(5, new RecordFull(88));
            Tests.Assert(arr1.Length == 510);

            int expectedCount = arr1.Count + arr2.Count;
            ILink<RecordFull> link = (ILink<RecordFull>)arr2;
            arr1.AddAll(link);
            Tests.Assert(arr1.Count == expectedCount);

            arr1.Clear();
            Tests.Assert(0 == arr1.Length);
            arr2.Length = 1024;
            Tests.Assert(arr2.Length == 1024);
            arr2.Length = 128;
            Tests.Assert(arr2.Length == 128);
            db.Close();
        }
    }

}