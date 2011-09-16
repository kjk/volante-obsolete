// Copyright: Krzysztof Kowalczyk
// License: BSD

namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestLinkPArray : ITest
    {
        public class RelMember : Persistent
        {
            long l;

            public RelMember()
            {
            }

            public RelMember(long v)
            {
                l = v;
            }
        }

        public class Root : Persistent
        {
            public IPArray<RecordFull> arr;
            public ILink<RecordFull> link;
            public RecordFull relOwner;
            public Relation<RelMember, RecordFull> rel;
        }

        public void Run(TestConfig config)
        {
            RecordFull r;
            RelMember rm;
            RecordFull[] recs;
            RelMember[] rmArr;
            RecordFull notInArr1;
            IDatabase db = config.GetDatabase();
            config.Result = new TestResult();
            Root root = new Root();
            var arr = db.CreateArray<RecordFull>(256);
            arr = db.CreateArray<RecordFull>();
            var link = db.CreateLink<RecordFull>(256);
            link = db.CreateLink<RecordFull>();
            root.relOwner = new RecordFull();
            var rel = db.CreateRelation<RelMember, RecordFull>(root.relOwner);
            Tests.Assert(rel.Owner == root.relOwner);
            rel.SetOwner(new RecordFull(88));
            Tests.Assert(rel.Owner != root.relOwner);
            rel.Owner = root.relOwner;
            Tests.Assert(rel.Owner == root.relOwner);
            root.arr = arr;
            root.link = link;
            root.rel = rel;
            db.Root = root;
            Tests.Assert(arr.Count == 0);
            Tests.Assert(((IGenericPArray)arr).Size() == 0);

            var inMem = new List<RecordFull>();
            for (long i = 0; i < 256; i++)
            {
                r = new RecordFull(i);
                rm = new RelMember(i);
                inMem.Add(r);
                arr.Add(r);
                Tests.Assert(arr.Count == i + 1);
                link.Add(r);
                rel.Add(rm);
                Tests.Assert(link.Count == i + 1);
            }
            recs = arr.ToArray();
            rmArr = rel.ToArray();
            Tests.Assert(recs.Length == rmArr.Length);
            Tests.Assert(rel.Count == rel.Length);
            Tests.Assert(rel.Size() == rel.Count);
            rel.CopyTo(rmArr, 0);
            Tests.Assert(recs.Length == arr.Length);
            for (int j = 0; j < recs.Length; j++)
            {
                Tests.Assert(recs[j] == arr[j]);
                Tests.Assert(rmArr[j] == rel[j]);
            }
            recs = inMem.ToArray();

            arr.AddAll(recs);

            rel.AddAll(rmArr);

            notInArr1 = new RecordFull(256);
            inMem.Add(notInArr1);
            db.Commit();

            var e = link.GetEnumerator();
            int idx = 0;
            while (e.MoveNext())
            {
                Tests.Assert(e.Current == inMem[idx++]);
            }
            Tests.AssertException<InvalidOperationException>(
                () => { var tmp = e.Current; });
            Tests.Assert(!e.MoveNext());
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

            var e3 = rel.GetEnumerator();
            while (e3.MoveNext())
            {
                Tests.Assert(e3.Current != null);
            }
            Tests.Assert(!e3.MoveNext());
            Tests.AssertException<InvalidOperationException>(
                () => { var tmp = e3.Current; });
            e3.Reset();
            Tests.Assert(e3.MoveNext());

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

            rel.Length = rel.Length / 2;
            idx = rel.Length / 2;
            Tests.Assert(null != rel.Get(idx));
            rel[idx] = new RelMember(55);
            db.Commit();
            IPersistent raw = rel.GetRaw(idx);
            Tests.Assert(raw.IsRaw());
            rm = rel[idx];
            Tests.Assert(rel.Contains(rm));
            Tests.Assert(rel.ContainsElement(idx, rm));
            Tests.Assert(rel.Remove(rm));
            Tests.Assert(!rel.Contains(rm));
            Tests.Assert(!rel.Remove(rm));
            idx = rel.Length / 2;
            rm = rel[idx];
            Tests.Assert(idx == rel.IndexOf(rm));
            int cnt = rel.Count;
            rel.RemoveAt(idx);
            Tests.Assert(rel.Count == cnt - 1);
            Tests.Assert(!rel.Contains(rm));
            rel.Add(rm);
            db.Commit();
            //TODO: LinkImpl.ToRawArray() seems wrong but changing it
            //breaks a lot of code
            //Array ra = rel.ToRawArray();
            Array ra2 = rel.ToArray();
            //Tests.Assert(ra2.Length == ra.Length);
            //Tests.Assert(ra.Length == rel.Count);
            rel.Insert(1, new RelMember(123));
            //Tests.Assert(rel.Count == ra.Length + 1);
            rel.Unpin();
            rel.Pin();
            rel.Unpin();
            rel.Clear();
            Tests.Assert(rel.Count == 0);
            db.Close();
        }
    }
}
