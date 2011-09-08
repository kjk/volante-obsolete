namespace Volante
{
    using System;

    public class TestIndexGuid : ITest
    {
        public class Record : Persistent
        {
            public Guid nval; // native value
            public Record(Guid v)
            {
                nval = v;
            }
            public Record()
            {
            }
        }

        static readonly Guid min = new Guid(new byte[] { 
            0, 0, 0, 0, 
            0, 0, 0, 0, 
            0, 0, 0, 0, 
            0, 0, 0, 0 });
        static readonly Guid max = new Guid(new byte[] { 
            0xff, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff });
        static readonly Guid mid = new Guid(new byte[] { 
            0x7f, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff, 
            0xff, 0xff, 0xff, 0xff });

        public static void pack4(byte[] arr, int offs, int val)
        {
            arr[offs] = (byte)(val >> 24);
            arr[offs + 1] = (byte)(val >> 16);
            arr[offs + 2] = (byte)(val >> 8);
            arr[offs + 3] = (byte)val;
        }
        public static void pack8(byte[] arr, int offs, long val)
        {
            pack4(arr, offs, (int)(val >> 32));
            pack4(arr, offs + 4, (int)val);
        }

        static Guid Clamp(long n)
        {
            var bytes = new byte[16];
            pack8(bytes, 0, n);
            return new Guid(bytes);
        }

        public void Run(TestConfig config)
        {
            int i, cmp;
            Record r = null;
            int count = config.Count;
            var res = new TestIndexNumericResult();
            config.Result = res;

            var start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<Guid, Record>(IndexType.NonUnique);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                Guid idxVal = Clamp(val);
                r = new Record(idxVal);
                idx.Put(idxVal, r);
                if (i % 100 == 0)
                    db.Commit();
                val = (3141592621L * val + 2718281829L) % 1000000007L;
            }
            idx.Put(min, new Record(min));
            idx.Put(max, new Record(max));

            Tests.Assert(idx.Count == count + 2);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Count == count + 2);

            start = System.DateTime.Now;
            Record[] recs = idx[min, mid];
            i = 0;
            foreach (var r2 in recs)
            {
                cmp = min.CompareTo(r2.nval);
                Tests.Assert(cmp == -1 || cmp == 0);
                cmp = mid.CompareTo(r2.nval);
                Tests.Assert(cmp == 1 || cmp == 0);
                i++;
            }
            recs = idx[mid, max];
            i = 0;
            foreach (var r2 in recs)
            {
                cmp = mid.CompareTo(r2.nval);
                Tests.Assert(cmp == -1 || cmp == 0);
                cmp = max.CompareTo(r2.nval);
                Tests.Assert(cmp == 1 || cmp == 0);
                i++;
            }
            Guid prev = min;
            i = 0;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                cmp = r.nval.CompareTo(prev);
                Tests.Assert(cmp == 1 || cmp == 0);
                prev = r.nval;
                i++;
            }
            Tests.VerifyEnumeratorDone(e1);

            prev = min;
            i = 0;
            foreach (var r2 in idx)
            {
                cmp = r2.nval.CompareTo(prev);
                Tests.Assert(cmp == 1 || cmp == 0);
                prev = r2.nval;
                i++;
            }

            prev = min;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.AscentOrder))
            {
                cmp = r2.nval.CompareTo(prev);
                Tests.Assert(cmp == 1 || cmp == 0);
                prev = r2.nval;
                i++;
            }

            prev = max;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.DescentOrder))
            {
                cmp = r2.nval.CompareTo(prev);
                Tests.Assert(cmp == -1 || cmp == 0);
                prev = r2.nval;
                i++;
            }

            prev = max;
            i = 0;
            foreach (var r2 in idx.Reverse())
            {
                cmp = r2.nval.CompareTo(prev);
                Tests.Assert(cmp == -1 || cmp == 0);
                prev = r2.nval;
                i++;
            }
            long usedBeforeDelete = db.UsedSize;
            recs = idx[min, max];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(!r2.IsDeleted());
                idx.Remove(r2.nval, r2);
                r2.Deallocate();
                i++;
            }
            Tests.Assert(idx.Count == 0);
            db.Commit();
            long usedAfterDelete = db.UsedSize;
            db.Gc();
            db.Commit();
            long usedAfterGc = db.UsedSize;
            db.Close();
        }
    }
}
