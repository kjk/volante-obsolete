namespace Volante
{
    using System;

    public class TestIndexLong : ITest
    {
        public class Record : Persistent
        {
            public long lval;
            public long nval; // native value
            public Record(long v)
            {
                nval = v;
                lval = v;
            }
            public Record()
            {
            }
        }

        const long min = long.MinValue;
        const long max = long.MaxValue;
        const long mid = 0;

        static long Clamp(long n)
        {
            return n;
        }

        public void Run(TestConfig config)
        {
            int i;
            Record r = null;
            int count = config.Count;
            var res = new TestIndexNumericResult();
            config.Result = res;

            var start = DateTime.Now;

            IDatabase db = config.GetDatabase();
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<long, Record>(IndexType.NonUnique);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                long idxVal = Clamp(val);
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
                Tests.Assert(r2.lval >= min && r2.lval <= mid);
                i++;
            }
            recs = idx[mid, max];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval >= mid && r2.lval <= max);
                i++;
            }

            recs = idx[min, max];
            Tests.Assert(recs.Length == count + 2);

            recs = idx[min, min];
            Tests.Assert(1 == recs.Length);

            recs = idx[max, max];
            Tests.Assert(1 == recs.Length);

            recs = idx[1999, 1999];
            Tests.Assert(1 == recs.Length);

            long prev = min;
            i = 0;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
                i++;
            }
            Tests.VerifyEnumeratorDone(e1);

            prev = min;
            i = 0;
            foreach (var r2 in idx)
            {
                Tests.Assert(r2.nval >= prev);
                prev = r2.nval;
                i++;
            }

            prev = min;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.AscentOrder))
            {
                Tests.Assert(r2.nval >= prev);
                prev = r2.nval;
                i++;
            }

            prev = max;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.DescentOrder))
            {
                Tests.Assert(prev >= r2.nval);
                prev = r2.nval;
                i++;
            }

            prev = max;
            i = 0;
            foreach (var r2 in idx.Reverse())
            {
                Tests.Assert(prev >= r2.nval);
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
