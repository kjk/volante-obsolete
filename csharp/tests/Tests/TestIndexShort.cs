namespace Volante
{
    using System;

    public class TestIndexShort : ITest
    {
        public class Record : Persistent
        {
            public long lval;
            public short nval; // native value
            public Record(short v)
            {
                nval = v;
                lval = (long)v;
            }
            public Record()
            {
            }
        }

        const short min = short.MinValue;
        const short max = short.MaxValue;
        const short mid = 0;

        static short Clamp(long n)
        {
            long range = max - min;
            long val = (n % range) + (long)min;
            return (short)val;
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
            var idx = db.CreateIndex<short, Record>(IndexType.NonUnique);
            db.Root = idx;
            int countOf1999 = 0;
            i = 0;
            foreach (long val in Tests.KeySeq(count))
            {
                short idxVal = Clamp(val);
                if (idxVal == 1999)
                    countOf1999++;
                Tests.Assert(idxVal != max);
                r = new Record(idxVal);
                idx.Put(idxVal, r);
                i++;
                if (i % 100 == 0)
                    db.Commit();
            }
            idx.Put(min, new Record(min));
            idx.Put(max, new Record(max));

            Tests.Assert(idx.Count == count + 2);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Count == count + 2);

            start = System.DateTime.Now;
            Record[] recs = idx[min, mid];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval >= min && r2.lval <= mid);
            }
            recs = idx[mid, max];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval >= mid && r2.lval <= max);
            }
            recs = idx[min, max];
            Tests.Assert(recs.Length == count + 2);

            recs = idx[min, min];
            Tests.Assert(1 == recs.Length);

            recs = idx[max, max];
            Tests.Assert(1 == recs.Length);

            recs = idx[1999, 1999];
            Tests.Assert(countOf1999 == recs.Length);

            recs = idx[min + 1, min + 1];
            Tests.Assert(0 == recs.Length);

            short prev = min;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }
            Tests.VerifyEnumeratorDone(e1);

            prev = min;
            foreach (var r2 in idx)
            {
                Tests.Assert(r2.nval >= prev);
                prev = r2.nval;
            }

            prev = min;
            foreach (var r2 in idx.Range(min, max, IterationOrder.AscentOrder))
            {
                Tests.Assert(r2.nval >= prev);
                prev = r2.nval;
            }

            prev = max;
            foreach (var r2 in idx.Range(min, max, IterationOrder.DescentOrder))
            {
                Tests.Assert(prev >= r2.nval);
                prev = r2.nval;
            }

            prev = max;
            foreach (var r2 in idx.Reverse())
            {
                Tests.Assert(prev >= r2.nval);
                prev = r2.nval;
            }
            long usedBeforeDelete = db.UsedSize;
            recs = idx[min, max];
            foreach (var r2 in recs)
            {
                Tests.Assert(!r2.IsDeleted());
                idx.Remove(r2.nval, r2);
                r2.Deallocate();
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
