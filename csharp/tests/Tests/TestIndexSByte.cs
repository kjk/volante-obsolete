namespace Volante
{
    using System;
    using System.Collections;
    using System.Diagnostics;

    public class TestIndexSByte
    {
        public class Record : Persistent
        {
            public long lval;
            public sbyte nval; // native value
            public Record(sbyte v)
            {
                nval = v;
                lval = (long)v;
            }
            public Record()
            {
            }
        }

        static sbyte Clamp(long n)
        {
            long range = sbyte.MaxValue - sbyte.MinValue;
            long val = (n % range) + (long)sbyte.MinValue;
            return (sbyte)val;
        }

        static public TestIndexNumericResult Run(int count, bool altBtree)
        {
            int i;
            Record r = null;
            string dbName = "testnumsbyte.dbs";
            Tests.SafeDeleteFile(dbName);
            var res = new TestIndexNumericResult()
            {
                Count = count,
                TestName = String.Format("TestIndexSByte, count={0}", count)
            };

            var tStart = DateTime.Now;
            var start = DateTime.Now;

            Storage db = StorageFactory.CreateStorage();
            if (altBtree)
                db.AlternativeBtree = true;
            db.Open(dbName);
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<sbyte, Record>(false);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                sbyte idxVal = Clamp(val);
                r = new Record(idxVal);
                idx.Put(idxVal, r);
                if (i % 100 == 0)
                    db.Commit();
            }
            idx.Put(sbyte.MinValue, new Record(sbyte.MinValue));
            idx.Put(sbyte.MaxValue, new Record(sbyte.MaxValue));

            Tests.Assert(idx.Count == count + 2);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Size() == count + 2);

            start = System.DateTime.Now;
            sbyte low = sbyte.MinValue;
            sbyte high = sbyte.MaxValue;
            sbyte mid = sbyte.MaxValue / 2;
            Record[] recs = idx[low, mid];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval <= mid && r2.lval >= low);
            }
            recs = idx[mid, high];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval >= mid && r2.lval <= high);
            }
            sbyte prev = sbyte.MinValue;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = sbyte.MinValue;
            foreach (var r2 in idx)
            {
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = sbyte.MinValue;
            foreach (var r2 in idx.Range(sbyte.MinValue, sbyte.MaxValue, IterationOrder.AscentOrder))
            {
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = sbyte.MaxValue;
            foreach (var r2 in idx.Range(sbyte.MinValue, sbyte.MaxValue, IterationOrder.DescentOrder))
            {
                Tests.Assert(prev >= r.nval);
                prev = r.nval;
            }

            prev = sbyte.MaxValue;
            foreach (var r2 in idx.Reverse())
            {
                Tests.Assert(prev >= r.nval);
                prev = r.nval;
            }
            long usedBeforeDelete = db.UsedSize;
            recs = idx[sbyte.MinValue, sbyte.MaxValue];
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
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}
