namespace Volante
{
    using System;

    public class TestIndexULong
    {
        public class Record : Persistent
        {
            public long lval;
            public ulong nval; // native value
            public Record(ulong v)
            {
                nval = v;
                lval = (long)v;
            }
            public Record()
            {
            }
        }

        const ulong min = ulong.MinValue;
        const ulong max = ulong.MaxValue;
        const ulong mid = max / 2;

        static ulong Clamp(long n)
        {
            return (ulong)n;
        }

        static public TestIndexNumericResult Run(int count, bool altBtree)
        {
            int i;
            Record r = null;
            string dbName = "testnumulong.dbs";
            Tests.SafeDeleteFile(dbName);
            var res = new TestIndexNumericResult()
            {
                Count = count,
                TestName = String.Format("TestIndexULong, count={0}, altBtree={1}", count, altBtree)
            };

            var tStart = DateTime.Now;
            var start = DateTime.Now;

            IStorage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.Open(dbName);
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<ulong, Record>(false);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                ulong idxVal = Clamp(val);
                r = new Record(idxVal);
                idx.Put(idxVal, r);
                if (i % 100 == 0)
                    db.Commit();
            }
            idx.Put(min, new Record(min));
            idx.Put(max, new Record(max));

            Tests.Assert(idx.Count == count + 2);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Size() == count + 2);

            start = System.DateTime.Now;
            Record[] recs = idx[min, mid];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.nval >= min && r2.nval <= mid);
                i++;
            }
            recs = idx[mid, max];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.nval >= mid && r2.nval <= max);
                i++;
            }
            ulong prev = min;
            i = 0;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
                i++;
            }

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
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}

