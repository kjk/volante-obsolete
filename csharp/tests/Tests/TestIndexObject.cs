namespace Volante
{
    using System;
    using System.Collections;
    using System.Diagnostics;

    public class TestIndexObject
    {
        public class Record : Persistent
        {
            public string str;
            public long n;
            public Record(long v)
            {
                n = v;
                str = Convert.ToString(v);
            }
            public Record()
            {
            }
        }

        static public TestIndexNumericResult Run(int count, bool altBtree)
        {
            int i;
            Record r = null;
            string dbName = "testnumobject.dbs";
            Tests.SafeDeleteFile(dbName);
            var res = new TestIndexNumericResult()
            {
                Count = count,
                TestName = String.Format("TestIndexObject, count={0}, altBtree={1}", count, altBtree)
            };

            var tStart = DateTime.Now;
            var start = DateTime.Now;

            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.Open(dbName);
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<Record, Record>(false);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                r = new Record(val);
                r.MakePersistent(db);
                idx.Put(r, r);
                if (i % 100 == 0)
                    db.Commit();
            }

            Tests.Assert(idx.Count == count);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Size() == count);
            Record[] recs = idx.ToArray();
            Array.Sort(recs, (r1, r2) => { return r1.Oid - r2.Oid; });
            Tests.Assert(recs.Length == count);
            Record min = recs[0];
            Record max = recs[recs.Length - 1];
            Record mid = recs[recs.Length / 2];
            start = System.DateTime.Now;
            recs = idx[min, mid];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.Oid >= min.Oid && r2.Oid <= mid.Oid);
                i++;
            }
            recs = idx[mid, max];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.Oid >= mid.Oid && r2.Oid <= max.Oid);
                i++;
            }
            long prev = min.Oid;
            i = 0;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.Oid >= prev);
                prev = r.Oid;
                i++;
            }

            prev = min.Oid;
            i = 0;
            foreach (var r2 in idx)
            {
                Tests.Assert(r2.Oid >= prev);
                prev = r2.Oid;
                i++;
            }

            prev = min.Oid;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.AscentOrder))
            {
                Tests.Assert(r2.Oid >= prev);
                prev = r2.Oid;
                i++;
            }

            prev = max.Oid;
            i = 0;
            foreach (var r2 in idx.Range(min, max, IterationOrder.DescentOrder))
            {
                Tests.Assert(prev >= r2.Oid);
                prev = r2.Oid;
                i++;
            }

            prev = max.Oid;
            i = 0;
            foreach (var r2 in idx.Reverse())
            {
                Tests.Assert(prev >= r2.Oid);
                prev = r2.Oid;
                i++;
            }
            long usedBeforeDelete = db.UsedSize;
            recs = idx[min, max];
            i = 0;
            foreach (var r2 in recs)
            {
                Tests.Assert(!r2.IsDeleted());
                idx.Remove(r2, r2);
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
