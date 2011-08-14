namespace Volante
{
    using System;
    using System.Collections;
    using System.Diagnostics;

    public class TestIndexNumericResult : TestResult
    {
        public TimeSpan InsertTime;
    }

    public class TestIndexByte
    {
        public class Record : Persistent
        {
            public long lval;
            public byte nval; // native value
            public Record(byte v)
            {
                nval = v;
                lval = (long)v;
            }
            public Record()
            {
            }
        }

        static byte Clamp(long n)
        {
            long range = byte.MaxValue - byte.MinValue;
            long val = (n % range) + (long)byte.MinValue;
            return (byte)val;
#if NOT_USED
            if (typeof(T) == typeof(sbyte))            {
                long range = sbyte.MaxValue - sbyte.MinValue;
                long val = (n % range) + (long)sbyte.MinValue;
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(short))
            {
                long range = short.MaxValue - short.MinValue;
                long val = (n % range) + (long)short.MinValue;
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(ushort))
            {
                long range = ushort.MaxValue - ushort.MinValue;
                long val = (n % range) + (long)ushort.MinValue;
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(int))
            {
                long range = (long)int.MaxValue + 1; // not really true
                long val = (n % range) + (long)(int.MinValue / 2);
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(uint))
            {
                long range = uint.MaxValue - uint.MinValue;
                long val = (n % range) + (long)uint.MinValue;
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(long))
            {
                long range = long.MaxValue; // not really true
                long val = (n % range) + (long.MinValue / 2);
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }
            if (typeof(T) == typeof(ulong))
            {
                long range = long.MaxValue; // not really true
                long val = (n % range) + (long)(ulong.MinValue / 2);
                res = (T)Convert.ChangeType(val, typeof(T));
                return;
            }

            Debug.Assert(false, String.Format("Unsupported type {0}", typeof(T)));
            res = default(T);
#endif
        }

        static public TestIndexNumericResult Run(int count, bool altBtree)
        {
            int i;
            Record r = null;
            string dbName = "testnumbyte.dbs";
            Tests.SafeDeleteFile(dbName);
            var res = new TestIndexNumericResult()
            {
                Count = count,
                TestName = String.Format("TestIndexByte, count={0}", count)
            };

            var tStart = DateTime.Now;
            var start = DateTime.Now;

            Storage db = StorageFactory.CreateStorage();
            if (altBtree)
                db.AlternativeBtree = true;
            db.Open(dbName);
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<byte, Record>(false);
            db.Root = idx;
            long val = 1999;
            for (i = 0; i < count; i++)
            {
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                byte idxVal = Clamp(val);
                r = new Record(idxVal);
                idx.Put(idxVal, r);
                if (i % 100 == 0)
                    db.Commit();
            }
            idx.Put(byte.MinValue, new Record(byte.MinValue));
            idx.Put(byte.MaxValue, new Record(byte.MaxValue));

            Tests.Assert(idx.Count == count + 2);
            db.Commit();
            res.InsertTime = DateTime.Now - start;
            Tests.Assert(idx.Size() == count + 2);

            start = System.DateTime.Now;
            Record[] recs = idx[byte.MinValue, 0];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval <= 0);
            }
            recs = idx[0, byte.MaxValue];
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.lval >= 0);
            }
            byte prev = byte.MinValue;
            var e1 = idx.GetEnumerator();
            while (e1.MoveNext())
            {
                r = e1.Current;
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = byte.MinValue;
            foreach (var r2 in idx)
            {
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = byte.MinValue;
            foreach (var r2 in idx.Range(byte.MinValue, byte.MaxValue, IterationOrder.AscentOrder))
            {
                Tests.Assert(r.nval >= prev);
                prev = r.nval;
            }

            prev = byte.MaxValue;
            foreach (var r2 in idx.Range(byte.MinValue, byte.MaxValue, IterationOrder.DescentOrder))
            {
                Tests.Assert(prev >= r.nval);
                prev = r.nval;
            }

            prev = byte.MaxValue;
            foreach (var r2 in idx.Reverse())
            {
                Tests.Assert(prev >= r.nval);
                prev = r.nval;
            }
            long usedBeforeDelete = db.DatabaseSize;
            recs = idx[byte.MinValue, byte.MaxValue];
            foreach (var r2 in recs)
            {
                Tests.Assert(!r2.IsDeleted());
                idx.Remove(r2.nval, r2);
                r2.Deallocate();
            }
            Tests.Assert(idx.Count == 0);
            db.Commit();
            long usedAfterDelete = db.DatabaseSize;
            db.Gc();
            db.Commit();
            long usedAfterGc = db.DatabaseSize;
            db.Close();
            // TODO: figure out why usedAfterDelete and
            // usedAfterGc are not smaller than usedBeforeDelete
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}
