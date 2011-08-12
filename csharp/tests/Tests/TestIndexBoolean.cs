namespace Volante
{
    using System;
    using System.Collections;

    public class TestIndexBooleanResult : TestResult
    {
        public TimeSpan InsertTime;
    }

    public class TestIndexBoolean
    {
        public class Record : Persistent
        {
            public long val;
            public bool ToBool()
            {
                return val % 2 == 1;
            }
        }

        static public TestIndexBooleanResult Run(int count, bool altBtree)
        {
            int i;
            Record r=null;
            string dbName = "testidxbool.dbs";
            Tests.SafeDeleteFile(dbName);

            var res = new TestIndexBooleanResult()
            {
                Count = count,
                TestName = String.Format("TestIndexBoolean(count={0}, altBtree={1}", count, altBtree)
            };
            var tStart = DateTime.Now;
            var start = DateTime.Now;

            Storage db = StorageFactory.CreateStorage();
            if (altBtree)
                db.AlternativeBtree = true;
            db.Open(dbName);

            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<Boolean, Record>(false);

            start = System.DateTime.Now;
            long val = 1999;
            int falseCount = 0;
            int trueCount = 0;
            for (i = 0; i < count; i++)
            {
                r = new Record();
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                r.val = val;
                idx.Put(r.ToBool(), r);
                if (r.ToBool())
                    trueCount += 1;
                else
                    falseCount += 1;
                if (i % 1000 == 0)
                    db.Commit();
            }
            Tests.Assert(count == trueCount + falseCount);
            db.Commit();
            Tests.Assert(idx.Size() == count);
            res.InsertTime = DateTime.Now - start;

            start = System.DateTime.Now;
            bool gotException = false;
            try
            {
                r = idx[true];
            }
            catch (StorageError exc)
            {
                if (exc.Code == StorageError.ErrorCode.KEY_NOT_UNIQUE)
                    gotException = true;
            }
            Tests.Assert(gotException);
            gotException = false;
            try
            {
                r = idx[false];
            }
            catch (StorageError exc)
            {
                if (exc.Code == StorageError.ErrorCode.KEY_NOT_UNIQUE)
                    gotException = true;
            }
            Tests.Assert(gotException);

            Record[] recs = idx[true, true];
            Tests.Assert(recs.Length == trueCount);
            foreach (var r2 in recs)
            {
                Tests.Assert(r2.ToBool());
            }
            recs = idx[false, false];
            Tests.Assert(recs.Length == falseCount);
            foreach (var r2 in recs)
            {
                Tests.Assert(!r2.ToBool());
            }

            Array a = idx.ToArray(typeof(Record));
            Tests.Assert(a.Length == count);
            for (i=0; i<a.Length; i++)
            {
                r = (Record)a.GetValue(i);
                // false should be before true
                if (i<falseCount)
                    Tests.Assert(!r.ToBool());
                else
                    Tests.Assert(r.ToBool());
            }

            var e1 = idx.GetEnumerator(false, true, IterationOrder.AscentOrder);
            Record first = null;
            i = 0;
            while (e1.MoveNext())
            {
                r = e1.Current;
                if (null == first)
                    first = r;
                if (i<falseCount)
                    Tests.Assert(!r.ToBool());
                else
                    Tests.Assert(r.ToBool());
            }

            e1 = idx.GetEnumerator(false, true, IterationOrder.DescentOrder);
            i = 0;
            while (e1.MoveNext())
            {
                r = e1.Current;
                if (i<trueCount)
                    Tests.Assert(r.ToBool());
                else
                    Tests.Assert(!r.ToBool());
            }
            Tests.Assert(first.val == r.val);

#if NOT_USED
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec = intIndex.Get(key);
                Record removed = intIndex.RemoveKey(key);
                Tests.Assert(removed == rec);
                strIndex.Remove(new Key(System.Convert.ToString(key)), rec);
                rec.Deallocate();
            }
#endif

            db.Close();
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }

    }
}
