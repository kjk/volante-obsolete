namespace Volante
{
    using System;

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

        public void Run(TestConfig config)
        {
            int i;
            Record r=null;
            int count = config.Count;
            var res = new TestIndexBooleanResult();
            config.Result = res;
            var start = DateTime.Now;
            IStorage db = config.GetDatabase();
            Tests.Assert(null == db.Root);
            var idx = db.CreateIndex<Boolean, Record>(false);
            db.Root = idx;

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
            Tests.AssertStorageException(() => { r = idx[true]; }, StorageError.ErrorCode.KEY_NOT_UNIQUE );

            Tests.AssertStorageException(() => { r = idx[false]; },
 StorageError.ErrorCode.KEY_NOT_UNIQUE );

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
                i++;
            }
            Tests.VerifyEnumeratorDone(e1);

            e1 = idx.GetEnumerator(false, true, IterationOrder.DescentOrder);
            i = 0;
            while (e1.MoveNext())
            {
                r = e1.Current;
                if (i<trueCount)
                    Tests.Assert(r.ToBool());
                else
                    Tests.Assert(!r.ToBool());
                i++;
            }
            Tests.Assert(first.val == r.val);
            Tests.VerifyEnumeratorDone(e1);

            i = 0;
            foreach (var r2 in idx.Range(false, true))
            {
                if (i < falseCount)
                    Tests.Assert(!r2.ToBool());
                else
                    Tests.Assert(r2.ToBool());
                i++;
            }
            Tests.Assert(i == count);

            i = 0;
            foreach (var r2 in idx.Range(false, true, IterationOrder.DescentOrder))
            {
                if (i < trueCount)
                    Tests.Assert(r2.ToBool());
                else
                    Tests.Assert(!r2.ToBool());
                i++;
            }
            Tests.Assert(i == count);

            i = 0;
            foreach (var r2 in idx.Reverse())
            {
                if (i < trueCount)
                    Tests.Assert(r2.ToBool());
                else
                    Tests.Assert(!r2.ToBool());
                i++;
            }
            Tests.Assert(i == count);

            Tests.Assert(idx.KeyType == typeof(Boolean));
            Tests.AssertStorageException(() => idx.Remove(new Key(true)), StorageError.ErrorCode.KEY_NOT_UNIQUE);
            Tests.AssertStorageException(() => idx.RemoveKey(true), StorageError.ErrorCode.KEY_NOT_UNIQUE);

            recs = idx[false, true];
            Tests.Assert(recs.Length == idx.Count);

            i = 0;
            int removedTrue = 0;
            int removedFalse = 0;
            foreach (var r2 in recs)
            {
                var b = r2.ToBool();
                if (i % 3 == 1)
                {
                    idx.Remove(b, r2);
                    if (r2.ToBool())
                        ++removedTrue;
                    else
                        ++removedFalse;
                    r2.Deallocate();
                }
                i++;
            }
            db.Commit();

            count -= (removedTrue + removedFalse);
            falseCount -= removedFalse;
            trueCount -= removedTrue;
            Tests.Assert(idx.Count == count);
            db.Close();
        }

    }
}
