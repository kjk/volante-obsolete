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
            Record r;
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
            for (i = 0; i < count; i++)
            {
                r = new Record();
                val = (3141592621L * val + 2718281829L) % 1000000007L;
                r.val = val;
                idx.Put(r.ToBool(), r);
                if (i % 1000 == 0)
                    db.Commit();
            }
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
