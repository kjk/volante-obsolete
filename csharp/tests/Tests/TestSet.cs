namespace Volante
{
    using System;
    using System.Collections.Generic;

    public class TestSet : ITest
    {
        public void Run(TestConfig config)
        {
            int i;
            int count = config.Count;
            var res = new TestIndexNumericResult();
            config.Result = res;

            var start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            Tests.Assert(null == db.Root);
            var idx = db.CreateSet<RecordFull>();
            db.Root = idx;
            long val = 1999;
            var recs = new List<RecordFull>();
            var rand = new Random();
            for (i = 0; i < count; i++)
            {
                var r = new RecordFull(val);
                Tests.Assert(!idx.Contains(r));
                idx.Add(r);
                idx.Add(r);
                if (rand.Next(0, 20) == 4 && recs.Count < 10)
                {
                    recs.Add(r);
                }
                Tests.Assert(idx.Contains(r));
                if (i % 100 == 0)
                    db.Commit();
                val = (3141592621L * val + 2718281829L) % 1000000007L;
            }

            Tests.Assert(idx.Count == count);
            db.Commit();
            Tests.Assert(idx.Count == count);
            Tests.Assert(idx.IsReadOnly == false);
            Tests.Assert(idx.ContainsAll(recs));

            var rOne = new RecordFull(val);
            Tests.Assert(!idx.Contains(rOne));
            Tests.Assert(idx.AddAll(new RecordFull[] { rOne }));
            Tests.Assert(!idx.AddAll(recs));
            Tests.Assert(idx.Count == count + 1);
            Tests.Assert(idx.Remove(rOne));
            Tests.Assert(!idx.Remove(rOne));

            Tests.Assert(idx.RemoveAll(recs));
            Tests.Assert(!idx.RemoveAll(recs));
            Tests.Assert(idx.Count == count - recs.Count);
            Tests.Assert(idx.AddAll(recs));
            Tests.Assert(idx.Count == count);
            db.Commit();

            res.InsertTime = DateTime.Now - start;

            start = System.DateTime.Now;
            foreach (var r2 in idx)
            {
                Tests.Assert(idx.Contains(r2));
            }

            idx.Invalidate();

            RecordFull[] recsArr = idx.ToArray();
            Tests.Assert(recsArr.Length == count);
            Array recsArr2 = idx.ToArray(typeof(RecordFull));
            Tests.Assert(recsArr2.Length == count);
            idx.Clear();
            Tests.Assert(idx.Count == 0);
            db.Commit();
            Tests.Assert(idx.Count == 0);
            idx.AddAll(recs);
            Tests.Assert(idx.Count == recs.Count);
            db.Commit();
            Tests.Assert(idx.Count == recs.Count);
            Tests.Assert(idx.GetHashCode() > 0);
            db.Gc();
            db.Commit();
            db.Close();
        }

    }
}
