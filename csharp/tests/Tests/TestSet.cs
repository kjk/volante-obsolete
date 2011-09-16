namespace Volante
{
    using System;
    using System.Collections;
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
            var set = db.CreateSet<RecordFull>();
            db.Root = set;
            long val = 1999;
            var recs = new List<RecordFull>();
            var rand = new Random();
            for (i = 0; i < count; i++)
            {
                var r = new RecordFull(val);
                Tests.Assert(!set.Contains(r));
                set.Add(r);
                set.Add(r);
                if (recs.Count < 10 && rand.Next(0, 20) == 4)
                    recs.Add(r);

                Tests.Assert(set.Contains(r));
                if (i % 100 == 0)
                    db.Commit();
                val = (3141592621L * val + 2718281829L) % 1000000007L;
            }

            Tests.Assert(set.Count == count);
            db.Commit();
            Tests.Assert(set.Count == count);
            Tests.Assert(set.IsReadOnly == false);
            Tests.Assert(set.ContainsAll(recs));

            var rOne = new RecordFull(val);
            Tests.Assert(!set.Contains(rOne));
            Tests.Assert(!set.ContainsAll(new RecordFull[] { rOne }));
            Tests.Assert(set.AddAll(new RecordFull[] { rOne }));
            Tests.Assert(!set.AddAll(recs));
            Tests.Assert(set.Count == count + 1);
            Tests.Assert(set.Remove(rOne));
            Tests.Assert(!set.Remove(rOne));

            Tests.Assert(set.RemoveAll(recs));
            Tests.Assert(!set.RemoveAll(recs));
            Tests.Assert(set.Count == count - recs.Count);
            Tests.Assert(set.AddAll(recs));
            Tests.Assert(set.Count == count);
            db.Commit();

            res.InsertTime = DateTime.Now - start;

            IEnumerator e = set.GetEnumerator();
            Tests.Assert(e != null);

            start = System.DateTime.Now;
            Tests.Assert(!set.Equals(null));
            Tests.Assert(set.Equals(set));

            var set2 = db.CreateSet<RecordFull>();
            Tests.Assert(!set.Equals(set2));
            foreach (var r2 in set)
            {
                Tests.Assert(set.Contains(r2));
                set2.Add(r2);
            }
            Tests.Assert(set.Equals(set2));

            RecordFull[] recsArr = set.ToArray();
            Tests.Assert(recsArr.Length == count);
            set.Clear();
            Tests.Assert(set.Count == 0);
            db.Commit();
            Tests.Assert(set.Count == 0);
            set.Invalidate();
            set.Load();
            set.AddAll(recs);
            Tests.Assert(set.Count == recs.Count);
            db.Commit();
            Tests.Assert(set.Count == recs.Count);
            Tests.Assert(set.GetHashCode() > 0);
            db.Gc();

            // tests for PersistentString
            var ps = new PersistentString();
            Tests.Assert(ps.Get() == "");
            ps = new PersistentString("Hello");
            var s = ps.ToString();
            Tests.Assert(s == "Hello");
            ps.Append("2");
            Tests.Assert("Hello2" == ps.Get());
            ps.Set("Lala");
            Tests.Assert("Lala" == ps.Get());
            string s2 = ps;
            Tests.Assert("Lala" == s2);
            PersistentString ps2 = "Lulu";
            Tests.Assert(ps2.Get() == "Lulu");
            db.Root = ps;
            set.Deallocate();
            db.Gc();
            db.Commit();
            db.Close();
        }

    }
}
