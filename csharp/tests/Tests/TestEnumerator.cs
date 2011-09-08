namespace Volante
{
    using System;

    public class TestEnumeratorResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan IterationTime;
    }

    public class TestEnumerator : ITest
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
        }

        class Indices : Persistent
        {
            internal IIndex<string, Record> strIndex;
            internal IIndex<long, Record> intIndex;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestEnumeratorResult();
            config.Result = res;

            var start = DateTime.Now;

            IDatabase db = config.GetDatabase();
            Indices root = (Indices)db.Root;
            Tests.Assert(root == null);
            root = new Indices();
            root.strIndex = db.CreateIndex<string, Record>(IndexType.NonUnique);
            root.intIndex = db.CreateIndex<long, Record>(IndexType.NonUnique);
            db.Root = root;
            IIndex<long, Record> intIndex = root.intIndex;
            IIndex<string, Record> strIndex = root.strIndex;
            Record[] records;

            long key = 1999;
            int i, j;
            for (i = 0; i < count; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec = new Record();
                rec.intKey = key;
                rec.strKey = Convert.ToString(key);
                for (j = (int)(key % 10); --j >= 0; )
                {
                    intIndex[rec.intKey] = rec;
                    strIndex[rec.strKey] = rec;
                }
            }
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            key = 1999;
            for (i = 0; i < count; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Key fromInclusive = new Key(key);
                Key fromInclusiveStr = new Key(Convert.ToString(key));
                Key fromExclusive = new Key(key, false);
                Key fromExclusiveStr = new Key(Convert.ToString(key), false);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Key tillInclusive = new Key(key);
                Key tillInclusiveStr = new Key(Convert.ToString(key));
                Key tillExclusive = new Key(key, false);
                Key tillExclusiveStr = new Key(Convert.ToString(key), false);

                // int key ascent order
                records = intIndex.Get(fromInclusive, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(fromInclusive, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(fromInclusive, null);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, null);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(null, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.Get(null, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = intIndex.ToArray();
                j = 0;
                foreach (Record rec in intIndex)
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                // int key descent order
                records = intIndex.Get(fromInclusive, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(fromInclusive, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(fromExclusive, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(fromExclusive, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(fromInclusive, null);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(fromExclusive, null);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(null, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.Get(null, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = intIndex.ToArray();
                j = records.Length;
                foreach (Record rec in intIndex.Reverse())
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                // str key ascent order
                records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(fromInclusiveStr, null);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, null);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(null, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.Get(null, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.AscentOrder))
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                records = strIndex.ToArray();
                j = 0;
                foreach (Record rec in strIndex)
                {
                    Tests.Assert(rec == records[j++]);
                }
                Tests.Assert(j == records.Length);

                // str key descent order
                records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(fromInclusiveStr, null);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, null);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(null, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.Get(null, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.DescentOrder))
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

                records = strIndex.ToArray();
                j = records.Length;
                foreach (Record rec in strIndex.Reverse())
                {
                    Tests.Assert(rec == records[--j]);
                }
                Tests.Assert(j == 0);

            }
            res.IterationTime = DateTime.Now - start;

            strIndex.Clear();
            intIndex.Clear();

            Tests.Assert(!strIndex.GetEnumerator().MoveNext());
            Tests.Assert(!intIndex.GetEnumerator().MoveNext());
            Tests.Assert(!strIndex.Reverse().GetEnumerator().MoveNext());
            Tests.Assert(!intIndex.Reverse().GetEnumerator().MoveNext());
            db.Commit();
            db.Gc();
            db.Close();
        }
    }
}
