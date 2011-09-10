namespace Volante
{
    using System;

    public class TestMultiFieldIndex : ITest
    {
        public class Root : Persistent
        {
            public IMultiFieldIndex<RecordFullWithProperty> idx;
            public IMultiFieldIndex<RecordFullWithProperty> idxNonUnique;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;

            IDatabase db = config.GetDatabase();
            Tests.Assert(db.Root == null);
            Root root = new Root();

            Tests.AssertDatabaseException(() =>
                { root.idx = db.CreateFieldIndex<RecordFullWithProperty>(new string[] { "NonExistent" }, IndexType.NonUnique); },
                DatabaseException.ErrorCode.INDEXED_FIELD_NOT_FOUND);

            root.idx = db.CreateFieldIndex<RecordFullWithProperty>(new string[] { "Int64Prop", "StrVal" }, IndexType.Unique);
            root.idxNonUnique = db.CreateFieldIndex<RecordFullWithProperty>(new string[] { "Int64Val", "ByteVal" }, IndexType.NonUnique);
            db.Root = root;
            Tests.Assert(root.idx.IndexedClass == typeof(RecordFullWithProperty));
            Tests.Assert(root.idx.KeyField.Name == "Int64Prop");
            Tests.Assert(root.idx.KeyFields[1].Name == "StrVal");

            Tests.AssertDatabaseException(() =>
                { root.idx.Append(new RecordFullWithProperty(0)); },
                DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE);

            RecordFullWithProperty rFirst = null;
            long firstkey = 0;
            foreach (var key in Tests.KeySeq(count))
            {
                RecordFullWithProperty rec = new RecordFullWithProperty(key);
                root.idx.Put(rec);
                root.idxNonUnique.Put(rec);
                if (rFirst == null)
                {
                    rFirst = rec;
                    firstkey = key;
                }
            }
            Tests.Assert(root.idx.Count == count);
            db.Commit();
            Tests.Assert(root.idx.Count == count);
            Tests.Assert(rFirst.IsPersistent());

            Tests.Assert(root.idx.Contains(rFirst));
            Tests.Assert(root.idxNonUnique.Contains(rFirst));

            var rTmp = new RecordFullWithProperty(firstkey);
            Tests.Assert(!root.idx.Contains(rTmp));
            Tests.Assert(!root.idxNonUnique.Contains(rTmp));

            RecordFullWithProperty[] recs = root.idx.ToArray();
            Tests.Assert(recs.Length == count);

            // TODO: figure out why Set() returns null
            var removed = root.idx.Set(rTmp);
            //Tests.Assert(removed == rFirst);
            removed = root.idxNonUnique.Set(rTmp);
            //Tests.Assert(removed == rFirst);

            long minKey = Int32.MaxValue;
            long maxKey = Int32.MinValue;
            foreach (var key in Tests.KeySeq(count))
            {
                String strKey = Convert.ToString(key);
                RecordFullWithProperty rec = root.idx.Get(new Key(new Object[] { key, strKey }));
                Tests.Assert(rec != null && rec.Int64Val == key && rec.StrVal.Equals(strKey));
                if (key < minKey)
                    minKey = key;
                if (key > maxKey)
                    maxKey = key;
            }

            int n = 0;
            string prevStr = "";
            long prevInt = minKey;
            foreach (RecordFullWithProperty rec in root.idx.Range(new Key(minKey, ""),
                                              new Key(maxKey + 1, "???"),
                                              IterationOrder.AscentOrder))
            {
                Tests.Assert(rec.Int64Val > prevInt || rec.Int64Val == prevInt && rec.StrVal.CompareTo(prevStr) > 0);
                prevStr = rec.StrVal;
                prevInt = rec.Int64Val;
                n += 1;
            }
            Tests.Assert(n == count);

            n = 0;
            prevInt = maxKey + 1;
            foreach (RecordFullWithProperty rec in root.idx.Range(new Key(minKey, "", false),
                                              new Key(maxKey + 1, "???", false),
                                              IterationOrder.DescentOrder))
            {
                Tests.Assert(rec.Int64Val < prevInt || rec.Int64Val == prevInt && rec.StrVal.CompareTo(prevStr) < 0);
                prevStr = rec.StrVal;
                prevInt = rec.Int64Val;
                n += 1;
            }
            Tests.Assert(n == count);

            rFirst = root.idx.ToArray()[0];
            Tests.Assert(root.idx.Contains(rFirst));
            Tests.Assert(root.idx.Remove(rFirst));
            Tests.Assert(!root.idx.Contains(rFirst));
            Tests.Assert(!root.idx.Remove(rFirst));

            rFirst = root.idxNonUnique.ToArray()[0];
            Tests.Assert(root.idxNonUnique.Contains(rFirst));
            Tests.Assert(root.idxNonUnique.Remove(rFirst));
            Tests.Assert(!root.idxNonUnique.Contains(rFirst));
            Tests.Assert(!root.idxNonUnique.Remove(rFirst));

            foreach (var o in root.idx.ToArray())
            {
                long key = o.Int64Val;
                String strKey = Convert.ToString(key);
                RecordFullWithProperty rec = root.idx.Get(new Key(new Object[] { key, strKey }));
                Tests.Assert(rec != null && rec.Int64Val == key && rec.StrVal.Equals(strKey));
                Tests.Assert(root.idx.Contains(rec));
                root.idx.Remove(rec);
            }
            Tests.Assert(!root.idx.GetEnumerator().MoveNext());
            Tests.Assert(!root.idx.Reverse().GetEnumerator().MoveNext());

            db.Close();
        }
    }
}
