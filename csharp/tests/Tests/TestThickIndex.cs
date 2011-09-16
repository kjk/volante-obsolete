namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestThickIndex : ITest
    {
        public class Root : Persistent
        {
            public IIndex<string, RecordFull> strIdx;
            public IIndex<byte, RecordFull> byteIdx;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestIndexResult();
            config.Result = res;
            IDatabase db = config.GetDatabase();

            Root root = (Root)db.Root;
            Tests.Assert(null == root);
            root = new Root();
            root.strIdx = db.CreateThickIndex<string, RecordFull>();
            root.byteIdx = db.CreateThickIndex<byte, RecordFull>();
            db.Root = root;
            Tests.Assert(typeof(string) == root.strIdx.KeyType);
            Tests.Assert(typeof(byte) == root.byteIdx.KeyType);

            int startWithOne = 0;
            int startWithFive = 0;
            string strFirst = "z";
            string strLast = "0";

            int n = 0;
            var inThickIndex = new Dictionary<byte, List<RecordFull>>();
            foreach (var key in Tests.KeySeq(count))
            {
                RecordFull rec = new RecordFull(key);
                if (rec.StrVal[0] == '1')
                    startWithOne += 1;
                else if (rec.StrVal[0] == '5')
                    startWithFive += 1;
                if (rec.StrVal.CompareTo(strFirst) < 0)
                    strFirst = rec.StrVal;
                else if (rec.StrVal.CompareTo(strLast) > 0)
                    strLast = rec.StrVal;
                root.strIdx.Put(rec.StrVal, rec);
                root.byteIdx.Put(rec.ByteVal, rec);
                n++;
                if (n % 100 == 0)
                    db.Commit(); ;
            }
            db.Commit();

            Tests.Assert(root.strIdx.Count == count);
            Tests.Assert(root.byteIdx.Count <= count);

            Tests.AssertDatabaseException(() =>
                { root.strIdx.RemoveKey(""); },
                DatabaseException.ErrorCode.KEY_NOT_UNIQUE);

            Tests.AssertDatabaseException(() =>
                { root.strIdx.Remove(new Key("")); },
                DatabaseException.ErrorCode.KEY_NOT_UNIQUE);

            foreach (var mk in inThickIndex.Keys)
            {
                var list = inThickIndex[mk];
                while (list.Count > 1)
                {
                    var el = list[0];
                    list.Remove(el);
                    root.byteIdx.Remove(el.ByteVal, el);
                }
            }

            RecordFull[] recs;
            foreach (var key in Tests.KeySeq(count))
            {
                RecordFull rec1 = root.strIdx[Convert.ToString(key)];
                recs = root.byteIdx[rec1.ByteVal, rec1.ByteVal];
                Tests.Assert(rec1 != null && recs.Length >= 1);
                Tests.Assert(rec1.ByteVal == recs[0].ByteVal);
            }

            // test for non-existent key
            Tests.Assert(null == root.strIdx.Get("-122"));

            recs = root.byteIdx.ToArray();
            Tests.Assert(recs.Length == root.byteIdx.Count);

            var prevByte = byte.MinValue;
            n = 0;
            foreach (RecordFull rec in root.byteIdx)
            {
                Tests.Assert(rec.ByteVal >= prevByte);
                prevByte = rec.ByteVal;
                n += 1;
            }
            Tests.Assert(n == count);

            String prevStrKey = "";
            n = 0;
            foreach (RecordFull rec in  root.strIdx)
            {
                Tests.Assert(rec.StrVal.CompareTo(prevStrKey) >= 0);
                prevStrKey = rec.StrVal;
                n += 1;
            }
            IDictionaryEnumerator de = root.strIdx.GetDictionaryEnumerator();
            n = VerifyDictionaryEnumerator(de, IterationOrder.AscentOrder);
            Tests.Assert(n == count);

            string mid = "0";
            string max = long.MaxValue.ToString(); ;
            de = root.strIdx.GetDictionaryEnumerator(new Key(mid), new Key(max), IterationOrder.DescentOrder);
            VerifyDictionaryEnumerator(de, IterationOrder.DescentOrder);

            Tests.AssertDatabaseException(
                () => root.byteIdx.PrefixSearch("1"),
                DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            // TODO: FAILED_TEST broken for altbtree, returns no results
            if (!config.AltBtree)
            {
                recs = root.strIdx.GetPrefix("1");
                Tests.Assert(recs.Length > 0);
                foreach (var r in recs)
                {
                    Tests.Assert(r.StrVal.StartsWith("1"));
                }
            }
            recs = root.strIdx.PrefixSearch("1");
            Tests.Assert(startWithOne == recs.Length);
            foreach (var r in recs)
            {
                Tests.Assert(r.StrVal.StartsWith("1"));
            }
            recs = root.strIdx.PrefixSearch("5");
            Tests.Assert(startWithFive == recs.Length);
            foreach (var r in recs)
            {
                Tests.Assert(r.StrVal.StartsWith("5"));
            }
            recs = root.strIdx.PrefixSearch("0");
            Tests.Assert(0 == recs.Length);

            recs = root.strIdx.PrefixSearch("a");
            Tests.Assert(0 == recs.Length);

            recs = root.strIdx.PrefixSearch(strFirst);
            Tests.Assert(recs.Length >= 1);
            Tests.Assert(recs[0].StrVal == strFirst);
            foreach (var r in recs)
            {
                Tests.Assert(r.StrVal.StartsWith(strFirst));
            }

            recs = root.strIdx.PrefixSearch(strLast);
            Tests.Assert(recs.Length == 1);
            Tests.Assert(recs[0].StrVal == strLast);

            n = 0;
            foreach (var key in Tests.KeySeq(count))
            {
                n++;
                if (n % 3 == 0)
                    continue;
                string strKey = key.ToString();
                RecordFull rec = root.strIdx.Get(strKey);
                root.strIdx.Remove(strKey, rec);
            }
            root.byteIdx.Clear();

            int BTREE_THRESHOLD = 128;
            byte bKey = 1;
            for (int i = 0; i < BTREE_THRESHOLD + 10; i++)
            {
                RecordFull r = new RecordFull(0);
                if (i == 0)
                {
                    root.byteIdx[bKey] = r;
                    continue;
                }
                if (i == 1)
                {
                    root.byteIdx.Set(bKey, r);
                    continue;
                }
                root.byteIdx.Put(bKey, r);
            }

            Tests.AssertDatabaseException(
                () => root.byteIdx.Set(bKey, new RecordFull(1)),
                DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
            Tests.AssertDatabaseException(
                () => root.byteIdx.Get(bKey),
                DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
            recs = root.byteIdx.ToArray();
            foreach (var r in recs)
            {
                root.byteIdx.Remove(bKey, r);
            }
            Tests.AssertDatabaseException(
                () => root.byteIdx.Remove(bKey, new RecordFull(0)),
                DatabaseException.ErrorCode.KEY_NOT_FOUND);
            IEnumerator<RecordFull> e = root.byteIdx.GetEnumerator();
            while (e.MoveNext()) { }
            Tests.Assert(!e.MoveNext());
            db.Close();
        }

        static int VerifyDictionaryEnumerator(IDictionaryEnumerator de, IterationOrder order)
        {
            string prev = "";
            if (order == IterationOrder.DescentOrder)
                prev = "9999999999999999999";
            int i = 0;
            while (de.MoveNext())
            {
                DictionaryEntry e1 = (DictionaryEntry)de.Current;
                DictionaryEntry e2 = de.Entry;
                Tests.Assert(e1.Equals(e2));
                string k = (string)e1.Key;
                string k2 = (string)de.Key;
                Tests.Assert(k == k2);
                RecordFull v1 = (RecordFull)e1.Value;
                RecordFull v2 = (RecordFull)de.Value;
                Tests.Assert(v1.Equals(v2));
                Tests.Assert(v1.StrVal == k);
                if (order == IterationOrder.AscentOrder)
                    Tests.Assert(k.CompareTo(prev) >= 0);
                else
                    Tests.Assert(k.CompareTo(prev) <= 0);
                prev = k;
                i++;
            }
            Tests.VerifyDictionaryEnumeratorDone(de);
            return i;
        }
    }
}
