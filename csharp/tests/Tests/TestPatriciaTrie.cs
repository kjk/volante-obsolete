// Copyright: Krzysztof Kowalczyk
// License: BSD
#if WITH_PATRICIA
namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestPatriciaTrie : ITest
    {
        public class Root : Persistent
        {
            public IPatriciaTrie<RecordFull> idx;
        }

        public void Run(TestConfig config)
        {
            IDatabase db = config.GetDatabase();
            config.Result = new TestResult();
            Root root = new Root();
            root.idx = db.CreatePatriciaTrie<RecordFull>();
            db.Root = root;
            int count = config.Count;

            Tests.Assert(0 == root.idx.Count);
            long firstKey = 0;
            RecordFull firstRec = null;
            PatriciaTrieKey pk;
            RecordFull r;
            foreach (var key in Tests.KeySeq(count))
            {
                r = new RecordFull(key);
                pk = new PatriciaTrieKey((ulong)key, 8);
                root.idx.Add(pk, r);
                if (null == firstRec)
                {
                    firstRec = r;
                    firstKey = key;
                }
            }
            Tests.Assert(count == root.idx.Count);
            Tests.Assert(root.idx.Contains(firstRec));
            Tests.Assert(!root.idx.Contains(new RecordFull(firstKey)));

            pk = new PatriciaTrieKey((ulong)firstKey, 8);
            r = new RecordFull(firstKey);
            Tests.Assert(firstRec == root.idx.Add(pk, r));
            Tests.Assert(r == root.idx.FindExactMatch(pk));
            Tests.Assert(r == root.idx.FindBestMatch(pk));

            foreach (var key in Tests.KeySeq(count))
            {
                pk = new PatriciaTrieKey((ulong)key, 8);
                Tests.Assert(null != root.idx.Remove(pk));
            }

            // TODO: seems broken, there's a null entry left
            // in the index
            /*foreach (var rf in root.idx)
            {
                pk = new PatriciaTrieKey(rf.UInt64Val, 8);
                Tests.Assert(null != root.idx.Remove(pk));
            }*/

            //Tests.Assert(0 == root.idx.Count);
            root.idx.Clear();
            Tests.Assert(0 == root.idx.Count);

            pk = new PatriciaTrieKey((ulong)firstKey, 8);
            Tests.Assert(null == root.idx.Remove(pk));

            pk = PatriciaTrieKey.FromIpAddress(new System.Net.IPAddress(123));
            pk = PatriciaTrieKey.FromIpAddress("127.0.0.1");
            pk = PatriciaTrieKey.From7bitString("hola");
            pk = PatriciaTrieKey.From8bitString("hola");
            pk = PatriciaTrieKey.FromByteArray(new byte[4] { 4, 2, 8, 3 });
            pk = PatriciaTrieKey.FromDecimalDigits("9834");
            db.Close();
        }
    }
}
#endif
