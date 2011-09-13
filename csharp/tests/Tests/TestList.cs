namespace Volante
{
    using System;

    public class TestListResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan TraverseReadTime;
        public TimeSpan TraverseModifyTime;
        public TimeSpan InsertTime4;
    }

    public class TestList : ITest
    {
        public abstract class LinkNode : Persistent
        {
            public abstract int Number
            {
                get;
                set;
            }

            public abstract LinkNode Next
            {
                get;
                set;
            }
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestListResult();
            config.Result = res;

            var start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            db.Root = db.CreateClass(typeof(LinkNode));
            LinkNode header = (LinkNode)db.Root;
            LinkNode current;
            current = header;
            for (int i = 0; i < count; i++)
            {
                current.Next = (LinkNode)db.CreateClass(typeof(LinkNode));
                current = current.Next;
                current.Number = i;
            }
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            int number = 0; // A variable used to validate the data in list
            current = header;
            while (current.Next != null) // Traverse the whole list in the database
            {
                current = current.Next;
                Tests.Assert(current.Number == number++);
            }
            res.TraverseReadTime = DateTime.Now - start;

            start = DateTime.Now;
            number = 0;
            current = header;
            while (current.Next != null) // Traverse the whole list in the database
            {
                current = current.Next;
                Tests.Assert(current.Number == number++);
                current.Number = -current.Number;
            }
            res.TraverseModifyTime = DateTime.Now - start;
            db.Close();
        }
    }

    public class TestL2List : ITest
    {
        public class Record : L2ListElem<Record>
        {
            public long v;
            public string s;

            public Record()
            {
            }

            public Record(long n)
            {
                v = n;
                s = n.ToString();
            }
        }

        public class Root : Persistent
        {
            public L2List<Record> l;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestListResult();
            config.Result = res;

            var start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            var root = new Root();
            root.l = new L2List<Record>();
            var l = root.l;
            db.Root = root;

            Tests.Assert(null == l.Head);
            Tests.Assert(null == l.Tail);
            Tests.Assert(0 == l.Count);
            foreach (var k in Tests.KeySeq(count))
            {
                Record r = new Record(k);
                if (k % 3 == 0)
                    l.Append(r);
                else if (k % 3 == 1)
                    l.Prepend(r);
                else
                    l.Add(r);
            }
            Tests.Assert(count == l.Count);
            Tests.Assert(null != l.Head);
            Tests.Assert(null != l.Tail);
            Tests.Assert(l.Contains(l.Head));
            Tests.Assert(l.Contains(l.Tail));
            Tests.Assert(!l.Contains(new Record(-1234)));

            var e = l.GetEnumerator();
            Record rFirst = null;
            while (e.MoveNext())
            {
                Tests.Assert(e.Current != null);
                if (null == rFirst)
                {
                    rFirst = e.Current;
                    Tests.Assert(null == rFirst.Prev);
                    Tests.Assert(null != rFirst.Next);
                }
            }
            Tests.AssertException<InvalidOperationException>(
                () => { var tmp = e.Current; });
            Tests.Assert(!e.MoveNext());
            e.Reset();
            Tests.Assert(e.MoveNext());

            l.Remove(l.Head);
            l.Remove(l.Tail);

            l.Clear();
            Tests.Assert(0 == l.Count);
            var rTmp = new Record(0);
            l.Add(rTmp);
            Tests.Assert(rTmp == l.Head);
            Tests.Assert(rTmp == l.Tail);
            var rTmp2 = new Record(1);
            l.Add(rTmp2);
            Tests.Assert(rTmp2 == l.Tail);
            Tests.Assert(rTmp == l.Head);
            Tests.Assert(2 == l.Count);
            db.Commit();
            db.Close();
        }

    }
}
