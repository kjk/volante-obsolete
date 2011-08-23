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

    public class TestList
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
}
