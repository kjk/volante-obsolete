// Copyright: Krzysztof Kowalczyk
// License: BSD
// Smaller test cases that don't deserve their own file

namespace Volante
{
    using System;

    public class TestRemove00
    {
        public class Record : Persistent
        {
            public DateTime dt;
            public long lval;

            // persistent objects require empty constructor
            public Record()
            {
            }

            public Record(long val)
            {
                this.lval = val;
                this.dt = DateTime.Now;
            }
        }

        public class Root : Persistent
        {
            public IIndex<long, Record> idx;
        }

        // Test that IIndex.Remove(K key) throws on non-unique index.
        // Test that clearing the index removes all objects pointed by
        // the index
        public void Run(TestConfig config)
        {
            int n;
            IDatabase db = config.GetDatabase();
            Root root = new Root();
            root.idx = db.CreateIndex<long, Record>(IndexType.NonUnique);
            db.Root = root;

            root.idx.Put(1, new Record(1));
            root.idx.Put(2, new Record(1));
            root.idx.Put(3, new Record(2));
            db.Commit();
            n = Tests.DbInstanceCount(db, typeof(Record));
            Tests.Assert(3 == n);
            Tests.AssertDatabaseException(() =>
            {
                root.idx.Remove(new Key((long)1));
            }, DatabaseError.ErrorCode.KEY_NOT_UNIQUE);
            root.idx = null;
            root.Modify();
            db.Commit();
            n = Tests.DbInstanceCount(db, typeof(Record));
            Tests.Assert(0 == n);
            db.Close();
        }
    }

}