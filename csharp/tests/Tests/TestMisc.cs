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

    // test that deleting an object referenced by another objects
    // corrupts the database
    public class TestCorrupt00
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
            public Record r;
        }

        public void Run(TestConfig config)
        {
            int n;
            IDatabase db = config.GetDatabase();
            Root root = new Root();
            Record r = new Record();
            root.r = r;
            db.Root = root;
            db.Commit();
            // delete the object from the database
            r.Deallocate();
            db.Commit();
            db.Close();

            db = config.GetDatabase(false);
            // r was explicitly deleted from the database but it's
            // still referenced by db.Root. Loading root object will
            // try to recursively load Record object but since it's
            // been deleted, we should get an exception
            Tests.AssertDatabaseException(() =>
            {
                root = (Root)db.Root;
            }, DatabaseError.ErrorCode.DELETED_OBJECT);
            r = root.r;
            db.Close();
        }
    }

}