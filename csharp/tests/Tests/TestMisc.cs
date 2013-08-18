// Copyright: Krzysztof Kowalczyk
// License: BSD
// Smaller test cases that don't deserve their own file

namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class TestRemove00 : ITest
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
            }, DatabaseException.ErrorCode.KEY_NOT_UNIQUE);
            root.idx = null;
            root.Modify();
            db.Commit();
            n = Tests.DbInstanceCount(db, typeof(Record));
            Tests.Assert(0 == n);
            db.Close();
        }
    }

    // test that deleting an object referenced by another objects
    // corrupts the database.
    public class TestCorrupt00 : ITest
    {
        public class Root : Persistent
        {
            public RecordFull r;
        }

        public void Run(TestConfig config)
        {
            IDatabase db = config.GetDatabase();
            Root root = new Root();
            var r = new RecordFull();
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
            }, DatabaseException.ErrorCode.DELETED_OBJECT);
            r = root.r;
            db.Close();
        }
    }

    // force recover by not closing the database propery
    public class TestCorrupt01 : ITest
    {
        public class Root : Persistent
        {
            public IIndex<string, RecordFull> idx;
        }

        public void Run(TestConfig config)
        {
            Debug.Assert(!config.IsTransient);

            IDatabase db = DatabaseFactory.CreateDatabase();
            Tests.AssertDatabaseException(
                () => { var r = db.Root; },
                DatabaseException.ErrorCode.DATABASE_NOT_OPENED);

            db = config.GetDatabase();
            Root root = new Root();
            var idx = db.CreateIndex<string, RecordFull>(IndexType.NonUnique);
            root.idx = idx;
            db.Root = root;
            db.Commit();

            for (int i = 0; i < 10; i++)
            {
                var r = new RecordFull(i);
                idx.Put(r.StrVal, r);
            }
            var f = db.File;
            OsFile of = (OsFile)f;
            of.Close();

            IDatabase db2 = config.GetDatabase(false);
            try
            {
                db.Close();
            }
            catch
            {
            }
            db2.Close();
        }
    }

    // Corner cases for key search
    public class TestIndexRangeSearch : ITest
    {
        public class Record : Persistent
        {
            public long lval;
            public byte[] data;

            public Record()
            {
                data = new byte[4] { 1, 4, 0, 3 };                
            }
        }

        public class Root : Persistent
        {
            public IIndex<long, Record> idx;
        }

        public void Run(TestConfig config)
        {
            Record[] recs;
            IDatabase db = config.GetDatabase();
            Tests.Assert(db.IsOpened);
            Tests.AssertDatabaseException(() =>
                { db.Open(new NullFile(), 0); }, DatabaseException.ErrorCode.DATABASE_ALREADY_OPENED);

            var expectedData = new byte[4] { 1, 4, 0, 3 };

            Root root = new Root();
            root.idx = db.CreateIndex<long, Record>(IndexType.Unique);
            db.Root = root;
            root.idx[1] = new Record { lval = 1 };
            root.idx[2] = new Record { lval = 2 };
            root.idx[4] = new Record { lval = 4 };
            root.idx[5] = new Record { lval = 5 };
            db.Commit();
            Tests.Assert(db.DatabaseSize > 0);
            recs = root.idx[-1, -1];
            Tests.Assert(recs.Length == 0);
            recs = root.idx[0, 0];
            Tests.Assert(recs.Length == 0);
            recs = root.idx[1, 1];
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = root.idx[2, 2];
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = root.idx[3, 3];
            Tests.Assert(recs.Length == 0);
            recs = root.idx[5, 5];
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = root.idx[6, 6];
            Tests.Assert(recs.Length == 0);
            recs = root.idx[long.MinValue, long.MaxValue];
            Tests.Assert(recs.Length == 4);
            recs = root.idx[1, 5];
            Tests.Assert(recs.Length == 4);
            recs = root.idx[long.MinValue, long.MinValue];
            Tests.Assert(recs.Length == 0);
            recs = root.idx[long.MaxValue, long.MaxValue];
            Tests.Assert(recs.Length == 0);

            recs = GetInRange(root.idx, -1);
            Tests.Assert(recs.Length == 0);
            recs = GetInRange(root.idx, 0);
            Tests.Assert(recs.Length == 0);
            recs = GetInRange(root.idx, 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = GetInRange(root.idx, 2);
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = GetInRange(root.idx, 3);
            Tests.Assert(recs.Length == 0);
            recs = GetInRange(root.idx, 5);
            Tests.Assert(recs.Length == 1);
            Tests.Assert(Tests.ByteArraysEqual(recs[0].data, expectedData));
            recs = GetInRange(root.idx, 6);
            Tests.Assert(recs.Length == 0);

            db.Close();
            Tests.Assert(!db.IsOpened);
        }

        Record[] GetInRange(IIndex<long,Record> idx, long range)
        {
            List<Record> recs = new List<Record>();
            foreach (var r in idx.Range(range, range))
            {
                recs.Add(r);
            }
            return recs.ToArray();
        }
    }
}