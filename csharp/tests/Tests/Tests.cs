namespace Volante
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;

    public class Tests
    {
        internal static int totalTests = 0;
        internal static int failedTests = 0;

        public static int TotalTests
        {
            get { return totalTests; }
        }

        public static int FailedTests
        {
            get { return failedTests; }
        }

        public static void AssertThat(bool cond)
        {
            totalTests += 1;
            if (cond) return;
            failedTests += 1;
            // TODO: record callstacks of all failed exceptions
        }

        public static void SafeDeleteFile(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch { }
        }
    }

    public class Record : Persistent
    {
        public string strKey;
        public long intKey;
    }

    public class StringInt : Persistent
    {
        public string s;
        public int no;
        public StringInt()
        {
        }
        public StringInt(string s, int no)
        {
            this.s = s;
            this.no = no;
        }
    }

    public class Test1
    {
        public static void CheckStrings(Index<string, StringInt> root, string[] strs)
        {
            int no = 1;
            foreach (string s in strs)
            {
                StringInt o = root[s];
                Tests.AssertThat(o.no == no++);
            }
        }

        public static void Run(bool useAltBtree)
        {
            string dbName = @"testblob.dbs";
            Tests.SafeDeleteFile(dbName);
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName);
            Index<string, StringInt> root = (Index<string, StringInt>)db.Root;
            Tests.AssertThat(null == root);
            root = db.CreateIndex<string, StringInt>(true);
            db.Root = root;

            int no = 1;
            string[] strs = new string[] { "one", "two", "three", "four" };
            foreach (string s in strs)
            {
                var o = new StringInt(s, no++);
                root[s] = o;
            }

            CheckStrings(root, strs);
            db.Close();

            db = StorageFactory.CreateStorage();
            db.Open(dbName);
            root = (Index<string, StringInt>)db.Root;
            Tests.AssertThat(null != root);
            CheckStrings(root, strs);
            db.Close();
        }
    }

    public class Test2
    {
        public class Root : Persistent
        {
            public Index<string, Record> strIndex;
        }

        public static void Run(bool useAltBtree)
        {
            string dbName = @"testidx.dbs";
            Tests.SafeDeleteFile(dbName);

            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName);
            Root root = (Root)db.Root;
            Tests.AssertThat(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            db.Root = root;
            int no = 0;
            string[] strs = new string[] { "one", "two", "three", "four" };
            foreach (string s in strs)
            {
                Record o = new Record();
                o.strKey = s;
                o.intKey = no++;
                root.strIndex[s] = o;
            }
            db.Commit();

            // Test that modyfing an index while traversing it throws an exception
            // Tests AltBtree.BtreeEnumerator
            long n = -1;
            bool gotException = false;
            try
            {
                foreach (Record r in root.strIndex)
                {
                    n = r.intKey;
                    string expectedStr = strs[n];
                    string s = r.strKey;
                    Tests.AssertThat(s == expectedStr);

                    if (n == 0)
                    {
                        Record o = new Record();
                        o.strKey = "five";
                        o.intKey = 5;
                        root.strIndex[o.strKey] = o;
                    }
                }
            }
            catch (InvalidOperationException)
            {
                gotException = true;
            }
            Tests.AssertThat(gotException);
            Tests.AssertThat(n == 0);

            // Test that modyfing an index while traversing it throws an exception
            // Tests AltBtree.BtreeSelectionIterator

            Key keyStart = new Key("four", true);
            Key keyEnd = new Key("three", true);
            gotException = false;
            try
            {
                foreach (Record r in root.strIndex.Range(keyStart, keyEnd, IterationOrder.AscentOrder))
                {
                    n = r.intKey;
                    string expectedStr = strs[n];
                    string s = r.strKey;
                    Tests.AssertThat(s == expectedStr);

                    Record o = new Record();
                    o.strKey = "six";
                    o.intKey = 6;
                    root.strIndex[o.strKey] = o;
                }
            }
            catch (InvalidOperationException)
            {
                gotException = true;
            }
            Tests.AssertThat(gotException);

            db.Close();
        }

    }

}

