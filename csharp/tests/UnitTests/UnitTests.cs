using System;
using NachoDB;
using System.IO;

public class UnitTests
{
    internal static int totalTests = 0;
    internal static int failedTests = 0;

    public static int TotalTests {
        get { return totalTests; }
    }

    public static int FailedTests {
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
        catch {}
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

public class UnitTest1
{
#if USE_GENERICS
    public static void CheckStrings(Index<string,StringInt> root, string[] strs)
#else
    public static void CheckStrings(Index root, string[] strs)
#endif
    {
        int no = 1;
        foreach (string s in strs)
        {
#if USE_GENERICS
            StringInt o = root[s];
#else
            StringInt o = (StringInt)root[s];
#endif
            UnitTests.AssertThat(o.no == no++);
        }
    }

    public static void Run()
    {
        string dbName = @"testblob.dbs";
        UnitTests.SafeDeleteFile(dbName);
        Storage db = StorageFactory.CreateStorage();
        db.Open(dbName);
#if USE_GENERICS
        Index<string,StringInt> root = (Index<string,StringInt>)db.Root;
#else
        Index root = (Index)db.Root;
#endif
        UnitTests.AssertThat(null == root);
#if USE_GENERICS
        root = db.CreateIndex<string,StringInt>(true);
#else
        root = db.CreateIndex(typeof(string), true);
#endif
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
#if USE_GENERICS
        root = (Index<string,StringInt>)db.Root;
#else
        root = (Index)db.Root;
#endif
        UnitTests.AssertThat(null != root);
        CheckStrings(root, strs);
        db.Close();
    }
}

public class UnitTest2
{
    public class Root : Persistent
    {
#if USE_GENERICS
        public Index<string, Record> strIndex;
#else
        public Index strIndex;
#endif
    }
    
    public static void Run()
    {
        string dbName = @"testidx.dbs";
        UnitTests.SafeDeleteFile(dbName);
        
        Storage db = StorageFactory.CreateStorage();
        db.AlternativeBtree = true;
        db.Open(dbName);
        Root root = (Root)db.Root;
        UnitTests.AssertThat(null == root);
        root = new Root();
#if USE_GENERICS
        root.strIndex = db.CreateIndex<string,Record>(true);
#else
        root.strIndex = db.CreateIndex(typeof(String), true);
#endif
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
                UnitTests.AssertThat(s == expectedStr);

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
        UnitTests.AssertThat(gotException);
        UnitTests.AssertThat(n == 0);

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
                UnitTests.AssertThat(s == expectedStr);

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
        UnitTests.AssertThat(gotException);
        
        db.Close();
    }

}

public class UnitTestXml
{
    class Record : Persistent
    {
        internal String strKey;
        internal long   intKey;
        internal double realKey;
    }

    class Root : Persistent
    {
        internal Index    strIndex;
        internal FieldIndex intIndex;
    }

    internal static int pagePoolSize = 32 * 1024 * 1024;

    public static void Run(bool useAltBtree)
    {
        string dbName = @"testxml.dbs";
        UnitTests.SafeDeleteFile(dbName);
        Storage db = StorageFactory.CreateStorage();
        db.AlternativeBtree = useAltBtree;
        db.Open(dbName, pagePoolSize);
        Root root = (Root)db.Root;
        UnitTests.AssertThat(null == root);
        root = new Root();
        root.strIndex = db.CreateIndex(typeof(System.String), true);
        root.intIndex = db.CreateFieldIndex(typeof(Record), "intKey", true);
        db.Root = root;
        DateTime start = DateTime.Now;
        long key = 1999;
        FieldIndex intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        int i, nRecords=100;
        for (i = 0; i < nRecords; i++)
        {
            Record rec = new Record();
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = System.Convert.ToString(key);
            rec.realKey = (double)key;
            strIndex.Put(new Key(rec.strKey), rec);
            intIndex.Put(rec);
        }
        db.Commit();

        System.IO.StreamWriter writer = new System.IO.StreamWriter("test.xml");
        db.ExportXML(writer);
        writer.Close();
        db.Close();

        UnitTests.SafeDeleteFile(dbName);
        db.Open(dbName, pagePoolSize);
        System.IO.StreamReader reader = new System.IO.StreamReader("test.xml");
        db.ImportXML(reader);
        reader.Close();

        root = (Root)db.Root;
        strIndex = root.strIndex;
        intIndex = root.intIndex;

        key = 1999;
        for (i = 0; i < nRecords; i++)
        {
            key = (3141592621L * key + 2718281829L) % 1000000007L;
            String strKey = System.Convert.ToString(key);
            Record rec1 = (Record) strIndex.Get(new Key(strKey));
            Record rec2 = (Record) intIndex.Get(new Key(key));
            //Record rec3 = compoundIndex.Get(new Key(strKey, key));
            UnitTests.AssertThat(rec1 != null);
            UnitTests.AssertThat(rec1 == rec2);
            UnitTests.AssertThat(rec1.intKey == key);
            UnitTests.AssertThat(rec1.realKey == (double)key);
            UnitTests.AssertThat(strKey.Equals(rec1.strKey));
            /*
            Debug.Assert(rec1 == rec3);
            */
        }
        db.Close();
    }
}

public class UnitTestsRunner
{ 
    public static void Main(string[] args) 
    {
        UnitTestXml.Run(false);
        UnitTestXml.Run(true);
        UnitTest1.Run();
        UnitTest2.Run();
        Console.WriteLine(String.Format("Failed {0} out of {1} tests", UnitTests.FailedTests, UnitTests.TotalTests));
    }
}

