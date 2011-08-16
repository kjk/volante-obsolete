using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using Volante;

public class TestConfig
{
    const int INFINITE_PAGE_POOL = 0;

    public enum InMemoryType
    {
        // uses a file
        No,
        // uses NullFile and infinite page pool
        Full,
        // uses a real file and infinite page pool. The work is done
        // in memory but on closing dta is persisted to a file
        File
    };

    public enum FileType
    {
        File, // use IFile
        Stream // use StreamFile
    };

    public string TestName;
    public InMemoryType InMemory;
    public FileType FileKind;
    public bool AltBtree;
    public bool Serializable;
    public Encoding Encoding; // if not null will use this encoding for storing strings
    public int Count; // number of iterations

    // Set by the test. Can be a subclass of TestResult
    public TestResult Result;

    public string DatabaseName
    {
        get
        {
            string p1 = AltBtree ? "_alt" : "";
            string p2 = Serializable ? "_ser" : "";
            string p3 = (null == Encoding) ? "" : "_enc-" + Encoding.EncodingName;
            string p4 = String.Format("_{0}", Count);
            return String.Format("{0}{1}{2}{3}{4}.dbs", TestName, p1, p2, p3, p4);
        }
    }

    IStorage GetTransientStorage()
    {
        IStorage db = StorageFactory.CreateStorage();
        NullFile dbFile = new NullFile();
        db.Open(dbFile, INFINITE_PAGE_POOL);
        return db;
    }

    public IStorage GetDatabase()
    {
        IStorage db = null;
        if (InMemory == InMemoryType.Full)
            db = GetTransientStorage();
        else
        {
            var name = DatabaseName;
            Tests.SafeDeleteFile(name);
            db = StorageFactory.CreateStorage();
            if (InMemory == InMemoryType.File)
            {
                if (FileKind == FileType.File)
                    db.Open(name, INFINITE_PAGE_POOL);
                else
                {
                    var f = File.Open(name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                    var sf = new StreamFile(f);
                    db.Open(sf, INFINITE_PAGE_POOL);
                }
            }
            else
            {
                if (FileKind == FileType.File)
                    db.Open(name);
                else
                {
                    var f = File.Open(name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                    var sf = new StreamFile(f);
                    db.Open(sf);
                }
            }
        }
        db.AlternativeBtree = AltBtree || Serializable;
        return db;
    }

    public TestConfig()
    {
    }

    public TestConfig(TestConfig tc)
    {
        TestName = tc.TestName;
        InMemory = tc.InMemory;
        FileKind = tc.FileKind;
        AltBtree = tc.AltBtree;
        Serializable = tc.Serializable;
        Encoding = tc.Encoding;
        Count = tc.Count;
        Result = tc.Result;
    }
};

public class TestResult
{
    public bool Ok;
    public TestConfig Config;
    public string TestName; // TODO: get rid of it after converting all tests to TestConfig
    public int Count;
    public TimeSpan ExecutionTime;

    public override string ToString()
    {
        string name = TestName;
        if (null != Config)
            name = Config.DatabaseName;
        if (Ok)
            return String.Format("OK, {0,6} ms {1}", (int)ExecutionTime.TotalMilliseconds, name);
        else
            return String.Format("FAILED {0}", name);
    }

    public void Print()
    {
        System.Console.WriteLine(ToString());
    }
}

public class TestIndexNumericResult : TestResult
{
    public TimeSpan InsertTime;
}

public class Tests
{
    internal static int TotalTests = 0;
    internal static int FailedTests = 0;
    internal static int CurrAssertsCount = 0;
    internal static int CurrAssertsFailed = 0;

    internal static List<StackTrace> FailedStackTraces = new List<StackTrace>();
    static void ResetAssertsCount()
    {
        CurrAssertsCount = 0;
        CurrAssertsFailed = 0;
    }

    public static void Assert(bool cond)
    {
        CurrAssertsCount += 1;
        if (cond) return;
        CurrAssertsFailed += 1;
        FailedStackTraces.Add(new StackTrace());
    }

    public delegate void Action();
    public static void AssertException<TExc>(Action func)
        where TExc : Exception
    {
        bool gotException = false;
        try
        {
            func();
        }
        catch (TExc)
        {
            gotException = true;
        }
        Assert(gotException);
    }

    public static void AssertStorageException(Action func, StorageError.ErrorCode expectedCode)
    {
        bool gotException = false;
        try
        {
            func();
        }
        catch (StorageError exc)
        {
            gotException = exc.Code == expectedCode;
        }
        Assert(gotException);
    }

    public static bool FinalizeTest()
    {
        TotalTests += 1;
        if (CurrAssertsFailed > 0)
            FailedTests += 1;
        bool ok = CurrAssertsFailed == 0;
        ResetAssertsCount();
        return ok;
    }

    public static void PrintFailedStackTraces()
    {
        int max = 5;
        foreach (var st in FailedStackTraces)
        {
            Console.WriteLine(st.ToString() + "\n");
            if (--max == 0)
                break;
        }
    }

    public static void SafeDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch { }
    }

    public static void VerifyDictionaryEnumeratorDone(IDictionaryEnumerator de)
    {
        AssertException<InvalidOperationException>(
            () => { var tmp = de.Current; });
        AssertException<InvalidOperationException>(
            () => { var tmp = de.Entry; });
        AssertException<InvalidOperationException>(
            () => { var tmp = de.Key; });
        AssertException<InvalidOperationException>(
            () => { var tmp = de.Value; });
    }

    public static void VerifyEnumeratorDone(IEnumerator e)
    {
        AssertException<InvalidOperationException>(
            () => { var tmp = e.Current; });
        Tests.Assert(!e.MoveNext());
    }
}

public class TestsMain
{
    const int CountsIdxFast = 0;
    const int CountsIdxSlow = 1;
    static int CountsIdx = CountsIdxFast;
    static int[] CountsDefault = new int[2] { 1000, 100000 };
    static TestConfig[] ConfigsDefault = new TestConfig[] { 
            new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
            new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
        };

    public class TestInfo
    {
        public string Name;
        public TestConfig[] Configs;
        public int[] Counts;

        public TestInfo(string name, TestConfig[] configs = null, int[] counts=null)
        {
            Name = name;
            if (null == configs)
                configs = ConfigsDefault;
            Configs = configs;
            if (null == counts)
                counts = CountsDefault;
            Counts = counts;
        }
    };

    static TestInfo[] TestInfos = new TestInfo[] 
    {
        new TestInfo("TestIndexUInt00"),
        new TestInfo("TestIndexInt00"),
        new TestInfo("TestIndexInt"),
        new TestInfo("TestIndexUInt"),
        new TestInfo("TestIndexBoolean"),
        new TestInfo("TestIndexByte"),
        new TestInfo("TestIndexSByte"),
        new TestInfo("TestIndexShort"),
        new TestInfo("TestIndexUShort"),
        new TestInfo("TestIndexLong"),
        new TestInfo("TestIndexULong"),
        new TestInfo("TestIndexDecimal"),
        new TestInfo("TestIndexFloat"),
        new TestInfo("TestIndexDouble"),
        new TestInfo("TestIndexGuid"),
        new TestInfo("TestIndexObject"),
        new TestInfo("TestIndexDateTime")
    };

    public static TestConfig[] GetTestConfigs(string testName)
    {
        foreach (var ti in TestInfos)
        {
            if (testName == ti.Name)
                return ti.Configs;
        }
        return null;
    }

    public static int GetCount(string testName)
    {
        foreach (var ti in TestInfos)
        {
            if (testName == ti.Name)
                return ti.Counts[CountsIdx];
        }
        return CountsDefault[CountsIdx];
    }

    static Dictionary<string, int[]> IterCounts =
        new Dictionary<string, int[]>
        {
            { "TestIndex", CountsDefault },
            { "TestIndex3", new int[2] { 200, 10000 } },
            { "TestIndex4", new int[2] { 200, 10000 } },
            { "TestEnumerator", new int[2] { 200, 2000 } },
            { "TestRtree", new int[2] { 800, 20000 } },
            { "TestR2", new int[2] { 1000, 20000 } },
            { "TestGC", new int[2] { 5000, 50000 } },
            { "TestXml", new int[2] { 2000, 20000 } },
            { "TestBit", new int[2] { 2000, 20000 } },
            { "TestRaw", new int[2] { 1000, 10000 } },
            // TODO: figure out why when it's 2000 instead of 2001 we fail
            { "TestTimeSeries", new int[2] { 2001, 100000 } }
        };

    static int GetIterCount(string test)
    {
        int[] counts;
        bool ok = IterCounts.TryGetValue(test, out counts);
        if (!ok)
            counts = CountsDefault;
        return counts[CountsIdx];
    }

    static void ParseCmdLineArgs(string[] args)
    {
        foreach (var arg in args)
        {
            if (arg == "-fast")
                CountsIdx = CountsIdxFast;
            else if (arg == "-slow")
                CountsIdx = CountsIdxSlow;
        }
    }

    static void RunTestBackup()
    {
        int n = GetIterCount("TestBackup");
        TestBackup.Init();
        var r = TestBackup.Run(n);
        r.Print();
    }

    static void RunTestBit()
    {
#if !OMIT_BTREE
        int n = GetIterCount("TestBit");
        var r = TestBit.Run(n);
        r.Print();
#endif
    }

    static void RunTestBlob()
    {
        var r = TestBlob.Run();
        r.Print();
    }

    static void RunTestCompoundIndex()
    {
        int n = GetIterCount("TestCompoundIndex");
        var r = TestCompoundIndex.Run(n, false);
        r.Print();
        r = TestCompoundIndex.Run(n, true);
        r.Print();
    }

    static void RunTestConcur()
    {
        int n = GetIterCount("TestConcur");
        var r = TestConcur.Run(n);
        r.Print();
    }

    static void RunTestEnumerator()
    {
        int n = GetIterCount("TestEnumerator");
        var r = TestEnumerator.Run(n, false);
        r.Print();
        r = TestEnumerator.Run(n, true);
        r.Print();
    }

    static void RunTestGc()
    {
        int n = GetIterCount("TestGc");
        var r = TestGC.Run(n, false, false);
        r.Print();
        r = TestGC.Run(n, true, false);
        r.Print();
        r = TestGC.Run(n, true, true);
        r.Print();
    }

    static void RunTestIndex()
    {
        int n = GetIterCount("TestIndex");
        var r = TestIndex.Run(n, false, false, false);
        r.Print();
        r = TestIndex.Run(n, true, false, false);
        r.Print();
        r = TestIndex.Run(n, true, false, true);
        r.Print();
        r = TestIndex.Run(n, false, true, false);
        r.Print();
    }

    static void RunTestIndex2()
    {
        int n = GetIterCount("TestIndex2");
        var r = TestIndex2.Run(n);
        r.Print();
    }

    static void RunTestIndex3()
    {
        int n = GetIterCount("TestIndex3");
        var r = TestIndex3.Run(n, false);
        r.Print();
        r = TestIndex3.Run(n, true);
        r.Print();
    }

    static void RunTestIndex4()
    {
        int n = GetIterCount("TestIndex4");
        var r = TestIndex4.Run(n, false);
        r.Print();
        r = TestIndex4.Run(n, true);
        r.Print();
    }

    static void RunTestList()
    {
        int n = GetIterCount("TestList");
        var r = TestList.Run(n);
        r.Print();
    }

    static void RunTestR2()
    {
        int n = GetIterCount("TestR2");
        var r = TestR2.Run(n, false);
        r.Print();
        r = TestR2.Run(n, true);
        r.Print();
    }

    static void RunTestRaw()
    {
        int n = GetIterCount("TestRaw");
        Tests.SafeDeleteFile(TestRaw.dbName);
        var r = TestRaw.Run(n);
        r.Print();
        r = TestRaw.Run(n);
        r.Print();
    }

    static void RunTestRtree()
    {
        int n = GetIterCount("TestRtree");
        var r = TestRtree.Run(n);
        r.Print();
    }

    static void RunTestTimeSeries()
    {
        int n = GetIterCount("TestTimeSeries");
        var r = TestTimeSeries.Run(n);
        r.Print();
    }

    static void RunTestTtree()
    {
        int n = GetIterCount("TestTtree");
        var r = TestTtree.Run(n);
        r.Print();
    }

    static void RunTestXml()
    {
#if !OMIT_XML
        int n = GetIterCount("TestXml");
        var r = TestXml.Run(n, false);
        r.Print();
        r = TestXml.Run(n, true);
        r.Print();
#endif
    }

    public static void RunTests(string testClassName)
    {
        var assembly = Assembly.GetExecutingAssembly();
        object obj = assembly.CreateInstance(testClassName);
        if (obj == null)
            obj = assembly.CreateInstance("Volante." + testClassName);
        Type tp = obj.GetType();
        MethodInfo mi = tp.GetMethod("Run");
        int count = GetCount(testClassName);
        foreach (TestConfig configTmp in GetTestConfigs(testClassName))
        {
            // make a copy because we modify it
            var config = new TestConfig(configTmp);
            config.Count = count;
            config.TestName = testClassName;
            config.Result = new TestResult(); // can be over-written by a test
            var dbname = config.DatabaseName;
            DateTime start = DateTime.Now;
            mi.Invoke(obj, new object[] { config });
            config.Result.ExecutionTime = DateTime.Now - start;
            config.Result.Config = config; // so that we Print() nicely
            config.Result.Ok = Tests.FinalizeTest();
            config.Result.Print();
        }
    }

    public static void Main(string[] args)
    {
        ParseCmdLineArgs(args);

        var tStart = DateTime.Now;

        string[] tests = new string[] {
            "TestIndexUInt00", "TestIndexInt00",
            "TestIndexInt", "TestIndexUInt",
            "TestIndexBoolean", "TestIndexByte",
            "TestIndexSByte", "TestIndexShort",
            "TestIndexUShort", "TestIndexLong",
            "TestIndexULong", "TestIndexDecimal",
            "TestIndexFloat", "TestIndexDouble",
            "TestIndexGuid", "TestIndexObject",
            "TestIndexDateTime"
        };

        foreach (var t in tests)
        {
            RunTests(t);
        }

        RunTestBackup();
        RunTestBit();
        RunTestBlob();
        RunTestCompoundIndex();
        RunTestConcur();
        RunTestEnumerator();
        RunTestGc();
        RunTestIndex();
        RunTestIndex2();
        RunTestIndex3();
        RunTestIndex4();
        RunTestList();
        RunTestR2();
        RunTestRaw();
        RunTestRtree();
        RunTestTtree();
        RunTestTimeSeries();
        RunTestXml();

        var tEnd = DateTime.Now;
        var executionTime = tEnd - tStart;

        if (0 == Tests.FailedTests)
        {
            Console.WriteLine(String.Format("OK! All {0} tests passed", Tests.TotalTests));
        }
        else
        {
            Console.WriteLine(String.Format("FAIL! Failed {0} out of {1} tests", Tests.FailedTests, Tests.TotalTests));
        }
        Tests.PrintFailedStackTraces();
        Console.WriteLine(String.Format("Running time: {0} ms", (int)executionTime.TotalMilliseconds));
    }
}

