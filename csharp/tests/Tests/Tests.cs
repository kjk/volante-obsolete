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
    public InMemoryType InMemory = InMemoryType.No;
    public FileType FileKind = FileType.File;
    public bool AltBtree = false;
    public bool Serializable = false;
    public bool BackgroundGc = false;
    public Encoding Encoding = null; // if not null will use this encoding for storing strings
    public int Count; // number of iterations

    // Set by the test. Can be a subclass of TestResult
    public TestResult Result;

    public string DatabaseName
    {
        get
        {
            string p1 = AltBtree ? "_alt" : "";
            string p2 = Serializable ? "_ser" : "";
            string p3 = (InMemory == InMemoryType.Full) ? "_mem" : "";
            string p4 = "";
            if (InMemory != InMemoryType.Full)
                p4 = (FileKind == FileType.File) ? "_file" : "_stream";
            string p5 = (null == Encoding) ? "" : "_enc-" + Encoding.EncodingName;
            string p6 = String.Format("_{0}", Count);
            return String.Format("{0}{1}{2}{3}{4}{5}{6}.dbs", TestName, p1, p2, p3, p4, p5, p6);
        }
    }

    void OpenTransientDatabase(IDatabase db)
    {
        NullFile dbFile = new NullFile();
        db.Open(dbFile, INFINITE_PAGE_POOL);
    }

    public IDatabase GetDatabase(bool delete=true)
    {
        IDatabase db = DatabaseFactory.CreateDatabase();
#if WITH_OLD_BTREE
        db.AlternativeBtree = AltBtree || Serializable;
#endif
        db.BackgroundGc = BackgroundGc;
        // TODO: make it configurable?
        // TODO: make it bigger (1000000 - the original value for h)
        if (BackgroundGc)
            db.GcThreshold = 100000;
        db.StringEncoding = Encoding;

        if (InMemory == InMemoryType.Full)
            OpenTransientDatabase(db);
        else
        {
            var name = DatabaseName;
            if (delete)
                Tests.SafeDeleteFile(name);
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
        BackgroundGc = tc.BackgroundGc;
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

    public static void DumpMemoryUsage(ICollection<MemoryUsage> usages)
    {
        Console.WriteLine("Memory usage");
        foreach (MemoryUsage usage in usages)
        {
            Console.WriteLine(" " + usage.type.Name + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
        }
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

    public static void AssertStorageException(Action func, DatabaseError.ErrorCode expectedCode)
    {
        bool gotException = false;
        try
        {
            func();
        }
        catch (DatabaseError exc)
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
            () => { Console.WriteLine(de.Current); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Entry); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Key); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Value); });
    }

    public static void VerifyEnumeratorDone(IEnumerator e)
    {
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(e.Current); });
        Tests.Assert(!e.MoveNext());
    }
}

public class TestsMain
{
    const int CountsIdxFast = 0;
    const int CountsIdxSlow = 1;
    static int CountsIdx = CountsIdxFast;
    static int[] CountsDefault = new int[2] { 1000, 100000 };
    static int[] Counts1 = new int[2] { 200, 10000 };

    static TestConfig[] ConfigsDefault = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsEncoding = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.File, AltBtree = true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.File, AltBtree = true, Encoding = Encoding.UTF8 }
    };

    static TestConfig[] ConfigsR2 = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        // TODO: should have a separate NoFlush flag
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsRaw = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsNoAlt = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full }
    };

    static TestConfig[] ConfigsOnlyAlt = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsIndex = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, Serializable=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=false },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, FileKind = TestConfig.FileType.Stream, AltBtree=true }
    };

    static TestConfig[] ConfigsDefaultFile = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.No },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true }
    };

    static TestConfig[] ConfigsGc = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.No },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true, BackgroundGc = true }
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
        new TestInfo("TestIndexDateTime", ConfigsDefaultFile),
        new TestInfo("TestIndex", ConfigsIndex, Counts1),
        new TestInfo("TestIndex2"),
        new TestInfo("TestIndex3"),
        new TestInfo("TestIndex4", ConfigsDefaultFile, Counts1),
#if WITH_OLD_BTREE
        new TestInfo("TestBit", ConfigsNoAlt, new int[2] { 2000, 20000 }),
#endif
        new TestInfo("TestR2", ConfigsR2, new int[2] { 1000, 20000 }),
        new TestInfo("TestRaw", ConfigsRaw, new int[2] { 1000, 10000 }),
        new TestInfo("TestRtree", ConfigsDefault, new int[2] { 800, 20000 }),
        new TestInfo("TestTtree"),
        new TestInfo("TestBlob", ConfigsDefaultFile),
        new TestInfo("TestCompoundIndex"),
        new TestInfo("TestConcur"),
        new TestInfo("TestEnumerator", ConfigsDefault, new int[2] { 50, 1000 }),
        new TestInfo("TestEncoding", ConfigsEncoding, new int[2] { 50000, 50000 }),
        // TODO: figure out why running it twice throws an exception from reflection
        // about trying to create a duplicate wrapper class
        new TestInfo("TestList", ConfigsOnlyAlt),
#if WITH_XML
        new TestInfo("TestXml", ConfigsDefaultFile, new int[2] { 2000, 20000 }),
#endif
        // TODO: figure out why when it's 2000 instead of 2001 we fail
        new TestInfo("TestTimeSeries", ConfigsDefault, new int[2] { 2001, 100000 }),
        new TestInfo("TestBackup", ConfigsDefaultFile),
        new TestInfo("TestGc", ConfigsGc, new int[2] { 5000, 50000 })
    };

    public static int GetCount(string testName)
    {
        foreach (var ti in TestInfos)
        {
            if (testName == ti.Name)
                return ti.Counts[CountsIdx];
        }
        return CountsDefault[CountsIdx];
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

    public static TestConfig[] GetTestConfigs(TestInfo testInfo)
    {
        return testInfo.Configs ?? ConfigsDefault;
    }

    public static void RunTests(TestInfo testInfo)
    {
        string testClassName = testInfo.Name;
        var assembly = Assembly.GetExecutingAssembly();
        object obj = assembly.CreateInstance(testClassName);
        if (obj == null)
            obj = assembly.CreateInstance("Volante." + testClassName);
        Type tp = obj.GetType();
        MethodInfo mi = tp.GetMethod("Run");
        int count = GetCount(testClassName);
        TestConfig[] configs = GetTestConfigs(testInfo);
        foreach (TestConfig configTmp in configs)
        {
#if !WITH_OLD_BTREE
            bool useAltBtree = configTmp.AltBtree || configTmp.Serializable;
            if (!useAltBtree)
                continue;
#endif
            // make a copy because we modify it
            var config = new TestConfig(configTmp);
            config.Count = count;
            config.TestName = testClassName;
            config.Result = new TestResult(); // can be over-written by a test
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

        foreach (var t in TestInfos)
        {
            RunTests(t);
        }

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
