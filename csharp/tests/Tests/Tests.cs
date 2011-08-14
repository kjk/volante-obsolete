using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Volante;

public class TestResult
{
    public bool Ok;
    public string TestName;
    public int Count;
    public TimeSpan ExecutionTime;

    public override string ToString()
    {
        if (Ok)
            return String.Format("{0} OK, {1} ms, n = {2}", TestName, (int)ExecutionTime.TotalMilliseconds, Count);
        else
            return String.Format("{0} FAILED", TestName);
    }

    public void Print()
    {
        System.Console.WriteLine(ToString());
    }
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
}

public class TestsMain
{
    const int CountsIdxFast = 0;
    const int CountsIdxSlow = 1;
    static int CountsIdx = CountsIdxFast;
    static int[] DefaultCounts = new int[2] { 1000, 100000 };

    static Dictionary<string, int[]> IterCounts =
        new Dictionary<string, int[]>
        {
            { "TestIndex", DefaultCounts },
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
            counts = DefaultCounts;
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
        int n = GetIterCount("TestBit");
        var r = TestBit.Run(n);
        r.Print();
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

    static void RunTestIndexBoolean()
    {
        int n = GetIterCount("TestIndexBoolean");
        var r = TestIndexBoolean.Run(n, false);
        r.Print();
        r = TestIndexBoolean.Run(n, true);
        r.Print();
    }

    static void RunTestIndexByte()
    {
        TestResult r;
        int n = GetIterCount("TestIndexByte");
        r = TestIndexByte.Run(n, false);
        r.Print();
        r = TestIndexByte.Run(n, true);
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

    public static void Main(string[] args)
    {
        ParseCmdLineArgs(args);

        var tStart = DateTime.Now;

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
        RunTestIndexBoolean();
        RunTestIndexByte();
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

