using System;
using Volante;
using System.Collections.Generic;

public class TestsMain
{
    const int CountsIdxFast = 0;
    const int CountsIdxSlow = 1;
    static int CountsIdx = CountsIdxFast;
    static int[] DefaultCounts = new int[2] { 100, 100000 };

    static Dictionary<string, int[]> IterCounts =
        new Dictionary<string, int[]>
        {
            { "TestIndex", DefaultCounts },
            { "TestEnumerator", new int[2] { 20, 200 } },
            { "TestRtree", new int[2] { 200, 20000 } },
            { "TestR2", new int[2] { 1000, 20000 } },
            { "TestGC", new int[2] { 5000, 50000 } },
            { "TestXml", new int[2] { 200, 20000 } },
            { "TestBit", new int[2] { 200, 20000 } },
            { "TestRaw", new int[2] { 100, 1000 } }
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
        RunTestList();
        RunTestR2();

        RunTestRaw();
        RunTestRtree();
        RunTestTtree();
        RunTestTimeSeries();
        RunTestXml();

        Test1.Run(false);
        Test1.Run(true);
        Test2.Run(false);
        Test2.Run(true);

        var tEnd = DateTime.Now;

        if (0 == Tests.FailedTests)
        {
            Console.WriteLine(String.Format("OK! All {0} tests passed", Tests.TotalTests));
        }
        else
        {
            Console.WriteLine(String.Format("FAIL! Failed {0} out of {1} tests", Tests.FailedTests, Tests.TotalTests));
        }
        var t = tEnd - tStart;
        Console.WriteLine(String.Format("Running time: {0} ms", (int)t.TotalMilliseconds));
    }
}

