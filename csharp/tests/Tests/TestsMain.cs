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
            { "TestR2", new int[2] { 200, 20000 } },
            { "TestGC", new int[2] { 200, 20000 } },
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

    static void RunIndexTests()
    {
        int n = GetIterCount("TestIndex");
        TestIndex.Run(n, false, false, false);
        TestIndex.Run(n, true, false, false);
        TestIndex.Run(n, true, false, true);
        TestIndex.Run(n, false, true, false);
    }

    static void RunIndex2Tests()
    {
        int n = GetIterCount("TestIndex2");
        TestIndex2.Run(n);
    }

    static void RunEnumeratorTests()
    {
        int n = GetIterCount("TestEnumerator");
        TestEnumerator.Run(n, false);
        TestEnumerator.Run(n, true);
    }

    static void RunCompoundIndexTests()
    {
        int n = GetIterCount("TestCompoundIndex");
        TestCompoundIndex.Run(false, n);
        TestCompoundIndex.Run(true, n);
    }

    static void RunRtreeTests()
    {
        int n = GetIterCount("TestRtree");
        TestRtree.Run(n);
    }

    static void RunR2Tests()
    {
        int n = GetIterCount("TestR2");
        TestR2.Run(n, false);
        TestR2.Run(n, true);
    }

    static void RunTtreeTests()
    {
        int n = GetIterCount("TestTtree");
        TestRtree.Run(n);
    }

    static void RunRawTests()
    {
        int n = GetIterCount("TestRaw");
        Tests.SafeDeleteFile(TestRaw.dbName);
        TestRaw.Run(n);
        TestRaw.Run(n);
    }

    static void RunListTests()
    {
        int n = GetIterCount("TestList");
        TestList.Run(n);
    }

    static void RunBitTests()
    {
        int n = GetIterCount("TestBit");
        TestBit.Run(n);
    }

    static void RunBlobTests()
    {
        Tests.SafeDeleteFile(TestBlob.dbName);
        TestBlob.Run();
        TestBlob.Run();
    }

    static void RunTimeSeriesTests()
    {
        int n = GetIterCount("TestTimeSeries");
        TestTimeSeries.Run(n);
    }

    static void RunGcTests()
    {
        int n = GetIterCount("TestGc");
        TestGC.Run(n, false, false);
        TestGC.Run(n, true, false);
        TestGC.Run(n, true, true);
    }

    static void RunConcurTests()
    {
        int n = GetIterCount("TestConcur");
        TestConcur.Run(n);
    }

    static void RunXmlTests()
    {
#if !OMIT_XML
        int n = GetIterCount("TestXml");
        TestXml.Run(n, false);
        TestXml.Run(n, true);
#endif
    }

    public static void Main(string[] args)
    {
        ParseCmdLineArgs(args);

        var tStart = DateTime.Now;

        RunIndexTests();
        RunIndex2Tests();
        RunEnumeratorTests();
        RunCompoundIndexTests();
        RunTtreeTests();
        //TODO: fix TestTimeSeries assert
        //RunTimeSeriesTests();
        RunRtreeTests();
        RunR2Tests();
        RunRawTests();
        RunListTests();

        RunGcTests();
        RunConcurTests();

        RunBitTests();
        RunBlobTests();
        RunXmlTests();
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
        Console.WriteLine(String.Format("Running time: {0} ms", t.Milliseconds));
    }
}

