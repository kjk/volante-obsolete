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
            { "TestIndex", DefaultCounts }
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
        int count = GetIterCount("TestIndex");
        TestIndex.Run(count, false, false, false);
        TestIndex.Run(count, true, false, false);
        TestIndex.Run(count, true, false, true);
        TestIndex.Run(count, false, true, false);
    }

    static void RunIndex2Tests()
    {
        int count = GetIterCount("TestIndex2");
        TestIndex2.Run(count);
    }

    public static void Main(string[] args)
    {
        ParseCmdLineArgs(args);

        RunIndexTests();
        RunIndex2Tests();

        TestTtree.Run(100);

        //TODO: fix TestTimeSeries assert
        //TestTimeSeries.Run(100);
        TestRtree.Run(100);

        Tests.SafeDeleteFile(TestRaw.dbName);
        TestRaw.Run();
        TestRaw.Run();

        TestR2.Run(1000, false);
        TestR2.Run(1000, true);

        TestList.Run(1000);

        TestGC.Run(100, false, false);
        TestGC.Run(100, true, false);
        TestGC.Run(100, true, true);
        TestEnumerator.Run(100, false);
        TestEnumerator.Run(100, true);
        TestConcur.Run(100);
        TestCompoundIndex.Run(false, 100);
        TestCompoundIndex.Run(true, 100);
        TestBit.Run(100);
        Tests.SafeDeleteFile(TestBlob.dbName);
        TestBlob.Run();
        TestBlob.Run();
#if !OMIT_XML
        TestXml.Run(100, false);
        TestXml.Run(100, true);
#endif

        Test1.Run(false);
        Test1.Run(true);
        Test2.Run(false);
        Test2.Run(true);
        if (0 == Tests.FailedTests)
        {
            Console.WriteLine(String.Format("OK! All {0} tests passed", Tests.TotalTests));
        }
        else
        {
            Console.WriteLine(String.Format("FAIL! Failed {0} out of {1} tests", Tests.FailedTests, Tests.TotalTests));
        }
    }
}

