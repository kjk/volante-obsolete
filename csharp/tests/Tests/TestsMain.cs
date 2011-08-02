using System;
using Volante;

public class TestsMain
{
    public static void Main(string[] args)
    {
        TestTtree.Run(100);

        TestTimeSeries.Run(100);
        TestRtree.Run(100);

        UnitTests.SafeDeleteFile(TestRaw.dbName);
        TestRaw.Run();
        TestRaw.Run();

        TestR2.Run(1000, false);
        TestR2.Run(1000, true);

        TestList.Run(1000);

        TestIndex2.Run(100);

        TestIndex.Run(100, false, false, false);
        TestIndex.Run(100, true, false, false);
        TestIndex.Run(100, true, false, true);
        TestIndex.Run(100, false, true, false);

        TestGC.Run(100, false, false);
        TestGC.Run(100, true, false);
        TestGC.Run(100, true, true);
        TestEnumerator.Run(100, false);
        TestEnumerator.Run(100, true);
        TestConcur.Run(100);
        TestCompoundIndex.Run(false, 100);
        TestCompoundIndex.Run(true, 100);
        TestBit.Run(100);
        UnitTests.SafeDeleteFile(TestBlob.dbName);
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
        if (0 == UnitTests.FailedTests)
        {
            Console.WriteLine(String.Format("OK! All {0} tests passed", UnitTests.TotalTests));
        }
        else
        {
            Console.WriteLine(String.Format("FAIL! Failed {0} out of {1} tests", UnitTests.FailedTests, UnitTests.TotalTests));
        }
    }
}

