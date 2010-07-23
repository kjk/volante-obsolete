using System;
using NachoDB;

public class UnitTestsRunner
{
    public static void Main(string[] args)
    {
        UnitTestBit.Run(100);
        UnitTests.SafeDeleteFile(UnitTestBlob.dbName);
        UnitTestBlob.Run();
        UnitTestBlob.Run();
        UnitTestXml.Run(100, false);
        UnitTestXml.Run(100, true);
        UnitTest1.Run(false);
        UnitTest1.Run(true);
        UnitTest2.Run(false);
        UnitTest2.Run(true);
        Console.WriteLine(String.Format("Failed {0} out of {1} tests", UnitTests.FailedTests, UnitTests.TotalTests));
    }
}
