using System;
using NachoDB;

public class TestRawRunner
{
    public static void Main(String[] args) 
    {
        UnitTests.SafeDeleteFile(TestRaw.dbName);
        TestRaw.Run();
        TestRaw.Run();
    }
}
