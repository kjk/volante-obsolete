using System;
using NachoDB;

public class TestBlob
{
    public static void Main(string[] args)
    {
        UnitTests.SafeDeleteFile(UnitTestBlob.dbName);
        UnitTestBlob.Run();
        UnitTestBlob.Run();
    }
}
