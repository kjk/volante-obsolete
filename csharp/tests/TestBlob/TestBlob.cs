using System;
using Volante;

public class TestBlobRunner
{
    public static void Main(string[] args)
    {
        UnitTests.SafeDeleteFile(TestBlob.dbName);
        TestBlob.Run();
        TestBlob.Run();
    }
}
